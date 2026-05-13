import { db } from "@/lib/db";
import { claimReviews } from "@/lib/db/schema";
import { and, asc, count, desc, eq, ilike, isNotNull, sql, type SQL } from "drizzle-orm";

export type ClaimReview = typeof claimReviews.$inferSelect;

export interface GetClaimsParams {
  page?: number;
  limit?: number;
  search?: string;
  risk?: string;
  status?: string;
  sort?: string;
  order?: string;
}

export interface PaginatedClaims {
  claims: ClaimReview[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

export async function getClaims(params: GetClaimsParams = {}): Promise<PaginatedClaims> {
  const page = Math.max(1, params.page ?? 1);
  const limit = Math.min(100, Math.max(1, params.limit ?? 20));
  const offset = (page - 1) * limit;

  const filters: SQL[] = [];

  if (params.search?.trim()) {
    filters.push(ilike(claimReviews.claimId, `%${params.search.trim()}%`));
  }
  if (params.risk && params.risk !== "all") {
    filters.push(eq(claimReviews.riskLevel, params.risk));
  }
  if (params.status && params.status !== "all") {
    filters.push(eq(claimReviews.status, params.status));
  }

  const whereClause = filters.length > 0 ? and(...filters) : undefined;

  const [claimsResult, countResult] = await Promise.all([
    db
      .select()
      .from(claimReviews)
      .where(whereClause)
      .orderBy(
        desc(isNotNull(claimReviews.riskScore)),
        getOrderBy(params.sort ?? "riskScore", params.order ?? "desc"),
      )
      .limit(limit)
      .offset(offset),
    db
      .select({ total: count() })
      .from(claimReviews)
      .where(whereClause),
  ]);

  const total = countResult[0]?.total ?? 0;

  return {
    claims: claimsResult,
    total,
    page,
    limit,
    totalPages: Math.ceil(total / limit),
  };
}

export async function getClaimReviewByClaimId(
  claimId: string,
): Promise<ClaimReview | null> {
  const result = await db
    .select()
    .from(claimReviews)
    .where(eq(claimReviews.claimId, claimId))
    .limit(1);

  return result[0] ?? null;
}

export async function getClaimStatuses(): Promise<
  { claimId: string; riskLevel: string | null; status: string; analyzedAt: Date | null }[]
> {
  return db
    .select({
      claimId: claimReviews.claimId,
      riskLevel: claimReviews.riskLevel,
      status: claimReviews.status,
      analyzedAt: claimReviews.analyzedAt,
    })
    .from(claimReviews)
    .orderBy(claimReviews.claimId);
}

export async function upsertClaimReview(data: {
  claimId: string;
  riskScore: number;
  riskLevel: string;
  narrative: string;
}): Promise<void> {
  const id = `cr_${data.claimId}`;
  await db
    .insert(claimReviews)
    .values({
      id,
      claimId: data.claimId,
      riskScore: data.riskScore,
      riskLevel: data.riskLevel,
      narrative: data.narrative,
      status: "new",
      analyzedAt: new Date(),
    })
    .onConflictDoUpdate({
      target: claimReviews.claimId,
      set: {
        riskScore: data.riskScore,
        riskLevel: data.riskLevel,
        narrative: data.narrative,
        analyzedAt: new Date(),
      },
    });
}

export interface ClaimStats {
  risk: {
    high: number;
    medium: number;
    low: number;
  };
  status: {
    new: number;
    reviewed: number;
    actioned: number;
  };
  total: number;
}

export async function getClaimStats(): Promise<ClaimStats> {
  const [riskStats, statusStats] = await Promise.all([
    db
      .select({
        riskLevel: claimReviews.riskLevel,
        count: count(),
      })
      .from(claimReviews)
      .where(isNotNull(claimReviews.riskLevel))
      .groupBy(claimReviews.riskLevel),
    db
      .select({
        status: claimReviews.status,
        count: count(),
      })
      .from(claimReviews)
      .groupBy(claimReviews.status),
  ]);

  const total = statusStats.reduce((sum, s) => sum + s.count, 0);

  return {
    risk: {
      high: riskStats.find((r) => r.riskLevel === "high")?.count ?? 0,
      medium: riskStats.find((r) => r.riskLevel === "medium")?.count ?? 0,
      low: riskStats.find((r) => r.riskLevel === "low")?.count ?? 0,
    },
    status: {
      new: statusStats.find((s) => s.status === "new")?.count ?? 0,
      reviewed: statusStats.find((s) => s.status === "reviewed")?.count ?? 0,
      actioned: statusStats.find((s) => s.status === "actioned")?.count ?? 0,
    },
    total,
  };
}

export async function updateClaimStatus(
  claimId: string,
  status: "new" | "reviewed" | "actioned",
  reviewedById: string,
): Promise<{ ok: boolean }> {
  const result = await db
    .update(claimReviews)
    .set({
      status,
      reviewedAt: new Date(),
      reviewedById,
    })
    .where(eq(claimReviews.claimId, claimId))
    .returning({ id: claimReviews.id });

  return { ok: result.length > 0 };
}

function getOrderBy(sort: string, order: string) {
  const dir = order === "asc" ? asc : desc;
  switch (sort) {
    case "riskScore":
      return dir(sql`COALESCE(${claimReviews.riskScore}, -1)`);
    case "analyzedAt":
      return dir(sql`COALESCE(${claimReviews.analyzedAt}::text, '')`);
    case "claimId":
      return dir(claimReviews.claimId);
    default:
      return dir(sql`COALESCE(${claimReviews.riskScore}, -1)`);
  }
}
