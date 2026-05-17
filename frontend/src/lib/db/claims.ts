import { getDb } from "@/lib/db";
import { claimEvents, claimFeedback, claimReviews, claimSyncState, user } from "@/lib/db/schema";
import { and, asc, count, desc, eq, ilike, isNotNull, sql, type SQL } from "drizzle-orm";

export type ClaimReview = typeof claimReviews.$inferSelect;
export type ClaimSyncState = typeof claimSyncState.$inferSelect;
export type ClaimFeedback = typeof claimFeedback.$inferSelect;
export type ClaimEvent = typeof claimEvents.$inferSelect;
export type ClaimRiskFilter = "all" | "high" | "medium" | "low";
export type ClaimSortField = "riskScore" | "analyzedAt" | "claimId";
export type ClaimSortOrder = "asc" | "desc";
export type ClaimStatusFilter = "all" | "new" | "reviewed" | "actioned";
export type ClaimFeedbackRating = "useful" | "not_useful";
export type ClaimFeedbackReason =
  | "wrong_risk_reason"
  | "missing_policy"
  | "too_vague"
  | "not_actionable";
export type ClaimEventType =
  | "analysis_generated"
  | "status_changed"
  | "feedback_recorded"
  | "report_generated";
export type ClaimReviewWithReviewer = ClaimReview & {
  reviewedByEmail: string | null;
};

export interface DiscoveredClaimId {
  claimId: string;
  ingestedAt: Date;
}

export interface ClaimSyncCursor {
  lastClaimId: string | null;
  lastIngestedAt: Date | null;
}

export interface GetClaimsParams {
  page?: number;
  limit?: number;
  search?: string;
  risk?: ClaimRiskFilter;
  status?: ClaimStatusFilter;
  sort?: ClaimSortField;
  order?: ClaimSortOrder;
}

export interface PaginatedClaims {
  claims: ClaimReview[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

export async function getClaims(params: GetClaimsParams = {}): Promise<PaginatedClaims> {
  const db = getDb();
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
): Promise<ClaimReviewWithReviewer | null> {
  const db = getDb();
  const result = await db
    .select({
      analyzedAt: claimReviews.analyzedAt,
      claimId: claimReviews.claimId,
      id: claimReviews.id,
      narrative: claimReviews.narrative,
      reviewedAt: claimReviews.reviewedAt,
      reviewedByEmail: user.email,
      reviewedById: claimReviews.reviewedById,
      riskLevel: claimReviews.riskLevel,
      riskScore: claimReviews.riskScore,
      status: claimReviews.status,
      topReason: claimReviews.topReason,
    })
    .from(claimReviews)
    .leftJoin(user, eq(claimReviews.reviewedById, user.id))
    .where(eq(claimReviews.claimId, claimId))
    .limit(1);

  return result[0] ?? null;
}

export async function getClaimStatuses(): Promise<
  { claimId: string; riskLevel: string | null; status: string; analyzedAt: Date | null }[]
> {
  const db = getDb();
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
  topReason?: string | null;
}): Promise<void> {
  const db = getDb();
  const id = `cr_${data.claimId}`;
  await db
    .insert(claimReviews)
    .values({
      id,
      claimId: data.claimId,
      riskScore: data.riskScore,
      riskLevel: data.riskLevel,
      narrative: data.narrative,
      topReason: data.topReason ?? null,
      status: "new",
      analyzedAt: new Date(),
    })
    .onConflictDoUpdate({
      target: claimReviews.claimId,
      set: {
        riskScore: data.riskScore,
        riskLevel: data.riskLevel,
        narrative: data.narrative,
        topReason: data.topReason ?? null,
        analyzedAt: new Date(),
      },
    });
}

export async function getClaimFeedbackByClaimId(
  claimId: string,
  userId: string,
): Promise<ClaimFeedback | null> {
  const rows = await getDb()
    .select()
    .from(claimFeedback)
    .where(and(eq(claimFeedback.claimId, claimId), eq(claimFeedback.userId, userId)))
    .limit(1);

  return rows[0] ?? null;
}

export async function upsertClaimFeedback(data: {
  claimId: string;
  comment: string;
  rating: ClaimFeedbackRating;
  reason: ClaimFeedbackReason | null;
  userId: string;
  userEmail: string;
}): Promise<ClaimFeedback> {
  const createdAt = new Date();
  const id = `cf_${data.claimId}_${data.userId}`;
  const rows = await getDb()
    .insert(claimFeedback)
    .values({
      claimId: data.claimId,
      comment: data.comment,
      createdAt,
      id,
      rating: data.rating,
      reason: data.reason,
      userId: data.userId,
    })
    .onConflictDoUpdate({
      target: claimFeedback.id,
      set: {
        comment: data.comment,
        createdAt,
        rating: data.rating,
        reason: data.reason,
      },
    })
    .returning();

  return rows[0];
}

export async function logClaimEvent(data: {
  actorEmail?: string | null;
  actorUserId?: string | null;
  claimId: string;
  eventType: ClaimEventType;
  metadata?: Record<string, unknown> | null;
}): Promise<void> {
  await getDb().insert(claimEvents).values({
    actorEmail: data.actorEmail ?? null,
    actorUserId: data.actorUserId ?? null,
    claimId: data.claimId,
    createdAt: new Date(),
    eventType: data.eventType,
    id: `ce_${data.claimId}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    metadata: data.metadata ?? null,
  });
}

export async function getClaimTimeline(claimId: string): Promise<ClaimEvent[]> {
  return getDb()
    .select()
    .from(claimEvents)
    .where(eq(claimEvents.claimId, claimId))
    .orderBy(desc(claimEvents.createdAt));
}

export async function getTopClaims(limit = 5): Promise<ClaimReview[]> {
  const db = getDb();
  return db
    .select()
    .from(claimReviews)
    .where(isNotNull(claimReviews.riskScore))
    .orderBy(desc(sql`COALESCE(${claimReviews.riskScore}, -1)`))
    .limit(limit);
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

export interface ClaimSyncResult {
  discovered: number;
  inserted: number;
  skipped: number;
  syncedAt: Date;
}

export async function getClaimStats(): Promise<ClaimStats> {
  const db = getDb();
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

export async function getClaimSyncState(
  sourceTable: string,
): Promise<ClaimSyncState | null> {
  const db = getDb();
  const result = await db
    .select()
    .from(claimSyncState)
    .where(eq(claimSyncState.sourceTable, sourceTable))
    .limit(1);

  return result[0] ?? null;
}

export async function syncDiscoveredClaimIds(
  sourceTable: string,
  discoveredClaims: DiscoveredClaimId[],
): Promise<ClaimSyncResult> {
  const db = getDb();
  const syncedAt = new Date();

  return db.transaction(async (tx) => {
    let inserted = 0;

    if (discoveredClaims.length > 0) {
      const insertedRows = await tx
        .insert(claimReviews)
        .values(
          discoveredClaims.map((claim) => ({
            analyzedAt: null,
            claimId: claim.claimId,
            id: `cr_${claim.claimId}`,
            narrative: "",
            riskLevel: null,
            riskScore: null,
            status: "new",
            topReason: null,
          })),
        )
        .onConflictDoNothing({ target: claimReviews.claimId })
        .returning({ claimId: claimReviews.claimId });

      inserted = insertedRows.length;
    }

    const latestClaim = discoveredClaims.at(-1) ?? null;
    await upsertClaimSyncState(tx, {
      discovered: discoveredClaims.length,
      inserted,
      lastClaimId: latestClaim?.claimId ?? null,
      lastIngestedAt: latestClaim?.ingestedAt ?? null,
      sourceTable,
      syncedAt,
    });

    return {
      discovered: discoveredClaims.length,
      inserted,
      skipped: discoveredClaims.length - inserted,
      syncedAt,
    };
  });
}

async function upsertClaimSyncState(
  tx: Pick<ReturnType<typeof getDb>, "insert">,
  data: {
    discovered: number;
    inserted: number;
    lastClaimId: string | null;
    lastIngestedAt: Date | null;
    sourceTable: string;
    syncedAt: Date;
  },
): Promise<void> {
  const sourceTableSql = sql.raw(`"claim_sync_state"`);
  const excludedSql = sql.raw("excluded");
  const cursorShouldAdvanceSql = sql`(
        ${excludedSql}.last_ingested_at IS NOT NULL
        AND (
        ${sourceTableSql}.last_ingested_at IS NULL
          OR ${excludedSql}.last_ingested_at > ${sourceTableSql}.last_ingested_at
        OR (
            ${excludedSql}.last_ingested_at = ${sourceTableSql}.last_ingested_at
            AND COALESCE(${excludedSql}.last_claim_id, '') > COALESCE(${sourceTableSql}.last_claim_id, '')
          )
        )
      )`;

  await tx
    .insert(claimSyncState)
    .values({
      lastClaimId: data.lastClaimId,
      lastDiscoveredCount: data.discovered,
      lastIngestedAt: data.lastIngestedAt,
      lastInsertedCount: data.inserted,
      lastSyncedAt: data.syncedAt,
      sourceTable: data.sourceTable,
    })
    .onConflictDoUpdate({
      set: {
        lastClaimId: sql`CASE WHEN ${cursorShouldAdvanceSql} THEN excluded.last_claim_id ELSE ${sourceTableSql}.last_claim_id END`,
        lastDiscoveredCount: data.discovered,
        lastIngestedAt: sql`CASE WHEN ${cursorShouldAdvanceSql} THEN excluded.last_ingested_at ELSE ${sourceTableSql}.last_ingested_at END`,
        lastInsertedCount: data.inserted,
        lastSyncedAt: sql`GREATEST(${sourceTableSql}.last_synced_at, excluded.last_synced_at)`,
      },
      target: claimSyncState.sourceTable,
    });
}

export async function updateClaimStatus(
  claimId: string,
  status: "new" | "reviewed" | "actioned",
  reviewedById: string,
): Promise<{ ok: boolean }> {
  const db = getDb();
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

function getOrderBy(sort: ClaimSortField, order: ClaimSortOrder) {
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
