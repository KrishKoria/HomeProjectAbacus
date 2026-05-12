import { db } from "@/lib/db";
import { claimReviews } from "@/lib/db/schema";
import { desc, eq } from "drizzle-orm";

export type ClaimReview = typeof claimReviews.$inferSelect;

export async function getClaims(): Promise<ClaimReview[]> {
  return db
    .select()
    .from(claimReviews)
    .orderBy(desc(claimReviews.riskScore));
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
