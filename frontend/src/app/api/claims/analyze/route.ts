import { requireAuthorizedSession } from "@/lib/auth-session";
import { analyzeClaim } from "@/lib/databricks/analysis";
import { fetchFeatureRow } from "@/lib/databricks/sql";
import { logClaimEvent, upsertClaimReview } from "@/lib/db/claims";
import { z } from "zod";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const requestSchema = z.object({
  claimId: z.string().min(1),
});

export async function POST(request: Request) {
  let session: Awaited<ReturnType<typeof requireAuthorizedSession>>;
  try {
    session = await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const parsed = requestSchema.safeParse(body);
  if (!parsed.success) {
    return Response.json(
      { error: "Validation error", details: parsed.error.issues },
      { status: 400 },
    );
  }

  const { claimId } = parsed.data;

  const featureRow = await fetchFeatureRow(claimId);
  if (!featureRow.ok) {
    if (featureRow.status === 404) {
      return Response.json({ error: "Claim not found" }, { status: 404 });
    }
    return Response.json({ error: "Failed to fetch features" }, { status: 502 });
  }

  const analysis = await analyzeClaim(claimId, featureRow.row);
  if (!analysis.ok) {
    return Response.json({ error: "Analysis failed" }, { status: 502 });
  }

  let persisted = true;
  try {
    const topReason =
      analysis.data.topReasons?.find((r) => r.direction === "increases_risk")?.description ??
      analysis.data.topReasons?.[0]?.description ??
      null;

    await upsertClaimReview({
      claimId,
      riskScore: analysis.data.riskScore,
      riskLevel: analysis.data.riskLevel.toLowerCase(),
      narrative: analysis.data.narrative ?? "",
      topReason,
    });
    await logClaimEvent({
      actorEmail: session.user.email ?? null,
      actorUserId: session.user.id,
      claimId,
      eventType: "analysis_generated",
    });
  } catch (err) {
    console.error("[analyze] failed to upsert claim review", err);
    persisted = false;
  }

  if (!persisted) {
    return Response.json({ error: "Analysis completed but failed to persist results" }, { status: 500 });
  }

  return Response.json({
    ...analysis.data,
    features: featureRow.row,
  });
}
