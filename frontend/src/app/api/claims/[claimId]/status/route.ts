import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaimReviewByClaimId, logClaimEvent, updateClaimStatus } from "@/lib/db/claims";
import { z } from "zod";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const bodySchema = z.object({
  status: z.enum(["new", "reviewed", "actioned"]),
});

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ claimId: string }> },
) {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { claimId } = await params;
  const claimReview = await getClaimReviewByClaimId(claimId);

  if (!claimReview) {
    return Response.json({ error: "Claim not found" }, { status: 404 });
  }

  return Response.json({
    claimId: claimReview.claimId,
    reviewedAt: claimReview.reviewedAt?.toISOString() ?? null,
    reviewedByEmail: claimReview.reviewedByEmail ?? null,
    reviewedById: claimReview.reviewedById ?? null,
    status: claimReview.status,
  });
}

export async function PATCH(
  request: Request,
  { params }: { params: Promise<{ claimId: string }> },
) {
  let session: Awaited<ReturnType<typeof requireAuthorizedSession>>;
  try {
    session = await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { claimId } = await params;

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const parsed = bodySchema.safeParse(body);
  if (!parsed.success) {
    return Response.json({ error: "Invalid status value" }, { status: 400 });
  }

  const result = await updateClaimStatus(claimId, parsed.data.status, session.user.id);
  if (!result.ok) {
    return Response.json({ error: "Claim not found" }, { status: 404 });
  }

  await logClaimEvent({
    actorEmail: session.user.email ?? null,
    actorUserId: session.user.id,
    claimId,
    eventType: "status_changed",
    metadata: { status: parsed.data.status },
  });

  return Response.json({ ok: true, status: parsed.data.status });
}
