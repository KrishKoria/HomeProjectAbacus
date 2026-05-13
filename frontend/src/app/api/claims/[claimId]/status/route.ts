import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaimReviewByClaimId, updateClaimStatus } from "@/lib/db/claims";
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

  return Response.json({ ok: true, status: parsed.data.status });
}
