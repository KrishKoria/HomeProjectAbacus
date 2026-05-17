import { requireAuthorizedSession } from "@/lib/auth-session";
import {
  getClaimFeedbackByClaimId,
  getClaimReviewByClaimId,
  logClaimEvent,
  type ClaimFeedbackReason,
  upsertClaimFeedback,
} from "@/lib/db/claims";
import { z } from "zod";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const bodySchema = z.object({
  comment: z.string().default(""),
  rating: z.enum(["useful", "not_useful"]),
  reason: z
    .enum(["wrong_risk_reason", "missing_policy", "too_vague", "not_actionable"])
    .nullable()
    .optional(),
});

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ claimId: string }> },
) {
  let session: Awaited<ReturnType<typeof requireAuthorizedSession>>;
  try {
    session = await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { claimId } = await params;
  const feedback = await getClaimFeedbackByClaimId(claimId, session.user.id);

  return Response.json({
    feedback: feedback
      ? {
          ...feedback,
          createdAt: feedback.createdAt.toISOString(),
        }
      : null,
  });
}

export async function POST(
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
    return Response.json({ error: "Invalid feedback payload" }, { status: 400 });
  }

  const claimReview = await getClaimReviewByClaimId(claimId);
  if (!claimReview || !claimReview.analyzedAt) {
    return Response.json({ error: "Claim has not been analyzed yet" }, { status: 409 });
  }

  const feedback = await upsertClaimFeedback({
    claimId,
    comment: parsed.data.comment,
    rating: parsed.data.rating,
    reason: (parsed.data.reason ?? null) as ClaimFeedbackReason | null,
    userEmail: session.user.email ?? "",
    userId: session.user.id,
  });

  await logClaimEvent({
    actorEmail: session.user.email ?? null,
    actorUserId: session.user.id,
    claimId,
    eventType: "feedback_recorded",
    metadata: {
      rating: parsed.data.rating,
      reason: parsed.data.reason ?? null,
    },
  });

  return Response.json({
    feedback: {
      ...feedback,
      createdAt: feedback.createdAt.toISOString(),
    },
  });
}
