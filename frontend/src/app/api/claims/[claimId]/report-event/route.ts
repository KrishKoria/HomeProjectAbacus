import { requireAuthorizedSession } from "@/lib/auth-session";
import { logClaimEvent } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(
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

  await logClaimEvent({
    actorEmail: session.user.email ?? null,
    actorUserId: session.user.id,
    claimId,
    eventType: "report_generated",
  });

  return Response.json({ ok: true });
}
