import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaimTimeline } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

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
  const events = await getClaimTimeline(claimId);

  return Response.json({
    events: events.map((event) => ({
      ...event,
      createdAt: event.createdAt.toISOString(),
    })),
  });
}
