import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaimSyncState } from "@/lib/db/claims";
import { env } from "@/lib/server/env";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const syncState = await getClaimSyncState(env.CLAIMOPS_FEATURE_TABLE);

  return Response.json({
    syncState: syncState
      ? {
          ...syncState,
          lastIngestedAt: syncState.lastIngestedAt?.toISOString() ?? null,
          lastSyncedAt: syncState.lastSyncedAt.toISOString(),
        }
      : null,
  });
}
