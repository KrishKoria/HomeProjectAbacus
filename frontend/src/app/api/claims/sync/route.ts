import { requireAuthorizedSession } from "@/lib/auth-session";
import { fetchClaimIdsForSync } from "@/lib/databricks/sql";
import { getClaimSyncState, syncDiscoveredClaimIds } from "@/lib/db/claims";
import { env } from "@/lib/server/env";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const currentState = await getClaimSyncState(env.CLAIMOPS_FEATURE_TABLE);
    const syncResult = await fetchClaimIdsForSync({
      lastClaimId: currentState?.lastClaimId ?? null,
      lastIngestedAt: currentState?.lastIngestedAt ?? null,
    });

    if (!syncResult.ok) {
      return Response.json(
        { error: syncResult.message || "Failed to sync claim IDs" },
        { status: syncResult.status || 502 },
      );
    }

    const persisted = await syncDiscoveredClaimIds(
      env.CLAIMOPS_FEATURE_TABLE,
      syncResult.rows,
    );

    return Response.json({
      discovered: persisted.discovered,
      inserted: persisted.inserted,
      skipped: persisted.skipped,
      syncedAt: persisted.syncedAt.toISOString(),
    });
  } catch (error) {
    console.error("[claim-sync] failed to sync claim ids:", error);
    return Response.json(
      { error: "Failed to sync claim IDs" },
      { status: 500 },
    );
  }
}
