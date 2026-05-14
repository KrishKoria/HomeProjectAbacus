import { beforeEach, describe, expect, it, vi } from "vitest";

const requireAuthorizedSession = vi.fn();
const getClaimSyncState = vi.fn();
const fetchClaimIdsForSync = vi.fn();
const syncDiscoveredClaimIds = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/db/claims", () => ({
  getClaimSyncState,
  syncDiscoveredClaimIds,
}));

vi.mock("@/lib/databricks/sql", () => ({
  fetchClaimIdsForSync,
}));

vi.mock("@/lib/server/env", () => ({
  env: {
    CLAIMOPS_FEATURE_TABLE: "healthcare.gold.claim_features",
  },
}));

describe("claim sync route", () => {
  beforeEach(() => {
    requireAuthorizedSession.mockReset();
    getClaimSyncState.mockReset();
    fetchClaimIdsForSync.mockReset();
    syncDiscoveredClaimIds.mockReset();
    requireAuthorizedSession.mockResolvedValue({ user: { id: "user-1" } });
  });

  it("rejects unauthorized sync requests", async () => {
    requireAuthorizedSession.mockRejectedValueOnce(new Error("Unauthorized"));

    const { POST } = await import("@/app/api/claims/sync/route");
    const response = await POST();

    expect(response.status).toBe(401);
  });

  it("loads the previous cursor and returns persisted sync counts", async () => {
    const syncedAt = new Date("2026-05-14T10:00:00.000Z");
    getClaimSyncState.mockResolvedValue({
      lastClaimId: "C0010",
      lastIngestedAt: new Date("2026-05-14T09:00:00.000Z"),
      sourceTable: "healthcare.gold.claim_features",
    });
    fetchClaimIdsForSync.mockResolvedValue({
      ok: true,
      rows: [
        {
          claimId: "C0011",
          ingestedAt: new Date("2026-05-14T09:30:00.000Z"),
        },
      ],
    });
    syncDiscoveredClaimIds.mockResolvedValue({
      discovered: 1,
      inserted: 1,
      skipped: 0,
      syncedAt,
    });

    const { POST } = await import("@/app/api/claims/sync/route");
    const response = await POST();

    expect(fetchClaimIdsForSync).toHaveBeenCalledWith({
      lastClaimId: "C0010",
      lastIngestedAt: new Date("2026-05-14T09:00:00.000Z"),
    });
    await expect(response.json()).resolves.toEqual({
      discovered: 1,
      inserted: 1,
      skipped: 0,
      syncedAt: syncedAt.toISOString(),
    });
  });

  it("passes through Databricks sync failures", async () => {
    getClaimSyncState.mockResolvedValue(null);
    fetchClaimIdsForSync.mockResolvedValue({
      message: "warehouse unavailable",
      ok: false,
      status: 502,
    });

    const { POST } = await import("@/app/api/claims/sync/route");
    const response = await POST();

    expect(syncDiscoveredClaimIds).not.toHaveBeenCalled();
    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({
      error: "warehouse unavailable",
    });
  });
});
