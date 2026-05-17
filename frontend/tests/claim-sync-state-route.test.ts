import { beforeEach, describe, expect, it, vi } from "vitest";

const requireAuthorizedSession = vi.fn();
const getClaimSyncState = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/db/claims", () => ({
  getClaimSyncState,
}));

vi.mock("@/lib/server/env", () => ({
  env: {
    CLAIMOPS_FEATURE_TABLE: "healthcare.gold.claim_features",
  },
}));

describe("claim sync-state route", () => {
  beforeEach(() => {
    requireAuthorizedSession.mockReset();
    getClaimSyncState.mockReset();
    requireAuthorizedSession.mockResolvedValue({ user: { id: "user-1" } });
  });

  it("returns the latest persisted sync state without triggering a sync", async () => {
    getClaimSyncState.mockResolvedValue({
      lastClaimId: "C0011",
      lastDiscoveredCount: 12,
      lastIngestedAt: new Date("2026-05-17T04:00:00.000Z"),
      lastInsertedCount: 10,
      lastSyncedAt: new Date("2026-05-17T04:05:00.000Z"),
      sourceTable: "healthcare.gold.claim_features",
    });

    const { GET } = await import("@/app/api/claims/sync-state/route");
    const response = await GET();

    expect(response.status).toBe(200);
    expect(getClaimSyncState).toHaveBeenCalledWith("healthcare.gold.claim_features");
    await expect(response.json()).resolves.toEqual({
      syncState: {
        lastClaimId: "C0011",
        lastDiscoveredCount: 12,
        lastIngestedAt: "2026-05-17T04:00:00.000Z",
        lastInsertedCount: 10,
        lastSyncedAt: "2026-05-17T04:05:00.000Z",
        sourceTable: "healthcare.gold.claim_features",
      },
    });
  });
});
