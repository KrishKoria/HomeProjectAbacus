import { beforeEach, describe, expect, it, vi } from "vitest";

const getDb = vi.fn();

vi.mock("@/lib/db", () => ({
  getDb,
}));

function containsDate(value: unknown, seen = new WeakSet<object>()): boolean {
  if (value instanceof Date) return true;
  if (value === null || typeof value !== "object") return false;
  if (seen.has(value)) return false;

  seen.add(value);

  return Reflect.ownKeys(value).some((key) =>
    containsDate((value as Record<PropertyKey, unknown>)[key], seen),
  );
}

describe("syncDiscoveredClaimIds", () => {
  beforeEach(() => {
    getDb.mockReset();
  });

  it("does not bind Date objects inside raw conflict-update SQL", async () => {
    let conflictUpdateConfig: unknown = null;

    const tx = {
      insert: vi.fn(() => {
        const builder = {
          onConflictDoNothing: vi.fn(() => builder),
          onConflictDoUpdate: vi.fn(async (config) => {
            conflictUpdateConfig = config;
          }),
          returning: vi.fn(async () => [{ claimId: "C0997" }]),
          values: vi.fn(() => builder),
        };

        return builder;
      }),
    };

    getDb.mockReturnValue({
      transaction: vi.fn(async (callback) => callback(tx)),
    });

    const { syncDiscoveredClaimIds } = await import("@/lib/db/claims");

    await syncDiscoveredClaimIds("healthcare.gold.claim_features", [
      {
        claimId: "C0997",
        ingestedAt: new Date("2026-05-07T08:37:10.177Z"),
      },
    ]);

    const config = conflictUpdateConfig as { set: Record<string, unknown> };

    expect(config).toMatchObject({ set: expect.any(Object) });
    expect(containsDate(config.set)).toBe(false);
  });
});
