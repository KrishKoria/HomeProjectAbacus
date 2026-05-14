import { beforeEach, describe, expect, it, vi } from "vitest";

const databricksFetch = vi.fn();

vi.mock("@/lib/databricks/client", () => ({
  databricksFetch,
}));

vi.mock("@/lib/server/env", () => ({
  env: {
    CLAIMOPS_FEATURE_TABLE: "healthcare.gold.claim_features",
    DATABRICKS_SQL_WAREHOUSE_ID: "warehouse-1",
  },
}));

describe("fetchClaimIdsForSync", () => {
  beforeEach(() => {
    databricksFetch.mockReset();
  });

  it("fetches chunked claim-id results", async () => {
    databricksFetch
      .mockResolvedValueOnce({ ok: true, data: { state: "RUNNING" } })
      .mockResolvedValueOnce({
        ok: true,
        data: {
          result: {
            data_array: [["C0001", "2026-05-14 09:00:00.000"]],
            next_chunk_internal_link: "/api/2.0/sql/statements/stmt-1/result/chunks/1",
          },
          statement_id: "stmt-1",
          status: { state: "SUCCEEDED" },
        },
      })
      .mockResolvedValueOnce({
        ok: true,
        data: {
          data_array: [["C0002", "2026-05-14 09:05:00.000"]],
          next_chunk_internal_link: null,
        },
      });

    const { fetchClaimIdsForSync } = await import("@/lib/databricks/sql");
    const result = await fetchClaimIdsForSync({
      lastClaimId: null,
      lastIngestedAt: null,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.rows).toHaveLength(2);
      expect(result.rows[1].claimId).toBe("C0002");
    }
  });

  it("uses the stored cursor in the Databricks SQL statement", async () => {
    databricksFetch
      .mockResolvedValueOnce({ ok: true, data: { state: "RUNNING" } })
      .mockResolvedValueOnce({
        ok: true,
        data: {
          result: {
            data_array: [],
            next_chunk_internal_link: null,
          },
          statement_id: "stmt-2",
          status: { state: "SUCCEEDED" },
        },
      });

    const { fetchClaimIdsForSync } = await import("@/lib/databricks/sql");
    await fetchClaimIdsForSync({
      lastClaimId: "C0010",
      lastIngestedAt: new Date("2026-05-14T09:00:00.000Z"),
    });

    expect(databricksFetch).toHaveBeenNthCalledWith(
      2,
      "/api/2.0/sql/statements",
      expect.objectContaining({
        body: expect.stringContaining("WHERE _ingested_at > :lastIngestedAt"),
      }),
    );
    expect(databricksFetch).toHaveBeenNthCalledWith(
      2,
      "/api/2.0/sql/statements",
      expect.objectContaining({
        body: expect.stringContaining(
          "claim_id > :lastClaimId",
        ),
      }),
    );
  });
});
