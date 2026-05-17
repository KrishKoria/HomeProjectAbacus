import type { ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import UploadsPage from "@/app/(app)/uploads/page";

const mockUseQuery = vi.fn();
const invalidateQueries = vi.fn();

vi.mock("@tanstack/react-query", async () => {
  return {
    useQuery: (...args: unknown[]) => mockUseQuery(...args),
    useQueryClient: () => ({
      invalidateQueries,
    }),
  };
});

vi.mock("@/components/app-shell", () => ({
  AppShell: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

describe("Uploads page", () => {
  beforeEach(() => {
    invalidateQueries.mockReset();
    mockUseQuery.mockReset();
    mockUseQuery
      .mockReturnValueOnce({
        data: {
          datasets: [
            {
              acceptedContentTypes: ["text/csv"],
              datasetKey: "claims",
              description: "Claims adjudication export consumed by Bronze ingestion.",
              displayName: "Claims",
              extension: ".csv",
              hasPhi: true,
              landingSubdirectory: "claims",
              maxBytes: 100_000_000,
              requiredColumns: ["claim_id"],
            },
          ],
        },
        isLoading: false,
      })
      .mockReturnValueOnce({
        data: {
          uploads: [
            {
              byteSize: 128,
              completedAt: "2026-05-17T10:05:00.000Z",
              contentType: "text/csv",
              createdAt: "2026-05-17T10:00:00.000Z",
              datasetKey: "claims",
              errorMessage: null,
              gcsGeneration: "1700000000000000",
              id: "upl_test",
              objectName: "claims/upl_test.csv",
              status: "uploaded",
              uploadedByEmail: "analyst@example.com",
              volumePath: "/Volumes/healthcare/bronze/raw_landing/claims/upl_test.csv",
            },
          ],
        },
        isLoading: false,
      })
      .mockReturnValueOnce({
        data: {
          syncState: {
            lastClaimId: "C0011",
            lastDiscoveredCount: 12,
            lastIngestedAt: "2026-05-17T10:06:00.000Z",
            lastInsertedCount: 10,
            lastSyncedAt: "2026-05-17T10:07:00.000Z",
            sourceTable: "healthcare.gold.claim_features",
          },
        },
        isLoading: false,
      });
  });

  it("shows sync state and uploaded-by history", () => {
    const html = renderToStaticMarkup(<UploadsPage />);

    expect(html).toContain("Latest claim sync");
    expect(html).toContain("Discovered");
    expect(html).toContain("Uploaded by");
    expect(html).toContain("analyst@example.com");
    expect(html).toContain("1700000000000000");
  });
});
