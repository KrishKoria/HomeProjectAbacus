import type { ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import DataUploadPage from "@/app/(app)/data-upload/page";

const invalidateQueries = vi.fn();
const mockUseMutation = vi.fn();
const mockUseQuery = vi.fn();

vi.mock("@tanstack/react-query", async () => {
  return {
    useMutation: (...args: unknown[]) => mockUseMutation(...args),
    useQuery: (...args: unknown[]) => mockUseQuery(...args),
    useQueryClient: () => ({
      invalidateQueries,
    }),
  };
});

vi.mock("@/components/app-shell", () => ({
  AppShell: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

describe("Data upload page", () => {
  beforeEach(() => {
    invalidateQueries.mockReset();
    mockUseMutation.mockReset();
    mockUseQuery.mockReset();
    mockUseMutation.mockReturnValue({
      isPending: false,
      mutate: vi.fn(),
    });
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
              errorMessage: "Uploaded object dataset metadata did not match signed request",
              gcsGeneration: "1700000000000000",
              id: "upl_test",
              objectName: "claims/upl_test.csv",
              status: "failed",
              uploadedByEmail: "analyst@example.com",
              volumePath: "/Volumes/healthcare/bronze/raw_landing/claims/upl_test.csv",
            },
          ],
        },
        isLoading: false,
      });
  });

  it("shows ingestion timing guidance and expanded recent upload details", () => {
    const html = renderToStaticMarkup(<DataUploadPage />);

    expect(html).toContain("Ingestion timing");
    expect(html).toContain("waits 60 seconds after the last file change");
    expect(html).toContain("Upload ID");
    expect(html).toContain("Completed");
    expect(html).toContain("Generation");
    expect(html).toContain("Error");
    expect(html).toContain("upl_test");
    expect(html).toContain("1700000000000000");
    expect(html).toContain("Uploaded object dataset metadata did not match signed request");
  });
});
