import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import ClaimsPage from "@/app/(app)/claims/page";

const enqueueBatch = vi.fn();
const mockUseQuery = vi.fn();
const claimsRefetch = vi.fn();
const statusesRefetch = vi.fn();

vi.mock("next/navigation", () => ({
  usePathname: () => "/claims",
  useRouter: () => ({ replace: vi.fn() }),
  useSearchParams: () => new URLSearchParams(""),
}));

vi.mock("@tanstack/react-query", async () => {
  const actual = await vi.importActual("@tanstack/react-query");
  return {
    ...actual,
    useQuery: (...args: unknown[]) => mockUseQuery(...args),
  };
});

vi.mock("@/components/app-shell", () => ({
  AppShell: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock("@/hooks/use-analysis-queue", () => ({
  useAnalysisQueue: () => ({
    enqueueBatch,
    isProcessing: false,
    progress: {
      completed: 0,
      current: null,
      errors: [],
      failed: 0,
      total: 0,
    },
  }),
}));

describe("Claims page", () => {
  beforeEach(() => {
    enqueueBatch.mockReset();
    claimsRefetch.mockReset();
    statusesRefetch.mockReset();
    mockUseQuery.mockReset();
  });

  it("syncs on open, auto-enqueues the first 20 pending claims, and keeps the bulk action", async () => {
    const pendingStatuses = Array.from({ length: 25 }, (_, index) => ({
      analyzedAt: null,
      claimId: `C${String(index + 1).padStart(4, "0")}`,
      riskLevel: null,
      status: "new",
    }));

    const visibleClaims = pendingStatuses.slice(0, 20).map((status) => ({
      analyzedAt: null,
      claimId: status.claimId,
      id: `cr_${status.claimId}`,
      narrative: "",
      reviewedAt: null,
      reviewedById: null,
      riskLevel: null,
      riskScore: null,
      status: "new",
      topReason: null,
    }));

    mockUseQuery
      .mockReturnValueOnce({
        data: {
          claims: visibleClaims,
          total: 25,
          totalPages: 2,
        },
        isError: false,
        isLoading: false,
        refetch: claimsRefetch,
      })
      .mockReturnValueOnce({
        data: { statuses: pendingStatuses },
        isError: false,
        isLoading: false,
        refetch: statusesRefetch,
      })
      .mockReturnValueOnce({
        dataUpdatedAt: 1,
        error: null,
        isError: false,
        isSuccess: true,
        refetch: vi.fn(),
      });

    render(<ClaimsPage />);

    await waitFor(() =>
      expect(enqueueBatch).toHaveBeenCalledWith(
        pendingStatuses.slice(0, 20).map((status) => status.claimId),
        1,
      ),
    );
    await waitFor(() => expect(claimsRefetch).toHaveBeenCalled());
    await waitFor(() => expect(statusesRefetch).toHaveBeenCalled());
    expect(
      screen.getByRole("button", { name: "Analyze all pending" }),
    ).toBeInTheDocument();
    expect(screen.getByText("5 waiting")).toBeInTheDocument();
  });
});
