import type { ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import DashboardPage from "@/app/(app)/dashboard/page";

const mockUseQuery = vi.fn();

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
}));

vi.mock("@tanstack/react-query", async () => {
  return {
    useQuery: (...args: unknown[]) => mockUseQuery(...args),
  };
});

vi.mock("@/components/app-shell", () => ({
  AppShell: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

describe("Dashboard page", () => {
  beforeEach(() => {
    mockUseQuery.mockReset();
    mockUseQuery
      .mockReturnValueOnce({
        data: { user: { id: "user-1" } },
        isFetched: true,
        isLoading: false,
      })
      .mockReturnValueOnce({
        data: {
          risk: { high: 3, medium: 4, low: 5 },
          status: { new: 6, reviewed: 2, actioned: 1 },
          total: 9,
        },
        isLoading: false,
        isPending: false,
      })
      .mockReturnValueOnce({
        data: [
          {
            analyzedAt: "2026-05-17T10:00:00.000Z",
            claimId: "C0009",
            id: "cr_C0009",
            narrative: "",
            reviewedAt: null,
            reviewedById: null,
            riskLevel: "high",
            riskScore: 0.91,
            status: "new",
            topReason: null,
          },
        ],
        isLoading: false,
      })
      .mockReturnValueOnce({
        data: {
          highRiskNew: [
            {
              analyzedAt: "2026-05-17T10:00:00.000Z",
              claimId: "C0009",
              id: "cr_C0009",
              narrative: "",
              reviewedAt: null,
              reviewedById: null,
              riskLevel: "high",
              riskScore: 0.91,
              status: "new",
              topReason: null,
            },
          ],
          mediumRiskNew: [],
          missingAnalysis: [
            {
              analyzedAt: null,
              claimId: "C0011",
              riskLevel: null,
              status: "new",
            },
          ],
          recentlyDiscovered: [
            {
              analyzedAt: null,
              claimId: "C0011",
              riskLevel: null,
              status: "new",
            },
          ],
        },
        isLoading: false,
      });
  });

  it("renders the operational work queue buckets", () => {
    const html = renderToStaticMarkup(<DashboardPage />);

    expect(html).toContain("Work Queue");
    expect(html).toContain("Highest-risk new claims");
    expect(html).toContain("Claims missing analysis");
    expect(html).toContain("Recently discovered claims");
    expect(html).toContain("C0011");
  });
});
