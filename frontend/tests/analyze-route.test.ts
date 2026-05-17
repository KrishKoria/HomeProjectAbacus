import { beforeEach, describe, expect, it, vi } from "vitest";

const requireAuthorizedSession = vi.fn();
const analyzeClaim = vi.fn();
const fetchFeatureRow = vi.fn();
const logClaimEvent = vi.fn();
const upsertClaimReview = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/databricks/analysis", () => ({
  analyzeClaim,
}));

vi.mock("@/lib/databricks/sql", () => ({
  fetchFeatureRow,
}));

vi.mock("@/lib/db/claims", () => ({
  logClaimEvent,
  upsertClaimReview,
}));

describe("claim analyze route", () => {
  beforeEach(() => {
    requireAuthorizedSession.mockReset();
    analyzeClaim.mockReset();
    fetchFeatureRow.mockReset();
    logClaimEvent.mockReset();
    upsertClaimReview.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
  });

  it("returns selected model inputs alongside analysis output", async () => {
    fetchFeatureRow.mockResolvedValue({
      ok: true,
      row: {
        amount_to_benchmark_ratio: 2.4,
        dx_px_compatible: 0,
        is_procedure_missing: 1,
        provider_30d_denial_rate: 0.31,
      },
    });
    analyzeClaim.mockResolvedValue({
      ok: true,
      data: {
        claimId: "C0001",
        generatedAt: "2026-05-17T04:30:00.000Z",
        model: "claimops-analysis-v1",
        narrative: "Claim looks high risk due to amount and compatibility signals.",
        policyCitations: ["guideline.pdf"],
        policyGuidance: [],
        predictionLabel: 1,
        riskLevel: "high",
        riskScore: 0.87,
        topReasons: [],
      },
    });

    const { POST } = await import("@/app/api/claims/analyze/route");
    const response = await POST(
      new Request("http://localhost/api/claims/analyze", {
        body: JSON.stringify({ claimId: "C0001" }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      }),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      claimId: "C0001",
      features: {
        amount_to_benchmark_ratio: 2.4,
        dx_px_compatible: 0,
        is_procedure_missing: 1,
        provider_30d_denial_rate: 0.31,
      },
      generatedAt: "2026-05-17T04:30:00.000Z",
      model: "claimops-analysis-v1",
      narrative: "Claim looks high risk due to amount and compatibility signals.",
      policyCitations: ["guideline.pdf"],
      policyGuidance: [],
      predictionLabel: 1,
      riskLevel: "high",
      riskScore: 0.87,
      topReasons: [],
    });
  });
});
