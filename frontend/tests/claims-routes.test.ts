import { beforeEach, describe, expect, it, vi } from "vitest";

const getClaims = vi.fn();
const getClaimReviewByClaimId = vi.fn();
const updateClaimStatus = vi.fn();
const requireAuthorizedSession = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/db/claims", () => ({
  getClaims,
  getClaimReviewByClaimId,
  updateClaimStatus,
}));

describe("claims API route", () => {
  beforeEach(() => {
    getClaims.mockReset();
    getClaimReviewByClaimId.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({ user: { id: "user-1" } });
  });

  it("falls back to safe defaults for invalid pagination params", async () => {
    getClaims.mockResolvedValue({
      claims: [],
      limit: 20,
      page: 1,
      total: 0,
      totalPages: 0,
    });

    const { GET } = await import("@/app/api/claims/route");
    const request = new Request(
      "http://localhost/api/claims?page=abc&limit=abc&sort=nope&order=sideways",
    );

    const response = await GET(request);

    expect(response.status).toBe(200);
    expect(getClaims).toHaveBeenCalledWith({
      limit: 20,
      order: "desc",
      page: 1,
      risk: "all",
      search: "",
      sort: "riskScore",
      status: "all",
    });
  });
});

describe("claim status route", () => {
  beforeEach(() => {
    getClaimReviewByClaimId.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({ user: { id: "user-1" } });
  });

  it("uses exact claim id lookup for status reads", async () => {
    getClaimReviewByClaimId.mockResolvedValue({
      claimId: "C0001",
      status: "reviewed",
    });

    const { GET } = await import("@/app/api/claims/[claimId]/status/route");
    const response = await GET(new Request("http://localhost/api/claims/C0001/status"), {
      params: Promise.resolve({ claimId: "C0001" }),
    });

    expect(response.status).toBe(200);
    expect(getClaimReviewByClaimId).toHaveBeenCalledWith("C0001");
    expect(getClaimReviewByClaimId).not.toHaveBeenCalledWith("C00010");
    await expect(response.json()).resolves.toEqual({
      claimId: "C0001",
      status: "reviewed",
    });
  });

  it("updates claim status with the exact route param", async () => {
    updateClaimStatus.mockResolvedValue({ ok: true });

    const { PATCH } = await import("@/app/api/claims/[claimId]/status/route");
    const response = await PATCH(
      new Request("http://localhost/api/claims/C0001/status", {
        body: JSON.stringify({ status: "actioned" }),
        headers: { "Content-Type": "application/json" },
        method: "PATCH",
      }),
      { params: Promise.resolve({ claimId: "C0001" }) },
    );

    expect(response.status).toBe(200);
    expect(updateClaimStatus).toHaveBeenCalledWith("C0001", "actioned", "user-1");
  });
});
