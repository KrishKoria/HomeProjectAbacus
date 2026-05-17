import { beforeEach, describe, expect, it, vi } from "vitest";

const getClaims = vi.fn();
const getClaimReviewByClaimId = vi.fn();
const getClaimFeedbackByClaimId = vi.fn();
const getClaimTimeline = vi.fn();
const logClaimEvent = vi.fn();
const upsertClaimFeedback = vi.fn();
const updateClaimStatus = vi.fn();
const requireAuthorizedSession = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/db/claims", () => ({
  getClaims,
  getClaimFeedbackByClaimId,
  getClaimReviewByClaimId,
  getClaimTimeline,
  logClaimEvent,
  upsertClaimFeedback,
  updateClaimStatus,
}));

describe("claims API route", () => {
  beforeEach(() => {
    getClaims.mockReset();
    getClaimFeedbackByClaimId.mockReset();
    getClaimReviewByClaimId.mockReset();
    getClaimTimeline.mockReset();
    logClaimEvent.mockReset();
    upsertClaimFeedback.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
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
      "http://localhost/api/claims?page=abc&limit=abc&sort=nope&order=sideways&risk=urgent&status=pending",
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
    getClaimFeedbackByClaimId.mockReset();
    getClaimReviewByClaimId.mockReset();
    getClaimTimeline.mockReset();
    logClaimEvent.mockReset();
    upsertClaimFeedback.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
  });

  it("uses exact claim id lookup for status reads", async () => {
    getClaimReviewByClaimId.mockResolvedValue({
      claimId: "C0001",
      reviewedAt: new Date("2026-05-17T03:00:00.000Z"),
      reviewedByEmail: "analyst@example.com",
      reviewedById: "user-1",
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
      reviewedAt: "2026-05-17T03:00:00.000Z",
      reviewedByEmail: "analyst@example.com",
      reviewedById: "user-1",
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

describe("claim feedback route", () => {
  beforeEach(() => {
    getClaimFeedbackByClaimId.mockReset();
    getClaimReviewByClaimId.mockReset();
    getClaimTimeline.mockReset();
    logClaimEvent.mockReset();
    upsertClaimFeedback.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
  });

  it("returns claim feedback for the exact claim id", async () => {
    getClaimFeedbackByClaimId.mockResolvedValue({
      claimId: "C0001",
      comment: "Need a clearer billing benchmark callout.",
      createdAt: new Date("2026-05-17T04:00:00.000Z"),
      rating: "useful",
      reason: "too_vague",
      userId: "user-1",
    });

    const { GET } = await import("@/app/api/claims/[claimId]/feedback/route");
    const response = await GET(
      new Request("http://localhost/api/claims/C0001/feedback"),
      { params: Promise.resolve({ claimId: "C0001" }) },
    );

    expect(response.status).toBe(200);
    expect(getClaimFeedbackByClaimId).toHaveBeenCalledWith("C0001", "user-1");
    await expect(response.json()).resolves.toEqual({
      feedback: {
        claimId: "C0001",
        comment: "Need a clearer billing benchmark callout.",
        createdAt: "2026-05-17T04:00:00.000Z",
        rating: "useful",
        reason: "too_vague",
        userId: "user-1",
      },
    });
  });

  it("rejects feedback when the claim has not been analyzed", async () => {
    getClaimReviewByClaimId.mockResolvedValue({
      analyzedAt: null,
      claimId: "C0001",
      status: "new",
    });

    const { POST } = await import("@/app/api/claims/[claimId]/feedback/route");
    const response = await POST(
      new Request("http://localhost/api/claims/C0001/feedback", {
        body: JSON.stringify({ rating: "useful" }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      }),
      { params: Promise.resolve({ claimId: "C0001" }) },
    );

    expect(response.status).toBe(409);
    expect(upsertClaimFeedback).not.toHaveBeenCalled();
  });

  it("stores one-click feedback for the exact claim id", async () => {
    getClaimReviewByClaimId.mockResolvedValue({
      analyzedAt: new Date("2026-05-17T03:30:00.000Z"),
      claimId: "C0001",
      status: "reviewed",
    });
    upsertClaimFeedback.mockResolvedValue({
      claimId: "C0001",
      comment: "",
      createdAt: new Date("2026-05-17T04:05:00.000Z"),
      rating: "not_useful",
      reason: "missing_policy",
      userId: "user-1",
    });

    const { POST } = await import("@/app/api/claims/[claimId]/feedback/route");
    const response = await POST(
      new Request("http://localhost/api/claims/C0001/feedback", {
        body: JSON.stringify({
          rating: "not_useful",
          reason: "missing_policy",
        }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      }),
      { params: Promise.resolve({ claimId: "C0001" }) },
    );

    expect(response.status).toBe(200);
    expect(upsertClaimFeedback).toHaveBeenCalledWith({
      claimId: "C0001",
      comment: "",
      rating: "not_useful",
      reason: "missing_policy",
      userId: "user-1",
      userEmail: "analyst@example.com",
    });
  });
});

describe("claim timeline route", () => {
  beforeEach(() => {
    getClaimFeedbackByClaimId.mockReset();
    getClaimReviewByClaimId.mockReset();
    getClaimTimeline.mockReset();
    logClaimEvent.mockReset();
    upsertClaimFeedback.mockReset();
    updateClaimStatus.mockReset();
    requireAuthorizedSession.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
  });

  it("returns ordered claim activity events", async () => {
    getClaimTimeline.mockResolvedValue([
      {
        actorEmail: "analyst@example.com",
        claimId: "C0001",
        createdAt: new Date("2026-05-17T04:00:00.000Z"),
        eventType: "status_changed",
        metadata: { status: "reviewed" },
      },
      {
        actorEmail: null,
        claimId: "C0001",
        createdAt: new Date("2026-05-17T03:45:00.000Z"),
        eventType: "analysis_generated",
        metadata: null,
      },
    ]);

    const { GET } = await import("@/app/api/claims/[claimId]/timeline/route");
    const response = await GET(
      new Request("http://localhost/api/claims/C0001/timeline"),
      { params: Promise.resolve({ claimId: "C0001" }) },
    );

    expect(response.status).toBe(200);
    expect(getClaimTimeline).toHaveBeenCalledWith("C0001");
    await expect(response.json()).resolves.toEqual({
      events: [
        {
          actorEmail: "analyst@example.com",
          claimId: "C0001",
          createdAt: "2026-05-17T04:00:00.000Z",
          eventType: "status_changed",
          metadata: { status: "reviewed" },
        },
        {
          actorEmail: null,
          claimId: "C0001",
          createdAt: "2026-05-17T03:45:00.000Z",
          eventType: "analysis_generated",
          metadata: null,
        },
      ],
    });
  });
});
