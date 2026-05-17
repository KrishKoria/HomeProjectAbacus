import { beforeEach, describe, expect, it, vi } from "vitest";

const requireAuthorizedSession = vi.fn();
const createIngestionUpload = vi.fn();
const getIngestionUploadById = vi.fn();
const listRecentIngestionUploads = vi.fn();
const markIngestionUploadUploaded = vi.fn();
const markIngestionUploadFailed = vi.fn();
const createSignedUploadPolicy = vi.fn();
const deleteUploadedObject = vi.fn();
const verifyUploadedObject = vi.fn();

vi.mock("@/lib/auth-session", () => ({
  requireAuthorizedSession,
}));

vi.mock("@/lib/uploads/db", () => ({
  createIngestionUpload,
  getIngestionUploadById,
  listRecentIngestionUploads,
  markIngestionUploadUploaded,
  markIngestionUploadFailed,
}));

vi.mock("@/lib/uploads/storage", () => ({
  createSignedUploadPolicy,
  deleteUploadedObject,
  verifyUploadedObject,
}));

vi.mock("@/lib/server/env", () => ({
  env: {
    CLAIMOPS_GCS_LANDING_BUCKET: "claimops-landing",
    CLAIMOPS_GCS_LANDING_PREFIX: "claimops-raw-landing",
    CLAIMOPS_UPLOAD_CSV_MAX_BYTES: 10_000_000,
    CLAIMOPS_UPLOAD_PDF_MAX_BYTES: 50_000_000,
    CLAIMOPS_UPLOAD_SIGNED_POLICY_TTL_SECONDS: 900,
  },
}));

describe("upload API routes", () => {
  beforeEach(() => {
    requireAuthorizedSession.mockReset();
    createIngestionUpload.mockReset();
    getIngestionUploadById.mockReset();
    listRecentIngestionUploads.mockReset();
    markIngestionUploadUploaded.mockReset();
    markIngestionUploadFailed.mockReset();
    createSignedUploadPolicy.mockReset();
    deleteUploadedObject.mockReset();
    verifyUploadedObject.mockReset();
    requireAuthorizedSession.mockResolvedValue({
      user: { email: "analyst@example.com", id: "user-1" },
    });
  });

  it("returns dataset metadata for authorized users", async () => {
    const { GET } = await import("@/app/api/uploads/datasets/route");
    const response = await GET();

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      datasets: expect.arrayContaining([
        expect.objectContaining({ datasetKey: "claims", hasPhi: true }),
        expect.objectContaining({ datasetKey: "policies", hasPhi: false }),
      ]),
    });
  });

  it("rejects unauthorized upload initiation", async () => {
    requireAuthorizedSession.mockRejectedValueOnce(new Error("Unauthorized"));

    const { POST } = await import("@/app/api/uploads/initiate/route");
    const response = await POST(
      new Request("http://localhost/api/uploads/initiate", {
        body: JSON.stringify({
          byteSize: 100,
          contentType: "text/csv",
          datasetKey: "claims",
          fileName: "claims.csv",
          headers: ["claim_id"],
        }),
        method: "POST",
      }),
    );

    expect(response.status).toBe(401);
  });

  it("creates an upload row and returns a signed POST policy", async () => {
    createIngestionUpload.mockResolvedValue({
      datasetKey: "claims",
      id: "upl_test",
      objectName: "claims/upl_test.csv",
      uploadedById: "user-1",
      volumePath: "/Volumes/healthcare/bronze/raw_landing/claims/upl_test.csv",
    });
    createSignedUploadPolicy.mockResolvedValue({
      expiresAt: "2026-05-16T10:15:00.000Z",
      fields: { key: "claimops-raw-landing/claims/upl_test.csv" },
      url: "https://storage.googleapis.com/claimops-landing",
    });

    const { POST } = await import("@/app/api/uploads/initiate/route");
    const response = await POST(
      new Request("http://localhost/api/uploads/initiate", {
        body: JSON.stringify({
          byteSize: 128,
          contentType: "text/csv",
          datasetKey: "claims",
          fileName: "claims.csv",
          headers: ["claim_id", "patient_id", "provider_id", "diagnosis_code", "procedure_code", "billed_amount", "date", "claim_status", "denial_reason_code", "allowed_amount", "paid_amount", "is_denied", "follow_up_required"],
        }),
        method: "POST",
      }),
    );

    expect(response.status).toBe(200);
    expect(createIngestionUpload).toHaveBeenCalledWith(
      expect.objectContaining({
        byteSize: 128,
        contentType: "text/csv",
        datasetKey: "claims",
        uploadedByEmail: "analyst@example.com",
        uploadedById: "user-1",
      }),
    );
    expect(createSignedUploadPolicy).toHaveBeenCalledWith(
      expect.objectContaining({
        byteSize: 128,
        contentType: "text/csv",
        datasetKey: "claims",
        objectName: "claims/upl_test.csv",
        uploadId: "upl_test",
        uploaderId: "user-1",
      }),
    );
    await expect(response.json()).resolves.toMatchObject({
      uploadId: "upl_test",
      volumePath: "/Volumes/healthcare/bronze/raw_landing/claims/upl_test.csv",
    });
  });

  it("marks uploads complete only after verifying the GCS object", async () => {
    getIngestionUploadById.mockResolvedValue({
      byteSize: 128,
      contentType: "text/csv",
      datasetKey: "claims",
      id: "upl_test",
      objectName: "claims/upl_test.csv",
      uploadedById: "user-1",
      volumePath: "/Volumes/healthcare/bronze/raw_landing/claims/upl_test.csv",
    });
    verifyUploadedObject.mockResolvedValue({
      generation: "1700000000000000",
      ok: true,
    });
    markIngestionUploadUploaded.mockResolvedValue(undefined);

    const { POST } = await import("@/app/api/uploads/complete/route");
    const response = await POST(
      new Request("http://localhost/api/uploads/complete", {
        body: JSON.stringify({ uploadId: "upl_test" }),
        method: "POST",
      }),
    );

    expect(response.status).toBe(200);
    expect(verifyUploadedObject).toHaveBeenCalledWith({
      byteSize: 128,
      contentType: "text/csv",
      datasetKey: "claims",
      objectName: "claims/upl_test.csv",
      uploadId: "upl_test",
      uploaderId: "user-1",
    });
    expect(markIngestionUploadUploaded).toHaveBeenCalledWith(
      "upl_test",
      "1700000000000000",
    );
    await expect(response.json()).resolves.toMatchObject({
      gcsGeneration: "1700000000000000",
      uploadId: "upl_test",
      volumePath: expect.stringContaining("/Volumes/healthcare/bronze/raw_landing/claims/"),
    });
  });

  it("marks failed and deletes the object when verification detects a mismatch", async () => {
    getIngestionUploadById.mockResolvedValue({
      byteSize: 128,
      contentType: "text/csv",
      datasetKey: "claims",
      id: "upl_test",
      objectName: "claims/upl_test.csv",
      uploadedById: "user-1",
    });
    verifyUploadedObject.mockResolvedValue({
      errorMessage: "Uploaded object dataset metadata did not match signed request",
      ok: false,
    });

    const { POST } = await import("@/app/api/uploads/complete/route");
    const response = await POST(
      new Request("http://localhost/api/uploads/complete", {
        body: JSON.stringify({ uploadId: "upl_test" }),
        method: "POST",
      }),
    );

    expect(response.status).toBe(400);
    expect(deleteUploadedObject).toHaveBeenCalledWith("claims/upl_test.csv");
    expect(markIngestionUploadFailed).toHaveBeenCalledWith(
      "upl_test",
      "Uploaded object dataset metadata did not match signed request",
    );
  });

  it("rejects CSV upload initiation when headers are missing", async () => {
    const { POST } = await import("@/app/api/uploads/initiate/route");
    const response = await POST(
      new Request("http://localhost/api/uploads/initiate", {
        body: JSON.stringify({
          byteSize: 128,
          contentType: "text/csv",
          datasetKey: "claims",
          fileName: "claims.csv",
        }),
        method: "POST",
      }),
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining("must include CSV headers"),
    });
  });
});
