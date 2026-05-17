import { describe, expect, it } from "vitest";

import {
  buildUploadObjectName,
  getUploadDataset,
  parseInitiateUploadInput,
} from "@/lib/uploads/registry";

describe("upload dataset registry", () => {
  it("mirrors the ETL landing datasets and PHI classification", () => {
    expect(getUploadDataset("claims")).toMatchObject({
      datasetKey: "claims",
      extension: ".csv",
      hasPhi: true,
      landingSubdirectory: "claims",
    });
    expect(getUploadDataset("claims").requiredColumns).toContain("claim_id");
    expect(getUploadDataset("dx_px_mapping").requiredColumns).toEqual([
      "diagnosis_code",
      "procedure_code",
      "compatible",
      "procedure_category",
      "pair_risk_prior",
    ]);
    expect(getUploadDataset("policies")).toMatchObject({
      datasetKey: "policies",
      extension: ".pdf",
      hasPhi: false,
      maxBytes: 50_000_000,
    });
  });

  it("lands each dataset in the ETL volume subdirectory expected by Bronze", () => {
    expect(getUploadDataset("claims").landingSubdirectory).toBe("claims");
    expect(getUploadDataset("providers").landingSubdirectory).toBe("providers");
    expect(getUploadDataset("diagnosis").landingSubdirectory).toBe("diagnosis");
    expect(getUploadDataset("cost").landingSubdirectory).toBe("cost");
    expect(getUploadDataset("dx_px_mapping").landingSubdirectory).toBe("dx_px_mapping");
    expect(getUploadDataset("policies").landingSubdirectory).toBe("policies");

    expect(
      buildUploadObjectName({
        datasetKey: "providers",
        fileName: "providers.csv",
        uploadId: "upl_provider",
      }),
    ).toBe("providers/upl_provider.csv");
    expect(
      buildUploadObjectName({
        datasetKey: "dx_px_mapping",
        fileName: "dx_px_mapping.csv",
        uploadId: "upl_mapping",
      }),
    ).toBe("dx_px_mapping/upl_mapping.csv");
  });

  it("rejects mismatched file types and oversize uploads", () => {
    expect(() =>
      parseInitiateUploadInput({
        byteSize: 50_000_001,
        contentType: "application/pdf",
        datasetKey: "policies",
        fileName: "policy.pdf",
      }),
    ).toThrow("File exceeds");

    expect(() =>
      parseInitiateUploadInput({
        byteSize: 100,
        contentType: "application/pdf",
        datasetKey: "claims",
        fileName: "claims.pdf",
      }),
    ).toThrow("CSV");
  });

  it("uses generated names for CSV datasets and sanitized policy names for PDFs", () => {
    expect(
      buildUploadObjectName({
        datasetKey: "claims",
        fileName: "patient export 2026.csv",
        uploadId: "upl_123",
      }),
    ).toBe("claims/upl_123.csv");

    expect(
      buildUploadObjectName({
        datasetKey: "policies",
        fileName: "Payer Policy v2 FINAL!.pdf",
        uploadId: "upl_456",
      }),
    ).toBe("policies/upl_456-payer-policy-v2-final.pdf");
  });
});
