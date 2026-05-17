import { z } from "zod";

export const CSV_UPLOAD_MAX_BYTES = 100_000_000;
export const PDF_UPLOAD_MAX_BYTES = 50_000_000;

const CSV_CONTENT_TYPES = new Set([
  "application/csv",
  "application/octet-stream",
  "application/vnd.ms-excel",
  "text/csv",
  "text/plain",
]);

const PDF_CONTENT_TYPES = new Set(["application/pdf"]);

export const UPLOAD_DATASETS = {
  claims: {
    datasetKey: "claims",
    displayName: "Claims",
    description: "Claims adjudication export consumed by Bronze ingestion.",
    extension: ".csv",
    acceptedContentTypes: [...CSV_CONTENT_TYPES],
    hasPhi: true,
    landingSubdirectory: "claims",
    maxBytes: CSV_UPLOAD_MAX_BYTES,
    requiredColumns: [
      "claim_id",
      "patient_id",
      "provider_id",
      "diagnosis_code",
      "procedure_code",
      "billed_amount",
      "date",
      "claim_status",
      "denial_reason_code",
      "allowed_amount",
      "paid_amount",
      "is_denied",
      "follow_up_required",
    ],
  },
  providers: {
    datasetKey: "providers",
    displayName: "Providers",
    description: "Provider roster used for trusted provider dimensions.",
    extension: ".csv",
    acceptedContentTypes: [...CSV_CONTENT_TYPES],
    hasPhi: false,
    landingSubdirectory: "providers",
    maxBytes: CSV_UPLOAD_MAX_BYTES,
    requiredColumns: ["provider_id", "doctor_name", "specialty", "location"],
  },
  diagnosis: {
    datasetKey: "diagnosis",
    displayName: "Diagnosis",
    description: "Diagnosis code reference data.",
    extension: ".csv",
    acceptedContentTypes: [...CSV_CONTENT_TYPES],
    hasPhi: false,
    landingSubdirectory: "diagnosis",
    maxBytes: CSV_UPLOAD_MAX_BYTES,
    requiredColumns: ["diagnosis_code", "category", "severity"],
  },
  cost: {
    datasetKey: "cost",
    displayName: "Cost",
    description: "Procedure cost benchmarks by region.",
    extension: ".csv",
    acceptedContentTypes: [...CSV_CONTENT_TYPES],
    hasPhi: false,
    landingSubdirectory: "cost",
    maxBytes: CSV_UPLOAD_MAX_BYTES,
    requiredColumns: ["procedure_code", "average_cost", "expected_cost", "region"],
  },
  dx_px_mapping: {
    datasetKey: "dx_px_mapping",
    displayName: "Diagnosis-Procedure Mapping",
    description: "Compatibility and risk priors for diagnosis/procedure pairs.",
    extension: ".csv",
    acceptedContentTypes: [...CSV_CONTENT_TYPES],
    hasPhi: false,
    landingSubdirectory: "dx_px_mapping",
    maxBytes: CSV_UPLOAD_MAX_BYTES,
    requiredColumns: [
      "diagnosis_code",
      "procedure_code",
      "compatible",
      "procedure_category",
      "pair_risk_prior",
    ],
  },
  policies: {
    datasetKey: "policies",
    displayName: "Policies",
    description: "Policy documents for RAG chunking and citation.",
    extension: ".pdf",
    acceptedContentTypes: [...PDF_CONTENT_TYPES],
    hasPhi: false,
    landingSubdirectory: "policies",
    maxBytes: PDF_UPLOAD_MAX_BYTES,
    requiredColumns: [],
  },
} as const;

export type UploadDatasetKey = keyof typeof UPLOAD_DATASETS;
export type UploadDataset = (typeof UPLOAD_DATASETS)[UploadDatasetKey];

export interface InitiateUploadInput {
  byteSize: number;
  contentType: string;
  datasetKey: UploadDatasetKey;
  fileName: string;
  headers?: string[];
}

export interface UploadSizeLimits {
  csvMaxBytes?: number;
  pdfMaxBytes?: number;
}

const initiateUploadSchema = z.object({
  byteSize: z.number().int().positive(),
  contentType: z.string().min(1),
  datasetKey: z.enum(Object.keys(UPLOAD_DATASETS) as [UploadDatasetKey, ...UploadDatasetKey[]]),
  fileName: z.string().min(1).max(255),
  headers: z.array(z.string().min(1)).optional(),
});

export function getUploadDatasets(): UploadDataset[] {
  return Object.values(UPLOAD_DATASETS);
}

export function getUploadDataset(datasetKey: UploadDatasetKey): UploadDataset {
  return UPLOAD_DATASETS[datasetKey];
}

export function parseInitiateUploadInput(
  value: unknown,
  limits: UploadSizeLimits = {},
): InitiateUploadInput {
  const parsed = initiateUploadSchema.parse(value);
  const dataset = getUploadDataset(parsed.datasetKey);
  const maxBytes =
    dataset.extension === ".pdf"
      ? (limits.pdfMaxBytes ?? dataset.maxBytes)
      : (limits.csvMaxBytes ?? dataset.maxBytes);
  const fileName = parsed.fileName.toLowerCase();
  const contentType = parsed.contentType.toLowerCase();

  if (!fileName.endsWith(dataset.extension)) {
    throw new Error(`${dataset.displayName} uploads must use ${dataset.extension.toUpperCase()} files`);
  }

  if (!dataset.acceptedContentTypes.includes(contentType)) {
    throw new Error(`${dataset.displayName} uploads do not accept ${parsed.contentType}`);
  }

  if (parsed.byteSize > maxBytes) {
    throw new Error(`File exceeds ${formatBytes(maxBytes)} limit`);
  }

  const headers = parsed.headers?.map(normalizeHeader).filter(Boolean);
  if (dataset.extension === ".csv") {
    if (!headers || headers.length === 0) {
      throw new Error(`${dataset.displayName} uploads must include CSV headers`);
    }

    const missingColumns = dataset.requiredColumns.filter(
      (column) => !headers.includes(column),
    );
    if (missingColumns.length > 0) {
      throw new Error(`Missing columns: ${missingColumns.join(", ")}`);
    }
  }

  return {
    ...parsed,
    contentType,
    headers,
  };
}

export function buildUploadObjectName(input: {
  datasetKey: UploadDatasetKey;
  fileName: string;
  uploadId: string;
}): string {
  const dataset = getUploadDataset(input.datasetKey);
  if (input.datasetKey !== "policies") {
    return `${dataset.landingSubdirectory}/${input.uploadId}${dataset.extension}`;
  }

  const stem = sanitizePolicyStem(input.fileName.replace(/\.pdf$/i, ""));
  return `${dataset.landingSubdirectory}/${input.uploadId}-${stem}${dataset.extension}`;
}

export function buildVolumePath(objectName: string): string {
  return `/Volumes/healthcare/bronze/raw_landing/${objectName}`;
}

function sanitizePolicyStem(value: string): string {
  const sanitized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return sanitized || "policy-document";
}

function formatBytes(value: number): string {
  return `${Math.floor(value / 1_000_000)} MB`;
}

export function normalizeHeader(value: string): string {
  return value.trim().replace(/^"|"$/g, "").replace(/^\uFEFF/, "");
}
