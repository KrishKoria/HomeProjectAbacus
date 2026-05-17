import type { UploadDatasetKey } from "@/lib/uploads/registry";

export interface UploadDataset {
  acceptedContentTypes: string[];
  datasetKey: UploadDatasetKey;
  description: string;
  displayName: string;
  extension: ".csv" | ".pdf";
  hasPhi: boolean;
  landingSubdirectory: string;
  maxBytes: number;
  requiredColumns: string[];
}

export interface UploadRecord {
  byteSize: number;
  completedAt: string | null;
  contentType: string;
  createdAt: string;
  datasetKey: string;
  errorMessage: string | null;
  gcsGeneration: string | null;
  id: string;
  objectName: string;
  status: "initiated" | "uploaded" | "failed";
  uploadedByEmail: string;
  volumePath: string;
}

export interface ClaimSyncStateSummary {
  lastClaimId: string | null;
  lastDiscoveredCount: number;
  lastIngestedAt: string | null;
  lastInsertedCount: number;
  lastSyncedAt: string;
  sourceTable: string;
}

export interface SelectedUpload {
  controller?: AbortController;
  error?: string;
  file: File;
  headers?: string[];
  missingColumns?: string[];
  progress: number;
  gcsGeneration?: string;
  status:
    | "selected"
    | "invalid"
    | "signing"
    | "uploading"
    | "verifying"
    | "landed"
    | "failed"
    | "cancelled";
  uploadId?: string;
  volumePath?: string;
}

export interface SignedPolicy {
  fields: Record<string, string>;
  url: string;
}

export interface InitiateUploadResponse {
  policy: SignedPolicy;
  uploadId: string;
  volumePath: string;
}

export const STATUS_COPY: Record<SelectedUpload["status"], string> = {
  cancelled: "Cancelled",
  failed: "Failed",
  invalid: "Invalid",
  landed: "Landed",
  selected: "Ready",
  signing: "Signing",
  uploading: "Uploading",
  verifying: "Verifying",
};
