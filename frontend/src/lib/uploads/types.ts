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
