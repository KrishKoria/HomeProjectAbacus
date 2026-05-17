import { Storage } from "@google-cloud/storage";
import { env } from "@/lib/server/env";

interface UploadObjectInput {
  byteSize: number;
  contentType: string;
  datasetKey: string;
  objectName: string;
  uploadId: string;
  uploaderId: string;
}

export interface SignedUploadPolicy {
  expiresAt: string;
  fields: Record<string, string>;
  url: string;
}

export interface UploadedObjectVerification {
  errorMessage?: string;
  generation?: string;
  ok: boolean;
}

let storage: Storage | null = null;

function getStorage() {
  storage ??= new Storage();
  return storage;
}

export async function createSignedUploadPolicy(
  input: UploadObjectInput,
): Promise<SignedUploadPolicy> {
  const key = getLandingObjectKey(input.objectName);
  const metadataFields = {
    "x-goog-meta-dataset-key": input.datasetKey,
    "x-goog-meta-upload-id": input.uploadId,
    "x-goog-meta-uploader-id": input.uploaderId,
  };
  const expiresAt = new Date(
    Date.now() + env.CLAIMOPS_UPLOAD_SIGNED_POLICY_TTL_SECONDS * 1000,
  );
  const [policy] = await getStorage()
    .bucket(env.CLAIMOPS_GCS_LANDING_BUCKET)
    .file(key)
    .generateSignedPostPolicyV4({
      conditions: [
        ["eq", "$key", key],
        ["eq", "$Content-Type", input.contentType],
        ...Object.entries(metadataFields).map(([field, value]) => [
          "eq",
          `$${field}`,
          value,
        ]),
        ["content-length-range", 1, input.byteSize],
      ],
      expires: expiresAt,
      fields: {
        "Content-Type": input.contentType,
        key,
        ...metadataFields,
      },
    });

  return {
    expiresAt: expiresAt.toISOString(),
    fields: policy.fields,
    url: policy.url,
  };
}

export async function verifyUploadedObject(
  input: UploadObjectInput,
): Promise<UploadedObjectVerification> {
  try {
    const [metadata] = await getStorage()
      .bucket(env.CLAIMOPS_GCS_LANDING_BUCKET)
      .file(getLandingObjectKey(input.objectName))
      .getMetadata();

    if (Number(metadata.size) !== input.byteSize) {
      return { ok: false, errorMessage: "Uploaded object size did not match signed request" };
    }
    if ((metadata.contentType ?? "").toLowerCase() !== input.contentType.toLowerCase()) {
      return { ok: false, errorMessage: "Uploaded object content type did not match signed request" };
    }
    if (metadata.metadata?.["upload-id"] !== input.uploadId) {
      return { ok: false, errorMessage: "Uploaded object upload metadata did not match signed request" };
    }
    if (metadata.metadata?.["dataset-key"] !== input.datasetKey) {
      return { ok: false, errorMessage: "Uploaded object dataset metadata did not match signed request" };
    }
    if (metadata.metadata?.["uploader-id"] !== input.uploaderId) {
      return { ok: false, errorMessage: "Uploaded object uploader metadata did not match signed request" };
    }

    return { ok: true, generation: String(metadata.generation ?? "") };
  } catch {
    return { ok: false, errorMessage: "Uploaded object was not found in GCS" };
  }
}

export async function deleteUploadedObject(objectName: string): Promise<void> {
  try {
    await getStorage()
      .bucket(env.CLAIMOPS_GCS_LANDING_BUCKET)
      .file(getLandingObjectKey(objectName))
      .delete({ ignoreNotFound: true });
  } catch {
    // Cleanup is best-effort after verification failure; the upload row keeps the failure.
  }
}

export function getLandingObjectKey(objectName: string): string {
  const prefix = env.CLAIMOPS_GCS_LANDING_PREFIX.replace(/^\/+|\/+$/g, "");
  return prefix ? `${prefix}/${objectName}` : objectName;
}
