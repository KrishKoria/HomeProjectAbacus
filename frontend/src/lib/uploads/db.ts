import { desc, eq } from "drizzle-orm";
import { getDb } from "@/lib/db";
import { ingestionUploads } from "@/lib/db/schema";
import type { UploadDatasetKey } from "@/lib/uploads/registry";

export type IngestionUpload = typeof ingestionUploads.$inferSelect;

export interface CreateIngestionUploadInput {
  byteSize: number;
  contentType: string;
  datasetKey: UploadDatasetKey;
  id: string;
  objectName: string;
  uploadedByEmail: string;
  uploadedById: string;
  volumePath: string;
}

export async function createIngestionUpload(
  input: CreateIngestionUploadInput,
): Promise<IngestionUpload> {
  const rows = await getDb()
    .insert(ingestionUploads)
    .values({
      ...input,
      createdAt: new Date(),
      status: "initiated",
    })
    .returning();

  return rows[0];
}

export async function getIngestionUploadById(id: string): Promise<IngestionUpload | null> {
  const rows = await getDb()
    .select()
    .from(ingestionUploads)
    .where(eq(ingestionUploads.id, id))
    .limit(1);
  return rows[0] ?? null;
}

export async function listRecentIngestionUploads(limit = 25): Promise<IngestionUpload[]> {
  return getDb()
    .select()
    .from(ingestionUploads)
    .orderBy(desc(ingestionUploads.createdAt))
    .limit(limit);
}

export async function markIngestionUploadUploaded(
  id: string,
  gcsGeneration: string,
): Promise<void> {
  await getDb()
    .update(ingestionUploads)
    .set({
      completedAt: new Date(),
      errorMessage: null,
      gcsGeneration,
      status: "uploaded",
    })
    .where(eq(ingestionUploads.id, id));
}

export async function markIngestionUploadFailed(
  id: string,
  errorMessage: string,
): Promise<void> {
  await getDb()
    .update(ingestionUploads)
    .set({
      completedAt: new Date(),
      errorMessage,
      status: "failed",
    })
    .where(eq(ingestionUploads.id, id));
}
