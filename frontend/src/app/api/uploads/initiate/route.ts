import { randomUUID } from "crypto";
import { NextResponse } from "next/server";
import { requireAuthorizedSession } from "@/lib/auth-session";
import { env } from "@/lib/server/env";
import { createIngestionUpload } from "@/lib/uploads/db";
import {
  buildUploadObjectName,
  buildVolumePath,
  parseInitiateUploadInput,
} from "@/lib/uploads/registry";
import { createSignedUploadPolicy } from "@/lib/uploads/storage";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const session = await requireAuthorizedSession();
    const parsed = parseInitiateUploadInput(await request.json(), {
      csvMaxBytes: env.CLAIMOPS_UPLOAD_CSV_MAX_BYTES,
      pdfMaxBytes: env.CLAIMOPS_UPLOAD_PDF_MAX_BYTES,
    });
    const uploadId = `upl_${randomUUID()}`;
    const objectName = buildUploadObjectName({
      datasetKey: parsed.datasetKey,
      fileName: parsed.fileName,
      uploadId,
    });
    const row = await createIngestionUpload({
      byteSize: parsed.byteSize,
      contentType: parsed.contentType,
      datasetKey: parsed.datasetKey,
      id: uploadId,
      objectName,
      uploadedByEmail: session.user.email,
      uploadedById: session.user.id,
      volumePath: buildVolumePath(objectName),
    });
    const policy = await createSignedUploadPolicy({
      byteSize: row.byteSize ?? parsed.byteSize,
      contentType: row.contentType ?? parsed.contentType,
      objectName: row.objectName,
    });

    return NextResponse.json({
      objectName: row.objectName,
      policy,
      uploadId: row.id,
      volumePath: row.volumePath,
    });
  } catch (error) {
    return uploadRouteError(error);
  }
}

function uploadRouteError(error: unknown) {
  const message = error instanceof Error ? error.message : "Invalid upload request";
  const status = message === "Forbidden" ? 403 : message === "Unauthorized" ? 401 : 400;
  return NextResponse.json({ error: message }, { status });
}
