import { NextResponse } from "next/server";
import { requireAuthorizedSession } from "@/lib/auth-session";
import {
  getIngestionUploadById,
  markIngestionUploadFailed,
  markIngestionUploadUploaded,
} from "@/lib/uploads/db";
import { deleteUploadedObject, verifyUploadedObject } from "@/lib/uploads/storage";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const session = await requireAuthorizedSession();
    const body = await request.json();
    const uploadId = typeof body.uploadId === "string" ? body.uploadId : "";
    if (!uploadId) {
      return NextResponse.json({ error: "uploadId is required" }, { status: 400 });
    }

    const upload = await getIngestionUploadById(uploadId);
    if (!upload) {
      return NextResponse.json({ error: "Upload was not found" }, { status: 404 });
    }
    if (upload.uploadedById !== session.user.id) {
      return NextResponse.json({ error: "Forbidden" }, { status: 403 });
    }

    const verification = await verifyUploadedObject({
      byteSize: upload.byteSize,
      contentType: upload.contentType,
      objectName: upload.objectName,
    });

    if (!verification.ok) {
      const errorMessage =
        verification.errorMessage ??
        ("error" in verification && typeof verification.error === "string"
          ? verification.error
          : "Uploaded object could not be verified");
      await deleteUploadedObject(upload.objectName);
      await markIngestionUploadFailed(upload.id, errorMessage);
      return NextResponse.json({ error: errorMessage }, { status: 400 });
    }

    await markIngestionUploadUploaded(upload.id, verification.generation ?? "");
    return NextResponse.json({
      status: "uploaded",
      uploadId: upload.id,
      volumePath: upload.volumePath,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Invalid upload completion";
    const status = message === "Forbidden" ? 403 : message === "Unauthorized" ? 401 : 400;
    return NextResponse.json({ error: message }, { status });
  }
}
