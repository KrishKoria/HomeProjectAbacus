import { NextResponse } from "next/server";
import { requireAuthorizedSession } from "@/lib/auth-session";
import { env } from "@/lib/server/env";
import { getUploadDatasets } from "@/lib/uploads/registry";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
    return NextResponse.json({
      datasets: getUploadDatasets().map((dataset) => ({
        ...dataset,
        maxBytes:
          dataset.extension === ".pdf"
            ? env.CLAIMOPS_UPLOAD_PDF_MAX_BYTES
            : env.CLAIMOPS_UPLOAD_CSV_MAX_BYTES,
      })),
    });
  } catch (error) {
    return uploadAuthError(error);
  }
}

function uploadAuthError(error: unknown) {
  const message = error instanceof Error ? error.message : "Unauthorized";
  return NextResponse.json(
    { error: message },
    { status: message === "Forbidden" ? 403 : 401 },
  );
}
