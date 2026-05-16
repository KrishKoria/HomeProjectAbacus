import { NextResponse } from "next/server";
import { requireAuthorizedSession } from "@/lib/auth-session";
import { listRecentIngestionUploads } from "@/lib/uploads/db";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
    const uploads = await listRecentIngestionUploads(25);
    return NextResponse.json({ uploads });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unauthorized";
    return NextResponse.json(
      { error: message },
      { status: message === "Forbidden" ? 403 : 401 },
    );
  }
}
