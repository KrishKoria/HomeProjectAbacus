import { requireSession } from "@/lib/auth-session";
import { getClaimStatuses } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const statuses = await getClaimStatuses();
    return Response.json({ statuses });
  } catch (err) {
    console.error("[statuses] failed to fetch claim statuses:", err);
    return Response.json({ error: "Failed to fetch claim statuses" }, { status: 500 });
  }
}
