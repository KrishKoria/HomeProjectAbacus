import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaimStats } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const stats = await getClaimStats();
    return Response.json(stats);
  } catch (err) {
    console.error("[stats] failed to fetch claim stats:", err);
    return Response.json({ error: "Failed to fetch claim stats" }, { status: 500 });
  }
}
