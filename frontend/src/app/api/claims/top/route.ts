import { requireAuthorizedSession } from "@/lib/auth-session";
import { getTopClaims } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const claims = await getTopClaims(5);
  return Response.json({ claims });
}
