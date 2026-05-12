import { requireSession } from "@/lib/auth-session";
import { getClaims } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const claims = await getClaims();
  return Response.json({ claims });
}
