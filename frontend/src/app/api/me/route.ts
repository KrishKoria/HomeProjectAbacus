import { getOptionalSession } from "@/lib/auth-session";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const session = await getOptionalSession();
  return Response.json(session?.user ?? null);
}
