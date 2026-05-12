import { requireSession } from "@/lib/auth-session";
import { getClaims } from "@/lib/db/claims";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  try {
    await requireSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);

  const page = parseInt(searchParams.get("page") ?? "1", 10);
  const limit = parseInt(searchParams.get("limit") ?? "20", 10);
  const search = searchParams.get("search") ?? "";
  const risk = searchParams.get("risk") ?? "all";
  const status = searchParams.get("status") ?? "all";
  const sort = searchParams.get("sort") ?? "riskScore";
  const order = searchParams.get("order") ?? "desc";

  try {
    const result = await getClaims({ page, limit, search, risk, status, sort, order });
    return Response.json(result);
  } catch (err) {
    console.error("[claims] failed to fetch claims:", err);
    return Response.json({ error: "Failed to fetch claims" }, { status: 500 });
  }
}
