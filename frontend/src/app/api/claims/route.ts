import { requireAuthorizedSession } from "@/lib/auth-session";
import { getClaims } from "@/lib/db/claims";
import { z } from "zod";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const claimsQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).catch(20),
  order: z.enum(["asc", "desc"]).catch("desc"),
  page: z.coerce.number().int().min(1).catch(1),
  risk: z.string().catch("all"),
  search: z.string().catch(""),
  sort: z.enum(["riskScore", "analyzedAt", "claimId"]).catch("riskScore"),
  status: z.string().catch("all"),
});

export async function GET(request: Request) {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const parsedQuery = claimsQuerySchema.parse({
    limit: searchParams.get("limit"),
    order: searchParams.get("order"),
    page: searchParams.get("page"),
    risk: searchParams.get("risk"),
    search: searchParams.get("search"),
    sort: searchParams.get("sort"),
    status: searchParams.get("status"),
  });

  try {
    const result = await getClaims(parsedQuery);
    return Response.json(result);
  } catch (err) {
    console.error("[claims] failed to fetch claims:", err);
    return Response.json({ error: "Failed to fetch claims" }, { status: 500 });
  }
}
