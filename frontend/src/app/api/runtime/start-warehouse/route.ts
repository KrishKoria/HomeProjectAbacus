import { requireAuthorizedSession } from "@/lib/auth-session";
import { databricksFetch } from "@/lib/databricks/client";
import { env } from "@/lib/server/env";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const result = await databricksFetch(
    `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}/start`,
    { method: "POST" },
  );

  if (!result.ok) {
    return Response.json({ error: "Failed to start warehouse" }, { status: 502 });
  }

  return Response.json({ started: true });
}
