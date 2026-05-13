import { requireAuthorizedSession } from "@/lib/auth-session";
import { databricksFetch } from "@/lib/databricks/client";
import { env } from "@/lib/server/env";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

interface SqlStatementResponse {
  status: { state: string };
  manifest?: { columns: Array<{ name: string }> };
  result?: { data_array: Array<Array<unknown>> };
}

export async function GET() {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const query = `SELECT DISTINCT claim_id FROM ${env.CLAIMOPS_FEATURE_TABLE} LIMIT 20`;

  const result = await databricksFetch<SqlStatementResponse>(
    "/api/2.0/sql/statements",
    {
      method: "POST",
      body: JSON.stringify({
        statement: query,
        warehouse_id: env.DATABRICKS_SQL_WAREHOUSE_ID,
        disposition: "INLINE",
        max_wait_seconds: 30,
      }),
    },
  );

  if (!result.ok) {
    console.error("[samples] Databricks SQL failed:", result.message);
    return Response.json(
      { error: result.message || "Failed to fetch sample claims" },
      { status: result.status || 502 },
    );
  }

  const rows = result.data.result?.data_array ?? [];
  const claimIds = rows.map((r) => String(r[0]));

  return Response.json({ claimIds });
}
