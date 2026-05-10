import { requireSession } from "@/lib/auth-session";
import { databricksFetch } from "@/lib/databricks/client";
import { env } from "@/lib/server/env";
import type { DatabricksStatus } from "@/lib/databricks/types";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const status: DatabricksStatus = {
    oauth: false,
    sqlWarehouse: false,
    analysisEndpoint: false,
    vectorSearch: false,
    modelServing: false,
  };

  const oauthResult = await databricksFetch<{ access_token: string }>("/oidc/v1/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        scope: "all-apis",
        client_id: env.DATABRICKS_CLIENT_ID,
        client_secret: env.DATABRICKS_CLIENT_SECRET,
      }),
  });
  if (oauthResult.ok) status.oauth = true;

  const warehouseResult = await databricksFetch<{ state: string }>(
    `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}`,
  );
  if (warehouseResult.ok) {
    status.sqlWarehouse = true;
    if (warehouseResult.data.state === "STOPPED") {
      await databricksFetch(`/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}/start`, {
        method: "POST",
      });
    }
  }

  const endpointResult = await databricksFetch<{ state: string }>(
    `/api/2.0/serving-endpoints/${env.CLAIMOPS_ANALYSIS_ENDPOINT}`,
  );
  if (endpointResult.ok) {
    status.analysisEndpoint = endpointResult.data.state === "READY";
  }

  return Response.json(status);
}
