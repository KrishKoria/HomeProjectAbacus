import { requireAuthorizedSession } from "@/lib/auth-session";
import { getToken } from "@/lib/databricks/oauth";
import { databricksFetch } from "@/lib/databricks/client";
import { env } from "@/lib/server/env";
import type { DatabricksStatus } from "@/lib/databricks/types";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAuthorizedSession();
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

  try {
    await getToken();
    status.oauth = true;
  } catch {
    // oauth remains false
  }

  const [warehouseResult, endpointResult] = await Promise.all([
    databricksFetch<{ state: string }>(
      `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}`,
    ),
    databricksFetch(
      `/api/2.0/serving-endpoints/${env.CLAIMOPS_ANALYSIS_ENDPOINT}`,
    ),
  ]);

  if (warehouseResult.ok) status.sqlWarehouse = true;
  if (endpointResult.ok) status.analysisEndpoint = true;

  return Response.json(status);
}
