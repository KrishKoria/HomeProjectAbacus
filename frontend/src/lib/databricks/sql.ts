import { databricksFetch } from "@/lib/databricks/client";
import { FEATURE_COLUMNS } from "@/lib/contracts/claimops";
import type { ClaimFeatureRow } from "@/lib/databricks/types";
import { env } from "@/lib/server/env";

interface SqlStatementResponse {
  statement_id: string;
  status: {
    state: string;
  };
  manifest?: {
    format?: string;
    schema?: {
      column_count?: number;
      columns?: Array<{ name: string; position?: number; type_name?: string }>;
    };
  };
  result?: {
    data_array: Array<Array<unknown>>;
  };
}

async function ensureWarehouseRunning(): Promise<void> {
  const whResult = await databricksFetch<{ state: string }>(
    `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}`,
  );
  if (whResult.ok && whResult.data.state === "STOPPED") {
    console.log(
      `[sql] Warehouse ${env.DATABRICKS_SQL_WAREHOUSE_ID} is STOPPED, starting...`,
    );
    await databricksFetch(
      `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}/start`,
      { method: "POST" },
    );
    for (let i = 0; i < 30; i++) {
      await new Promise((r) => setTimeout(r, 2000));
      const statusResult = await databricksFetch<{ state: string }>(
        `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}`,
      );
      if (statusResult.ok && statusResult.data.state === "RUNNING") {
        console.log("[sql] Warehouse is now RUNNING");
        return;
      }
    }
    throw new Error("SQL warehouse failed to start within 60s");
  }
}

export async function fetchFeatureRow(
  claimId: string,
): Promise<
  | { ok: true; row: ClaimFeatureRow }
  | { ok: false; status: number; message: string }
> {
  const columns = FEATURE_COLUMNS.join(", ");
  const query = `SELECT ${columns} FROM ${env.CLAIMOPS_FEATURE_TABLE} WHERE claim_id = :claimId`;

  await ensureWarehouseRunning();

  const statementResult = await databricksFetch<SqlStatementResponse>(
    "/api/2.0/sql/statements",
    {
      method: "POST",
      body: JSON.stringify({
        statement: query,
        warehouse_id: env.DATABRICKS_SQL_WAREHOUSE_ID,
        disposition: "INLINE",
        wait_timeout: "30s",
        parameters: [{ name: "claimId", value: claimId }],
      }),
    },
  );

  if (!statementResult.ok) {
    return statementResult;
  }

  const { data: stmt } = statementResult;

  if (stmt.status.state === "PENDING" || stmt.status.state === "RUNNING") {
    const pollResult = await pollStatement(stmt.statement_id);
    if (!pollResult.ok) return pollResult;
    return extractRow(pollResult.data);
  }

  return extractRow(stmt);
}

async function pollStatement(
  statementId: string,
  maxRetries = 10,
  delayMs = 1000,
): Promise<
  | { ok: true; data: SqlStatementResponse }
  | { ok: false; status: number; message: string }
> {
  for (let i = 0; i < maxRetries; i++) {
    await new Promise((r) => setTimeout(r, delayMs));
    const result = await databricksFetch<SqlStatementResponse>(
      `/api/2.0/sql/statements/${statementId}`,
    );
    if (!result.ok) return result;
    if (
      result.data.status.state === "SUCCEEDED" ||
      result.data.status.state === "FAILED"
    ) {
      return result;
    }
  }
  return { ok: false, status: 504, message: "SQL statement timed out" };
}

function extractRow(
  stmt: SqlStatementResponse,
):
  | { ok: true; row: ClaimFeatureRow }
  | { ok: false; status: number; message: string } {
  if (stmt.status.state === "FAILED") {
    return { ok: false, status: 500, message: "SQL statement failed" };
  }

  const columns = stmt.manifest?.schema?.columns?.map((c) => c.name) ?? [];
  const rows = stmt.result?.data_array ?? [];

  if (rows.length === 0) {
    return {
      ok: false,
      status: 404,
      message: "Claim ID not found in feature table",
    };
  }

  const row: Record<string, unknown> = {};
  for (let i = 0; i < columns.length; i++) {
    row[columns[i]] = coerceSqlValue(rows[0][i]);
  }

  return { ok: true, row: row as unknown as ClaimFeatureRow };
}

function coerceSqlValue(raw: unknown): number | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === "number" && !Number.isNaN(raw)) return raw;
  if (typeof raw === "boolean") return raw ? 1 : 0;
  const str = String(raw).trim();
  if (str === "" || str === "null") return null;
  if (str === "true") return 1;
  if (str === "false") return 0;
  const num = Number(str);
  return Number.isNaN(num) ? null : num;
}
