import { databricksFetch } from "@/lib/databricks/client";
import { FEATURE_COLUMNS } from "@/lib/contracts/claimops";
import type { ClaimFeatureRow } from "@/lib/databricks/types";
import { env } from "@/lib/server/env";

export interface DatabricksClaimIdRow {
  claimId: string;
  ingestedAt: Date;
}

export interface ClaimSyncCursor {
  lastClaimId: string | null;
  lastIngestedAt: Date | null;
}

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
    next_chunk_internal_link?: string | null;
  };
}

interface SqlChunkResponse {
  data_array?: Array<Array<unknown>>;
  next_chunk_internal_link?: string | null;
}

async function ensureWarehouseRunning(): Promise<void> {
  const whResult = await databricksFetch<{ state: string }>(
    `/api/2.0/sql/warehouses/${env.DATABRICKS_SQL_WAREHOUSE_ID}`,
  );
  if (whResult.ok && whResult.data.state === "STOPPED") {
    console.info(
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
        console.info("[sql] Warehouse is now RUNNING");
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

export async function fetchClaimIdsForSync(
  cursor: ClaimSyncCursor,
): Promise<
  | { ok: true; rows: DatabricksClaimIdRow[] }
  | { ok: false; status: number; message: string }
> {
  const query = buildClaimIdSyncQuery(cursor);
  const parameters = buildClaimIdSyncParameters(cursor);

  const statementResult = await executeSqlStatement(query, parameters);
  if (!statementResult.ok) {
    return statementResult;
  }

  const rows = await collectStatementRows(statementResult.data);
  if (!rows.ok) {
    return rows;
  }

  return {
    ok: true,
    rows: rows.data.map((row) => ({
      claimId: String(row[0]),
      ingestedAt: parseDatabricksTimestampAsUtc(String(row[1])),
    })),
  };
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

async function executeSqlStatement(
  statement: string,
  parameters: Array<{ name: string; value: string; type?: string }> = [],
): Promise<
  | { ok: true; data: SqlStatementResponse }
  | { ok: false; status: number; message: string }
> {
  await ensureWarehouseRunning();

  const statementResult = await databricksFetch<SqlStatementResponse>(
    "/api/2.0/sql/statements",
    {
      method: "POST",
      body: JSON.stringify({
        disposition: "INLINE",
        format: "JSON_ARRAY",
        parameters,
        statement,
        wait_timeout: "30s",
        warehouse_id: env.DATABRICKS_SQL_WAREHOUSE_ID,
      }),
    },
  );

  if (!statementResult.ok) {
    return statementResult;
  }

  if (
    statementResult.data.status.state === "PENDING" ||
    statementResult.data.status.state === "RUNNING"
  ) {
    return pollStatement(statementResult.data.statement_id);
  }

  return statementResult;
}

async function collectStatementRows(
  stmt: SqlStatementResponse,
): Promise<
  | { ok: true; data: Array<Array<unknown>> }
  | { ok: false; status: number; message: string }
> {
  if (stmt.status.state === "FAILED") {
    return { ok: false, status: 500, message: "SQL statement failed" };
  }

  const rows = [...(stmt.result?.data_array ?? [])];
  let nextChunkLink = stmt.result?.next_chunk_internal_link ?? null;

  while (nextChunkLink) {
    const chunkResult = await databricksFetch<SqlChunkResponse>(nextChunkLink);
    if (!chunkResult.ok) {
      return chunkResult;
    }

    rows.push(...(chunkResult.data.data_array ?? []));
    nextChunkLink = chunkResult.data.next_chunk_internal_link ?? null;
  }

  return { ok: true, data: rows };
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

function buildClaimIdSyncQuery(cursor: ClaimSyncCursor): string {
  const baseQuery = [
    `SELECT claim_id, _ingested_at`,
    `FROM ${env.CLAIMOPS_FEATURE_TABLE}`,
  ];

  if (cursor.lastIngestedAt) {
    baseQuery.push(
      `WHERE _ingested_at > :lastIngestedAt`,
      `OR (_ingested_at = :lastIngestedAt AND claim_id > :lastClaimId)`,
    );
  }

  baseQuery.push(`ORDER BY _ingested_at ASC, claim_id ASC`);
  return baseQuery.join(" ");
}

function buildClaimIdSyncParameters(
  cursor: ClaimSyncCursor,
): Array<{ name: string; value: string; type?: string }> {
  if (!cursor.lastIngestedAt) {
    return [];
  }

  return [
    {
      name: "lastIngestedAt",
      type: "TIMESTAMP",
      value: formatDatabricksTimestamp(cursor.lastIngestedAt),
    },
    {
      name: "lastClaimId",
      value: cursor.lastClaimId ?? "",
    },
  ];
}

function parseDatabricksTimestampAsUtc(raw: string): Date {
  const normalized = raw.trim();
  if (normalized === "") {
    return new Date(NaN);
  }

  const hasExplicitTimezone = /[zZ]|[+-]\d{2}:?\d{2}$/.test(normalized);
  const isoLike = normalized.replace(" ", "T");
  const utcCandidate = hasExplicitTimezone ? isoLike : `${isoLike}Z`;

  return new Date(utcCandidate);
}

function formatDatabricksTimestamp(value: Date): string {
  return value.toISOString().replace("T", " ").replace("Z", "");
}
