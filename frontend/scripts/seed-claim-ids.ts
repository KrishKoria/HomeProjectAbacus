import { databricksFetch } from "../src/lib/databricks/client";
import postgres from "postgres";

interface SqlStatementResponse {
  status: { state: string };
  manifest?: { columns: Array<{ name: string }> };
  result?: { data_array: Array<Array<unknown>> };
}

async function main() {
  const dbUrl = process.env.DATABASE_URL;
  const warehouseId = process.env.DATABRICKS_SQL_WAREHOUSE_ID;
  const featureTable = process.env.CLAIMOPS_FEATURE_TABLE;

  if (!dbUrl) {
    console.error("DATABASE_URL is not set");
    process.exit(1);
  }
  if (!warehouseId) {
    console.error("DATABRICKS_SQL_WAREHOUSE_ID is not set");
    process.exit(1);
  }
  if (!featureTable) {
    console.error("CLAIMOPS_FEATURE_TABLE is not set");
    process.exit(1);
  }

  console.log(`Querying ${featureTable} for DISTINCT claim_id...`);
  const query = `SELECT DISTINCT claim_id FROM ${featureTable}`;

  const result = await databricksFetch<SqlStatementResponse>(
    "/api/2.0/sql/statements",
    {
      method: "POST",
      body: JSON.stringify({
        statement: query,
        warehouse_id: warehouseId,
        disposition: "INLINE",
        max_wait_seconds: 60,
      }),
      timeout: 90_000,
    },
  );

  if (!result.ok) {
    console.error("Databricks SQL failed:", result.message);
    process.exit(1);
  }

  const rows = result.data.result?.data_array ?? [];
  const claimIds = rows.map((r) => String(r[0]));
  console.log(`Found ${claimIds.length} claim IDs`);

  const sql = postgres(dbUrl);

  let inserted = 0;
  let skipped = 0;

  for (const claimId of claimIds) {
    try {
      const res = await sql`
        INSERT INTO claim_reviews (id, claim_id, risk_score, risk_level, narrative, status, analyzed_at)
        VALUES (${`cr_${claimId}`}, ${claimId}, NULL, NULL, '', 'new', NULL)
        ON CONFLICT (claim_id) DO NOTHING
        RETURNING id
      `;
      if (res.length > 0) {
        inserted++;
      } else {
        skipped++;
      }
    } catch (err) {
      console.error(`Failed to insert ${claimId}:`, err);
    }
  }

  console.log(`Done. Inserted: ${inserted}, Skipped (already exists): ${skipped}`);
  await sql.end();
}

main().catch((err) => {
  console.error("Seed failed:", err);
  process.exit(1);
});
