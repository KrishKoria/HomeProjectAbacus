import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import postgres from "postgres";

type JournalEntry = {
  idx: number;
  tag: string;
  when: number;
};

type Journal = {
  entries: JournalEntry[];
};

const MIGRATIONS_FOLDER = "./drizzle";
const EXPECTED_COLUMNS = new Map<string, string[]>([
  [
    "account",
    [
      "id",
      "user_id",
      "account_id",
      "provider_id",
      "access_token",
      "refresh_token",
      "access_token_expires_at",
      "refresh_token_expires_at",
      "scope",
      "id_token",
      "password",
      "created_at",
      "updated_at",
    ],
  ],
  [
    "claim_events",
    [
      "id",
      "claim_id",
      "event_type",
      "actor_user_id",
      "actor_email",
      "metadata",
      "created_at",
    ],
  ],
  [
    "claim_feedback",
    ["id", "claim_id", "user_id", "rating", "reason", "comment", "created_at"],
  ],
  [
    "claim_reviews",
    [
      "id",
      "claim_id",
      "risk_score",
      "risk_level",
      "narrative",
      "status",
      "analyzed_at",
      "reviewed_at",
      "reviewed_by_id",
      "top_reason",
    ],
  ],
  [
    "claim_sync_state",
    [
      "source_table",
      "last_ingested_at",
      "last_claim_id",
      "last_synced_at",
      "last_discovered_count",
      "last_inserted_count",
    ],
  ],
  [
    "ingestion_uploads",
    [
      "id",
      "dataset_key",
      "object_name",
      "volume_path",
      "content_type",
      "byte_size",
      "status",
      "uploaded_by_id",
      "uploaded_by_email",
      "created_at",
      "completed_at",
      "gcs_generation",
      "error_message",
    ],
  ],
  [
    "session",
    [
      "id",
      "user_id",
      "token",
      "expires_at",
      "ip_address",
      "user_agent",
      "created_at",
      "updated_at",
    ],
  ],
  [
    "user",
    [
      "id",
      "name",
      "email",
      "email_verified",
      "image",
      "created_at",
      "updated_at",
      "role",
      "status",
    ],
  ],
  ["verification", ["id", "identifier", "value", "expires_at", "created_at", "updated_at"]],
]);

function readJournal(): Journal {
  return JSON.parse(
    readFileSync(join(MIGRATIONS_FOLDER, "meta", "_journal.json"), "utf8"),
  ) as Journal;
}

function readMigrationHash(tag: string): string {
  const sql = readFileSync(join(MIGRATIONS_FOLDER, `${tag}.sql`), "utf8");
  return createHash("sha256").update(sql).digest("hex");
}

async function main() {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for migration baselining.");
  }

  const client = postgres(databaseUrl, {
    connect_timeout: 10,
    max: 1,
    onnotice: () => {},
  });

  try {
    await client`CREATE SCHEMA IF NOT EXISTS drizzle`;
    await client`
      CREATE TABLE IF NOT EXISTS drizzle.__drizzle_migrations (
        id SERIAL PRIMARY KEY,
        hash text NOT NULL,
        created_at bigint
      )
    `;

    const journalRows = await client`
      SELECT count(*)::int AS count
      FROM drizzle.__drizzle_migrations
    `;
    if (journalRows[0].count > 0) {
      console.log("Drizzle migration journal already populated; skipping baseline.");
      return;
    }

    const columnRows = await client`
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
    `;

    const actualColumns = new Map<string, Set<string>>();
    for (const row of columnRows) {
      const tableName = String(row.table_name);
      const columnName = String(row.column_name);
      const tableColumns = actualColumns.get(tableName) ?? new Set<string>();
      tableColumns.add(columnName);
      actualColumns.set(tableName, tableColumns);
    }

    const existingExpectedTables = [...EXPECTED_COLUMNS.keys()].filter((table) =>
      actualColumns.has(table),
    );
    if (existingExpectedTables.length === 0) {
      console.log("No existing application tables found; normal migrations will run.");
      return;
    }

    const missingTables = [...EXPECTED_COLUMNS.keys()].filter(
      (table) => !actualColumns.has(table),
    );
    const missingColumns = [...EXPECTED_COLUMNS.entries()].flatMap(
      ([table, expectedColumns]) => {
        const tableColumns = actualColumns.get(table);
        if (!tableColumns) {
          return [];
        }
        return expectedColumns
          .filter((column) => !tableColumns.has(column))
          .map((column) => `${table}.${column}`);
      },
    );

    if (missingTables.length > 0 || missingColumns.length > 0) {
      throw new Error(
        [
          "Refusing to baseline a partially migrated database.",
          missingTables.length ? `Missing tables: ${missingTables.join(", ")}` : "",
          missingColumns.length ? `Missing columns: ${missingColumns.join(", ")}` : "",
        ]
          .filter(Boolean)
          .join(" "),
      );
    }

    const journal = readJournal();
    await client.begin(async (tx) => {
      for (const entry of journal.entries) {
        await tx`
          INSERT INTO drizzle.__drizzle_migrations (hash, created_at)
          VALUES (${readMigrationHash(entry.tag)}, ${entry.when})
        `;
      }
    });

    console.log(`Baselined ${journal.entries.length} Drizzle migrations.`);
  } finally {
    await client.end({ timeout: 5 });
  }
}

await main();
