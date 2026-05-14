CREATE TABLE "claim_sync_state" (
	"source_table" text PRIMARY KEY NOT NULL,
	"last_ingested_at" timestamp,
	"last_claim_id" text,
	"last_synced_at" timestamp NOT NULL,
	"last_discovered_count" integer DEFAULT 0 NOT NULL,
	"last_inserted_count" integer DEFAULT 0 NOT NULL
);
