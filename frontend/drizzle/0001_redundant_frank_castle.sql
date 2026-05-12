CREATE TABLE "claim_reviews" (
	"id" text PRIMARY KEY NOT NULL,
	"claim_id" text NOT NULL,
	"risk_score" real,
	"risk_level" text,
	"narrative" text DEFAULT '',
	"status" text DEFAULT 'new' NOT NULL,
	"analyzed_at" timestamp,
	"reviewed_at" timestamp,
	"reviewed_by_id" text,
	CONSTRAINT "claim_reviews_claim_id_unique" UNIQUE("claim_id")
);
--> statement-breakpoint
ALTER TABLE "claim_reviews" ADD CONSTRAINT "claim_reviews_reviewed_by_id_user_id_fk" FOREIGN KEY ("reviewed_by_id") REFERENCES "public"."user"("id") ON DELETE no action ON UPDATE no action;