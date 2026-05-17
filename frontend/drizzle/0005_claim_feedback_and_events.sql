CREATE TABLE "claim_feedback" (
	"id" text PRIMARY KEY NOT NULL,
	"claim_id" text NOT NULL,
	"user_id" text NOT NULL,
	"rating" text NOT NULL,
	"reason" text,
	"comment" text DEFAULT '' NOT NULL,
	"created_at" timestamp NOT NULL,
	CONSTRAINT "claim_feedback_user_id_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE cascade ON UPDATE no action
);

CREATE TABLE "claim_events" (
	"id" text PRIMARY KEY NOT NULL,
	"claim_id" text NOT NULL,
	"event_type" text NOT NULL,
	"actor_user_id" text,
	"actor_email" text,
	"metadata" jsonb,
	"created_at" timestamp NOT NULL,
	CONSTRAINT "claim_events_actor_user_id_user_id_fk" FOREIGN KEY ("actor_user_id") REFERENCES "user"("id") ON DELETE no action ON UPDATE no action
);
