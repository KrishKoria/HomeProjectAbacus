CREATE TABLE "ingestion_uploads" (
	"id" text PRIMARY KEY NOT NULL,
	"dataset_key" text NOT NULL,
	"object_name" text NOT NULL,
	"volume_path" text NOT NULL,
	"content_type" text NOT NULL,
	"byte_size" integer NOT NULL,
	"status" text DEFAULT 'initiated' NOT NULL,
	"uploaded_by_id" text NOT NULL,
	"uploaded_by_email" text NOT NULL,
	"created_at" timestamp NOT NULL,
	"completed_at" timestamp,
	"gcs_generation" text,
	"error_message" text,
	CONSTRAINT "ingestion_uploads_uploaded_by_id_user_id_fk" FOREIGN KEY ("uploaded_by_id") REFERENCES "user"("id") ON DELETE no action ON UPDATE no action
);
