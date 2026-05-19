# Deployment Guide

## Target Topology

- Frontend/BFF runtime: **Cloud Run**
- App database: **Neon PostgreSQL**
- Secrets: **Secret Manager**
- Build/deploy: **Cloud Build + Artifact Registry**
- Data/ML/RAG backend: **Databricks on GCP**

## 1) Prerequisites

1. Enable APIs:
   - `run.googleapis.com`
   - `cloudbuild.googleapis.com`
   - `artifactregistry.googleapis.com`
   - `secretmanager.googleapis.com`
2. Create Artifact Registry Docker repo.
3. Create Neon PostgreSQL database and record both connection strings:
   - pooled URL for Cloud Run runtime: `neon-database-url`
   - direct URL for Drizzle migrations: `neon-database-direct-url`
4. Create Cloud Run runtime service account and grant:
   - `roles/secretmanager.secretAccessor`
5. Create Secret Manager secrets for:
   - `BETTER_AUTH_SECRET`
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `DATABASE_URL` using secret `neon-database-url`
   - migration-only `DATABASE_URL` using secret `neon-database-direct-url`
   - `DATABRICKS_CLIENT_ID`
   - `DATABRICKS_CLIENT_SECRET`
   - `CLAIMOPS_ALLOWED_EMAIL_DOMAINS`
   - `CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS`
6. Grant the Cloud Build execution service account:
   - `roles/secretmanager.secretAccessor`
   - `roles/cloudbuild.builds.editor` if you want `cloudbuild.yaml` to launch `cloudbuild.migrations.yaml` as a child build

## 2) Environment Contract

See [frontend/.env.example](/C:/Users/Krish/Desktop/projects/homeprojectabacus/frontend/.env.example).

Runtime uses `DATABASE_URL`. Use the pooled Neon URL for Cloud Run and local app runtime. Use the direct Neon URL only for Drizzle migrations.

## 3) Build and Deploy

Use [cloudbuild.yaml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/cloudbuild.yaml) for normal frontend deploys.

```bash
gcloud builds submit --config cloudbuild.yaml --region asia-south1 --project monthhome
```

The pipeline will:
1. Build `frontend/Dockerfile`
2. Push image to Artifact Registry
3. Deploy Cloud Run revision with:
   - any old Cloud SQL attachment cleared
   - `--set-env-vars` for non-sensitive config
   - `--set-secrets` for sensitive config

Database migrations can still be run as a dedicated build when committed files under `frontend/drizzle/` change:

```bash
gcloud builds submit --config cloudbuild.migrations.yaml --region asia-south1 --project monthhome
```

The migration pipeline reads `neon-database-direct-url` from Secret Manager, runs the guarded Drizzle baseline helper for Neon cutovers that already have tables but lack `drizzle.__drizzle_migrations` rows, and then applies committed Drizzle migrations with `bunx drizzle-kit migrate`.

## 3.1) GCS-backed ETL Uploads

The `Data Upload` page signs browser uploads into the GCS prefix that backs the external Unity Catalog volume `healthcare.bronze.raw_landing`.

Required Cloud Run env vars:

| Variable | Purpose |
|---|---|
| `CLAIMOPS_APP_ORIGIN` | Public Cloud Run or custom-domain origin used in bucket CORS |
| `CLAIMOPS_GCS_LANDING_BUCKET` | Bucket that backs the external raw landing volume |
| `CLAIMOPS_GCS_LANDING_PREFIX` | Object prefix for uploaded ETL inputs |
| `CLAIMOPS_UPLOAD_CSV_MAX_BYTES` | Max CSV upload size |
| `CLAIMOPS_UPLOAD_PDF_MAX_BYTES` | Max policy PDF upload size |
| `CLAIMOPS_UPLOAD_SIGNED_POLICY_TTL_SECONDS` | Signed POST policy lifetime |

Before enabling uploads in production:

1. Convert or recreate `healthcare.bronze.raw_landing` as an external volume at `gs://homeprojectabacus-etl-landing-monthhome/claimops-raw-landing/`. Do not write to a managed volume's hidden `__unitystorage` path.
2. Configure bucket CORS for the app origin with `POST` and `OPTIONS`.
3. Grant the Cloud Run runtime service account object create/read/delete permissions scoped to the landing bucket or prefix, plus service-account signing permission required for V4 signed POST policies.
4. Run database migrations so the `ingestion_uploads` audit table exists.
5. Validate Databricks can read `/Volumes/healthcare/bronze/raw_landing/`, then deploy the bundle.

If you want the main deploy to launch the migration build first, set `_RUN_DB_MIGRATIONS=true` on the main build:

```bash
gcloud builds submit --config cloudbuild.yaml --region asia-south1 --project monthhome --substitutions=_RUN_DB_MIGRATIONS=true
```

Cloud Build does not provide a native `include` or `import` feature for one build config to embed another. The `_RUN_DB_MIGRATIONS=true` path works by starting a nested child build with `gcloud builds submit --config cloudbuild.migrations.yaml .`, which resubmits the current workspace as build source before the Cloud Run deploy step is allowed to continue.

## 4) OAuth Callback Configuration

Set Google OAuth redirect URIs:

| Environment | URL |
|---|---|
| Local dev | `http://localhost:3000/api/auth/callback/google` |
| Cloud Run / Custom domain | `https://<your-domain>/api/auth/callback/google` |

## 5) Databricks Runtime Identity

Create a Databricks service principal for the Cloud Run BFF and grant least privilege:
- SQL warehouse: `CAN_USE`
- Feature table: `SELECT` on `healthcare.gold.claim_features` (+ `USE CATALOG`, `USE SCHEMA`)
- Serving endpoints: `CAN_QUERY` on claim analysis endpoint and chat model endpoint

Use OAuth M2M credentials for:
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`

## 6) Verification

```bash
# Frontend runtime checks
cd frontend
bun run typecheck
bun run test
bun run build

# Python/serving checks
cd ..
uv run pytest -q tests/test_claim_analysis_serving.py tests/test_frontend_contract_generation.py
```

Operational checks:
1. `GET /api/runtime/status` returns healthy Databricks dependencies.
2. Sign in flow succeeds.
3. Claim analysis API completes and persists status to Neon.

## 7) Rollback

List revisions:

```bash
gcloud run revisions list --service <service> --region <region>
```

Route all traffic back to previous stable revision:

```bash
gcloud run services update-traffic <service> --region <region> --to-revisions <revision>=100
```

Restore normal latest-revision behavior:

```bash
gcloud run services update-traffic <service> --region <region> --to-latest
```

## 8) Phase-2 Hardening (Not Blocking First Deploy)

- External HTTPS Load Balancer + Cloud Armor policy
- IAP fronting pattern (if app-level auth policy changes)
- Private networking / PSC-based reachability refinements
