# Deployment Guide

## Target Topology

- Frontend/BFF runtime: **Cloud Run**
- App database: **Cloud SQL for PostgreSQL**
- Secrets: **Secret Manager**
- Build/deploy: **Cloud Build + Artifact Registry**
- Data/ML/RAG backend: **Databricks on GCP**

## 1) Prerequisites

1. Enable APIs:
   - `run.googleapis.com`
   - `cloudbuild.googleapis.com`
   - `artifactregistry.googleapis.com`
   - `secretmanager.googleapis.com`
   - `sqladmin.googleapis.com`
2. Create Artifact Registry Docker repo.
3. Create Cloud SQL PostgreSQL instance and database/user.
4. Create Cloud Run runtime service account and grant:
   - `roles/secretmanager.secretAccessor`
   - `roles/cloudsql.client`
5. Create Secret Manager secrets for:
   - `BETTER_AUTH_SECRET`
   - `GOOGLE_CLIENT_ID`
   - `GOOGLE_CLIENT_SECRET`
   - `DB_PASSWORD`
   - `DATABRICKS_CLIENT_ID`
   - `DATABRICKS_CLIENT_SECRET`
   - `CLAIMOPS_ALLOWED_EMAIL_DOMAINS`
   - `CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS`

## 2) Environment Contract

See [frontend/.env.example](/C:/Users/Krish/Desktop/projects/homeprojectabacus/frontend/.env.example).

Runtime supports two DB modes:
- **Local/dev mode**: `DATABASE_URL`
- **Cloud Run mode**: `CLOUD_SQL_CONNECTION_NAME` + `DB_USER` + `DB_PASSWORD` + `DB_NAME` (+ optional `DB_PORT`)

## 3) Build and Deploy

Use [cloudbuild.yaml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/cloudbuild.yaml).

```bash
gcloud builds submit --config cloudbuild.yaml --region us-central1
```

The pipeline will:
1. Build `frontend/Dockerfile`
2. Push image to Artifact Registry
3. Deploy Cloud Run revision with:
   - Cloud SQL instance attachment
   - `--set-env-vars` for non-sensitive config
   - `--set-secrets` for sensitive config

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
bun test
bun run build

# Python/serving checks
cd ..
uv run pytest -q tests/test_claim_analysis_serving.py tests/test_frontend_contract_generation.py
```

Operational checks:
1. `GET /api/runtime/status` returns healthy Databricks dependencies.
2. Sign in flow succeeds.
3. Claim analysis API completes and persists status to Cloud SQL.

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
