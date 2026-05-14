# ADR 0003: Cloud Run + Cloud SQL Runtime for ClaimOps Frontend

**Status**: Accepted  
**Date**: 2026-05-13  
**Supersedes**: ADR 0002 (Neon PostgreSQL runtime choice)

---

## Context

The frontend/BFF is now deployed as a GCP-native runtime while Databricks remains the ETL/ML/RAG backend on GCP. The prior runtime decision used Neon PostgreSQL for Better Auth and `claim_reviews` persistence.

For production-grade GCP deployment, we need:
- a first-party runtime target in the same cloud footprint,
- explicit IAM boundaries,
- predictable deploy/rollback operations through Cloud Build and Cloud Run,
- and a database integration pattern aligned with Cloud Run operational primitives.

## Decision

Adopt **Cloud Run + Cloud SQL (PostgreSQL)** for the frontend runtime:

1. Deploy Next.js standalone container on Cloud Run.
2. Persist Better Auth and `claim_reviews` data in Cloud SQL PostgreSQL.
3. Use Secret Manager for all sensitive credentials.
4. Keep Databricks OAuth M2M credentials server-side only in Cloud Run.
5. Keep external API route contracts unchanged.

## Rationale

- Keeps runtime, IAM, and networking controls in one cloud provider.
- Simplifies operational story for deployment, scaling, and rollback.
- Preserves existing app architecture: Next.js BFF calls Databricks SQL and serving endpoints server-side.
- Avoids rework in ML/RAG codepaths by leaving model/feature pipelines in Databricks.

## Consequences

- Env contract now supports two DB modes:
  - local/dev: `DATABASE_URL`
  - Cloud Run production: `CLOUD_SQL_CONNECTION_NAME` + `DB_USER` + `DB_PASSWORD` + `DB_NAME`
- Runtime service account must have:
  - `roles/cloudsql.client`
  - `roles/secretmanager.secretAccessor`
- Databricks runtime principal must be granted least privilege:
  - SQL warehouse `CAN_USE`
  - UC read on feature table
  - serving endpoint `CAN_QUERY`
- Phase-2 hardening (IAP/private ingress/PSC) remains explicitly deferred and non-blocking for first production-grade demo deploy.
