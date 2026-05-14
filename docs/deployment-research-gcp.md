# GCP Deployment Research Gate

**Date:** 2026-05-13  
**Status:** Completed (blocking gate satisfied before implementation edits)

## Objective

Validate current official guidance for deploying the existing Next.js BFF app to Google Cloud Run with Cloud SQL, while keeping Databricks as the data/ML/RAG backend.

## Decision Matrix

| Topic | Options Evaluated | Recommendation | Rejected Option(s) | Why Rejected |
|---|---|---|---|---|
| Next.js runtime packaging | `next start`, standalone output | **Standalone output + `node .next/standalone/server.js`** | `next start` | Next.js standalone is the supported minimal runtime output for container deployments. |
| Cloud Run secrets | Plain env vars, Secret Manager integration | **Secret Manager via `--set-secrets`** | Plain env for secrets | Plain env values are visible in service config and operational metadata. |
| Cloud Run ↔ Cloud SQL | Public IP string only, Cloud SQL unix socket attachment | **Cloud SQL instance attachment + unix socket path `/cloudsql/<instance>`** | Public-IP-only credentials path | Socket path avoids direct host exposure and matches Cloud Run Cloud SQL integration pattern. |
| Public ingress pattern | Public Cloud Run URL, external HTTPS LB + Cloud Armor, private-only ingress | **Public HTTPS now, custom domain + Cloud Armor after smoke deploy** | Private-only for phase 1 | Private-only slows first production-demo delivery and conflicts with immediate public OAuth callback needs. |
| Databricks auth mode | PAT, OAuth M2M service principal | **OAuth M2M service principal** | PAT | OAuth M2M is the preferred/modern machine auth path in Databricks docs. |
| Databricks serving permissions | broad admin, query-only runtime identity | **Least privilege runtime identity (`CAN_QUERY`, SQL warehouse use, UC table read)** | Broad admin grants | Avoid unnecessary blast radius and preserve production-grade IAM posture. |

## Locked Implementation Decisions

1. **Database connection mode**
   - Runtime uses Cloud SQL unix socket (`/cloudsql/<project>:<region>:<instance>`).
   - Local/dev can continue to use `DATABASE_URL`.
2. **Service account split**
   - One Google Cloud runtime service account for Cloud Run (`secretAccessor`, `cloudsql.client`).
   - Separate Databricks service principal for BFF API access.
3. **Ingress pattern**
   - Phase 1: public HTTPS + app authentication.
   - Phase 2 hardening: IAP/internal ingress/PSC tracked but not blocking.
4. **Rollout / rollback workflow**
   - Deploy through Cloud Build -> Cloud Run revision.
   - Rollback via Cloud Run traffic update to previous revision.
5. **Cost and scale guardrails**
   - Set explicit Cloud Run min/max instance limits and concurrency.
   - Keep Cloud SQL sizing conservative for first deploy, then tune with load metrics.

## Risk Log

| Risk | Impact | Mitigation |
|---|---|---|
| DB connection storms from autoscaling | Cloud SQL saturation, request failures | Start with bounded Cloud Run max instances + sane concurrency + app-side pool limits. |
| Secret sprawl / accidental exposure | Credential leakage risk | Move all sensitive values to Secret Manager; restrict IAM to runtime SA only. |
| OAuth callback mismatch after domain change | Login outage | Keep local + prod callback URLs documented and update IdP before cutover. |
| Databricks runtime SP over-privilege | Lateral access risk | Grant only warehouse use, UC read on feature table, serving endpoint query. |
| Long cold-start path (warehouse wake + serving) | Latency spikes on first requests | Use `/api/runtime/status` preflight and operational warmup checks before demo windows. |

## Primary Sources (Official)

- Next.js deployment and standalone output:
  - <https://nextjs.org/docs/app/getting-started/deploying>
  - <https://nextjs.org/docs/app/api-reference/config/next-config-js/output>
- Cloud Build -> Cloud Run deployment flow:
  - <https://cloud.google.com/build/docs/deploying-builds/deploy-cloud-run>
- Cloud Run secret configuration:
  - <https://cloud.google.com/run/docs/configuring/services/secrets>
- Cloud Run environment variables:
  - <https://cloud.google.com/run/docs/configuring/services/environment-variables>
- Cloud Run deploy CLI reference:
  - <https://cloud.google.com/sdk/gcloud/reference/run/deploy>
- Cloud Run rollback/traffic management:
  - <https://cloud.google.com/run/docs/rollouts-rollbacks-traffic-migration>
- Cloud SQL from Cloud Run:
  - <https://cloud.google.com/sql/docs/postgres/connect-run>
- Cloud Armor with serverless NEGs / external HTTPS LB:
  - <https://cloud.google.com/armor/docs/integrating-cloud-armor>
  - <https://cloud.google.com/load-balancing/docs/negs/serverless-neg-concepts>
- Databricks OAuth M2M:
  - <https://docs.databricks.com/gcp/en/dev-tools/auth/oauth-m2m.html>
- Databricks model serving permissions (`CAN_QUERY`):
  - <https://docs.databricks.com/gcp/en/machine-learning/model-serving/manage-serving-endpoints>
- Databricks SQL warehouse access control:
  - <https://docs.databricks.com/en/compute/sql-warehouse/index.html>
