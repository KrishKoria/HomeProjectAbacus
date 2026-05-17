This file provides guidance to Claude Code when working with code in this repository.

## Deferred decisions

[`docs/deferred.md`](docs/deferred.md) records all deliberately-not-implemented features (`claim_type`, `ServiceVerifier` Protocol, `ops_service_*` tables, `verify_gold.py`, ETL constant parameterization, `build_analytics` CLI args, DAB `model_version` lookup variable). Read it before proposing any of those; each entry names the trigger that justifies reviving it.

## Project overview

AI-Powered Claim Denial Prevention & Remediation System — Databricks + Spark + Delta Lake + MLflow + Next.js.

**Medallion:** Bronze (raw ingest) → Silver (trusted) → Gold (`healthcare.gold.claim_features`, 13 features) → ML (`healthcare.ml.claim_denial_model@champion`).

**Release gate:** Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85. Gate-failing models are not pickled or registered.

## Commands

```bash
# Install all deps (core + dev)
uv sync
uv sync --group dev

# Run tests
uv run pytest -q                          # all tests
uv run pytest -q tests/test_ml_contract.py  # single file
uv run pytest -q -k "test_name"             # single test

# Databricks bundle
databricks bundle validate -t dev --profile dev
databricks bundle deploy -t dev --profile dev
databricks bundle run -t dev --profile dev <job_key>

# Run Next.js frontend locally
cd frontend && bun run dev
```

## Architecture

### Dual package layout

Two installable packages via hatchling (`pyproject.toml` → `[tool.hatch.build.targets.wheel] packages = ["src", "ETL/common"]`):

| Package       | Purpose                                                                                     |
| ------------- | ------------------------------------------------------------------------------------------- |
| `src/`        | All business logic — ML, RAG, XAI, analytics, scripts, common helpers, framework            |
| `ETL/common/` | One-line proxies that re-export from `src.common.*` for Databricks ETL import compatibility |

Every `ETL/common/<module>.py` is exactly: `from src.common.<module> import *  # noqa: F401,F403`

### Source tree

```
src/
  analytics/     — claims analytics and observability assets
  common/        — Shared config, constants, PHI registry, log messages, diagnostics
  framework/     — Service verifier, manifest validation (HealthCheckResult)
  ml/            — Train, evaluate, predict, features, retrain gate
  rag/           — Embeddings, retriever, synthesizer, vector search, policy labels
  scripts/       — Databricks spark_python_task entry points (10 scripts)
  xai/           — SHAP explainer, feature reasons
ETL/
  common/        — Re-export proxies for src.common.*
  pipelines/     — Bronze/Silver/Gold Delta Lake ETL transforms
resources/
  schemas/       — Unity Catalog schema declarations
  volumes/       — UC volume declarations
services/        — Databricks bundle job definitions, grouped by domain
  etl/resources/        — analytics_observability, etl_file_arrival, etl_fast_dev
  ml/training/resources/  — training.job.yml
  rag/vector_index/resources/ — vector_index.job.yml
  infrastructure/setup/resources/ — setup_infrastructure.job.yml
```

### Databricks bundle

Bundle name: `healthcare-claim-ops`. Engine: `direct`. Targets: `dev` (default), `prod`.

All jobs use `spark_python_task` with `--editable ${workspace.file_path}` dependency. The `src/` package is deployed as workspace files but is NOT importable via editable install in serverless runtime — hence the sys.path bootstrap pattern.

Every `src/scripts/*.py` entry point **must** include this bootstrap before any `from src.*` imports:

```python
from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
_PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
```

## Hard rules

- Every module starts with `from __future__ import annotations`.
- Module constants typed `Final[...]`.
- `__all__` alphabetically sorted.
- ETL/common files are one-line proxies only.
- Logs never interpolate PHI — use `MESSAGE_TEMPLATE_*` + `render_*` helpers; reference identifiers (claim_id, provider_id) only.
- Bare `except Exception` must call `logger.warning(..., exc_info=True)` — never silent swallow.


<claude-mem-context>
# Memory Context

# [homeprojectabacus] recent context, 2026-05-17 11:55am GMT+5:30

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (19,184t read) | 605,067t work | 97% savings

### May 13, 2026
1106 5:47p 🔴 Add unzip dependency to Cloud Build migration step
1107 6:05p 🔵 Better Auth state validation failures blocking OAuth callback
1108 " 🔵 Cloud SQL proxy attached but service references multiple instances
1109 6:06p 🔵 No recent build history found or migration logs unavailable
1110 " 🔵 Database schema includes Better Auth tables but deployment migration status unknown
1111 6:07p 🔵 Better Auth route handler uses default Next.js integration without database initialization
1112 6:08p 🔵 OAuth callback state validation failure traced in HTTP request logs
1113 6:11p 🔵 Better Auth OAuth State Validation Issues in Cloud Run Production
1114 6:12p 🔵 Better Auth Issue #7023 State Mismatch Fix for Next.js v1.4.4+
1115 " 🔵 Better Auth Configuration Missing Production Deployment Settings
1116 6:13p 🔵 Better Auth Production Cookie and OAuth State Configuration Requirements
1117 6:16p ✅ Better Auth Configuration Updated for Production OAuth State Validation
1118 6:51p 🔵 Current Cloud Build pipeline issues and GCP infrastructure state analyzed
1070 9:43p ⚖️ Deployment Architecture Migrated from Vercel to GCP Cloud Run
1071 " 🟣 GCP Cloud Run Deployment Guide Documented
1072 " ✅ Environment and Database Configuration Updated for GCP Cloud Run
1073 " ✅ Next.js Configuration Updated for Container Deployment
1074 " ✅ Project Documentation Aligned to GCP/ClaimOps Identity
1075 " ✅ Architecture Documentation Migrated from Vercel to GCP Cloud Run Model
1076 " ✅ Deployment Handbook Extended with GCP Cloud Run and Phase-2 Hardening Sections
1077 9:45p 🟣 Cloud Build Pipeline Configured for GCP Cloud Run Deployment
1078 " 🟣 Multi-Stage Dockerfile for Next.js Standalone Container
1079 " 🟣 Database Connection Layer Refactored for Dual-Mode Operation
1080 " ✅ Environment Schema Refactored to Support Cloud SQL Configuration
1081 " 🟣 Better Auth Configuration Extended with Trusted Origins
1082 " ⚖️ ADR 0003 Accepted: Cloud Run + Cloud SQL Runtime Architecture
1083 " 🔐 Secret Values Replaced with Secret Manager Reference Names in cloudbuild.yaml
1084 9:55p 🔵 IAM Policy Constraints Require Conditions for New Bindings
1085 9:58p 🔵 homeprojectabacus Missing .gcloudignore Configuration
1086 " 🔵 homeprojectabacus .gitignore Excludes Claude Code and Development Tools
1087 " ✅ .gcloudignore Configuration Created for Cloud Build
1088 10:03p 🔵 Next.js Docker build fails due to missing app/pages directory
1089 " ✅ Added root-level path prefixes to gcloudignore exclusions
1090 10:07p 🔵 Artifact Registry repository located in asia-south1
1091 10:15p ✅ homeprojectabacus frontend deployed to GCP artifact registry
1092 10:21p 🔵 Project Stack Assessment for GCP Deployment
1093 " 🔵 GCP Cloud Build Deployment Pipeline Configuration
1094 10:23p ✅ Added Database Migration Step to Cloud Build Pipeline
1095 10:41p 🔵 Cloud Build Migration Step Fails Due to Secret Manager IAM Permission Denial
1096 10:48p 🔵 Root Cause Analysis: db-password Secret Likely Missing from Google Secret Manager
1097 10:49p 🔵 Cloud Build Secret Resolution: Substitutions Supported in availableSecrets with dynamic_substitutions
1098 " 🔵 db-password Secret Exists in Secret Manager; Root Cause is Secret-Level Permissions
1099 10:50p ✅ Hardcode Secret Name in cloudbuild.yaml to Eliminate Substitution Variable
1100 10:54p 🔵 IAM Permission Denied for Secret Manager Access in Cloud Build
1101 11:00p 🔵 Cloud Build Secret Injection Root Cause: Service Agent Permission Gap
1102 " ⚖️ Cloud Build Secret Handling: Two-Step Approach with Direct gcloud Retrieval
1103 11:01p ✅ Cloud Build Pipeline Refactored for Direct Secret Retrieval
S278 Deploy GCP project after git changes - research and provide step-by-step deployment guide for Cloud Build pipeline (May 13, 11:01 PM)
1104 11:08p ✅ Consolidated GCP Cloud Build migrations step and optimized secret handling
S285 User asked whether IAM changes would take time to propagate to Databricks and requested troubleshooting guidance for credential validation delays (May 13, 11:08 PM)
### May 16, 2026
1119 4:00p 🔵 Explored ETL data landing and storage architecture for upload feature planning
S286 Operational questions on ETL file arrival job: Can the paused schedule/trigger for /Volumes/healthcare/bronze/raw_landing/ be unpaused now? Will Databricks deploy automatically create the volume once the external location exists? (May 16, 5:41 PM)
S287 Confirmed readiness to unpause ETL file-arrival job trigger after verifying external volume configuration and accessibility (May 16, 5:45 PM)
S288 Unpause Databricks file-arrival trigger and verify frontend upload paths align with ETL expected directory structure (May 16, 5:49 PM)
S289 Clarify dataset upload routing behavior: does system auto-detect file content type or rely on user-selected dataset? (May 16, 5:56 PM)
S290 Verify prerequisites and deployment requirements for upload feature before Cloud Build frontend deploy (May 16, 5:58 PM)
S291 Debug file arrival trigger failure in ETL pipeline — datasets uploaded via frontend should automatically trigger ETL file arrival job, but trigger is not firing (May 16, 6:02 PM)
S292 Debug why file arrival trigger isn't automatically triggering ETL job when datasets are uploaded via frontend — identified trigger timing behavior and delays (May 16, 6:10 PM)
### May 17, 2026
1120 11:48a 🔵 Five proposed backend and frontend improvements all map to real code patterns and unaddressed risks
S293 Continue implementation of five validated backend/frontend improvements to homeprojectabacus healthcare claims platform. Items cover: (1) Cloud SQL connection pool limits, (2) Query validation enums, (3) GCS metadata fields, (4) Upload status messaging, (5) Server-side CSV validation. (May 17, 11:51 AM)
**Investigated**: - Previous session validation notes confirming all 5 items are production-grade improvements with specific code locations
    - Parallel documentation queries for postgres.js connection pooling, Google Cloud Storage metadata APIs, Next.js App Router patterns, and project configuration (shadcn/ui, Tailwind v4, environment schema)
    - Examined test files (runtime-env.test.ts, claims-page.test.tsx, claims-routes.test.ts, uploads-routes.test.ts) to understand mocking and testing patterns
    - Reviewed database schema (ingestion_uploads table structure with gcs_generation field already present)
    - Confirmed shadcn/ui project configuration: base library, phosphor icons, "base-lyra" style, Tailwind v4, Next.js 16.2.6 with RSC enabled

**Learned**: - Postgres.js supports pool configuration via options object: max, idle_timeout, connect_timeout (all in seconds)
    - Cloud Run deployment uses --max-instances=6 and --concurrency=40, requiring conservative per-instance pool sizing (~15 max)
    - GCS signed POST policies can enforce custom metadata fields via x-goog-meta-* with conditions matching form fields
    - Upload status component already exists in /data-upload page; primary work is enhancement (delay messaging, metadata columns) not creation
    - CSV validation is currently client-only; server validation must check headers before signing uploads to prevent tampering
    - Project environment validates database config at runtime with superRefine rules

**Completed**: - Created 5 structured implementation tasks in task management system (all marked completed after implementation guidance provided)
    - Delivered complete, production-grade code implementations for all 5 items with:
      * File locations and line numbers
      * Exact code changes with rationale
      * Integration points with existing codebase
      * Error handling and fallback patterns
    - Task #1: Pool configuration with max=15, idle_timeout=30s, connect_timeout=10s
    - Task #2: Query validation via z.enum() for risk/status with .catch("all") fallback
    - Task #3: GCS metadata policy with x-goog-meta-* fields, fields/conditions, and getMetadata verification
    - Task #4: Upload status messaging component with elapsed time and Databricks latency explanation (6-minute window)
    - Task #5: Server-side CSV header validation before signing, with 400 error listing missing columns

**Next Steps**: - Background system has active queries for detailed technical documentation and project inspection
    - Sequential thinking phase analyzed each improvement item and confirmed viability
    - Anticipated next: Code file modifications, test implementation, and build/type verification
    - Potential secondary review: Database connection testing under load, metadata round-trip verification, upload flow E2E testing


Access 605k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>