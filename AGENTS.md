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

# [homeprojectabacus] recent context, 2026-05-18 1:28am GMT+5:30

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (19,879t read) | 789,361t work | 97% savings

### May 13, 2026
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
1104 11:08p ✅ Consolidated GCP Cloud Build migrations step and optimized secret handling
### May 16, 2026
1119 4:00p 🔵 Explored ETL data landing and storage architecture for upload feature planning
S290 Verify prerequisites and deployment requirements for upload feature before Cloud Build frontend deploy (May 16, 5:58 PM)
S291 Debug file arrival trigger failure in ETL pipeline — datasets uploaded via frontend should automatically trigger ETL file arrival job, but trigger is not firing (May 16, 6:02 PM)
S292 Debug why file arrival trigger isn't automatically triggering ETL job when datasets are uploaded via frontend — identified trigger timing behavior and delays (May 16, 6:10 PM)
S293 Continue implementation of five validated backend/frontend improvements to homeprojectabacus healthcare claims platform. Items cover: (1) Cloud SQL connection pool limits, (2) Query validation enums, (3) GCS metadata fields, (4) Upload status messaging, (5) Server-side CSV validation. (May 16, 6:11 PM)
### May 17, 2026
1120 11:48a 🔵 Five proposed backend and frontend improvements all map to real code patterns and unaddressed risks
S294 Deliver implementation guidance for five validated backend/frontend improvements to homeprojectabacus healthcare claims platform covering database pooling, query validation, GCS metadata tracking, upload status UX, and server-side CSV validation. (May 17, 11:51 AM)
S295 Evaluate and implement five frontend hardening improvements: Cloud SQL connection pooling, claim-list query validation with enums, GCS metadata tracking, recent uploads/ingestion status UI, and CSV header validation before upload signing. (May 17, 11:54 AM)
1121 11:59a 🟣 Cloud SQL Connection Pooling Configuration
1122 " 🟣 Claims Query Validation with Enum Constraints
1123 " 🟣 Server-Side CSV Header Validation
1124 " 🟣 GCS Metadata Fields for Upload Traceability
1125 " 🟣 Upload Completion Metadata Verification
1126 " 🟣 Enhanced Upload Status UI with Ingestion Timing Guidance
1127 " 🟣 Comprehensive Test Coverage for New Validations
S296 Evaluate 10 proposed feature additions to healthcare claims analysis project (remediation checklist, review report generation, upload dashboard, CSV preview, ingestion results, work queue, feedback loop, policy citations, audit timeline, advanced feature view) and determine worthiness with detailed implementation plan (May 17, 12:09 PM)
1128 12:19p 🔵 Homeprojectabacus: Existing infrastructure and completed phases mapped from project memory
S297 Implementation of 10 feature additions to healthcare claims analysis platform (homeprojectabacus) with focus on finalizing code quality, fixing linting/type errors, and ensuring all tests pass (May 17, 12:29 PM)
1129 12:31p 🔵 Frontend test suite times out after 120+ seconds
1130 12:33p 🔵 Claim detail page structure and components mapped
1131 " 🔵 Test suite structure and patterns identified for claims, uploads, and pages
1132 12:35p 🔵 Database schema and helper functions architecture mapped for claims and uploads
1133 12:41p 🟣 Upload/ingestion status dashboard implemented
S298 Create comprehensive architecture diagram of ClaimOps project using imagegen skill, covering frontend, Databricks ETL, ML/RAG, and all system components (May 17, 12:59 PM)
### May 18, 2026
S299 Create a comprehensive frontend architecture diagram for ClaimOps by exploring the Next.js codebase in depth, understanding how all components interact, and generating a visual diagram using imagegen skill (May 18, 1:12 AM)
**Investigated**: Conducted exhaustive frontend codebase scan including: file structure (pages, API routes, components, libs), design context (PRODUCT.md, DESIGN.md, CONTEXT.md, frontend/CLAUDE.md), auth system (Better Auth + Google OAuth), database schema (Drizzle ORM with PostgreSQL/Neon), data fetching (React Query), styling system (Tailwind v4 + shadcn/ui), Databricks integration (OAuth, SQL warehouse, ML serving endpoints), upload system (GCS + signed policies), API routes (claims analysis, chat, sync, status), React components (AppShell, claims table, chat panel, risk score display, uploads), and deployment infrastructure (Dockerfile, Cloud Build config)

**Learned**: ClaimOps frontend is a clinical diagnostic console: Next.js 15 + React 19 + TypeScript architecture using split-panel layout (62%/38%) for claim details with always-visible chat at ≥1024px. Data flows: PostgreSQL (auth/sessions via Better Auth + Drizzle) ↔ React Query (client state) ↔ API routes (Node.js) ↔ Databricks (ML analysis, SQL queries, LLM chat). Risk-sorted claims queue with filtering (High/Medium/Low, New/Reviewed/Actioned). Per-claim chat uses Databricks Llama 70B with claim context. Upload flow: client → GCS signed policy → server verification → Databricks Volume. Design: flat surfaces, semantic colors only (risk levels + status), typography-forward hierarchy, sharp corners, light mode, no decorative elements. Deployed via Cloud Run with Cloud SQL Proxy, secrets management via Google Secret Manager

**Completed**: Full codebase exploration complete: 50+ files read spanning pages, API routes, components, libs, configs, and deployment infrastructure. Design/product context loaded and analyzed. Architecture mentally modeled across auth, data access, state management, UI layers, and external integrations

**Next Steps**: Generate frontend architecture diagram using imagegen skill that visualizes: (1) Browser UI layer with pages and components; (2) React state/data fetching (React Query + hooks); (3) API routes as middleware layer; (4) Server libs (Databricks client, SQL, auth, uploads); (5) External services (PostgreSQL, Databricks, GCS); (6) Data flow arrows between layers showing claim analysis, chat, uploads, sync workflows. Diagram should be 16:9 infographic, legible, showing component groups and key interactions without overwhelming detail


Access 789k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>