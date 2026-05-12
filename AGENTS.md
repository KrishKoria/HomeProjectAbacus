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

# [homeprojectabacus] recent context, 2026-05-12 3:55pm GMT+5:30

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (15,895t read) | 625,789t work | 97% savings

### May 12, 2026
976 12:46p 🟣 Claim analysis API now persists results to database
977 12:47p 🟣 GET /api/claims endpoint created to list all claim reviews
978 12:48p 🟣 PATCH /api/claims/[claimId]/status endpoint for workflow transitions
979 " 🟣 POST /api/claims/[claimId]/chat endpoint for AI claim assistant
980 12:49p 🔄 AppShell component refactored with breadcrumb support and header polish
981 " 🔄 AppSidebar redesigned with Claims nav, session display, and Phosphor icons
982 " 🟣 Claims list page built with filtering, sorting, and all state coverage
983 12:50p 🔄 Claim detail page redesigned with two-column layout and AI chat panel
984 12:54p 🔄 Dashboard page redesigned with queue overview stats grid
985 12:55p ✅ Root redirect changed from /dashboard to /claims
986 1:00p 🔵 Frontend build infrastructure uses Bun runtime
987 " 🔴 TypeScript build error in claims page mutation handler
988 " 🔴 Fixed TypeScript type error in Select onValueChange handler
989 1:02p ✅ Added explicit defaultValue to Supporting Policy accordion
990 1:03p ✅ Migrated Accordion props to base-ui API from Radix API
991 1:05p ✅ Supporting Policy accordion switched to single-open mode
992 1:06p ✅ Next.js production build now passes successfully
993 1:08p 🔵 Project's accordion uses non-standard base-ui backend instead of Radix
994 1:12p 🔵 Shadcn skill docs document base-ui vs Radix API differences
995 1:13p 🔵 Select component also uses @base-ui/react not Radix UI
996 " ✅ Refactored Select to use base-ui `items` prop and SelectGroup
997 1:14p ✅ Accordion re-enabled multi-open mode with base-ui `multiple` prop
998 " 🔄 Closed React Fragment wrapper for Accordion children
999 1:15p 🔴 Added missing SelectGroup import for base-ui Select refactoring
1000 1:16p ✅ Production build passing with all UI refactoring changes
1001 " 🔵 CSS files not found via glob from frontend directory
1002 " 🔵 Accordion animation classes not found in project CSS files
1003 1:17p 🔵 Accordion animations provided by tw-animate-css and shadcn packages
1004 " 🔵 Accordion keyframe animations sourced from shadcn npm package
1005 " 🔄 Replaced accordion keyframe animations with CSS transition
1006 1:18p ✅ Production build passing with all UI refactoring complete
1007 1:21p 🔵 User confirms three UX bugs in claims system
S248 Bug fixes and queue population design for HomeProject Abacus — determining the optimal lazy-load strategy for pre-populating the claims queue with risk scores (May 12, 1:27 PM)
S249 Design approval for batch populate flow on HomeProject Abacus — presented the architecture for the "Available claims" section and progressive queue population (May 12, 1:35 PM)
S250 Fix bugs and improve UX on Claims queue/detail page - queue shows 0 high risk after analysis, filters broken, no claim discovery mechanism (May 12, 1:36 PM)
S251 Fix two critical bugs on HomeProject Abacus claims detail page: (1) analyzing claims returns HIGH risk but queue stays empty (0 high risk), filters broken; (2) queue is empty by default with no discoverability for users who don't know claim IDs (May 12, 1:38 PM)
S252 Fix two critical bugs on the HomeProject Abacus claims detail page: (1) queue shows 0 / filters broken after analyzing C0001 as HIGH risk, and (2) empty queue with no discovery path for users who don't know a claim ID. The solution pivoted from a dashboard "Available claims" section with manual "Populate queue" button to auto-populating the queue automatically when the user visits `/claims`. (May 12, 1:46 PM)
S253 Fix riskLevel case-sensitivity mismatch (Databricks returns uppercase like "HIGH" but DB/comparison logic uses lowercase "high") and verify auto-populate claims queue feature end-to-end. (May 12, 1:54 PM)
S254 Verify the auto-populate feature works by truncating the database and testing end-to-end (May 12, 2:05 PM)
1008 2:10p 🟣 Auto-populate verification requested via database truncation
S255 Implement pagination, instant claim ID search, and priority-based analysis for the claims table with ~1000 Databricks records (May 12, 2:15 PM)
1009 2:22p 🔵 PowerShell commands incompatible with bash shell environment
1010 " 🔵 Claims API route structure discovered
1011 " 🔵 Claims page fetches all records with no pagination
1012 2:23p 🔵 Full claims data flow mapped for pagination refactor
1013 " 🔵 Explore agents confirm complete absence of pagination
S256 Implement pagination, instant claim ID search, and priority-based analysis for claims table with ~1000 records (May 12, 2:24 PM)
S257 Implement pagination, instant claim ID search, and priority-based analysis for claims table with ~1000 records (May 12, 2:27 PM)
1014 2:30p ⚖️ Search behavior design finalized as Option C (inline banner + auto-jump)
1015 3:44p 🔄 Project formatting improvements initiated
1016 3:50p 🔴 Fixed pagination layout wrapping in claims table
1017 " 🔄 Refactored claims page from client-side to server-side pagination
1018 " 🔄 Replaced inline auto-fill analysis logic with useAnalysisQueue hook
1019 " 🔄 Added null-state handling for unanalyzed claims in table display
1020 " 🔄 Replaced claim samples API with claim-statuses API for pre-analysis scan
1021 3:51p ✅ Created project overview memory for homeprojectabacus
1022 3:52p 🔵 Browser automation skill loaded for potential UI verification
1023 " 🔵 Claims pagination fix could not be visually verified due to auth gate
1024 3:53p 🔵 No authenticated browser tab available for UI verification
1025 " 🔵 Google OAuth sign-in flow confirmed in claims app

Access 626k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>