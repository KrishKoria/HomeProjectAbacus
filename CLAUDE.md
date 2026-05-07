This file provides guidance to Claude Code when working with code in this repository.

## Deferred decisions

[`docs/deferred.md`](docs/deferred.md) records all deliberately-not-implemented features (`claim_type`, `ServiceVerifier` Protocol, `ops_service_*` tables, `verify_gold.py`, ETL constant parameterization, `build_analytics` CLI args, DAB `model_version` lookup variable). Read it before proposing any of those; each entry names the trigger that justifies reviving it.

## Project overview

AI-Powered Claim Denial Prevention & Remediation System — Databricks + Spark + Delta Lake + MLflow + Streamlit.

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

# Run Streamlit app locally
uv run streamlit run src/analytics/app_streamlit.py
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
  analytics/     — Streamlit app, claims analytics, observability assets
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
