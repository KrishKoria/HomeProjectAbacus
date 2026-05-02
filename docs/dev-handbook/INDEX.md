# Developer Handbook — Healthcare Claim Denial Prevention System

## What this is

This handbook is the single source of truth for developers working on this project. It tells you **what to change and where to change it**. Use it to onboard, to find the right file for a task, to understand how the pieces fit together.

## Project at a glance

- **What:** AI-Powered Claim Denial Prevention & Remediation System
- **Platform:** Databricks on Google Cloud (GCP)
- **Stack:** Apache Spark, Delta Lake, MLflow, SDP (Lakeflow Pipelines), Streamlit
- **Languages:** Python 3.12+, YAML (DAB), SQL (materialized views)
- **Data architecture:** Medallion — Bronze (raw) → Silver (trusted) → Gold (features) → ML (model)
- **ML:** 6 model candidates (XGBoost, LightGBM, CatBoost, Voting Ensemble, Stacking Ensemble, Logistic Regression), Optuna tuning, MLflow Model Registry with champion alias
- **Release gate:** Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85 — failing models are not registered
- **Test command:** `uv run pytest -q` (150 passed, 1 skipped)

## How to use this handbook

| You want to... | Read this chapter |
|---|---|
| Understand the big picture | [Chapter 1: Architecture](01-architecture.md) |
| Understand DAB layout, targets, variables | [Chapter 2: Bundle Structure](02-bundle-structure.md) |
| Find which service does what | [Chapter 3: Services](03-services.md) |
| Understand ETL pipelines, features, constants | [Chapter 4: ETL Pipelines](04-etl-pipelines.md) |
| Understand ML training, models, Optuna, gates | [Chapter 5: ML Pipeline](05-ml-pipeline.md) |
| Look up a config constant, threshold, or env var | [Chapter 6: Configuration Reference](06-configuration.md) |
| Look up a CLI flag or script entry point | [Chapter 7: CLI Reference](07-cli-reference.md) |
| Deploy, run jobs, switch environments | [Chapter 8: Deployment](08-deployment.md) |
| Add a feature, model, flag, or test | [Chapter 9: Development How-To](09-development.md) |

## Quick directory map

```
homeprojectabacus/
├── databricks.yml              # DAB bundle root
├── pyproject.toml              # Python project config, deps
├── uv.lock                     # Locked dependency versions
├── AGENTS.md                   # Project instructions (read first)
├── CLAUDE.md                   # → @AGENTS.md pointer
│
├── src/
│   ├── common/                 # Shared config constants (Bronze/Silver/Gold/ML)
│   ├── ml/                     # ML training, evaluation, prediction, retrain gate
│   ├── scripts/                # CLI entry points (train, retrain, generators)
│   ├── analytics/              # Streamlit dashboard + asset builder
│   └── framework/              # Service manifest, observability, verifier
│
├── ETL/
│   ├── common/                 # Proxy re-exports of src/common
│   └── pipelines/
│       ├── bronze/              # Bronze ingestion pipeline (SDP)
│       ├── silver/              # Silver cleaning/normalization pipeline (SDP)
│       └── gold/                # Gold feature engineering pipeline (SDP)
│
├── services/
│   ├── bronze/ingestion/resources/   # Bronze SDP pipeline YAML
│   ├── silver/cleaning/resources/    # Silver SDP pipeline YAML
│   ├── gold/features/resources/      # Gold SDP pipeline YAML
│   ├── analytics/dashboard/resources/ # Analytics job YAML
│   ├── ml/training/resources/        # ML retrain job YAML
│   └── setup/resources/              # Setup + sample-data jobs YAML
│
├── resources/
│   ├── schemas/                # Unity Catalog schema definitions
│   └── volumes/                # Bronze landing volume definition
│
├── tools/                      # Synthetic data/policy generators
├── tests/                      # Contract tests (6 modules)
├── datasets/                   # Synthetic claims CSV, policy PDFs
└── docs/
    ├── dev-handbook/           # ← YOU ARE HERE
    ├── ARCHITECTURE.md         # Detailed architecture (legacy — handbook supersedes)
    ├── deferred.md             # Deferred decisions ledger
    ├── runbooks/               # Operator runbooks (GCP setup, deploy)
    └── ...
```

## Key conventions

- Every Python module starts with `from __future__ import annotations`
- Module constants are typed `Final[...]`
- `__all__` is alphabetically sorted
- ETL/common files are one-line proxies: `from src.common.xxx import *  # noqa: F401,F403`
- Logs never interpolate PHI — use reference identifiers only
- Bare `except Exception` blocks must call `logger.warning(..., exc_info=True)` — never silent swallow
