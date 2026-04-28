# Project Context for Claude

## ⚠️ TOP-OF-MIND: Deferred decision — `claim_type` feature

**Status:** intentionally NOT implemented in Week 4 (deferred 2026-04-28).

**What WEEK4.md asked for:** under §4 Step 2 → "Claim Features", `claim_type` is listed as a bullet alongside `claim_frequency`. WEEK4 provides **no semantic definition** for it (Inpatient vs Outpatient? Routine vs Emergency? Professional vs Institutional?), no derivation rule, and no business justification.

**Why we deferred:**
- The Bronze claims dataset (`datasets/claims_1000.csv`) has no `claim_type` column — adding it would require regenerating the dataset, evolving the Bronze schema, updating Silver pass-through, extending Gold feature engineering, and bumping `FEATURE_COLUMNS` from 13 → 14 with retraining of the registered model.
- That cascade touches ~11 files for a feature with no defined semantics. The current model already clears the §13 release gate (Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85) without it.
- We do not yet know whether subsequent Week 5+ implementations (Model Serving, RAG remediation, dashboard) will actually need `claim_type`.

**What to do if a future week reveals it IS needed:**
This becomes the **first task of that week**. The full waterfall is:

1. Define `claim_type` semantics in `scripts/generate_synthetic_claim_labels.py` (categorical with documented values, e.g. `INPATIENT | OUTPATIENT | EMERGENCY | ROUTINE`).
2. Regenerate `datasets/claims_1000.csv` — `uv run python scripts/generate_synthetic_claim_labels.py`.
3. Add `claim_type` to `src/common/bronze_sources.py` schema (operational, not PHI).
4. Update Bronze pipeline + tests (`tests/test_dataset_contract.py`).
5. Pass through Silver (`ETL/pipelines/silver/silver_claims.py`); update `tests/test_silver_contract.py`.
6. Carry into Gold (`ETL/pipelines/gold/gold_claim_features.py`); encode (one-hot or ordinal); update `tests/test_gold_contract.py`.
7. Add to `src/ml/__init__.py:FEATURE_COLUMNS` and `src/ml/features.py:DEFAULT_FILL_VALUES`; update sample DataFrames in `tests/test_ml_contract.py` (4 test classes).
8. Retrain via `scripts/train_denial_model.py --tune` — registers a new `champion` version under `healthcare.ml.claim_denial_model`.
9. Update ARCHITECTURE.md §9.3 features table.
10. Remove this deferral notice from CLAUDE.md.

**Do not implement this feature speculatively.** It has explicit "deferred until proven necessary" status. If you find yourself thinking "maybe I should add `claim_type` now" — re-read the paragraphs above and confirm a concrete downstream consumer needs it.

---

## Project quick-reference

- **What it is:** AI-Powered Claim Denial Prevention & Remediation System (Databricks + Spark + Delta Lake + MLflow + Streamlit).
- **Layers:** Bronze (raw ingest) → Silver (trusted) → Gold (`healthcare.gold.claim_features`, 13 features) → ML (`healthcare.ml.claim_denial_model@champion`).
- **Release gate (ARCHITECTURE.md §13):** Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85; gate-failing models are not pickled or registered.
- **Train command (Databricks notebook):** `from scripts.train_denial_model import main; main(["--tune"])`.
- **Load champion (anywhere):** `from src.ml.predict import load_from_registry; model = load_from_registry()`.
- **Test command:** `uv run pytest -q` (94 passed, 1 skipped baseline).
- **ML deps install:** `uv sync --group ml`.

## Hard rules (already in place)

- Every code module starts with `from __future__ import annotations`.
- Module constants are typed `Final[...]`.
- `__all__` is alphabetically sorted.
- ETL/common files are one-line proxies: `from src.common.xxx import *  # noqa: F401,F403`.
- Logs never interpolate PHI — use `MESSAGE_TEMPLATE_*` + `render_*` helpers and reference identifiers (claim_id, provider_id) only.
- Bare `except Exception` blocks must call `logger.warning(..., exc_info=True)` — never silent swallow.
