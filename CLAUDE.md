## Deferred decisions ledger

All "deliberately not implemented" decisions live in [`docs/deferred.md`](docs/deferred.md) — `claim_type`, `ServiceVerifier` Protocol, `ops_service_*` tables, `verify_gold.py`, ETL constant parameterization, `build_analytics` CLI args, DAB `model_version` lookup variable. Read that file before proposing any of those features; each entry names the trigger that justifies reviving it. The `claim_type` block above is the most load-bearing; everything else is in `docs/deferred.md`.

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
