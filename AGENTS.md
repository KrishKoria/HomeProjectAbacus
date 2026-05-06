## Deferred decisions ledger

All "deliberately not implemented" decisions live in [`docs/deferred.md`](docs/deferred.md) — `claim_type`, `ServiceVerifier` Protocol, `ops_service_*` tables, `verify_gold.py`, ETL constant parameterization, `build_analytics` CLI args, DAB `model_version` lookup variable. Read that file before proposing any of those features; each entry names the trigger that justifies reviving it. The `claim_type` block above is the most load-bearing; everything else is in `docs/deferred.md`.

## Project quick-reference

- **What it is:** AI-Powered Claim Denial Prevention & Remediation System (Databricks + Spark + Delta Lake + MLflow + Streamlit).
- **Layers:** Bronze (raw ingest) → Silver (trusted) → Gold (`healthcare.gold.claim_features`, 13 features) → ML (`healthcare.ml.claim_denial_model@champion`).
- **Release gate (ARCHITECTURE.md §13):** Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85; gate-failing models are not pickled or registered.
- **Train command (Databricks notebook):** `from src.scripts.train_denial_model import main; main(["--tune"])`.
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
## Deferred decisions ledger

All "deliberately not implemented" decisions live in [`docs/deferred.md`](docs/deferred.md) — `claim_type`, `ServiceVerifier` Protocol, `ops_service_*` tables, `verify_gold.py`, ETL constant parameterization, `build_analytics` CLI args, DAB `model_version` lookup variable. Read that file before proposing any of those features; each entry names the trigger that justifies reviving it. The `claim_type` block above is the most load-bearing; everything else is in `docs/deferred.md`.

## Project quick-reference

- **What it is:** AI-Powered Claim Denial Prevention & Remediation System (Databricks + Spark + Delta Lake + MLflow + Streamlit).
- **Layers:** Bronze (raw ingest) → Silver (trusted) → Gold (`healthcare.gold.claim_features`, 13 features) → ML (`healthcare.ml.claim_denial_model@champion`).
- **Release gate (ARCHITECTURE.md §13):** Recall@HIGH ≥ 0.80, Precision ≥ 0.70, ROC-AUC ≥ 0.85; gate-failing models are not pickled or registered.
- **Train command (Databricks notebook):** `from src.scripts.train_denial_model import main; main(["--tune"])`.
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


<claude-mem-context>
# Memory Context

# [homeprojectabacus] recent context, 2026-05-06 6:59pm GMT+5:30

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (14,954t read) | 823,665t work | 98% savings

### May 6, 2026
399 5:46p 🔵 Verified P2 retrain_gate.py fallback collect() behavior
400 " 🔵 Verified P3 gold_policy_embeddings.py uses bare module globals
401 " 🔵 Verifying P2 test_ml_contract.py mock targets pyfunc not sklearn
402 " 🔵 Confirmed P3 unsorted __all__ in claims_analytics.py
403 " 🔵 Discovered failing contract test for frontend runtime env vars
404 5:47p 🔵 P2 test mock confirmed dead — load_from_registry uses sklearn.load_model
405 " 🔵 P1 finding needs refinement — root app.yaml has env vars, frontend.app.yml does not
407 5:48p 🔵 Confirmed repo rules for __all__ sorting and Final constants
408 " 🔵 retrain_gate.py already uses Final typing on its constants
409 5:51p ⚖️ Plan scope substantially narrowed after user feedback
S71 Refine implementation plan scope for code review remediation based on user feedback (May 6, 5:51 PM)
S72 Comprehensive code review of homeprojectabacus project via superpowers requesting-code-review skill, checking coding standards consistency, performance bottlenecks, and simplification opportunities (May 6, 5:52 PM)
410 5:52p ⚖️ Execution plan finalized: 4 files to modify, narrow scope confirmed
411 5:54p ⚖️ Test strategy for retrain_gate fingerprint fix: two paths to cover
412 " 🔄 P1 test fix: test_frontend_app_runtime_envs_are_bundle_driven renamed and retargeted to app.yaml
413 6:00p 🔵 Code review initiated via superpowers skill
S73 Code review of the ETL directory for coding standards inconsistencies, performance bottlenecks, and simplification opportunities (May 6, 6:00 PM)
414 6:15p 🔵 Mapped all unsafe_allow_html=True usage in app_streamlit.py
415 " 🔵 Confirmed P2 injection vector in _render_risk_gauge
416 " 🔵 Confirmed P1 injection vector in _render_policy_guidance
419 " 🔴 Added html.escape import to app_streamlit.py
417 6:16p 🔵 Code review requested for ETL project directory
418 " 🔵 Code review session initialized for homeprojectabacus
420 " 🔵 Sequential thinking tool validation errors during planning
421 6:17p 🔴 Fixed P2: claim_id HTML injection in _render_risk_gauge
422 " 🔵 ETL codebase follows medallion architecture pattern
423 6:18p 🔴 Fixed P1: narrative text HTML injection in _render_policy_guidance
424 " 🔵 ETL common modules are 1-line re-export shims from src.common
425 " 🔵 Inconsistent from __future__ import annotations placement across ETL
426 " 🔴 Bare except Exception without exc_info in silver_policy_chunks.py
427 " 🔵 Hardcoded healthcare catalog reference in gold_policy_embeddings.py
428 6:19p 🔴 Extended HTML escaping to policy card chunk_text and doc_path
429 6:20p 🔵 Detailed inspection of silver_policy_chunks.py underway
430 " 🔴 Escaped SHAP reason text in _render_feature_row
S74 Fix P1 (narrative text HTML injection) and P2 (claim_id HTML injection) badge escape vulnerabilities in app_streamlit.py where dynamic text was interpolated into unsafe_allow_html=True markdown blocks without escaping (May 6, 6:20 PM)
431 6:21p 🔴 Verified syntax of all HTML escape fixes
432 " 🔴 Existing tests pass after HTML escape fixes
433 " 🔴 All 42 RAG and XAI tests pass after HTML escape fixes
434 " 🔴 HTML escape fixes staged in git
435 6:22p 🔴 Committed HTML injection fixes to git
S75 Targeted performance audit of the ETL directory to identify remaining optimization opportunities beyond the initial code review (May 6, 6:22 PM)
S76 Implementation plan for fixing all ETL code review performance findings (three targeted changes to silver_policy_chunks.py, gold_policy_embeddings.py, and contract tests) (May 6, 6:24 PM)
436 6:27p ⚖️ Planning implementation details for ETL performance fixes
S77 Progress checkpoint - no new activity in primary session (May 6, 6:27 PM)
S78 Comprehensive code review of ETL directory with follow-up performance audit and implementation of identified fixes. (May 6, 6:28 PM)
437 6:28p 🔵 Agent re-reading all target files before presenting final implementation plan
438 6:29p 🟣 Implemented ai_query failOnError and PREVIEW channel for gold_policy_embeddings
439 " 🟣 Implemented duplicate PDF parsing avoidance in silver_policy_chunks
440 " 🟣 Updated contract tests for gold_policy_embeddings pipeline changes
441 " 🟣 Added contract tests for duplicate PDF optimization in silver_policy_chunks
S79 Create implementation plan for five code review findings in src/common (May 6, 6:30 PM)
442 6:31p 🔵 Unsorted `__all__` lists found in three `src/common/` modules
443 " 🔵 Bare `except Exception:` patterns found in bronze_pipeline_config.py
444 " 🔵 Complete inventory of 12 code modules in `src/common/` reviewed
445 " 🔵 Cross-referenced `src/common/` function usage across ETL, analytics, ML, and tests
446 6:47p 🔵 Five code review findings identified in analytics codebase
S80 Create a fix plan for 5 code review findings in analytics codebase (May 6, 6:47 PM)
447 6:49p 🔴 Removed broadcast of claim-grain DataFrame and added cache lifecycle management
448 " 🔄 Replaced Python UDF with native Spark expressions in refresh-plans parser
449 " ✅ All five code review findings implemented and verified

Access 824k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>