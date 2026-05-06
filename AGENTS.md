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

# [homeprojectabacus] recent context, 2026-05-06 6:00pm GMT+5:30

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (16,404t read) | 773,296t work | 98% savings

### May 5, 2026
S62 Streamlit UI redesign for Claim Denial Risk Analyzer (`app_streamlit.py`) — transform bland Streamlit-native UI into a visually distinctive, information-rich dark-themed dashboard using the OKLCH color system, with improved information hierarchy, feature values alongside SHAP importance, latency waterfall, and compact policy cards. (May 5, 10:09 PM)
### May 6, 2026
S63 Generate expanded policy PDFs matching datasets for testing — user asked to go through dataset files and generate policy PDFs to properly test the application (May 6, 1:13 PM)
361 4:44p 🔵 Full ETL pipeline for PDFs mapped: Bronze→Silver→Gold
364 4:45p 🟣 Policy PDF generation from datasets
365 4:51p 🔵 Existing policy PDF corpus and generator identified
366 4:59p ✅ Expanded Policy PDFs Design Spec Created
S64 Generate expanded policy PDFs matching datasets for testing — user approved the approach and is now choosing execution method (subagent vs inline) (May 6, 5:02 PM)
367 5:04p ✅ Expanded Policy PDFs Implementation Plan Created
S65 Generate policy PDFs matching the datasets so the application can be properly tested — expand the policy corpus from 5 to 11 documents covering all 22 ML features in the SHAP explanation system. (May 6, 5:09 PM)
368 5:11p ⚖️ Plan Mode Exited — Implementation Phase Begins
369 " 🔵 Existing Policy Corpus and Generator Confirmed
370 5:17p 🟣 Expanded Policy PDF Generator Script Created
371 5:18p 🔵 Editable Package Import Dependency Discovered
372 5:19p 🟣 6 Expanded Policy PDFs Successfully Generated
S66 Fix ML model retraining trigger to consider changes to non-claims datasets — the model only retrained when claims.csv changed but ignored changes to providers, diagnosis codes, cost benchmarks, and dx_px_mapping reference data. (May 6, 5:20 PM)
373 5:29p 🔵 ML model retraining ignores changes to non-claims datasets
374 5:30p 🔵 Investigation into retraining trigger codebase
375 " 🔵 Retraining trigger investigation maps project surface area
376 " 🔵 Retrain gate only fingerprints a single Gold table
377 " 🔵 Retrain trigger bottleneck confirmed: ETL only triggered by raw_landing file arrival
378 5:31p 🔵 Test coverage confirms single-table fingerprinting — no multi-source tests
379 " 🔵 PSI drift detection exists as infrastructure but is not wired into retrain gate
380 5:32p 🔴 Retrain gate now catches reference data shifts with identical row counts
381 5:34p ✅ Fix successfully applied and syntax-validated
382 " 🟣 New test added for reference data shift detection path
383 5:35p 🔵 Code review initiated for HomeProjectAbacus project
384 5:36p 🔵 Code review strategy decisions made for HomeProjectAbacus
385 " 🔵 Retrain gate test suite not yet run — 209 other tests pass
386 " ✅ Retrain gate reference data shift fix staged — 52 additions, 10 deletions
387 5:37p 🔴 Retrain gate fix committed: reference data shift now triggers retraining
388 " 🔵 Uncommitted code changes found in retrain_gate and integration test
389 " 🔵 Static analysis scan revealed code patterns across the project
390 " 🔵 Project uses consistent typing conventions with Final annotations
S67 Comprehensive code review of the homeprojectabacus project seeking coding standards inconsistencies, performance bottlenecks, and simplification opportunities across all code files (May 6, 5:37 PM)
391 5:39p 🔵 Static analysis reveals coding standard inconsistencies across the codebase
392 " 🔵 Numerous bare except Exception handlers without exception logging
393 " 🔵 Long files with dense code identified for potential refactoring
394 " 🔵 Spark collect() calls concentrated in retrain gate logic
395 5:40p 🔴 Two pre-existing test failures found in integration and ML contract tests
S68 Create a remediation plan for 5 code review findings across homeprojectabacus (May 6, 5:41 PM)
396 5:42p ⚖️ Transitioning from code review to implementation plan mode
406 5:44p ⚖️ Code review remediation plan initiated for 5 findings
397 5:45p ⚖️ Code review findings presented for verification
398 " 🔵 Confirmed app_streamlit.py references SQL env vars at runtime
399 5:46p 🔵 Verified P2 retrain_gate.py fallback collect() behavior
400 " 🔵 Verified P3 gold_policy_embeddings.py uses bare module globals
401 " 🔵 Verifying P2 test_ml_contract.py mock targets pyfunc not sklearn
402 " 🔵 Confirmed P3 unsorted __all__ in claims_analytics.py
403 " 🔵 Discovered failing contract test for frontend runtime env vars
404 5:47p 🔵 P2 test mock confirmed dead — load_from_registry uses sklearn.load_model
405 " 🔵 P1 finding needs refinement — root app.yaml has env vars, frontend.app.yml does not
S69 Verify all 5 findings from a code review of the homeprojectabacus repo — P1 (missing env vars in frontend bundle), P2 (dead mock in test, OOM risk in fingerprint fallback), P3 (missing Final typing, unsorted __all__) (May 6, 5:48 PM)
407 5:48p 🔵 Confirmed repo rules for __all__ sorting and Final constants
408 " 🔵 retrain_gate.py already uses Final typing on its constants
S70 Refine implementation plan scope based on user feedback — narrow to test fixes, fail-closed fingerprint guard, one __all__ sort (May 6, 5:49 PM)
409 5:51p ⚖️ Plan scope substantially narrowed after user feedback
S71 Refine implementation plan scope for code review remediation based on user feedback (May 6, 5:52 PM)
410 5:52p ⚖️ Execution plan finalized: 4 files to modify, narrow scope confirmed
411 5:54p ⚖️ Test strategy for retrain_gate fingerprint fix: two paths to cover
412 " 🔄 P1 test fix: test_frontend_app_runtime_envs_are_bundle_driven renamed and retargeted to app.yaml

Access 773k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>