# Deferred Decisions Ledger

Living record of decisions made to **deliberately not implement** something. Each entry must answer: what, why deferred, what triggers reviving it, and where the full context lives.

When a deferral is revived, **delete its entry from this file** in the same PR that ships the implementation. Do not let stale entries accumulate.

Format for new entries:

```markdown
## <slug>

- **Status:** deferred YYYY-MM-DD
- **Source:** <doc§section that originated the decision>
- **Trigger to revive:** <concrete signal that forces this back on the roadmap>
- **Full context:** <link to the document that holds the rationale + waterfall>
```

---

## claim_type feature

- **Status:** deferred 2026-04-28
- **Source:** WEEK4.md §4 Step 2 → "Claim Features" lists it without semantic definition.
- **Trigger to revive:** any Week 5+ consumer (Model Serving / RAG remediation / dashboard) creates a concrete requirement that names `claim_type`.
- **Full context:** [`CLAUDE.md`](../CLAUDE.md) top-of-file block — "⚠️ TOP-OF-MIND: Deferred decision — `claim_type` feature". Includes the 11-file waterfall to revive it.

---

## ServiceVerifier Protocol + ServiceConfig dataclass

- **Status:** deferred 2026-04-29 (during the integration plan composable-framework refinement pass).
- **Source:** [`docs/integration_plan.md`](integration_plan.md) → Composable Service Framework → Python Verification Contract.
- **Why deferred:** the framework today has only one consumer pattern (verifier scripts that emit `HealthCheckResult`). Designing `ServiceVerifier` Protocol methods (`health_check()`, `verify()`) and a `ServiceConfig` base before a second non-pipeline service exists is speculative; the shape will likely need rework once a real second consumer arrives.
- **Trigger to revive:** the **second non-pipeline service type** lands (ml_serving endpoint, rag_indexing pipeline + Vector Search setup, agent job, or app). At that point both abstractions get real consumers and can be designed against concrete needs.
- **Full context:** see the "Deferred (introduce when second non-pipeline service lands)" subsection inside the Python Verification Contract section.

---

## ops*service*\* observability tables

- **Status:** deferred 2026-04-29.
- **Source:** [`docs/integration_plan.md`](integration_plan.md) → Service-Aware Observability (deferred).
- **Why deferred:** Phase 1 of the integration ships **stage-aware** observability (`ops_pipeline_*` tables gain a `pipeline_stage` column). The wider `ops_service_health` / `ops_service_metrics` / `ops_service_events` / `ops_service_failures` schema with `service_name` / `service_type` / `stage` columns was designed before any non-pipeline service emits metrics. Premature schema design will likely need rework.
- **Trigger to revive:** the first non-pipeline service that needs to emit observability — at that point either extend the existing `ops_pipeline_*` columns (rename `pipeline_stage` → `stage`) or introduce a single new table sized to the actual consumer's needs.
- **Full context:** Service-Aware Observability section in the integration plan.

---

## verify_gold.py

- **Status:** deferred 2026-04-29.
- **Source:** [`docs/integration_plan.md`](integration_plan.md) → Script Conversion Plan → `src/scripts/verify_gold.py` (deferred).
- **Why deferred:** Gold quality is asserted implicitly by `decide_retrain()` / `check_new_data` — fingerprint + row-count + column-list comparison against the champion's logged metadata acts as the de-facto Gold contract check (zero rows raises; column drift forces retrain; fingerprint drift forces retrain). Adding a separate verifier today duplicates that logic.
- **Trigger to revive:** a non-ML downstream consumer (dashboard, agent, external API) creates a Gold contract requirement that `decide_retrain` does not already cover (e.g. specific column non-null guarantees, distribution checks, cross-table referential integrity).
- **Full context:** verify_gold deferral note in the Script Conversion Plan section.

---

## ETL constant parameterization (catalog/schema via Spark config)

- **Status:** deferred 2026-04-29.
- **Source:** [`docs/integration_plan.md`](integration_plan.md) → Pipeline Resources → "Important limitation" + Out of Scope section.
- **Why deferred:** the current ETL modules import constants that default to `healthcare`, `bronze`, `silver`, `gold` (`src/common/{bronze,silver,gold}_pipeline_config.py`). Bundle variables alone do not create true dev/prod target isolation until the ETL constants read catalog/schema from Spark configuration. The first orchestration cut targets the fixed `healthcare.*` deployment.
- **Trigger to revive:** a second target environment (true `prod` workspace, or a per-developer `dev_<user>` catalog) that must run with different catalog/schema names without code changes.
- **Full context:** Pipeline Resources → "Important limitation" paragraph.

---

## build_analytics.py CLI parameterization

- **Status:** deferred 2026-04-29 (paired with ETL constant parameterization above).
- **Source:** integration plan exploratory review.
- **Why deferred:** `build_analytics.py` calls `build_and_persist_claims_assets()` which reads from `healthcare.silver.*` / `healthcare.bronze.*` via the same hardcoded constants. Adding `--catalog` / `--schema` here without ETL parameterization creates inconsistency — the analytics layer would be parameterized but ETL would not.
- **Trigger to revive:** at the same moment ETL constant parameterization is revived. They ship together.
- **Full context:** none beyond this entry — the script section in the integration plan does not document parameters.

---

## DAB lookup variable for `model_version`

- **Status:** deferred 2026-04-29.
- **Source:** [`docs/integration_plan.md`](integration_plan.md) → Modular DAB Bundle Structure → `databricks.yml` `variables:` block.
- **Why deferred:** dev currently defaults `model_version: "1"`; prod requires `--var=model_version=<n>` resolved by a CI step that queries the MLflow registry for the `champion` alias. A native DAB lookup variable that binds to a registered-model alias would remove that CI step. The feature is referenced as "preview" in Databricks docs; no clear GA date.
- **Trigger to revive:** Databricks GA's a `lookup:` variable form that resolves to a registered-model version by alias (e.g. `lookup: { registered_model_version_by_alias: { name: ..., alias: champion } }`). When that lands, replace the dev default + prod CI step with a single `lookup:` declaration.
- **Full context:** the `model_version` variable comment block in the databricks.yml example.

## fastapi-react-frontend

- **Status:** deferred 2026-05-05
- **Source:** Week 5+6 implementation planning (grill-with-docs session).
- **Why deferred:** Streamlit on Databricks serves the v1 UI need with zero infra overhead (no API gateway, no JWT/OAuth, no CORS, no VPC connector, no Cloud Run). FastAPI + React adds weeks of infra work before any business value. The project has no dedicated frontend team yet; React without a designer produces generic dashboards.
- **Trigger to revive:** a dedicated frontend team is formed OR the Streamlit app is insufficient for production UX requirements (thousands of concurrent users, complex real-time collaboration, offline mode).
- **Full context:** [`openspec/changes/add-week5-week6-xai-rag/design.md`](../openspec/changes/add-week5-week6-xai-rag/design.md) — Design §4 (Serving layer). Core logic lives in `src/xai/` and `src/rag/` as pure Python modules; when FastAPI is needed, FastAPI routes call those same modules with zero logic duplication. ARCHITECTURE.md §16-17 contain the aspirational FastAPI + React design.

---

## src/common/policy_chunks.py disposition

- **Status:** deferred 2026-05-05
- **Source:** Week 5+6 implementation planning (grill-with-docs session).
- **Why deferred:** The module provides programmatic PDF text extraction, normalization, and chunking outside Spark — potentially useful for single-document upload in the Streamlit UI. The Bronze → Silver SDP pipeline (`bronze_policies.py` → `silver_policy_chunks.py`) already handles bulk PDF ingestion with pdfplumber UDFs, so this module is duplicative for batch but may serve the interactive path. Deleting it now risks re-implementing it if the Streamlit ingestion flow needs it.
- **Trigger to revive (and remove):** the Streamlit UI stabilizes and confirms it does NOT use this module for any ingestion path. At that point, delete `src/common/policy_chunks.py` and remove its tests. If it IS used by Streamlit, keep it and remove this deferral entry.
- **Full context:** [`openspec/changes/add-week5-week6-xai-rag/design.md`](../openspec/changes/add-week5-week6-xai-rag/design.md) — Design §8.

---

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
