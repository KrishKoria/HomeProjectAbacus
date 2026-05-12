# Chapter 7: CLI Reference

Every script entry point in the project. Use this chapter to find the right flag, default value, or example invocation.

---

## 7.1 `src/scripts/train_denial_model.py`

**Purpose:** Train the ML model directly (bypasses retrain gate). Trains 6 model families (LogReg, XGBoost, LightGBM, CatBoost, Voting Ensemble, Stacking Ensemble), picks the best candidate by gate status then recall@HIGH then ROC-AUC, and optionally registers it in MLflow.

**Entry point:** `main()` function (line 318)

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--gold-table` | `str` | `healthcare.gold.claim_features` | Fully-qualified Gold feature table name |
| `--gold-csv` | `str` | `None` | CSV fallback path (for local runs without Spark); must contain engineered feature columns, NOT the raw Bronze CSV |
| `--catalog` | `str` | `healthcare` | Unity Catalog catalog name |
| `--model-output` | `str` | `models/claim_denial_model.pkl` | Output path for the trained model pickle file |
| `--tune` | flag | `False` | Run Optuna hyperparameter tuning (auto-enabled when no args passed) |
| `--no-tune` | flag | `False` | Skip Optuna tuning, use default XGBoost params |
| `--optuna-trials` | `int` | `50` | Number of Optuna trials when `--tune` is active |
| `--random-seed` | `int` | `42` | Random seed for reproducibility |
| `--mlflow-tracking-uri` | `str` | `None` | MLflow tracking URI (defaults to Databricks workspace) |
| `--registered-model-name` | `str` | `healthcare.ml.claim_denial_model` | MLflow Registry name (3-level UC name on Databricks; empty string = skip registration) |
| `--champion-alias` | `str` | `champion` | Registry alias to move onto the new version (empty = no alias) |

### Default behavior (no args)

The `_entrypoint_argv()` function (line 42-43) defaults to `["--tune"]` when `sys.argv` has no arguments beyond the script name. This means running the script with no CLI flags **enables tuning**.

### Pipeline (train_pipeline, line 146)

The script trains six model types in sequence:

| Model | Training Function | Calibration |
|---|---|---|
| Logistic Regression | `train_logistic_regression` | `select_best_calibration` (tests sigmoid vs isotonic) |
| XGBoost | `train_xgboost` or `tune_xgboost_optuna` | `select_best_calibration` (no-tune) or built-in (tune) |
| LightGBM | `train_lightgbm` or `tune_lightgbm_optuna` | Same pattern |
| CatBoost | `train_catboost` or `tune_catboost_optuna` | Same pattern |
| Voting Ensemble | `train_voting_ensemble` (soft voting) | N/A (uses calibrated base models) |
| Stacking Ensemble | `train_stacking_ensemble` (LogReg meta-learner, cv=5) | N/A (uses calibrated base models) |

Candidates are sorted by `(meets_thresholds(), recall_at_high, roc_auc)` descending (line 248-251).

### Release Gate (lines 357-367)

If the best model fails the release gate (recall@HIGH < 0.80, precision < 0.70, or ROC-AUC < 0.85), the script:
1. Prints the specific failing thresholds
2. Exits with code 1
3. Does NOT save the pickle or register in MLflow (unless `register_only_on_pass` is False)

### Examples

```bash
# Default: tune with 50 Optuna trials
python src/scripts/train_denial_model.py

# Explicit tune with 200 trials, different seed
python src/scripts/train_denial_model.py --tune --optuna-trials 200 --random-seed 123

# No tuning, use default XGBoost params
python src/scripts/train_denial_model.py --no-tune

# Skip MLflow registration entirely
python src/scripts/train_denial_model.py --tune --registered-model-name ""

# Local run with CSV fallback
python src/scripts/train_denial_model.py --gold-csv datasets/claims_features_exported.csv --no-tune --registered-model-name ""
```

---

## 7.2 `src/scripts/maybe_retrain_model.py`

**Purpose:** Run retrain gate first, then train only if the gate says "retrain". This is the production entry point called by the `ml_retrain_job` Databricks job.

**Entry point:** `main()` function (line 29)

### Flags

All flags from `train_denial_model.py` are accepted (passed through via `train_main(train_args)` at line 67), PLUS:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--force` | flag | `False` | Skip retrain-gate check and train unconditionally |
| `--catalog` | `str` | `healthcare` | Unity Catalog catalog name |
| `--gold-table` | `str` | `healthcare.gold.claim_features` | Gold feature table |
| `--registered-model-name` | `str` | `healthcare.ml.claim_denial_model` | Registry name |
| `--champion-alias` | `str` | `champion` | Registry alias |
| `--optuna-trials` | `int` | `50` | Optuna trials |
| `--random-seed` | `int` | `42` | Random seed |

### Gate Logic (lines 35-47)

1. Calls `decide_retrain()` from `src/ml/retrain_gate.py`
2. If `decision_status == "error"`: exits with code 1
3. If `should_retrain` is False: exits with code 0 (no training)
4. If `--force`: prints `FORCE: skipping retrain-gate check, training unconditionally.`
5. Otherwise: delegates to `train_denial_model.main()` with `--tune` hardcoded

### Examples

```bash
# Run retrain gate; train only if needed
python src/scripts/maybe_retrain_model.py

# Force retrain regardless of gate, with 200 trials
python src/scripts/maybe_retrain_model.py --force --optuna-trials 200

# Force retrain with explicit catalog
python src/scripts/maybe_retrain_model.py --force --catalog healthcare
```

---

## 7.3 `src/scripts/setup_retrain_decisions.py`

**Purpose:** Create or migrate the `healthcare.ml.retrain_decisions` Delta audit table. Designed to be run once during infrastructure setup.

**Entry point:** `main()` function (line 26)

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--catalog` | `str` | `healthcare` | Unity Catalog catalog name |
| `--ml-schema` | `str` | `ml` | ML schema name |

### Table Schema (lines 33-53)

| Column | Type |
|---|---|
| `decided_at` | `TIMESTAMP` |
| `decision_status` | `STRING` |
| `should_retrain` | `STRING` |
| `reason` | `STRING` |
| `error_detail` | `STRING` |
| `current_row_count` | `BIGINT` |
| `current_gold_version` | `BIGINT` |
| `current_gold_object_type` | `STRING` |
| `current_gold_last_altered` | `STRING` |
| `current_fingerprint` | `STRING` |
| `champion_run_id` | `STRING` |
| `previous_training_row_count` | `BIGINT` |
| `row_count_delta` | `BIGINT` |
| `row_count_delta_pct` | `DOUBLE` |

### Migration Columns (lines 8-16)

Columns that were added after initial creation, checked via `_MIGRATION_COLUMNS`:

| Column | Type |
|---|---|
| `decision_status` | `STRING` |
| `error_detail` | `STRING` |
| `previous_training_row_count` | `BIGINT` |
| `row_count_delta` | `BIGINT` |
| `row_count_delta_pct` | `DOUBLE` |
| `current_gold_object_type` | `STRING` |
| `current_gold_last_altered` | `STRING` |

### Example

```bash
python src/scripts/setup_retrain_decisions.py
```

---

## 7.4 `src/scripts/build_analytics.py`

**Purpose:** Build and persist analytics assets from Silver/Gold tables. Skips if `--upstream-status` is not `"success"`.

**Entry point:** `main()` function (line 25)

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--catalog` | `str` | `healthcare` | Unity Catalog catalog name |
| `--bronze-schema` | `str` | `bronze` | Bronze schema name |
| `--analytics-schema` | `str` | `analytics` | Analytics schema name |
| `--upstream-status` | `str` | `success` | Upstream pipeline status; non-"success" causes graceful skip |

### Upstream Status Gate (lines 29-39)

If `--upstream-status` (normalized to lowercase) is not `"success"`, the script prints a skip message and exits with code 0. This prevents analytics builds when upstream ETL has not completed successfully.

### Example

```bash
python src/scripts/build_analytics.py

python src/scripts/build_analytics.py --catalog healthcare --analytics-schema analytics
```

---

## 7.5 `tools/generate_synthetic_claim_labels.py`

**Purpose:** Regenerate deterministic synthetic adjudication labels for `datasets/claims_1000.csv`. The base claim columns (procedure_code, billed_amount, etc.) are treated as fixture input; this script recomputes only the 6 synthetic adjudication columns.

**Entry point:** `main()` function (line 205)

### Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--claims` | `Path` | `datasets/claims_1000.csv` | Input claims CSV path |
| `--providers` | `Path` | `datasets/providers_1000.csv` | Providers CSV for region lookup |
| `--cost` | `Path` | `datasets/cost.csv` | Cost CSV for expected cost benchmarking |
| `--output` | `Path` | (same as `--claims`) | Output path for regenerated CSV |
| `--check` | flag | `False` | Exit non-zero if labels would change (dry-run validation) |

### Label Columns Generated

| Column | Logic |
|---|---|
| `claim_status` | `"DENIED"` if reason != NONE, else `"APPROVED"` |
| `denial_reason_code` | `MISSING_PROCEDURE`, `MISSING_BILLED_AMOUNT`, `OVER_BENCHMARK`, `MEDICAL_REVIEW`, or `NONE` |
| `allowed_amount` | Derived from expected cost or billed amount |
| `paid_amount` | `"0.00"` if denied, else `allowed_amount` |
| `is_denied` | `"1"` if denied, else `"0"` |
| `follow_up_required` | Same as `is_denied` |

### Denial Logic (lines 148-168)

| Condition | Denial Reason | Allowed Amount |
|---|---|---|
| No `procedure_code` | `MISSING_PROCEDURE` | `None` |
| No `billed_amount` | `MISSING_BILLED_AMOUNT` | `None` |
| `billed / expected > 2.5` | `OVER_BENCHMARK` | `expected_cost` |
| Claim ID in medical review set | `MEDICAL_REVIEW` | `billed * 0.60` |
| Everything else | `NONE` | `min(billed, expected * 1.05)` or `billed * 0.85` |

The medical review set contains 85 hardcoded claim IDs (lines 31-123).

### Example

```bash
# Regenerate labels in-place
uv run python tools/generate_synthetic_claim_labels.py

# Check if labels would change (for CI)
uv run python tools/generate_synthetic_claim_labels.py --check

# Output to a different path
uv run python tools/generate_synthetic_claim_labels.py --output datasets/claims_new_labels.csv
```

---

## 7.6 `tools/generate_synthetic_policy_pdfs.py`

**Purpose:** Generate 5 synthetic policy PDFs in `datasets/policies/` for RAG indexing. Each PDF contains structured policy text (metadata table, summary, rules table, remediation guidance, RAG safety note).

**Entry point:** `generate()` function (line 306). Script runs via `__main__` block.

### Flags

**None.** The script has no CLI arguments. Run directly.

### Generated Documents

| Filename | Policy ID | Rules | Topics |
|---|---|---|---|
| `claim_submission_completeness_policy.pdf` | CLAIMOPS-POL-001 | COMP-01 through COMP-04 | Missing fields, completeness checks |
| `medical_necessity_by_diagnosis_policy.pdf` | CLAIMOPS-POL-002 | MED-01 through MED-05 | Diagnosis-procedure alignment |
| `procedure_cost_benchmark_policy.pdf` | CLAIMOPS-POL-003 | COST-01 through COST-07 | Cost benchmarks by region |
| `provider_documentation_policy.pdf` | CLAIMOPS-POL-004 | PROV-01 through PROV-04 | Provider reference validation |
| `denial_reason_remediation_policy.pdf` | CLAIMOPS-POL-005 | DENY-01 through DENY-05 | Remediation recommendations |

All policies are non-PHI synthetic text for demo use only.

### Example

```bash
uv run python tools/generate_synthetic_policy_pdfs.py
```

---

## 7.7 Databricks Bundle Jobs

### Job Key Reference

| Job Key | Bundle Run Command | Script / Notebook Called |
|---|---|---|
| `setup_infrastructure` | `databricks bundle run setup_infrastructure -t dev --profile dev` | `src/notebooks/grants.ipynb` + `src/scripts/setup_retrain_decisions.py` + `src/scripts/load_sample_data.py` (conditional) |
| `bronze_pipeline` | Triggered by `healthcare_etl_pipeline` DLT pipeline | `ETL/pipelines/bronze/*` (Auto Loader streaming) |
| `silver_pipeline` | Triggered by `healthcare_etl_pipeline` DLT pipeline | `ETL/pipelines/silver/*` (DLT materialized views) |
| `gold_pipeline` | Triggered by `healthcare_etl_pipeline` DLT pipeline | `ETL/pipelines/gold/*` (DLT materialized views) |
| `ml_retrain_job` | `databricks bundle run ml_retrain_job -t dev --profile dev` | `src/scripts/maybe_retrain_model.py` |
| `rag_vector_index_job` | `databricks bundle run rag_vector_index_job -t dev --profile dev` | `src/scripts/create_vector_index.py` |
| `analytics_job` | (Part of `analytics_observability_job`) | `src/scripts/build_observability.py` + `src/scripts/build_analytics.py` + `src/scripts/verify_silver.py` |

### ETL Pipeline (DLT)

The Bronze, Silver, and Gold pipelines are **not separate jobs** -- they are three stages of a single DLT pipeline defined in `services/etl/resources/etl.pipeline.yml`:

| Property | Value |
|---|---|
| Pipeline name | `[${bundle.target}] Healthcare ETL Pipeline` |
| Target catalog | `${var.catalog}` |
| Target schema | `${var.gold_schema}` |
| Serverless | `true` |
| Channel | `current` |
| Event log | `${var.catalog}.${var.analytics_schema}.etl_pipeline_event_log` |

### Analytics + Observability Job

Defined in `services/etl/resources/analytics_observability.job.yml`. Three sequential tasks:

| Task Key | Script | Dependencies |
|---|---|---|
| `build_observability` | `src/scripts/build_observability.py` | (none) |
| `build_analytics` | `src/scripts/build_analytics.py` | `build_observability` |
| `build_quality_assets` | `src/scripts/verify_silver.py` | `build_analytics` |

Parameters: `upstream_status` (default: `success`), `parent_job_name` (default: `manual`), `parent_run_id` (default: `0`), `pipeline_stage` (default: `etl`).

### Setup Infrastructure Job

Defined in `services/infrastructure/setup/resources/setup_infrastructure.job.yml`. Conditional sample data loading via `load_sample_data` parameter (default: `"false"`).

### ML Retrain Job

Defined in `services/ml/training/resources/training.job.yml`. Calls `maybe_retrain_model.py` with:

| Parameter | Value |
|---|---|
| `--catalog` | `${var.catalog}` |
| `--gold-table` | `${var.catalog}.${var.gold_schema}.claim_features` |
| `--registered-model-name` | `${var.catalog}.${var.ml_schema}.claim_denial_model` |
| `--champion-alias` | `champion` |
| `--optuna-trials` | `50` |
| `--random-seed` | `42` |

Job environment includes ML dependencies: `xgboost>=2.0,<3.0`, `lightgbm>=4.2,<5.0`, `catboost>=1.2,<2.0`, `scikit-learn>=1.5,<2.0`, `shap>=0.44,<1.0`, `optuna>=3.6,<4.0`, `mlflow>=3.0,<4.0`, `imbalanced-learn>=0.12,<1.0`.

### RAG Vector Index Job

Defined in `services/rag/vector_index/resources/vector_index.job.yml`. Calls `create_vector_index.py` with two table parameters:

| Parameter | Value |
|---|---|
| `--mv-source-table` | `${var.catalog}.${var.gold_schema}.policy_chunks` |
| `--source-table` | `${var.catalog}.${var.gold_schema}.policy_chunks_vs` |
| `--endpoint-name` | `${var.vector_search_endpoint_name}` |
| `--index-name` | `${var.vector_search_index_name}` |
| `--embedding-column` | `embedding_vector` |

This keeps Lakeflow ownership on the Gold MV while giving Vector Search a CDF-backed Delta Sync source.

### Common Invocation Pattern

```bash
# Dev target
databricks bundle run <job_key> -t dev --profile dev

# Prod target
databricks bundle run <job_key> -t prod --profile prod
```

### Full ETL + ML Pipeline Run

```bash
# Step 1: Infrastructure (one-time or when schemas change)
databricks bundle run setup_infrastructure -t dev --profile dev

# Step 2: Load sample data (if not loaded)
databricks bundle run setup_infrastructure -t dev --profile dev \
  --conf load_sample_data=true

# Step 3: Run ETL pipeline (Bronze -> Silver -> Gold)
databricks bundle run etl_fast_dev_job -t dev --profile dev

# Step 4: Retrain model (gate-checked)
databricks bundle run ml_retrain_job -t dev --profile dev

# Step 5: Create/sync vector index
databricks bundle run rag_vector_index_job -t dev --profile dev

# Step 6: Build analytics + observability
databricks bundle run analytics_observability_job -t dev --profile dev
```
