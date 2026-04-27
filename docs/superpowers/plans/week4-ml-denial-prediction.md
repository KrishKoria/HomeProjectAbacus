# Week 4 Implementation Plan: ML Denial Prediction

> **STATUS (post-execution, 2026-04-27):** Waves 1-5 are implemented and 93 tests pass (1 skipped). Wave 3 was a no-op — `build_gold_claim_denial_features` was never present in `src/analytics/claims_analytics.py`. A defect-cleanup follow-up wave (see §11) was completed alongside the original waves: 30-day window unit fix, `diagnosis_count` partition fix, target-leak surface drops, blocking release gate, and `Recall@HIGH` evaluation metric. The plan below is preserved for historical context.

## 1. Project Context

**Project**: AI-Powered Claim Denial Prevention & Remediation System
**Root**: `/Users/krish.koria/Documents/projects/HomeProjectAbacus`
**Current Phase**: Weeks 1-3 code-ready (Bronze ingestion + Silver processing + analytics)
**This Phase**: Week 4 — Gold feature layer + ML model training

### Current State (at plan-write time)

- Bronze: 5 Delta tables via SDP pipelines (`healthcare.bronze.{claims,providers,diagnosis,cost,policies}`)
- Silver: 5 trusted + quarantine tables (`healthcare.silver.{claims,providers,diagnosis,cost,policy_chunks}`)
- Claims dataset: 1000 rows with synthetic labels (`is_denied`, `denial_reason_code`, `claim_status`, etc.)
- Analytics: `src/analytics/claims_analytics.py` with dashboard builders (no Gold-feature builder; Wave 3 became a no-op)
- Tests: 71 passing baseline, 1 skipped (post-execution, with Gold + ML contract tests landed: 93 passing)
- Local dev: `local_dev/` workflow with local Bronze fallback and Streamlit dashboard

### Key Files

| Path                                         | Role                                                                     |
| -------------------------------------------- | ------------------------------------------------------------------------ |
| `ETL/pipelines/bronze/`                      | Bronze SDP pipelines (5 files)                                           |
| `ETL/pipelines/silver/`                      | Silver SDP pipelines (5 files)                                           |
| `src/common/`                                | Shared config, cleaning, observability, diagnostics, PHI registry        |
| `ETL/common/`                                | Thin proxy → `src/common/` (each file is `from src.common.xxx import *`) |
| `src/analytics/claims_analytics.py`          | Analytics builders + basic Gold features (to be refactored)              |
| `scripts/generate_synthetic_claim_labels.py` | Deterministic rule-based label generator                                 |
| `tests/`                                     | 3 test files: dataset, observability, silver contracts                   |
| `pyproject.toml`                             | Dependencies: pandas, streamlit, pyarrow, altair, pdfplumber             |

---

## 2. Requirements

### From WEEK4.md

- Step 1: Join claims + provider + diagnosis + cost → `gold_claim_base`
- Step 2: Feature engineering (cost, provider, diagnosis, claim features) → `gold_claim_features`
- Step 3: Dataset preparation (nulls, encoding, train/test split 70/30)
- Step 4: Train model (Logistic Regression + XGBoost)
- Step 5: Predict (denial probability)
- Step 6: Evaluate (accuracy, precision, recall, ROC-AUC)
- Step 7: Save model (`claim_denial_model.pkl`)

### From ARCHITECTURE.md §12.4 (Gold Layer)

- Join all four Silver tables
- Compute 8 denial risk features
- Rule-based `denial_label` as proxy training target
- Write to `healthcare.gold.claim_features`

### From ARCHITECTURE.md §9.3 (Derived Features)

| #   | Feature                        | Derivation                                      |
| --- | ------------------------------ | ----------------------------------------------- |
| 1   | `is_procedure_missing`         | procedure_code IS NULL                          |
| 2   | `is_amount_missing`            | billed_amount IS NULL                           |
| 3   | `amount_to_benchmark_ratio`    | billed_amount / expected_cost                   |
| 4   | `severity_procedure_mismatch`  | High-severity dx + low-cost procedure           |
| 5   | `specialty_diagnosis_mismatch` | Provider specialty ≠ diagnosis category         |
| 6   | `provider_location_missing`    | location IS NULL                                |
| 7   | `claim_frequency`              | Count claims per provider last 30 days (window) |
| 8   | `diagnosis_severity`           | From diagnosis.severity (High=1, Low=0)         |

### From WEEK4.md (Additional Features)

- `billed_vs_avg_cost` (billed vs average_cost)
- `high_cost_flag` (billed > threshold × expected)
- `provider_claim_count` (total claims per provider)
- `provider_risk_score` (denied/total ratio per provider)
- `diagnosis_count` (distinct diagnoses per claim)

### From ARCHITECTURE.md §13 (ML Spec)

- Models: Logistic Regression (baseline), XGBoost (primary)
- XGBoost params: max_depth=6, lr=0.1, early_stopping_rounds=50
- SHAP explanations bundled with model artifact
- MLflow model registry
- Optuna hyperparameter tuning
- Evaluation gates: Recall@HIGH > 0.80, Precision > 0.70, AUC-ROC > 0.85
- Model prediction latency: < 150ms p95

### Target Column

`is_denied` (1 = denied, 0 = approved) — already synthetic proxy labels from `generate_synthetic_claim_labels.py`

---

## 3. Decisions Locked In (from Grill Session)

| #   | Decision                                                            | Rationale                                                                             |
| --- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 1   | Gold pipeline as SDP at `ETL/pipelines/gold/gold_claim_features.py` | Matches existing Silver SDP pattern; Unity Catalog governance; pipeline observability |
| 2   | ML training: Databricks-native (SparkML + MLflow)                   | Production target per ARCHITECTURE.md; proper model registry and serving path         |
| 3   | Dependencies in `[dependency-groups] ml`                            | Keeps pyproject.toml organized; opt-in for local dev; mirrors `dev` group pattern     |
| 4   | Features: ALL including aggregations                                | Provider_claim_count_30d and provider_risk_score are real denial signals              |
| 5   | Hyperparameters: Optuna with 50 trials                              | ARCHITECTURE.md explicitly requires it; reusable when real data arrives               |
| 6   | Remove `build_gold_claim_denial_features` from analytics            | One source of truth; Gold belongs in ETL/pipelines not analytics                      |

---

## 4. Codebase Patterns (MUST FOLLOW)

### Import Pattern

```python
from __future__ import annotations
```

### ETL Common Proxy Pattern

```python
# ETL/common/gold_pipeline_config.py (1 line)
from src.common.gold_pipeline_config import *  # noqa: F401,F403
```

### Pipeline Imports

```python
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.gold_pipeline_config import (
    GOLD_SCHEMA_DEFAULT,
    gold_table_name,
    gold_table_properties,
    read_silver_snapshot,
)
from common.bronze_pipeline_config import CATALOG_DEFAULT
from common.silver_pipeline_config import SILVER_SCHEMA_DEFAULT, silver_table_name
from common.observability import (
    LOG_CATEGORY_SILVER_PIPELINE,  # or create LOG_CATEGORY_GOLD_PIPELINE
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,  # or create GOLD equivalent
)
```

### Pipeline Decorator Pattern (from Silver)

```python
@dp.materialized_view(
    name="claims_feature_base",
    private=True,
    comment="Private intermediate: joined claim features",
    table_properties=gold_table_properties("SENSITIVE", SENSITIVE_COLUMNS),
)
def _claims_feature_base():
    """Build the shared joined claims rows used by feature output."""
    claims = read_silver_snapshot(spark, SILVER_CLAIMS_TABLE)
    providers = read_silver_snapshot(spark, SILVER_PROVIDERS_TABLE)
    diagnosis = read_silver_snapshot(spark, SILVER_DIAGNOSIS_TABLE)
    cost = read_silver_snapshot(spark, SILVER_COST_TABLE)
    # ... joins and feature engineering ...
    return result

@dp.table(
    name=GOLD_CLAIM_FEATURES_TABLE,
    cluster_by=["claim_id", "date"],
    comment="Gold claim features table ready for ML training",
    table_properties=gold_table_properties("SENSITIVE", SENSITIVE_COLUMNS),
)
def gold_claim_features():
    """Emit the Gold claim features table."""
    return spark.read.table("claims_feature_base")
```

### Table Naming Pattern

```python
# {catalog}.{schema}.{table}
silver_table_name(CATALOG_DEFAULT, "claims", SILVER_SCHEMA_DEFAULT)  # healthcare.silver.claims
gold_table_name(CATALOG_DEFAULT, "claim_features", GOLD_SCHEMA_DEFAULT)  # healthcare.gold.claim_features
```

### Test Pattern (from test_silver_contract.py)

```python
import ast, unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# ... imports from common modules ...
# ... AST-based contract tests for pipeline structure ...
# ... unit tests for helper functions ...
# ... integration test classes ...
```

---

## 5. Implementation Plan — Waves

### Wave 1: Infrastructure (Independent, parallel)

#### Task 1a: Create Gold pipeline config module

**Files to create:**

- `src/common/gold_pipeline_config.py` — Gold schema constants, table naming, table properties, snapshot reader
- `ETL/common/gold_pipeline_config.py` — Thin proxy (1 line)

**Content needed:**

- `GOLD_SCHEMA_DEFAULT = "gold"`
- `gold_table_name(catalog, table_name, schema)` → `{catalog}.{schema}.{table_name}`
- `gold_table_properties(sensitivity, phi_columns)` → adds `claimops.layer = "gold"` to common props
- `read_silver_snapshot(spark, table_name)` → `spark.read.table(table_name)` (batch snapshot for Gold materialization)
- `GOLD_AUDIT_COLUMNS` — audit timestamp columns for Gold layer
- New observability log categories and message templates for Gold layer

**Pattern to follow**: `src/common/silver_pipeline_config.py` (lines 1-94) — exact same structure

**CRITICAL: Also update these observability files (discovered during verification):**

- `src/common/log_categories.py` — Add `LOG_CATEGORY_GOLD_PIPELINE = "gold_pipeline"`, update `LOG_CATEGORIES` tuple and `__all__`
- `src/common/log_messages.py` — Add `MESSAGE_TEMPLATE_GOLD_TABLE_READY = "Gold dataset ready: table={table_name} category={category} sensitivity={sensitivity}"`, add `render_gold_table_ready()` function, update `__all__`
- `src/common/observability.py` — Export new Gold log category and message template, update `__all__`
- `ETL/common/log_categories.py` — Add proxy line for new constant (but this is just `from src.common.log_categories import *` which already re-exports everything)
- `ETL/common/log_messages.py` — Same proxy pattern, already catches new exports via wildcard import
- `ETL/common/observability.py` — Same, already catches new exports

**Verification**: Import works, constants are stable, proxy resolves correctly

#### Task 1b: Add ML dependencies to pyproject.toml

**File to modify**: `pyproject.toml`

**Changes:**

```toml
[dependency-groups]
dev = ["pytest>=8.3.5,<9.0.0", "reportlab>=4.4,<5.0"]
ml = ["xgboost>=2.0,<3.0", "scikit-learn>=1.5,<2.0", "shap>=0.44,<1.0", "mlflow>=2.14,<3.0", "optuna>=3.6,<4.0"]
```

**Verification**: `uv sync --group ml` installs all packages, imports work

---

### Wave 2: Gold SDP Pipeline (Depends on Wave 1a)

#### Task 2: Create Gold claim features pipeline

**File to create**: `ETL/pipelines/gold/gold_claim_features.py`

**What it does:**

1. `@dp.materialized_view` (private): Join all 4 Silver tables, compute raw columns
2. `@dp.materialized_view` (private): Compute aggregation features (provider_claim_count_30d, provider_risk_score) using Spark window functions
3. `@dp.table`: Final feature table with all 12+ features including:
   - `claim_id`, `patient_id`, `provider_id`, `diagnosis_code`, `procedure_code`, `billed_amount`, `date`
   - `is_procedure_missing` (boolean)
   - `is_amount_missing` (boolean)
   - `amount_to_benchmark_ratio` (double)
   - `billed_vs_avg_cost` (double — billed_amount / average_cost)
   - `high_cost_flag` (boolean — ratio > threshold)
   - `severity_procedure_mismatch` (boolean)
   - `specialty_diagnosis_mismatch` (boolean)
   - `provider_location_missing` (boolean)
   - `diagnosis_severity_encoded` (int: High=1, Low=0)
   - `diagnosis_count` (int — distinct diagnoses per claim)
   - `provider_claim_count` (int — total claims per provider)
   - `provider_claim_count_30d` (int — window function)
   - `provider_risk_score` (double — denied/total per provider)
   - `denial_label` (int — is_denied as integer target)

**Key technical details:**

- `provider_claim_count_30d`: Window function using `Window.partitionBy("provider_id").orderBy("date").rangeBetween(-30*86400, 0)` with `F.count("*").over(window)`
- `provider_risk_score`: Aggregate denied/total ratio per provider, joined back via broadcast
- All features are nullable-safe (handle NULL procedure_code, NULL billed_amount gracefully)
- PHI columns flagged in table properties
- `cluster_by=["claim_id"]` for query performance
- SENSITIVE data classification (derived from PHI but not PHI)

**Pattern to follow**: `ETL/pipelines/silver/silver_claims.py` (lines 1-305) — same decorators, structure, PHI-safe logging

**Verification**: Pipeline parses without errors, all imports resolve, decorator configuration is valid

---

### Wave 3: Analytics Cleanup (Depends on Wave 2)

#### Task 3: Remove old Gold features from analytics

**File to modify**: `src/analytics/claims_analytics.py`

**Changes:**

1. Remove `build_gold_claim_denial_features()` function (lines 527-553)
2. Remove `"gold_claim_denial_features"` from `DASHBOARD_SOURCE_TABLES` tuple (line 44)
3. Remove `gold_claim_denial_features` from `build_and_persist_claims_assets()` outputs dict (lines 615-621) and from the `_build_claims_provider_cost_enriched` + `mismatch` calls that feed it
4. Keep `__all__` updated — remove `"build_gold_claim_denial_features"`

**Verification**: All existing tests still pass, analytics module imports cleanly, no references to removed function remain

---

### Wave 4: ML Package (Depends on Waves 1b, 2)

#### Task 4a: Create ML features module

**File to create**: `src/ml/__init__.py`
**File to create**: `src/ml/features.py`

**Content**: Feature preparation helpers

- `load_gold_features(spark, catalog, gold_schema)` → DataFrame from Gold table
- `prepare_training_data(df)` → handle nulls (impute/fill), encode categoricals, return X (features matrix) and y (target vector)
- `feature_names()` → list of feature column names (for SHAP/explainability)
- `train_test_split(df, test_size=0.3, stratify=True)` → stratified split on denial_label

#### Task 4b: Create ML training module

**File to create**: `src/ml/train.py`

**Content**: Model training pipeline

- `train_logistic_regression(X_train, y_train)` → LogisticRegression with class_weight='balanced'
- `train_xgboost(X_train, y_train, X_val, y_val)` → XGBClassifier with early stopping
- `tune_xgboost_optuna(X_train, y_train, n_trials=50)` → Optuna study tuning max_depth, learning_rate, n_estimators, subsample, colsample_bytree, scale_pos_weight
- `train_with_mlflow(model, params, metrics)` → MLflow experiment tracking
- `save_model(model, path)` → pickle + MLflow artifact

#### Task 4c: Create ML evaluation module

**File to create**: `src/ml/evaluate.py`

**Content**: Model evaluation

- `evaluate_model(model, X_test, y_test)` → dict with accuracy, precision, recall, f1, roc_auc
- `compute_shap_values(model, X_sample)` → SHAP explanation object
- `plot_feature_importance(shap_values, feature_names)` → SHAP summary plot
- `plot_confusion_matrix(y_true, y_pred)` → confusion matrix visualization
- `generate_evaluation_report(metrics, shap_values)` → structured report dict

#### Task 4d: Create prediction module

**File to create**: `src/ml/predict.py`

**Content**: Model inference

- `load_trained_model(path)` → load pickle/MLflow model
- `predict_single(feature_dict)` → denial probability + risk level
- `predict_batch(feature_df)` → batch predictions with probabilities
- `RiskLevel` enum: LOW (<0.3), MEDIUM (0.3-0.7), HIGH (>0.7)

#### Task 4e: Create CLI training script

**File to create**: `scripts/train_denial_model.py`

**Content**: Entry point for model training

- Parse args: --gold-table, --catalog, --model-output, --tune/--no-tune
- Orchestrate: load features → prepare → split → tune → train → evaluate → save
- Print evaluation metrics table to stdout
- Save model to specified path

**Verification**: Script runs end-to-end, produces model file, prints metrics meeting thresholds

---

### Wave 5: Tests (Depends on Wave 2, 4)

#### Task 5a: Gold pipeline contract tests

**File to create**: `tests/test_gold_contract.py`

**Test cases:**

1. `test_gold_config_constants_are_stable` — GOLD_SCHEMA_DEFAULT, audit columns, table naming
2. `test_gold_table_properties_include_layer_metadata` — `claimops.layer = "gold"` present
3. `test_gold_pipeline_uses_silver_snapshots_not_bronze` — pipeline reads from silver, not bronze
4. `test_gold_features_include_all_required_columns` — all 12+ features present in output schema
5. `test_gold_pipeline_uses_private_materialized_views` — AST check for @dp.materialized_view with private=True
6. `test_gold_pipeline_decorators_are_valid` — @dp.table and @dp.materialized_view present
7. `test_gold_feature_table_clustered_by_claim_id` — cluster_by includes claim_id
8. `test_gold_pipeline_phi_safe_log_messages` — no patient values in log templates
9. `test_gold_read_silver_snapshot_uses_batch_mode` — reads are batch (spark.read.table), not streaming

#### Task 5b: ML contract tests

**File to create**: `tests/test_ml_contract.py`

**Test cases:**

1. `test_feature_preparation_handles_nulls` — null values produce valid features, not exceptions
2. `test_train_test_split_maintains_stratification` — class balance preserved in split
3. `test_model_training_converges` — XGBoost trains without error, produces valid model
4. `test_model_predictions_in_valid_range` — all probabilities in [0, 1]
5. `test_risk_level_classification_correct` — LOW < 0.3, MEDIUM 0.3-0.7, HIGH > 0.7
6. `test_model_evaluation_metrics_computed` — all metrics present and in valid range
7. `test_shap_values_match_feature_count` — SHAP values array matches feature count
8. `test_mlflow_tracking_configured` — MLflow URI set, experiment created
9. `test_model_save_and_load_roundtrip` — saved model produces same predictions as original
10. `test_prediction_latency_under_150ms` — single prediction < 150ms

**Verification**: All tests pass, no regressions on existing 53 tests

---

### Wave 6: End-to-End Verification (Depends on all)

#### Task 6: Full verification

1. Run `uv sync --group ml` — install dependencies
2. Run `uv run pytest -q` — all tests pass (existing 53 + new ~20 = ~73 tests)
3. Run Gold pipeline locally: create a local Spark session, execute the pipeline functions, verify output
4. Run ML training: `uv run python scripts/train_denial_model.py`
5. Verify model metrics meet thresholds: Accuracy > 80%, Recall > 80%, ROC-AUC > 0.85
6. Verify model file exists and can be loaded for prediction
7. Run `lsp_diagnostics` on all new/changed files — clean

---

## 6. File Manifest

### New Files (10)

| File                                        | Wave | Purpose                                                   |
| ------------------------------------------- | ---- | --------------------------------------------------------- |
| `src/common/gold_pipeline_config.py`        | 1a   | Gold layer constants, naming, properties, snapshot reader |
| `ETL/common/gold_pipeline_config.py`        | 1a   | Thin proxy → src                                          |
| `ETL/pipelines/gold/gold_claim_features.py` | 2    | Gold SDP pipeline with full feature engineering           |
| `src/ml/__init__.py`                        | 4a   | ML package init                                           |
| `src/ml/features.py`                        | 4a   | Feature preparation, encoding, train/test split           |
| `src/ml/train.py`                           | 4b   | XGBoost + LogReg training, Optuna tuning, MLflow          |
| `src/ml/evaluate.py`                        | 4c   | Metrics, SHAP, confusion matrix, reports                  |
| `src/ml/predict.py`                         | 4d   | Model inference, risk classification                      |
| `scripts/train_denial_model.py`             | 4e   | CLI entry point for training                              |
| `tests/test_gold_contract.py`               | 5a   | Gold pipeline contract tests                              |
| `tests/test_ml_contract.py`                 | 5b   | ML pipeline contract tests                                |

### Modified Files (5)

| File                                | Wave | Changes                                                                                                                 |
| ----------------------------------- | ---- | ----------------------------------------------------------------------------------------------------------------------- |
| `pyproject.toml`                    | 1b   | Add `[dependency-groups] ml` with xgboost, scikit-learn, shap, mlflow, optuna                                           |
| `src/analytics/claims_analytics.py` | 3    | Remove `build_gold_claim_denial_features`, update `DASHBOARD_SOURCE_TABLES`, update `build_and_persist_claims_assets()` |
| `src/common/log_categories.py`      | 1a   | Add `LOG_CATEGORY_GOLD_PIPELINE`, update `LOG_CATEGORIES` tuple and `__all__`                                           |
| `src/common/log_messages.py`        | 1a   | Add `MESSAGE_TEMPLATE_GOLD_TABLE_READY`, add `render_gold_table_ready()`, update `__all__`                              |
| `src/common/observability.py`       | 1a   | Export new Gold log category and message template, update `__all__`                                                     |

**Notes on analytics cleanup (Wave 3):**

- `_build_claims_provider_cost_enriched` must STAY — it's used by `build_high_cost_claims_summary`, `build_claims_dashboard_summary`, `build_silver_claims_cost_enriched`, and `build_claims_provider_specialty_mismatch`
- `build_claims_provider_specialty_mismatch` and its cached `mismatch` variable must STAY — used by the specialty mismatch analytics table
- Only remove: `build_gold_claim_denial_features` function (lines 527-553), its entry in `DASHBOARD_SOURCE_TABLES` (line 43 `"gold_claim_denial_features"`), its entry in `build_and_persist_claims_assets()` outputs dict (lines 615-621), and its export in `__all__` (line 650 `"build_gold_claim_denial_features"`)

**PHI columns for Gold table (discovered during verification):**

- `patient_id` — PHI (must be in `hipaa.phi_columns`)
- `billed_amount` — PHI
- `diagnosis_code` — PHI
- Table sensitivity: `SENSITIVE` (derived features are not PHI, but source columns are)
- `cluster_by=["claim_id"]` for query performance

---

## 7. Delegation Categories + Skills

Each task should be delegated to category-optimized subagents:

| Wave | Task              | Category | Skills                                                                                                           |
| ---- | ----------------- | -------- | ---------------------------------------------------------------------------------------------------------------- |
| 1a   | Gold config       | `quick`  | `[]` — simple config module, matches existing pattern                                                            |
| 1b   | Dependencies      | `quick`  | `[]` — single file edit                                                                                          |
| 2    | Gold pipeline     | `deep`   | `[spark-python-data-source, databricks-spark-structured-streaming]` — complex SDP pipeline with window functions |
| 3    | Analytics cleanup | `quick`  | `[]` — surgical removal, update references                                                                       |
| 4a   | ML features       | `deep`   | `[]` — Spark DataFrame operations, null handling                                                                 |
| 4b   | ML training       | `deep`   | `[]` — XGBoost, Optuna, MLflow integration                                                                       |
| 4c   | ML evaluation     | `deep`   | `[]` — SHAP, metrics, visualization                                                                              |
| 4d   | ML predict        | `quick`  | `[]` — model loading, simple inference                                                                           |
| 4e   | CLI script        | `quick`  | `[]` — argparse, orchestration                                                                                   |
| 5a   | Gold tests        | `quick`  | `[]` — contract tests following existing patterns                                                                |
| 5b   | ML tests          | `deep`   | `[]` — ML-specific test patterns                                                                                 |
| 6    | Verification      | `quick`  | `[]` — run commands, verify output                                                                               |

---

## 8. Non-Functional Requirements

### Security

- Gold tables carry `SENSITIVE` classification (derived from PHI)
- PHI columns flagged in `hipaa.phi_columns` table property
- No PHI in log messages (only claim_id, provider_id as identifiers)
- Audit columns: `_gold_processed_at` timestamp
- ML model contains no raw data — only numerical features and weights

### Cost

- Gold pipeline: ~30 seconds for 1000 claims on minimal cluster
- XGBoost training: ~2-5 minutes with Optuna 50 trials on CPU
- Model size: ~100KB (negligible storage)
- No LLM/GPU costs for Week 4

### Speed/Latency

- Gold pipeline: < 30 seconds (batch)
- Feature engineering: vectorized Spark ops, < 5 seconds
- XGBoost inference: < 10ms per prediction (single claim)
- Batch prediction (100 claims): < 100ms
- All well within 150ms p95 target

### Compliance

- Gold Delta tables: CDF enabled, 2190-day log retention, 2190-day deleted file retention
- `claimops.layer = "gold"` table property for catalog governance
- Data lineage: Gold reads Silver → Silver reads Bronze → Bronze reads CSV
- Model audit: predictions logged with claim_id, model_version, timestamp

---

## 9. Risk Register

| Risk                                                                               | Likelihood | Impact | Mitigation                                                                        |
| ---------------------------------------------------------------------------------- | ---------- | ------ | --------------------------------------------------------------------------------- |
| Spark window functions fail with NULL dates                                        | Medium     | Medium | Filter NULL dates before window computation; use `rangeBetween` not `rowsBetween` |
| Optuna tuning overfits on 1000 rows                                                | Medium     | Low    | Use 5-fold cross-validation; early stopping; holdout test set                     |
| SHAP computation slow on full dataset                                              | Low        | Low    | Use sample of test data (200 rows) for SHAP                                       |
| Existing analytics consumers break after removing build_gold_claim_denial_features | Low        | High   | Verify no imports/references remain via grep before removing                      |
| Local Spark session fails for Gold pipeline                                        | Medium     | Medium | Use same local Spark setup pattern as silver tests                                |

---

## 10. Verification Findings (Post-Plan Review)

### Gap 1: Missing Gold observability constants

**Found**: `src/common/log_categories.py` and `src/common/log_messages.py` have no Gold-specific entries. The Silver pipeline uses `LOG_CATEGORY_SILVER_PIPELINE` and `MESSAGE_TEMPLATE_SILVER_TABLE_READY`. Gold needs equivalents.

**Fix**: Added to Task 1a — must update `log_categories.py` (add `LOG_CATEGORY_GOLD_PIPELINE`), `log_messages.py` (add `MESSAGE_TEMPLATE_GOLD_TABLE_READY` + `render_gold_table_ready()`), and `observability.py` (export both).

### Gap 2: `read_silver_snapshot` placement is in silver config, not separate

**Found**: `read_bronze_snapshot` lives in `silver_pipeline_config.py`, not `bronze_pipeline_config.py`. The pattern is that the consuming layer defines the read function for the source layer.

**Decision**: `gold_pipeline_config.py` should define its own `read_silver_snapshot(spark, table_name)` → `spark.read.table(table_name)` for symmetry. This mirrors how `silver_pipeline_config.py` defines `read_bronze_snapshot`.

### Gap 3: ETL proxy files for new observability modules

**Found**: `ETL/common/observability.py` and `ETL/common/log_categories.py` and `ETL/common/log_messages.py` all use `from src.common.xxx import *` wildcard imports, so they automatically pick up new exports. No explicit proxy lines needed — just make sure `src/common/` has the new exports in `__all__`.

**Decision**: No explicit ETL proxy file changes needed for observability. The wildcard imports already cover new exports.

### Gap 4: PHI columns in Gold table properties

**Found**: The Gold feature table will contain `patient_id`, `billed_amount`, and `diagnosis_code` — all classified as PHI per `src/common/bronze_sources.py`. The table sensitivity should be `SENSITIVE` (derived data) with PHI columns listed in `hipaa.phi_columns`.

**Decision**: `gold_table_properties("SENSITIVE", PHI_COLUMNS)` where `PHI_COLUMNS = ("billed_amount", "diagnosis_code", "patient_id")` — matching the existing PHI classification from the Bronze source metadata.

### Gap 5: `_build_claims_provider_cost_enriched` must not be removed

---

## 11. Defect Cleanup (post-execution)

After the original Waves 1-5 landed in tree, a verification pass surfaced six concrete defects, three architecture deviations, and a handful of coding-standard nits. All were resolved in the same follow-up commit.

### Implementation defects fixed

- **30-day provider window** (`ETL/pipelines/gold/gold_claim_features.py`): cast `date` through `timestamp` before `cast("long")` so the `rangeBetween(-30 * 86400, 0)` units (seconds) match the column. Previously `provider_claim_count_30d` was effectively unbounded and identical to `provider_claim_count`.
- **`diagnosis_count` partition** (same file): re-partition the window by `provider_id` only. Partitioning by `(provider_id, diagnosis_code)` and then taking `approx_count_distinct("diagnosis_code")` is always 1.
- **Target-leak surface** (same file): explicitly drop `claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`, `follow_up_required` inside `_claims_feature_base` so any future widening of the final `select` cannot leak the label into the feature surface.
- **`XGBoost_DEFAULT_PARAMS` casing** (`src/ml/train.py`): renamed to `XGBOOST_DEFAULT_PARAMS` and added `Final` typing. Default params now include `scale_pos_weight=2.5` so the `--no-tune` path matches §13.
- **Bronze CSV fallback** (`scripts/train_denial_model.py`): replaced silent fallback to `datasets/claims_1000.csv` (Bronze, no engineered features) with an explicit `--gold-csv` flag and clear error messages.
- **Release gate** (same file): `meets_thresholds()` now drives a `sys.exit(1)` and skips `pickle.dump` so failing models are not persisted under the production artifact path.

### Architecture alignment

- **`PHI_COLUMNS_GOLD`** widened to `(billed_amount, date, diagnosis_code, is_denied, patient_id)` to match the bronze authority for the columns that flow through the Gold final select.
- **§13 Recall@HIGH**: `EvaluationMetrics` gained a `recall_at_high` field, `evaluate.py` exposes a `recall_at_high()` helper plus `HIGH_RISK_PROBABILITY_THRESHOLD` and `DEFAULT_MIN_*` constants, and `meets_thresholds()` now gates on Recall@HIGH (not global recall) per the spec.
- **Risk-tier alignment**: `src/ml/predict.py` extracts `RISK_THRESHOLD_LOW = 0.3`, `RISK_THRESHOLD_HIGH = 0.7`, and `LATENCY_BUDGET_MS = 150.0`; a contract test asserts the inference threshold matches the evaluation threshold.
- **Latency observability**: `predict_single` emits a `logger.debug` line with `latency_ms` so production monitoring can track the §13 < 150 ms p95 budget.
- **MLflow scope clarified** in ARCHITECTURE.md §13.3: v1 uses experiment tracking + artifact logging; the train-script gate is the promotion barrier.

### Coding-standard fixes

- `__all__` re-sorted in `src/common/log_categories.py` and `src/common/log_messages.py`.
- Public functions across `src/ml/` got one-line docstrings explaining their role.
- Heavy third-party imports (`sklearn.model_selection`, `pickle`) lifted to module top.
- Bare `except Exception` blocks replaced with `logger.warning(..., exc_info=True)`.

### New regression tests (added to `tests/test_gold_contract.py` and `tests/test_ml_contract.py`)

- `test_gold_pipeline_30d_window_casts_date_through_timestamp` — guards the unit fix.
- `test_gold_pipeline_diagnosis_count_partition_is_provider_only` — guards the partition fix.
- `test_gold_pipeline_drops_target_leak_columns` — guards the leakage drops.
- `test_recall_at_high_only_counts_high_tier_positives` and `test_recall_at_high_returns_zero_when_no_positives` — unit tests for the new metric.
- `test_meets_thresholds_uses_recall_at_high_not_global_recall` — locks in the gate semantics.
- `test_risk_thresholds_align_with_evaluate_module` — keeps inference and evaluation thresholds in sync.
- `test_main_exits_nonzero_and_skips_save_when_metrics_fail` — proves the release gate blocks promotion.

### Verification

`uv sync --group ml && uv run pytest -q` → **93 passed, 1 skipped**.
