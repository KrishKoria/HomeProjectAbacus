# Chapter 6: Configuration Reference

This is **the** lookup chapter for every constant, threshold, environment variable, and magic value in the project.

---

## 6.1 Bronze Configuration

**File:** `src/common/bronze_pipeline_config.py`

| Constant / Function | Value / Signature | Line |
|---|---|---|
| `CATALOG_DEFAULT` | `"healthcare"` | 18 |
| `BRONZE_SCHEMA_DEFAULT` | `"bronze"` | 19 |
| `BRONZE_VOLUME_DEFAULT` | `"raw_landing"` | 20 |
| `BRONZE_VOLUME_ROOT` | `"/Volumes/healthcare/bronze/raw_landing"` | 21 |
| `RESCUED_DATA_COLUMN` | `"_rescued_data"` | 14 |
| `PIPELINE_RUN_ID_FORMAT` | `"yyyyMMdd_HHmmss"` | 15 |
| `PIPELINE_RUN_ID_CONF` | `"claimops.pipeline_run_id"` | 16 |
| `AUDIT_COLUMNS` | `("_ingested_at", "_source_file", "_pipeline_run_id")` | 9-13 |

### COMMON_DELTA_TABLE_PROPERTIES

| Property | Value |
|---|---|
| `delta.enableChangeDataFeed` | `"true"` |
| `delta.enableDeletionVectors` | `"true" |
| `delta.enableRowTracking` | `"true"` |
| `delta.logRetentionDuration` | `"interval 2190 days"` |
| `delta.deletedFileRetentionDuration` | `"interval 2190 days"` |

Defined at line 23-29.

### Functions

| Function | Signature | Returns | Lines |
|---|---|---|---|
| `table_name(catalog, schema, table)` | `str, str, str` | `f"{catalog}.{schema}.{table}"` | 32-34 |
| `bronze_table_name(table, *, catalog, schema)` | `str, **str` | FQN via `table_name()` | 37-44 |
| `bronze_volume_root(*, catalog, schema, volume)` | `**str` | `f"/Volumes/{catalog}/{schema}/{volume}"` | 47-54 |
| `bronze_volume_path(dataset_key, *, catalog, schema, volume)` | `str, **str` | Volume root + dataset folder | 57-65 |
| `csv_autoloader_options()` | none | Dict of Auto Loader CSV config | 68-76 |
| `binary_file_autoloader_options(path_glob_filter)` | `str` | Dict of binaryFile config (default `*.pdf`) | 94-99 |
| `stable_pipeline_run_id()` | none | Spark literal run ID | 79-91 |
| `table_properties_for_sensitivity(sensitivity, phi_columns)` | `str, Iterable[str]` | `COMMON_DELTA_TABLE_PROPERTIES` + `hipaa.*` keys | 102-110 |

### csv_autoloader_options() Output

| Key | Value |
|---|---|
| `cloudFiles.format` | `"csv"` |
| `header` | `"true"` |
| `cloudFiles.inferColumnTypes` | `"false"` |
| `cloudFiles.schemaEvolutionMode` | `"addNewColumns"` |
| `cloudFiles.rescuedDataColumn` | `"_rescued_data"` |

---

## 6.2 Silver Configuration

**File:** `src/common/silver_pipeline_config.py`

| Constant | Value | Line |
|---|---|---|
| `SILVER_SCHEMA_DEFAULT` | `"silver"` | 8 |
| `QUARANTINE_SCHEMA_DEFAULT` | `"quarantine"` | 9 |
| `ANALYTICS_SCHEMA_DEFAULT` | `"analytics"` | 10 |
| `SILVER_AUDIT_COLUMNS` | `("_data_quality_flags",)` | 12-14 |
| `QUARANTINE_AUDIT_COLUMNS` | `("diagnostic_id", "rule_name", "quarantine_reason")` | 15-19 |
| `POLICY_CHUNK_SIZE_TOKENS` | `512` | 21 |
| `POLICY_CHUNK_OVERLAP_TOKENS` | `64` | 22 |
| `MONEY_DECIMAL_PRECISION` | `18` | 23 |
| `MONEY_DECIMAL_SCALE` | `2` | 24 |
| `NON_PHI_TABLE_PROPERTIES` | `table_properties_for_sensitivity("NON-PHI")` | 26 |
| `PHI_TABLE_PROPERTIES` | `table_properties_for_sensitivity("PHI")` | 27 |
| `SENSITIVE_TABLE_PROPERTIES` | `table_properties_for_sensitivity("SENSITIVE")` | 28 |

### PHI_COLUMNS_SILVER

There is no standalone `PHI_COLUMNS_SILVER` constant. The Silver layer derives PHI columns from each dataset's `phi_columns` in `BRONZE_SOURCES` (`src/common/bronze_sources.py`). For claims, the PHI columns are:

| Column | PHI Basis (45 CFR) |
|---|---|
| `patient_id` | 164.514(b)(2)(ii) -- unique identifying code |
| `diagnosis_code` | 164.514(b)(2)(xvi) -- health condition linked to patient |
| `billed_amount` | 164.514(b)(2) -- financial health information |
| `date` | 164.514(b)(2)(iv) -- date of health service |
| `claim_status` | 164.514(b)(2) -- adjudication outcome |
| `denial_reason_code` | 164.514(b)(2) -- claims adjudication detail |
| `allowed_amount` | 164.514(b)(2) -- financial health information |
| `paid_amount` | 164.514(b)(2) -- financial health information |
| `is_denied` | 164.514(b)(2) -- adjudication outcome |
| `follow_up_required` | 164.514(b)(2) -- operational clinical signal |

The `providers`, `diagnosis`, and `cost` datasets have **no PHI columns** (they are operational reference data, not patient health information).

### Valid Code Lists

There are no standalone `valid_procedure_codes` or `valid_diagnosis_codes` constants in this project. Validation is performed at runtime against the Bronze reference tables:

- **Procedure codes:** validated against `bronze.cost` (6 rows: PROC1-PROC6, line 121-129 of `bronze_sources.py`)
- **Diagnosis codes:** validated against `bronze.diagnosis` (6 rows: D10-D60, lines 110-119 of `bronze_sources.py`)
- **Provider IDs:** validated against `bronze.providers` (21 rows, lines 100-108 of `bronze_sources.py`)

### Functions

| Function | Signature | Returns | Lines |
|---|---|---|---|
| `silver_table_name(catalog, table_name, schema)` | `str, str, str` | FQN: `f"{catalog}.{schema}.{table_name}"` | 31-33 |
| `quarantine_table_name(catalog, table_name, schema)` | `str, str, str` | FQN with quarantine schema | 36-42 |
| `create_required_schemas(spark, catalog, ...)` | `SparkSession, str, **str` | Creates schemas (idempotent) | 45-56 |
| `silver_table_properties(sensitivity, phi_columns)` | `str, Iterable[str]` | Delta props + `claimops.layer=silver` | 58-66 |
| `read_bronze_snapshot(spark, table_name)` | `SparkSession, str` | `spark.read.table(table_name)` | 69-71 |

### Silver Layer Table Properties

All Silver tables get `claimops.layer = "silver"` added on top of `COMMON_DELTA_TABLE_PROPERTIES` and the sensitivity-based HIPAA properties.

---

## 6.3 Gold Configuration

**File:** `src/common/gold_pipeline_config.py`

| Constant | Value | Line |
|---|---|---|
| `GOLD_SCHEMA_DEFAULT` | `"gold"` | 8 |
| `GOLD_AUDIT_COLUMNS` | `()` (empty) | 10 |
| `HIGH_COST_RATIO_THRESHOLD` | `1.5` | 20 |
| `HIGH_SEVERITY_EXPECTED_COST_FLOOR` | `5000.0` | 21 |
| `PROVIDER_LOOKBACK_WINDOW_DAYS` | `30` | 22 |
| `PROVIDER_LOOKBACK_WINDOW_60D` | `60` | 23 |
| `PROVIDER_LOOKBACK_WINDOW_90D` | `90` | 24 |
| `MIN_PROVIDER_RISK_COUNT` | `5` | 25 |

### PHI_COLUMNS_GOLD (line 12-18)

| Column |
|---|
| `billed_amount` |
| `date` |
| `diagnosis_code` |
| `is_denied` |
| `patient_id` |

### Functions

| Function | Signature | Returns | Lines |
|---|---|---|---|
| `gold_table_name(catalog, table_name, schema)` | `str, str, str` | FQN: `f"{catalog}.{schema}.{table_name}"` | 28-30 |
| `gold_table_properties(sensitivity, phi_columns)` | `str, Iterable[str]` | Delta props + `claimops.layer=gold` | 33-46 |
| `read_silver_snapshot(spark, table_name)` | `SparkSession, str` | Batch snapshot via `spark.read.table()` | 49-56 |

### Gold Table Properties Behavior

Gold tables carry `SENSITIVE` classification by convention (they join PHI-adjacent columns with engineered risk features). The `claimops.layer` property is set to `"gold"`.

---

## 6.4 ML Configuration

### Feature and Target Columns

**File:** `src/ml/__init__.py`

| Constant | Value | Line |
|---|---|---|
| `TARGET_COLUMN` | `"denial_label"` | 29 |

#### FEATURE_COLUMNS (all 20, lines 3-27)

| # | Feature Name | Category |
|---|---|---|
| 1 | `is_procedure_missing` | Data quality |
| 2 | `is_amount_missing` | Data quality |
| 3 | `amount_to_benchmark_ratio` | Cost |
| 4 | `billed_vs_avg_cost` | Cost |
| 5 | `high_cost_flag` | Cost |
| 6 | `severity_procedure_mismatch` | Clinical |
| 7 | `specialty_diagnosis_mismatch` | Clinical |
| 8 | `provider_location_missing` | Provider |
| 9 | `diagnosis_severity_encoded` | Clinical |
| 10 | `diagnosis_count` | Clinical |
| 11 | `provider_claim_count` | Provider (lifetime) |
| 12 | `provider_claim_count_30d` | Provider (rolling) |
| 13 | `provider_risk_score` | Provider |
| 14 | `provider_claim_count_60d` | Provider (rolling, Phase 1.4) |
| 15 | `provider_claim_count_90d` | Provider (rolling, Phase 1.4) |
| 16 | `cost_overbenchmark_and_highseverity` | Interaction (Phase 1.3) |
| 17 | `mismatch_and_overbenchmark` | Interaction (Phase 1.3) |
| 18 | `provider_30d_denial_rate` | Provider (Phase 1.3) |
| 19 | `missing_fields_count` | Data quality (Phase 1.3) |
| 20 | `low_volume_provider_risk` | Provider (Phase 1.3) |

### Feature Engineering Constants

**File:** `src/ml/features.py`

| Constant | Value | Line |
|---|---|---|
| `DEFAULT_TEST_SIZE` | `0.3` | 60 |
| `DEFAULT_RANDOM_SEED` | `42` | 61 |

#### BOOLEAN_FEATURES (lines 11-18)

`is_procedure_missing`, `is_amount_missing`, `high_cost_flag`, `severity_procedure_mismatch`, `specialty_diagnosis_mismatch`, `provider_location_missing`

#### NUMERIC_FEATURES (lines 20-35)

`amount_to_benchmark_ratio`, `billed_vs_avg_cost`, `diagnosis_severity_encoded`, `diagnosis_count`, `provider_claim_count`, `provider_claim_count_30d`, `provider_claim_count_60d`, `provider_claim_count_90d`, `provider_risk_score`, `cost_overbenchmark_and_highseverity`, `mismatch_and_overbenchmark`, `provider_30d_denial_rate`, `missing_fields_count`, `low_volume_provider_risk`

#### DEFAULT_FILL_VALUES (all 20, lines 37-58)

| Feature | Fill Value | Type |
|---|---|---|
| `is_procedure_missing` | `0` | int |
| `is_amount_missing` | `0` | int |
| `amount_to_benchmark_ratio` | `0.0` | float |
| `billed_vs_avg_cost` | `0.0` | float |
| `high_cost_flag` | `0` | int |
| `severity_procedure_mismatch` | `0` | int |
| `specialty_diagnosis_mismatch` | `0` | int |
| `provider_location_missing` | `0` | int |
| `diagnosis_severity_encoded` | `0` | int |
| `diagnosis_count` | `1` | int |
| `provider_claim_count` | `0` | int |
| `provider_claim_count_30d` | `0` | int |
| `provider_claim_count_60d` | `0` | int |
| `provider_claim_count_90d` | `0` | int |
| `provider_risk_score` | `0.0` | float |
| `cost_overbenchmark_and_highseverity` | `0.0` | float |
| `mismatch_and_overbenchmark` | `0.0` | float |
| `provider_30d_denial_rate` | `0.0` | float |
| `missing_fields_count` | `0` | int |
| `low_volume_provider_risk` | `0.0` | float |

### Evaluation Thresholds

**File:** `src/ml/evaluate.py`

| Constant | Value | Line | Source |
|---|---|---|---|
| `HIGH_RISK_PROBABILITY_THRESHOLD` | `0.7` | 20 | ARCHITECTURE.md Section 13 |
| `DEFAULT_MIN_RECALL_AT_HIGH` | `0.80` | 22 | ARCHITECTURE.md Section 13 |
| `DEFAULT_MIN_PRECISION` | `0.70` | 23 | ARCHITECTURE.md Section 13 |
| `DEFAULT_MIN_ROC_AUC` | `0.85` | 24 | ARCHITECTURE.md Section 13 |

The release gate (ARCHITECTURE.md Section 13) requires all three thresholds to be met simultaneously for a model to be promoted to champion.

### Training Constants

**File:** `src/ml/train.py`

| Constant | Value | Line |
|---|---|---|
| `OPTUNA_PRECISION_FLOOR` | `0.70` | 24 |

#### XGBOOST_DEFAULT_PARAMS (lines 56-68)

| Parameter | Value |
|---|---|
| `max_depth` | `6` |
| `learning_rate` | `0.1` |
| `n_estimators` | `100` |
| `objective` | `"binary:logistic"` |
| `eval_metric` | `"logloss"` |
| `early_stopping_rounds` | `50` |
| `scale_pos_weight` | `2.5` |
| `random_state` | `42` |

#### LIGHTGBM_DEFAULT_PARAMS (lines 70-81)

| Parameter | Value |
|---|---|
| `objective` | `"binary"` |
| `metric` | `"binary_logloss"` |
| `boosting_type` | `"gbdt"` |
| `num_leaves` | `31` |
| `learning_rate` | `0.1` |
| `n_estimators` | `100` |
| `scale_pos_weight` | `2.5` |
| `class_weight` | `"balanced"` |
| `random_state` | `42` |
| `verbose` | `-1` |

#### CATBOOST_DEFAULT_PARAMS (lines 83-93)

| Parameter | Value |
|---|---|
| `objective` | `"Logloss"` |
| `eval_metric` | `"Logloss"` |
| `learning_rate` | `0.1` |
| `depth` | `6` |
| `iterations` | `100` |
| `scale_pos_weight` | `2.5` |
| `random_seed` | `42` |
| `verbose` | `False` |
| `allow_writing_files` | `False` |

#### LOGREG_DEFAULT_PARAMS (lines 95-99)

| Parameter | Value |
|---|---|
| `max_iter` | `1000` |
| `class_weight` | `"balanced"` |
| `random_state` | `42` |

### Inference / Prediction Constants

**File:** `src/ml/predict.py`

| Constant | Value | Line | Purpose |
|---|---|---|---|
| `LATENCY_BUDGET_MS` | `150.0` | 24 | ARCHITECTURE.md Section 13 p99 budget |
| `RISK_THRESHOLD_LOW` | `0.3` | 20 | Below this = LOW risk tier |
| `RISK_THRESHOLD_HIGH` | `0.7` | 21 | Above this = HIGH risk tier |

Risk tiers: `prob < 0.3` = LOW, `0.3 <= prob < 0.7` = MEDIUM, `prob >= 0.7` = HIGH (lines 33-38).

### Retrain Gate Constants

**File:** `src/ml/retrain_gate.py`

| Constant | Value | Line | Purpose |
|---|---|---|---|
| `_RETRAIN_ROW_COUNT_MIN_DELTA` | `100` | 18 | Minimum absolute row difference |
| `_RETRAIN_ROW_COUNT_PCT_THRESHOLD` | `0.05` | 19 | Minimum 5% relative row difference |

The effective retrain threshold is `max(100, ceil(0.05 * previous_training_row_count))` (line 251).

---

## 6.5 PHI Registry

**File:** `src/common/phi_registry.py`

### PHI Columns by Bronze Dataset

| Dataset | PHI Columns |
|---|---|
| `claims` | `patient_id`, `diagnosis_code`, `billed_amount`, `date`, `claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`, `is_denied`, `follow_up_required` |
| `providers` | (none) |
| `diagnosis` | (none) |
| `cost` | (none) |

Defined in `bronze_sources.py` lines 59-130.

### Sensitive (PHI-Adjacent) Columns by Dataset

| Dataset | Sensitive Columns |
|---|---|
| `claims` | `claim_id`, `provider_id`, `procedure_code` |
| `diagnosis` | (none) |
| `providers` | (none) |
| `cost` | (none) |

Defined as `SENSITIVE_COLUMNS_BY_DATASET` at line 28-39 of `phi_registry.py`.

### What PHI Classification Means

Columns tagged as PHI in the registry:
- Are listed in the `hipaa.phi_columns` Delta table property via `table_properties_for_sensitivity()`
- Are excluded from observability log messages (never interpolated via MESSAGE_TEMPLATE_*)
- Must be encrypted at rest in production per 45 CFR 164.312(a)(2)(iv)

### Registry Functions

| Function | Signature | Returns | Line |
|---|---|---|---|
| `build_phi_columns_registry(catalog, schema)` | `**str` | `dict[str, frozenset[str]]` | 42-51 |
| `build_sensitive_columns_registry(catalog, schema)` | `**str` | `dict[str, frozenset[str]]` | 54-66 |
| `get_phi_columns(table_name)` | `str` | `frozenset[str]` | 73-75 |
| `get_sensitive_columns(table_name)` | `str` | `frozenset[str]` | 78-80 |
| `is_phi_column(table_name, column_name)` | `str, str` | `bool` | 83-86 |

---

## 6.6 Observability Constants

**File:** `src/common/log_categories.py`

| Constant | Value | Line |
|---|---|---|
| `LOG_CATEGORY_PIPELINE_OPS` | `"pipeline_ops"` | 6 |
| `LOG_CATEGORY_DATA_QUALITY` | `"data_quality"` | 7 |
| `LOG_CATEGORY_GOVERNANCE_AUDIT` | `"governance_audit"` | 8 |
| `LOG_CATEGORY_ANALYTICS_BUILD` | `"analytics_build"` | 9 |
| `LOG_CATEGORY_SILVER_PIPELINE` | `"silver_pipeline"` | 10 |
| `LOG_CATEGORY_QUARANTINE_AUDIT` | `"quarantine_audit"` | 11 |
| `LOG_CATEGORY_POLICY_CHUNKING` | `"policy_chunking"` | 12 |
| `LOG_CATEGORY_GOLD_PIPELINE` | `"gold_pipeline"` | 13 |
| `LOG_CATEGORIES` | `tuple` of all 8 above | 15-24 |

**File:** `src/common/log_messages.py`

| Constant | Value (template) | Line |
|---|---|---|
| `MESSAGE_BRONZE_APPEND_ONLY` | `"Do NOT apply transforms or deletes..."` | 6-8 |
| `MESSAGE_EVENT_LOG_SQL_BRIDGE` | `"Reading event_log() through a minimal SQL bridge..."` | 10-12 |
| `MESSAGE_TEMPLATE_EXPECTATION_METRIC` | `"Expectation metric recorded: expectation={expectation}..."` | 14-17 |
| `MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY` | `"Analytics dataset ready: table={table_name}..."` | 19-21 |
| `MESSAGE_TEMPLATE_PIPELINE_FAILURE` | `"Pipeline failure observed: diagnostic_id={diagnostic_id}..."` | 23-25 |
| `MESSAGE_TEMPLATE_SILVER_TABLE_READY` | `"Silver dataset ready: table={table_name}..."` | 27-29 |
| `MESSAGE_TEMPLATE_QUARANTINE_SUMMARY` | `"Quarantine summary recorded: dataset={dataset}..."` | 31-34 |
| `MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY` | `"Policy chunk extraction recorded: document_path={document_path}..."` | 36-39 |
| `MESSAGE_TEMPLATE_GOLD_TABLE_READY` | `"Gold dataset ready: table={table_name}..."` | 41-43 |

### render_* Helper Functions

| Function | Template Used | Line |
|---|---|---|
| `render_silver_table_ready(table_name, category, sensitivity)` | `MESSAGE_TEMPLATE_SILVER_TABLE_READY` | 46-52 |
| `render_gold_table_ready(table_name, category, sensitivity)` | `MESSAGE_TEMPLATE_GOLD_TABLE_READY` | 55-61 |
| `render_quarantine_summary(dataset, rule_name, diagnostic_id, quarantined_records)` | `MESSAGE_TEMPLATE_QUARANTINE_SUMMARY` | 64-76 |
| `render_policy_chunk_summary(document_path, chunk_count, diagnostic_id)` | `MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY` | 79-89 |

All render functions use `.format()` -- never string concatenation of PHI values. Templates accept only operational identifiers (dataset name, rule name, diagnostic ID, table name, category, sensitivity), never patient-level data.

---

## 6.7 DAB Variables (databricks.yml)

**File:** `databricks.yml`

### Variable Definitions (lines 12-33)

| Variable | Default | Description |
|---|---|---|
| `catalog` | `healthcare` | Unity Catalog name |
| `bronze_schema` | `bronze` | Bronze schema name |
| `silver_schema` | `silver` | Silver schema name |
| `gold_schema` | `gold` | Gold schema name |
| `analytics_schema` | `analytics` | Analytics schema name |
| `ml_schema` | `ml` | ML schema name |
| `node_type_id` | `n2-highmem-2` | GCP compute node type |
| `spark_version` | `17.3.x-cpu-ml-scala2.13` | Databricks runtime version |
| `model_version` | (no default) | MLflow model version (set by CI/CD in prod) |

### Target Definitions (lines 35-52)

| Property | dev | prod |
|---|---|---|
| `default` | `true` | (not default) |
| `mode` | `development` | `production` |
| `workspace.profile` | `dev` | `prod` |
| `workspace.root_path` | `/Workspace/Users/.../.bundle/.../${bundle.target}` | Same pattern |
| `variables.catalog` | `healthcare` | `healthcare` |
| `variables.model_version` | `"1"` | (resolved by CI/CD) |

### Bundle Metadata

| Property | Value |
|---|---|
| `bundle.name` | `healthcare-claim-ops` |
| `bundle.engine` | `direct` |
| `experimental.skip_name_prefix_for_schema` | `true` |

---

## 6.8 Environment Variables

| Variable | Set By | Purpose |
|---|---|---|
| `DATABRICKS_RUNTIME_VERSION` | Databricks runtime auto-set | Detects whether code runs on Databricks (e.g., `predict.py` line 56-58, `train.py` line 42) |
| `MLFLOW_EXPERIMENT_NAME` | User override | Overrides the experiment name resolution in `train.py` line 38-40 |
| `MLFLOW_TRACKING_URI` | User override | Overrides the MLflow tracking backend URI (set via `--mlflow-tracking-uri` CLI flag in practice) |

### DATABRICKS_RUNTIME_VERSION Usage

Used in three locations:
1. `src/ml/train.py` line 42: resolves experiment name to `/Users/{user}/claim_denial_{model_name}` on Databricks, relative name locally
2. `src/ml/predict.py` line 56-58: detects Databricks to configure registry URI
3. `src/ml/train.py` line 648: sets MLflow registry URI to `"databricks-uc"` on Databricks with 3-level model names

---

## 6.9 Additional Magic Values

### Silver Cleaning Constants

| Constant | File | Value |
|---|---|---|
| `MONEY_DECIMAL_PRECISION` | `src/common/silver_pipeline_config.py` | `18` |
| `MONEY_DECIMAL_SCALE` | `src/common/silver_pipeline_config.py` | `2` |
| `POLICY_CHUNK_SIZE_TOKENS` | `src/common/silver_pipeline_config.py` | `512` |
| `POLICY_CHUNK_OVERLAP_TOKENS` | `src/common/silver_pipeline_config.py` | `64` |

### Training Algorithm Constants

| Constant | Value | File |
|---|---|---|
| `positive_weight` (sample weights) | `3.0` | `src/ml/train.py` line 418 |
| `min_improvement` (champion promotion) | `0.01` | `src/ml/train.py` line 404 |
| `Optuna n_trials` default | `50` | `src/ml/train.py` line 487 |
| `CalibratedClassifierCV cv` | `3` (final), `2` (trial) | `src/ml/train.py` lines 190, 387 |
| `StratifiedKFold n_splits` (optuna) | `5` | `src/ml/train.py` line 378 |

### SHAP Evaluation Constants

| Constant | Value | File | Line |
|---|---|---|---|
| `max_samples` (SHAP) | `200` | `src/ml/evaluate.py` | 158 |
| `find_optimal_threshold` bins | `161` (linspace 0.1-0.9) | `src/ml/evaluate.py` | 94 |

### Pipeline Run ID

Generated as `datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")` at module load time in `bronze_pipeline_config.py` line 17, but overridable via Spark conf `claimops.pipeline_run_id`.
