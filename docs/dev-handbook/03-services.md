# Chapter 3: All Services

This chapter documents each of the services in the system, organized by the service manifest dependency order. Each section includes a service overview table, pipeline/job structure, key files, and table read/write patterns.

---

## 3.1 Bronze Ingestion

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `healthcare_etl_pipeline` |
| **Resource Type** | `pipelines` (SDP Lakeflow Pipeline) |
| **Trigger** | File arrival on landing volume (`/Volumes/healthcare/bronze/raw_landing/`) |
| **Manifest** | `services/etl/service.yml` |
| **Resource File** | `services/etl/resources/etl.pipeline.yml` |
| **Python Modules** | `ETL/pipelines/bronze/bronze_claims.py`, `ETL/pipelines/bronze/bronze_providers.py`, `ETL/pipelines/bronze/bronze_diagnosis.py`, `ETL/pipelines/bronze/bronze_cost.py`, `ETL/pipelines/bronze/bronze_policies.py` |

### What It Does

The Bronze ingestion step reads raw CSV files from the managed Unity Catalog volume `healthcare.bronze.raw_landing` using Databricks Auto Loader with `cloudFiles.format = csv` and `cloudFiles.schemaEvolutionMode = addNewColumns`. It runs as the first stage of the consolidated ETL pipeline (`healthcare_etl_pipeline`) and outputs to five Bronze Delta tables.

### Auto Loader Configuration

The shared configuration in `src/common/bronze_pipeline_config.py:70-76` defines the CSV Auto Loader options:

```python
def csv_autoloader_options() -> dict[str, str]:
    return {
        "cloudFiles.format": "csv",
        "header": "true",
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": RESCUED_DATA_COLUMN,  # "_rescued_data"
    }
```

Binary PDF ingestion uses a separate configuration (`binary_file_autoloader_options`) with `cloudFiles.format = binaryFile` and a `pathGlobFilter` of `*.pdf`.

### Tables Read and Written

| Table | Direction | Details |
|-------|-----------|---------|
| `/Volumes/healthcare/bronze/raw_landing/claims/` | Read | Source CSV files ingested via Auto Loader |
| `/Volumes/healthcare/bronze/raw_landing/providers/` | Read | Source CSV files ingested via Auto Loader |
| `/Volumes/healthcare/bronze/raw_landing/diagnosis/` | Read | Source CSV files ingested via Auto Loader |
| `/Volumes/healthcare/bronze/raw_landing/cost/` | Read | Source CSV files ingested via Auto Loader |
| `/Volumes/healthcare/bronze/raw_landing/policies/` | Read | Source PDF files ingested via Auto Loader |
| `healthcare.bronze.claims` | Write | Ingested claims rows with audit columns (`_ingested_at`, `_source_file`, `_pipeline_run_id`, `_rescued_data`) |
| `healthcare.bronze.providers` | Write | Ingested provider reference data |
| `healthcare.bronze.diagnosis` | Write | Ingested diagnosis code reference table |
| `healthcare.bronze.cost` | Write | Ingested procedure cost benchmarks |
| `healthcare.bronze.policies` | Write | Ingested policy PDFs as binary files with content, path, and metadata |

### Bronze Source Metadata

The `src/common/bronze_sources.py` module defines the `BronzeSource` dataclass for each dataset. Key metadata includes the expected row count, required columns, PHI column declarations (per 45 CFR 164.514(b)(2)), and canonical dataset name. This is used by bootstrap scripts for verification:

| Dataset | Source File | Expected Rows | Has PHI |
|---------|-------------|---------------|---------|
| `claims` | `datasets/claims_1000.csv` | 1,000 | Yes (10 columns) |
| `providers` | `datasets/providers_1000.csv` | 21 | No |
| `diagnosis` | `datasets/diagnosis.csv` | 6 | No |
| `cost` | `datasets/cost.csv` | 6 | No |

### Bronze Table Properties

All Bronze tables are created with Delta table properties that enable key features for downstream consumers:

```python
COMMON_DELTA_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.logRetentionDuration": "interval 2190 days",
    "delta.deletedFileRetentionDuration": "interval 2190 days",
}
```

Additionally, each table sets `hipaa.phi_columns` and `hipaa.data_sensitivity` (PUBLIC, INTERNAL, or SENSITIVE) via `table_properties_for_sensitivity()`.

---

## 3.2 Silver Cleaning

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `healthcare_etl_pipeline` (same pipeline, second stage) |
| **Resource Type** | `pipelines` (SDP Lakeflow Pipeline) |
| **Trigger** | Runs after Bronze stage within the same pipeline (DLT lineage) |
| **Manifest** | `services/etl/service.yml` (shared with Bronze) |
| **Resource File** | `services/etl/resources/etl.pipeline.yml` (shared) |
| **Python Modules** | `ETL/pipelines/silver/silver_claims.py`, `ETL/pipelines/silver/silver_providers.py`, `ETL/pipelines/silver/silver_diagnosis.py`, `ETL/pipelines/silver/silver_cost.py`, `ETL/pipelines/silver/silver_policy_chunks.py` |

### What It Does

The Silver stage reads Bronze tables as batch snapshots and applies cleaning transformations:

- **Claims** (`silver_claims.py`): Normalizes codes, deduplicates by `claim_id`, validates required fields, quarantines rows that fail validation, and writes to `healthcare.silver.claims`. Passes through: `claim_id`, `patient_id`, `provider_id`, `diagnosis_code`, `procedure_code`, `billed_amount`, `date`, `is_denied`, plus audit columns.
- **Providers** (`silver_providers.py`): Passes through provider reference data with validation.
- **Diagnosis** (`silver_diagnosis.py`): Passes through diagnosis code reference table.
- **Cost** (`silver_cost.py`): Passes through procedure cost benchmarks.
- **Policy Chunks** (`silver_policy_chunks.py`): Extracts text from PDFs via `pdfplumber`, splits into chunks, writes to `healthcare.silver.policy_chunks`.

### Quarantine

Rows that fail validation in the Silver pipeline are routed to `healthcare.quarantine.*` tables. The quarantine schema (`quarantine`) is declared in `resources/schemas/schemas.yml`. The quarantine tables preserve the original row alongside a `_quarantine_reason` column indicating what validation rule was violated.

### Tables Read and Written

| Table | Direction | Details |
|-------|-----------|---------|
| `healthcare.bronze.claims` | Read | Batch snapshot for cleaning |
| `healthcare.bronze.providers` | Read | Batch snapshot for cleaning |
| `healthcare.bronze.diagnosis` | Read | Batch snapshot for cleaning |
| `healthcare.bronze.cost` | Read | Batch snapshot for cleaning |
| `healthcare.bronze.policies` | Read | Binary PDF content for text extraction |
| `healthcare.silver.claims` | Write | Cleaned, deduplicated claims |
| `healthcare.silver.providers` | Write | Validated provider data |
| `healthcare.silver.diagnosis` | Write | Validated diagnosis codes |
| `healthcare.silver.cost` | Write | Validated cost benchmarks |
| `healthcare.silver.policy_chunks` | Write | PDF text chunks with source metadata |
| `healthcare.quarantine.*` | Write | Invalid rows with quarantine reason |

---

## 3.3 Gold Features

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `healthcare_etl_pipeline` (same pipeline, third stage) |
| **Resource Type** | `pipelines` (SDP Lakeflow Pipeline) |
| **Trigger** | Runs after Silver stage within the same pipeline (DLT lineage) |
| **Manifest** | `services/etl/service.yml` (shared) |
| **Resource File** | `services/etl/resources/etl.pipeline.yml` (shared) |
| **Python Module** | `ETL/pipelines/gold/gold_claim_features.py` |

### What It Does

The Gold stage engineers 22 predictive features from Silver tables through four Spark DLT materialized views defined in `ETL/pipelines/gold/gold_claim_features.py`.

### Materialized View 1: `claims_feature_base` (Private)

Joins Silver claims with provider, diagnosis, and cost reference data. Computes these base features:

| Feature | Derivation | Type |
|---------|-----------|------|
| `is_procedure_missing` | `procedure_code IS NULL` | Boolean |
| `is_amount_missing` | `billed_amount IS NULL` | Boolean |
| `amount_to_benchmark_ratio` | `billed_amount / expected_cost` | Float |
| `billed_vs_avg_cost` | `billed_amount / average_cost` | Float |
| `high_cost_flag` | `amount_to_benchmark_ratio >= 1.5` | Boolean |
| `severity_procedure_mismatch` | High severity + expected_cost < 5000 | Boolean |
| `specialty_diagnosis_mismatch` | `specialty != category` | Boolean |
| `provider_location_missing` | `location IS NULL` | Boolean |
| `diagnosis_severity_encoded` | High=1, Low=0 | Integer |
| `denial_label` | `is_denied -> 1 / 0` | Integer (target) |

The threshold constants are defined in `src/common/gold_pipeline_config.py`:
- `HIGH_COST_RATIO_THRESHOLD = 1.5` (`src/common/gold_pipeline_config.py:20`)
- `HIGH_SEVERITY_EXPECTED_COST_FLOOR = 5000.0` (`src/common/gold_pipeline_config.py:21`)

It also drops columns that could leak the target (`claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`, `follow_up_required`) as defense-in-depth.

### Materialized View 2: `provider_daily_stats` (Private)

Aggregates Silver claims by `(provider_id, event_date)`:

| Column | Derivation |
|--------|-----------|
| `daily_claim_count` | `COUNT(*)` per provider per day |
| `daily_denied_count` | `SUM(is_denied_int)` per provider per day |
| `daily_diagnosis_count` | `approx_count_distinct(diagnosis_code)` per provider per day |

### Materialized View 3: `provider_lifetime_stats` (Private)

Aggregates `provider_daily_stats` over the full history:

| Column | Derivation |
|--------|-----------|
| `provider_claim_count` | Sum of daily counts (lifetime) |
| `diagnosis_count` | Sum of daily diagnosis counts |
| `provider_risk_score` | `denied_count / claim_count` if `claim_count >= 5`, else NULL |

The minimum provider risk count threshold (`MIN_PROVIDER_RISK_COUNT = 5`) is defined at `src/common/gold_pipeline_config.py:26`.

### Materialized View 4: `gold_claim_features` (Incremental)

The final output view that joins `claims_feature_base` with `provider_lifetime_stats` and rolling window features from `provider_daily_stats`. The rolling windows use Spark's `rangeBetween` window function with three lookback periods:

| Window | Constant | Value |
|--------|----------|-------|
| 30-day | `PROVIDER_LOOKBACK_WINDOW_DAYS` | 30 |
| 60-day | `PROVIDER_LOOKBACK_WINDOW_60D` | 60 |
| 90-day | `PROVIDER_LOOKBACK_WINDOW_90D` | 90 |

The window uses a critical pattern: casting `date` through `timestamp` to `long` so the `rangeBetween` units are in seconds:

```python
# gold_claim_features.py:230-235
def _rolling_window(days: int) -> Window:
    return (
        Window.partitionBy("provider_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-days * 86400, 0)
    )
```

The final view adds five interaction / derived features:
- `provider_claim_count_60d`, `provider_claim_count_90d` -- wider temporal windows
- `cost_overbenchmark_and_highseverity` -- interaction: `amount_to_benchmark_ratio * diagnosis_severity_encoded`
- `mismatch_and_overbenchmark` -- interaction: `(severity_mismatch + specialty_mismatch) * amount_to_benchmark_ratio`
- `provider_30d_denial_rate` -- denial rate over 30-day window
- `missing_fields_count` -- count of missing fields among `is_procedure_missing`, `is_amount_missing`, `provider_location_missing`
- `low_volume_provider_risk` -- provider_risk_score for providers with fewer than `MIN_PROVIDER_RISK_COUNT` claims

The final `SELECT` includes 38 columns: 10 identifier/metadata columns, 22 feature columns (matching `FEATURE_COLUMNS` in `src/ml/__init__.py`), the target `denial_label`, and 4 audit/quality columns (`_ingested_at`, `_source_file`, `_pipeline_run_id`, `_data_quality_flags`).

### Tables Read and Written

| Table | Direction | Details |
|-------|-----------|---------|
| `healthcare.silver.claims` | Read | Batch snapshot via `read_silver_snapshot()` |
| `healthcare.silver.providers` | Read | Broadcast-hinted join for specialty/location |
| `healthcare.silver.diagnosis` | Read | Broadcast-hinted join for severity/category |
| `healthcare.silver.cost` | Read | Broadcast-hinted join for cost benchmarks |
| `healthcare.gold.claim_features` | Write | Final incremental materialized view |

### Gold Table Properties

```python
def gold_table_properties(sensitivity, phi_columns=()):
    properties = dict(COMMON_DELTA_TABLE_PROPERTIES)
    properties.update(table_properties_for_sensitivity(sensitivity, phi_columns))
    properties["claimops.layer"] = "gold"
    return properties
```

Gold tables are classified as `SENSITIVE` because they combine PHI-adjacent columns (`patient_id`, `billed_amount`, `diagnosis_code`) with engineered risk features. The `claimops.layer` property identifies the medallion layer for operational tooling.

Gold PHI columns are declared in `src/common/gold_pipeline_config.py:12-18`:
```python
PHI_COLUMNS_GOLD = (
    "billed_amount", "date", "diagnosis_code", "is_denied", "patient_id",
)
```

---

## 3.4 ML Training

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `ml_retrain_job` |
| **Resource Type** | `jobs` (Spark Python Job) |
| **Trigger** | Launched by `etl_file_arrival_job` via `run_job_task`, or manual |
| **Manifest** | `services/ml/training/service.yml` |
| **Resource File** | `services/ml/training/resources/training.job.yml` |
| **Entry Point** | `src/scripts/maybe_retrain_model.py` |

### Entry Point Scripts

There are two entry points that form the ML training pipeline:

**`src/scripts/maybe_retrain_model.py`**: The production entry point (invoked by the job). It runs the retrain gate first, then delegates to the training script only if new data is available (or `--force` is passed).

```python
# maybe_retrain_model.py:35-48
if not args.force:
    decision = decide_retrain(
        spark,
        gold_table=args.gold_table,
        feature_columns=list(FEATURE_COLUMNS),
        registered_model_name=args.registered_model_name,
        champion_alias=args.champion_alias,
    )
    if not decision.should_retrain:
        return 0
```

The retrain gate (`src/ml/retrain_gate.py`) compares the current Gold table's row count, fingerprint (hash of concatenated feature columns), and column list against the champion model's logged training metadata. If none have changed meaningfully, the gate returns `should_retrain=False` and the pipeline exits early.

**`src/scripts/train_denial_model.py`**: The training implementation called by `maybe_retrain_model.py`. It loads Gold features, trains 6 candidate models, evaluates them, and registers the best one.

### Six Model Candidates

The training pipeline (`train_denial_model.py:146-247`) produces six candidates, sorted by `(meets_thresholds, recall_at_high, roc_auc)`:

| # | Model | Tuning | Default Params |
|---|-------|--------|----------------|
| 1 | Logistic Regression | None (baseline) | `class_weight="balanced"`, `max_iter=1000` |
| 2 | XGBoost | Optuna (50-200 trials) + MedianPruner | `scale_pos_weight=2.5`, `max_depth=6`, `n_estimators=100` |
| 3 | LightGBM | Optuna (50-200 trials) + MedianPruner | `scale_pos_weight=2.5`, `num_leaves=31`, `class_weight="balanced"` |
| 4 | CatBoost | Optuna (50-200 trials) + MedianPruner | `auto_class_weights="Balanced"`, `depth=6` |
| 5 | Voting Ensemble | Soft voting of XGBoost + LightGBM + CatBoost | `voting="soft"` |
| 6 | Stacking Ensemble | Logistic Regression meta-learner, CV=5 | `cv=5`, LogisticRegression meta-learner |

### Optuna Tuning Detail

When `--tune` is passed, each tree-based model runs an Optuna study with:

- **Objective**: Maximize mean Recall@HIGH under a soft Precision floor of 0.70 (defined as `OPTUNA_PRECISION_FLOOR` at `src/ml/train.py:24`).
- **Pruner**: `MedianPruner(n_startup_trials=10, n_warmup_steps=3)` -- stops underperforming trials early.
- **Trials**: 50 per model by default, configurable via `--optuna-trials` (the job YAML passes `50`).
- **Cross-validation**: 5-fold StratifiedKFold (or GroupKFold by `provider_id` when groups are available). Each fold wraps the base estimator in `CalibratedClassifierCV(method="sigmoid", cv=2)` so trial-time scores match deployed model behavior.
- **Hyperparameter search ranges**: Each tuner (`_build_xgb_from_trial`, `_build_lgb_from_trial`, `_build_catboost_from_trial` in `src/ml/train.py`) defines model-specific parameter ranges including `max_depth`, `learning_rate`, `n_estimators`, `subsample`, `colsample_bytree`, `scale_pos_weight`, and regularization parameters.

### Calibration

All models are wrapped in `CalibratedClassifierCV` (Platt scaling) for calibrated probability outputs. The `select_best_calibration` function at `src/ml/train.py:259-295` tries both `sigmoid` and `isotonic` methods on a validation split, selecting whichever yields lower log-loss. This is critical because:

- The `HIGH_RISK_PROBABILITY_THRESHOLD = 0.7` cutoff requires that `proba >= 0.7` actually means "approximately 70% confidence."
- Models like XGBoost produce uncalibrated raw probabilities.
- The Optuna objective wraps calibration inside each fold so the search targets the same metric that production evaluates.

### MLflow Registration

On passing the release gate, `train_with_mlflow()` at `src/ml/train.py:654-746`:

1. Resolves the experiment name to an absolute Databricks workspace path.
2. Starts an MLflow run and logs: parameters, metrics, training metadata (row count, Gold table version, fingerprint, feature columns).
3. Logs the model with an inferred signature (input feature DataFrame, output probability column).
4. Registers the model as a new version in `healthcare.ml.claim_denial_model` (Unity Catalog).
5. Moves the `champion` alias to the new version so `models:/healthcare.ml.claim_denial_model@champion` resolves to it.

### Job Environment (Python Dependencies)

The ML training job declares its own environment (`environment_version: "5"`) with:
```
--editable ${workspace.file_path}    # The bundle's own Python package
xgboost>=2.0,<3.0
lightgbm>=4.2,<5.0
catboost>=1.2,<2.0
scikit-learn>=1.5,<2.0
shap>=0.44,<1.0
optuna>=3.6,<4.0
mlflow>=3.0,<4.0
imbalanced-learn>=0.12,<1.0
```

### Tables Read and Written

| Table | Direction | Details |
|-------|-----------|---------|
| `healthcare.gold.claim_features` | Read | Full table loaded via `spark.table().toPandas()` |
| `healthcare.ml.claim_denial_model` | Write | Model registered in Unity Catalog via MLflow |

---

## 3.5 Analytics Dashboard

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `analytics_observability_job` |
| **Resource Type** | `jobs` (Spark Python Job) |
| **Trigger** | Launched by `etl_file_arrival_job` after pipeline verification succeeds |
| **Manifest** | `services/etl/analytics_observability.service.yml` |
| **Resource File** | `services/etl/resources/analytics_observability.job.yml` |
| **Entry Points** | `src/scripts/build_observability.py`, `src/scripts/build_analytics.py`, `src/scripts/verify_silver.py` |

### Job Structure

The analytics observability job consists of three sequential tasks:

```
build_observability --> build_analytics --> build_quality_assets
```

### Task 1: `build_observability` (`src/scripts/build_observability.py`)

Reads the pipeline event log (`${var.catalog}.${var.analytics_schema}.etl_pipeline_event_log`) and builds observability tables in the `analytics` schema. Receives the `--pipeline-stage` parameter so observability metrics can be tagged with the originating pipeline stage (`etl`, `ml`, etc.).

### Task 2: `build_analytics` (`src/scripts/build_analytics.py`)

Calls `build_and_persist_claims_assets()` from `src/analytics/claims_analytics.py` to produce analytics assets from Silver and Bronze tables. Receives `--upstream-status` to skip processing if the upstream ETL pipeline failed:

```python
# build_analytics.py:29-32
if _normalize_state(args.upstream_status) != "success":
    print("Upstream status is not success; skipping analytics build.")
    return 0
```

### Task 3: `build_quality_assets` (`src/scripts/verify_silver.py`)

Validates Silver table quality by running row-level checks against quarantine tables. When `--emit-quality-assets` is passed, it persists quality metrics to the analytics schema for dashboard consumption. Also respects the `--upstream-status` skip.

### Job Parameters

The job accepts upstream context parameters for the observability chain:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `upstream_status` | `success` | Passed from the launching job's pipeline/verify result |
| `parent_job_name` | `manual` | Identifies which job launched this one |
| `parent_run_id` | `0` | Run ID for log correlation |
| `pipeline_stage` | `etl` | Stage tag for observability metrics |

### Tables Read and Written

| Table | Direction | Details |
|-------|-----------|---------|
| `${var.catalog}.${var.analytics_schema}.etl_pipeline_event_log` | Read | Pipeline event log for observability |
| `healthcare.silver.*` | Read | Silver tables for analytics assets |
| `healthcare.bronze.*` | Read | Bronze tables for analytics assets |
| `healthcare.quarantine.*` | Read | Quarantine tables for quality checks |
| `healthcare.analytics.*` | Write | Analytics/observability/quality output tables |

---

## 3.6 Setup

### Overview

| Property | Value |
|----------|-------|
| **Resource Key** | `setup_infrastructure` |
| **Resource Type** | `jobs` (multi-task job with condition branch) |
| **Trigger** | Manual (one-time or on-demand) |
| **Manifest** | `services/infrastructure/setup/service.yml` |
| **Resource File** | `services/infrastructure/setup/resources/setup_infrastructure.job.yml` |
| **Dependencies** | None (no `depends_on` in manifest) |

### Job Structure

The setup job has four tasks with a conditional branch:

```
apply_grants --> create_retrain_decisions --> should_load_sample_data
                                                  |
                                           (if "true")
                                                  |
                                            load_sample_data
```

### Task 1: `apply_grants` (Notebook Task)

Runs `src/notebooks/grants.ipynb` with the catalog and schema base parameters. Sets up Unity Catalog permissions for the workspace. This is a notebook task (not Spark Python) because it uses Databricks SQL GRANT statements.

### Task 2: `create_retrain_decisions` (Spark Python Task)

Runs `src/scripts/setup_retrain_decisions.py` to create the `retrain_decisions` audit table in the ML schema. This table records every retrain gate decision for observability and audit trails.

### Task 3: `should_load_sample_data` (Condition Task)

A pure condition check that evaluates the job parameter `load_sample_data`. If `"true"`, the `load_sample_data` task runs; otherwise it is skipped.

### Task 4: `load_sample_data` (Spark Python Task)

Runs `src/scripts/load_sample_data.py` to copy fixture datasets from `datasets/` into the Bronze landing volume:

```
datasets/claims_1000.csv       --> /Volumes/healthcare/bronze/raw_landing/claims/
datasets/providers_1000.csv    --> /Volumes/healthcare/bronze/raw_landing/providers/
datasets/diagnosis.csv         --> /Volumes/healthcare/bronze/raw_landing/diagnosis/
datasets/cost.csv              --> /Volumes/healthcare/bronze/raw_landing/cost/
(optional PDFs)                --> /Volumes/healthcare/bronze/raw_landing/policies/
```

The script reads `BRONZE_SOURCES` from `src/common/bronze_sources.py` to know which files to copy, and uses `bronze_volume_path()` from `src/common/bronze_pipeline_config.py` to construct the destination paths.

### Job Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `load_sample_data` | `"false"` | When `"true"`, samples the synthetic datasets into the landing volume |

### Tables Read and Written

| Table/Resource | Direction | Details |
|----------------|-----------|---------|
| `datasets/*.csv` (local) | Read | Fixture files from the project directory |
| `/Volumes/healthcare/bronze/raw_landing/*/` | Write | Landing volume directories created/staged |
| `healthcare.ml.retrain_decisions` | Write | Audit table for retrain gate decisions |

---

## 3.7 Resources: Schemas and Volumes

### Unity Catalog Schemas (`resources/schemas/schemas.yml`)

Six schemas are declared. This file is auto-discovered by DAB via the `include:` glob and ensures all schemas exist before any pipeline or job runs:

```yaml
bronze_schema:    catalog: ${var.catalog}  name: ${var.bronze_schema}
silver_schema:    catalog: ${var.catalog}  name: ${var.silver_schema}
quarantine_schema:catalog: ${var.catalog}  name: quarantine
gold_schema:      catalog: ${var.catalog}  name: ${var.gold_schema}
analytics_schema: catalog: ${var.catalog}  name: ${var.analytics_schema}
ml_schema:        catalog: ${var.catalog}  name: ${var.ml_schema}
```

### Managed Volume (`resources/volumes/volumes.yml`)

A single managed volume is defined for the Bronze landing zone:

```yaml
raw_landing_volume:
  catalog_name: ${var.catalog}
  schema_name: ${var.bronze_schema}
  name: raw_landing
  volume_type: MANAGED
```

This volume at `/Volumes/healthcare/bronze/raw_landing/` is the entry point for all data entering the system. The file arrival trigger on the `etl_file_arrival_job` watches this directory.

---

## 3.8 Orchestration: How the Services Chain Together

The services are orchestrated through Databricks job pipelines in a specific order:

```
  [File arrives on /Volumes/.../raw_landing/]
               |
  etl_file_arrival_job (triggered by file_arrival)
       |
   run_etl_pipeline
       |
   verify_etl_light
    |        |        |
    |        |        +--> launch_ml_retrain (ml_training)
    |        +--> sync_policy_vector_index (rag_vector_index)
    +--> launch_analytics_observability
               |
      analytics_observability_job
         |--------------------------|
      build_observability  build_analytics  build_quality_assets
```

1. **File arrival trigger** fires when a new file lands in the raw_landing volume.
2. **`etl_file_arrival_job`** starts, running the consolidated ETL pipeline (Bronze -> Silver -> Gold).
3. After the pipeline completes, **`verify_etl_light.py`** validates the output tables.
4. After verification succeeds, the ML retrain job and vector index sync job are launched via `run_job_task`.
5. `launch_analytics_observability.py` always runs (`ALL_DONE`) to preserve observability diagnostics, then triggers the analytics job with upstream context parameters.
6. The **`analytics_observability_job`** builds observability tables, analytics dashboard assets, and quality metrics in sequence.

Vector index sync uses a two-table contract by design:
- `healthcare.gold.policy_chunks` is the Gold materialized view owned by Lakeflow.
- `healthcare.gold.policy_chunks_vs` is a CDF-enabled Delta table used as the Vector Search Delta Sync source.
- `src/scripts/create_vector_index.py` incrementally merges MV rows into the `_vs` table and then syncs the index.

For development iteration, the **`etl_fast_dev_job`** runs only the ETL pipeline + verification (no analytics or ML), providing a faster feedback loop.
