# Chapter 4: ETL Pipelines

This chapter covers the three-layer medallion ETL: Bronze (raw ingest), Silver (trusted/cleaned), and Gold (engineered features). Each layer is implemented as a Databricks SDP (Lakeflow Pipelines) pipeline.

## 4.1 Bronze Pipeline

**File:** `ETL/pipelines/bronze/bronze_ingestion.py`

The Bronze pipeline reads raw claims CSV files from a Unity Catalog landing volume and writes them to `healthcare.bronze.claims` as a Delta table. It uses Databricks Auto Loader with a `file_arrival` trigger -- new files placed in the volume automatically trigger ingestion without manual intervention.

### Auto Loader Configuration

The shared CSV Auto Loader options are defined in `src/common/bronze_pipeline_config.py:68-76`:

```python
def csv_autoloader_options() -> dict[str, str]:
    return {
        "cloudFiles.format": "csv",
        "header": "true",
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": RESCUED_DATA_COLUMN,
    }
```

Key points:
- `inferColumnTypes: false` -- schema is explicitly declared, not inferred, preventing silent type changes.
- `schemaEvolutionMode: addNewColumns` -- new columns in incoming CSV files are added to the table schema without breaking existing rows.
- `rescuedDataColumn: _rescued_data` -- columns that do not match the declared schema are captured in this column instead of silently dropped.

For PDF policy documents, the Bronze pipeline uses a separate `binaryFile` format option defined in `src/common/bronze_pipeline_config.py:94-99`:

```python
def binary_file_autoloader_options(path_glob_filter: str = "*.pdf") -> dict[str, str]:
    return {
        "cloudFiles.format": "binaryFile",
        "pathGlobFilter": path_glob_filter,
    }
```

### Bronze Config

**File:** `src/common/bronze_pipeline_config.py`

Key constants:

| Constant | Value | Purpose |
|----------|-------|---------|
| `CATALOG_DEFAULT` | `"healthcare"` | Default Unity Catalog catalog name |
| `BRONZE_SCHEMA_DEFAULT` | `"bronze"` | Default Bronze schema name |
| `BRONZE_VOLUME_DEFAULT` | `"raw_landing"` | Volume name for CSV landing files |
| `BRONZE_VOLUME_ROOT` | `"/Volumes/healthcare/bronze/raw_landing"` | Full UC volume path |
| `RESCUED_DATA_COLUMN` | `"_rescued_data"` | Auto Loader rescued data column name |
| `PIPELINE_RUN_ID_FORMAT` | `"yyyyMMdd_HHmmss"` | Timestamp format for pipeline run tracking |

### Common Delta Table Properties

Defined at `src/common/bronze_pipeline_config.py:23-29`:

```python
COMMON_DELTA_TABLE_PROPERTIES: Final[dict[str, str]] = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.logRetentionDuration": "interval 2190 days",
    "delta.deletedFileRetentionDuration": "interval 2190 days",
}
```

Every table across all layers inherits these properties. The 2190-day (6-year) log retention is set for HIPAA compliance (45 CFR 164.308(a)(1)(ii)(D) -- retention of 6 years from creation or last use). CDF enables downstream consumers (Silver pipeline) to detect and process only changed rows.

### Bronze Sources

**File:** `src/common/bronze_sources.py`

The `BRONZE_SOURCES` dictionary at `src/common/bronze_sources.py:59-130` defines metadata for each dataset:

```python
BRONZE_SOURCES: Final[dict[str, BronzeSource]] = {
    "claims": BronzeSource(
        local_filename="claims_1000.csv",
        volume_subdirectory="claims",
        expected_row_count=1000,
        required_columns=(
            "claim_id", "patient_id", "provider_id", "diagnosis_code",
            "procedure_code", "billed_amount", "date",
            "claim_status", "denial_reason_code", "allowed_amount",
            "paid_amount", "is_denied", "follow_up_required",
        ),
        canonical_dataset="claims",
        phi_columns=frozenset({
            "patient_id", "diagnosis_code", "billed_amount", "date",
            "claim_status", "denial_reason_code", "allowed_amount",
            "paid_amount", "is_denied", "follow_up_required",
        }),
    ),
    "providers": BronzeSource(..., phi_columns=frozenset()),
    "diagnosis": BronzeSource(..., phi_columns=frozenset()),
    "cost": BronzeSource(..., phi_columns=frozenset()),
}
```

The `BronzeSource` dataclass (`src/common/bronze_sources.py:9-25`) requires explicit `phi_columns` declaration per 45 CFR 164.514(b)(2). The `phi_columns` field is a `frozenset` so it is hashable and immutable. The `has_phi` property (`bronze_sources.py:33-35`) returns whether the dataset contains any PHI.

## 4.2 Silver Pipeline

**File:** `ETL/pipelines/silver/silver_claims.py` (claims), plus `silver_providers.py`, `silver_diagnosis.py`, `silver_cost.py`

The Silver pipeline reads Bronze tables, normalizes codes, validates against lookup tables, deduplicates, and quarantines bad rows. It produces four trusted Silver tables:

| Silver Table | Bronze Source | Key Transformations |
|---|---|---|
| `silver.claims` | `bronze.claims` | Normalize codes to uppercase, parse dates/decimal/bool, validate against provider/diagnosis lookups, deduplicate by `claim_id`, quarantine bad rows |
| `silver.providers` | `bronze.providers` | Normalize names/specialty to title case, impute missing location to "Unknown", quarantine missing provider_id or doctor_name |
| `silver.diagnosis` | `bronze.diagnosis` | Normalize codes to uppercase, validate severity is "High" or "Low", quarantine invalid |
| `silver.cost` | `bronze.cost` | Normalize procedure codes to uppercase, ensure average/expected cost parsable and positive, quarantine bad rows |

### Silver Claims Pipeline (detailed)

The claims pipeline has three materialized views chained together:

**1. `_claims_validated_rows` (private MV)** -- `silver_claims.py:92-222`

This is the shared intermediate that both the trusted and quarantine outputs read from. It:

1. Reads Bronze claims via `read_bronze_snapshot()` (`silver_pipeline_config.py:69-71`)
2. Broadcast-joins provider and diagnosis lookup tables to validate references
3. Normalizes all code columns to uppercase via `spark_normalize_code()`
4. Parses `billed_amount`, `allowed_amount`, `paid_amount` to `decimal(18,2)` via `spark_decimal_or_null()`
5. Parses `date` via `spark_date_or_null()`
6. Converts `is_denied`, `follow_up_required` to boolean via `spark_bool_or_null()`
7. Computes quality flags: `missing_procedure_code`, `missing_billed_amount` stored in `_data_quality_flags` array
8. Deduplicates with `row_number()` over `Window.partitionBy("claim_id").orderBy(_ingested_at DESC, _pipeline_run_id DESC, _source_file DESC)`
9. Computes validation flags: `missing_claim_id`, `missing_patient_id`, `missing_provider_id`, `missing_diagnosis_code`, `invalid_claim_date`, `unknown_provider_reference`, `unknown_diagnosis_reference`, `inconsistent_denial_label`
10. Assigns `quarantine_reason`, `diagnostic_id`, and `rule_name` for the first failing rule

**2. `silver_claims` (incremental MV)** -- `silver_claims.py:225-262`

Filters `claims_validated_rows` to only rows where `diagnostic_id IS NULL` (passed all validation) and `_row_priority = 1` (first occurrence of each claim_id). Drops all validation helper columns, keeping only the trusted data columns.

**3. `quarantine_claims` (incremental MV)** -- `silver_claims.py:265-304`

Filters `claims_validated_rows` to rows where `diagnostic_id IS NOT NULL` (failed at least one validation rule). Adds a `status_message` column that concatenates dataset, rule_name, diagnostic_id, and quarantined_records count -- all PHI-safe identifiers, never patient values.

### Cleaning Utilities

**File:** `src/common/silver_cleaning.py`

This module provides Spark (and scalar) cleaning functions used across all Silver pipelines:

| Function | Purpose | Line |
|---|---|---|
| `spark_normalize_code(col)` | Uppercase + trim-to-null | 93-97 |
| `spark_normalize_title(col)` | Title-case with acronym preservation (MD, ENT, OB/GYN, etc.) | 100-107 |
| `spark_decimal_or_null(col, precision, scale)` | Cast to DECIMAL or NULL | 115-117 |
| `spark_date_or_null(col, fmt)` | Parse date or NULL | 120-124 |
| `spark_bool_or_null(col)` | Parse 1/TRUE/YES/Y -> True, 0/FALSE/NO/N -> False | 127-136 |
| `spark_quality_flags(flag_map)` | Build array of active quality flag names | 139-155 |

The scalar equivalents (`normalize_code_value`, `parse_decimal_value`, `parse_date_value`, `parse_bool_value`) are used in data generation tools and tests, not in the Spark pipelines themselves.

### Silver Config

**File:** `src/common/silver_pipeline_config.py`

Key constants:

| Constant | Value | Purpose |
|---|---|---|
| `SILVER_SCHEMA_DEFAULT` | `"silver"` | Default Silver schema |
| `QUARANTINE_SCHEMA_DEFAULT` | `"quarantine"` | Default quarantine schema |
| `ANALYTICS_SCHEMA_DEFAULT` | `"analytics"` | Analytics schema |
| `MONEY_DECIMAL_PRECISION` | `18` | Precision for money decimal columns |
| `MONEY_DECIMAL_SCALE` | `2` | Scale for money decimal columns |
| `POLICY_CHUNK_SIZE_TOKENS` | `512` | Token size for policy text chunks |
| `POLICY_CHUNK_OVERLAP_TOKENS` | `64` | Overlap between consecutive policy chunks |

### Silver Provider/Diagnosis/Cost Pipelines (overview)

Each follows the same pattern:
1. A `@dp.temporary_view()` normalizes the Bronze data, applying cleaning functions and computing validation flags.
2. An incremental `@dp.materialized_view()` filters to trusted rows (passing all validations), dropping helper columns.
3. An incremental quarantine `@dp.materialized_view()` captures rows that failed validation, with a PHI-safe `diagnostic_id` and `quarantine_reason`.

Notable differences:
- **Providers** (`silver_providers.py`): Imputes missing location to `"Unknown"` via `F.coalesce()` -- location gaps are survivable for provider lookups, so the row stays trusted with a quality flag.
- **Diagnosis** (`silver_diagnosis.py`): Validates that `severity` normalizes to exactly `"High"` or `"Low"` via a dedicated `invalid_severity` flag.
- **Cost** (`silver_cost.py`): Validates that both `average_cost` and `expected_cost` parse to positive decimals. Keyed on `(procedure_code, region)` composite key.

## 4.3 Gold Pipeline (THE MOST IMPORTANT)

**File:** `ETL/pipelines/gold/gold_claim_features.py`

The Gold pipeline is the feature-engineering layer. It takes cleaned Silver data and produces 20 engineered features for ML training. It uses four materialized views (three private, one public) in a dependency chain.

### View 1: `_claims_feature_base` (private MV)

**Lines 44-155**

Joins Silver claims with provider, diagnosis, and cost reference data. Computes the 9 base features:

```python
def _claims_feature_base():
    claims = read_silver_snapshot(spark, _silver_claims)
    providers = read_silver_snapshot(spark, _silver_providers)
    diagnosis = read_silver_snapshot(spark, _silver_diagnosis)
    cost = read_silver_snapshot(spark, _silver_cost)

    claims_provider = claims.join(
        F.broadcast(providers.select("provider_id", "specialty", "location")),
        on="provider_id", how="left",
    )

    claims_provider_diag = claims_provider.join(
        F.broadcast(diagnosis.select("diagnosis_code", "category", "severity")),
        on="diagnosis_code", how="left",
    )

    claim_cost = claims_provider_diag.join(
        F.broadcast(cost.select(
            F.col("procedure_code").alias("cost_procedure_code"),
            F.col("region").alias("cost_region"),
            "average_cost", "expected_cost",
        )),
        on=[F.col("procedure_code") == F.col("cost_procedure_code"),
            F.col("location") == F.col("cost_region")],
        how="left",
    )
```

Key design choices:
- Provider, diagnosis, and cost reference tables are broadcast-joined (small lookup tables).
- Cost joins on `(procedure_code, location/region)` -- billing region determines which cost benchmark applies.
- All joins are `LEFT` so a missing reference does not drop the claim; instead the feature column records the gap (e.g., `is_procedure_missing = True`).

**Target-leak defense** (lines 143-155): After feature computation, the following Silver columns are dropped specifically to prevent any future schema widening from leaking the training target into the feature surface:

```python
).drop(
    "cost_procedure_code",
    "cost_region",
    "claim_status",
    "denial_reason_code",
    "allowed_amount",
    "paid_amount",
    "follow_up_required",
)
```

`is_denied` is preserved because it is the source of `denial_label` and is explicitly excluded from `FEATURE_COLUMNS` in `src/ml/__init__.py`.

### View 2: `_provider_daily_stats` (private MV)

**Lines 157-179**

Groups Silver claims by `(provider_id, event_date)` to compute per-day aggregates:

```python
def _provider_daily_stats():
    claims = read_silver_snapshot(spark, _silver_claims)
    return (
        claims.filter(F.col("provider_id").isNotNull())
        .withColumn("event_date", F.to_date(F.col("date")))
        .withColumn("is_denied_int", F.when(F.col("is_denied") == F.lit(True), F.lit(1)).otherwise(F.lit(0)))
        .groupBy("provider_id", "event_date")
        .agg(
            F.count("*").alias("daily_claim_count"),
            F.sum("is_denied_int").alias("daily_denied_count"),
            F.approx_count_distinct("diagnosis_code").alias("daily_diagnosis_count"),
        )
    )
```

Uses `approx_count_distinct` for diagnosis count (HyperLogLog sketch) -- approximate cardinality is sufficient for a rolling feature and much cheaper than exact count.

### View 3: `_provider_lifetime_stats` (private MV)

**Lines 182-209**

Sums the daily stats to provider-level lifetime aggregates:

```python
def _provider_lifetime_stats():
    daily = spark.read.table("provider_daily_stats")
    return (
        daily.groupBy("provider_id")
        .agg(
            F.sum("daily_claim_count").alias("provider_claim_count"),
            F.sum("daily_denied_count").alias("provider_denied_count"),
            F.sum("daily_diagnosis_count").alias("diagnosis_count"),
        )
        .withColumn(
            "provider_risk_score",
            F.when(
                F.col("provider_claim_count") >= MIN_PROVIDER_RISK_COUNT,
                F.col("provider_denied_count").cast("double") / F.col("provider_claim_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("provider_denied_count")
    )
```

`provider_risk_score` is `NULL` when the provider has fewer than `MIN_PROVIDER_RISK_COUNT` (5) claims, preventing unreliable denial-rate estimates from tiny samples.

### View 4: `gold_claim_features` (incremental MV -- THE OUTPUT)

**Lines 212-365**

Joins the base features with lifetime and rolling-window provider stats. Computes rolling 30d/60d/90d windows, interaction features, and the final denial label.

**Rolling window implementation** (lines 230-236):

```python
def _rolling_window(days: int) -> Window:
    return (
        Window.partitionBy("provider_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-days * 86400, 0)
    )
```

The date is cast through `timestamp` to `long` (seconds since epoch), then `rangeBetween` uses `-days * 86400` (seconds). This is the correct approach because Spark's `date` to `long` cast yields days, which would make `rangeBetween(-30 * 86400, 0)` effectively unbounded.

**Interaction features** (lines 287-321):

```python
# Cost over benchmark combined with high severity
cost_overbenchmark_and_highseverity = (
    amount_to_benchmark_ratio * diagnosis_severity_encoded
)

# Mismatch count (severity_procedure + specialty_diagnosis) times cost overrun
mismatch_and_overbenchmark = (
    (severity_procedure_mismatch + specialty_diagnosis_mismatch) * amount_to_benchmark_ratio
)

# Total number of missing fields in this claim
missing_fields_count = is_procedure_missing + is_amount_missing + provider_location_missing

# 30d denial rate (denials / claims in rolling window)
provider_30d_denial_rate = (
    provider_30d_denial_rate_raw / provider_claim_count_30d
    when provider_claim_count_30d > 0
)

# Provider risk for low-volume providers only (NULL for high-volume)
low_volume_provider_risk = (
    provider_risk_score
    when provider_claim_count < MIN_PROVIDER_RISK_COUNT
)
```

### Complete Feature Table

All 20 features in `gold_claim_features`:

| # | Feature | Type | How Computed | Source Lines |
|---|---|---|---|---|
| 1 | `is_procedure_missing` | bool | `procedure_code IS NULL` after left join with cost | `gold_claim_features.py:90` |
| 2 | `is_amount_missing` | bool | `billed_amount IS NULL` | `gold_claim_features.py:91` |
| 3 | `amount_to_benchmark_ratio` | double | `billed_amount / expected_cost` (NULL when expected_cost is NULL or <= 0) | `gold_claim_features.py:92-98` |
| 4 | `billed_vs_avg_cost` | double | `billed_amount / average_cost` (NULL when average_cost is NULL or <= 0) | `gold_claim_features.py:99-105` |
| 5 | `high_cost_flag` | bool | `amount_to_benchmark_ratio >= HIGH_COST_RATIO_THRESHOLD (1.5)` | `gold_claim_features.py:106-113` |
| 6 | `severity_procedure_mismatch` | bool | Severity=High AND `expected_cost < HIGH_SEVERITY_EXPECTED_COST_FLOOR (5000.0)` | `gold_claim_features.py:114-122` |
| 7 | `specialty_diagnosis_mismatch` | bool (nullable) | `lower(specialty) != lower(category)` when both present; NULL if either missing | `gold_claim_features.py:123-131` |
| 8 | `provider_location_missing` | bool | `location IS NULL` | `gold_claim_features.py:132` |
| 9 | `diagnosis_severity_encoded` | int (0/1) | High -> 1, Low -> 0, else NULL | `gold_claim_features.py:133-138` |
| 10 | `diagnosis_count` | int | Sum of daily distinct diagnosis codes per provider (lifetime) | `provider_lifetime_stats.py:199` |
| 11 | `provider_claim_count` | int | Sum of daily claim counts per provider (lifetime) | `provider_lifetime_stats.py:197` |
| 12 | `provider_claim_count_30d` | int | Rolling sum of claim counts over 30-day window | `gold_claim_features.py:239` |
| 13 | `provider_claim_count_60d` | int | Rolling sum of claim counts over 60-day window | `gold_claim_features.py:240` |
| 14 | `provider_claim_count_90d` | int | Rolling sum of claim counts over 90-day window | `gold_claim_features.py:241` |
| 15 | `provider_risk_score` | double | Lifetime denial rate (denied/total), NULL when < 5 claims | `provider_lifetime_stats.py:201-207` |
| 16 | `cost_overbenchmark_and_highseverity` | double | `amount_to_benchmark_ratio * diagnosis_severity_encoded` | `gold_claim_features.py:287-290` |
| 17 | `mismatch_and_overbenchmark` | double | `(severity_procedure_mismatch + specialty_diagnosis_mismatch) * amount_to_benchmark_ratio` | `gold_claim_features.py:291-297` |
| 18 | `provider_30d_denial_rate` | double | 30d denied count / 30d claim count (rolling window) | `gold_claim_features.py:306-314` |
| 19 | `missing_fields_count` | int | Sum of `is_procedure_missing + is_amount_missing + provider_location_missing` | `gold_claim_features.py:298-305` |
| 20 | `low_volume_provider_risk` | double | `provider_risk_score` when `provider_claim_count < 5`, else NULL | `gold_claim_features.py:315-321` |

### Gold Config

**File:** `src/common/gold_pipeline_config.py`

All constants with their values:

```python
GOLD_SCHEMA_DEFAULT: Final[str] = "gold"

PHI_COLUMNS_GOLD: Final[tuple[str, ...]] = (
    "billed_amount",
    "date",
    "diagnosis_code",
    "is_denied",
    "patient_id",
)

HIGH_COST_RATIO_THRESHOLD: Final[float] = 1.5
HIGH_SEVERITY_EXPECTED_COST_FLOOR: Final[float] = 5000.0
PROVIDER_LOOKBACK_WINDOW_DAYS: Final[int] = 30
PROVIDER_LOOKBACK_WINDOW_60D: Final[int] = 60
PROVIDER_LOOKBACK_WINDOW_90D: Final[int] = 90
MIN_PROVIDER_RISK_COUNT: Final[int] = 5
```

Helper functions:

- `gold_table_name(catalog, table_name, schema)` at `gold_pipeline_config.py:28-30` -- returns `f"{catalog}.{schema}.{table_name}"`.
- `gold_table_properties(sensitivity, phi_columns)` at `gold_pipeline_config.py:33-46` -- merges `COMMON_DELTA_TABLE_PROPERTIES` with sensitivity metadata and sets `"claimops.layer": "gold"`.
- `read_silver_snapshot(spark, table_name)` at `gold_pipeline_config.py:49-56` -- returns `spark.read.table(table_name)` for batch reads of Silver tables.

## 4.4 How to Add a New Feature to Gold

This is a walkthrough for adding a new engineered feature to the Gold layer. As an example, suppose we want to add `claim_amount_anomaly_score` -- a feature that measures how unusual a claim's billed amount is compared to the provider's historical average.

### Step 1: Add a constant to `gold_pipeline_config.py` (if needed)

If the new feature uses a configurable threshold, add it as a typed `Final` constant:

```python
# src/common/gold_pipeline_config.py
CLAIM_AMOUNT_ANOMALY_STD_DEV_THRESHOLD: Final[float] = 2.0
```

Add it to `__all__` at the bottom of the file.

### Step 2: Add the column computation in `gold_claim_features()`

In `ETL/pipelines/gold/gold_claim_features.py`, inside the `gold_claim_features()` function, add a `.withColumn()` call before the final `.select()`. For the anomaly score, we would first need to compute per-provider statistics in the base or daily view, then compute the score in the final result.

If the feature can be expressed purely from columns already in the result set, add it inline:

```python
# Inside gold_claim_features(), before .drop() and .select(), around line 315
.withColumn(
    "claim_amount_anomaly_score",
    F.when(
        F.col("amount_to_benchmark_ratio").isNotNull(),
        F.abs(F.col("amount_to_benchmark_ratio") - F.lit(1.0)),
    ).otherwise(F.lit(None).cast("double")),
)
```

If the feature requires a new intermediate aggregation (like a provider-level stddev), add a new private materialized view:

```python
@dp.materialized_view(
    name="provider_amount_stats",
    private=True,
    comment="Private intermediate: provider-level billed amount statistics.",
    table_properties=gold_table_properties("SENSITIVE", CLAIMS_PHI_COLUMNS),
)
def _provider_amount_stats():
    claims = read_silver_snapshot(spark, _silver_claims)
    return (
        claims.filter(F.col("provider_id").isNotNull())
        .groupBy("provider_id")
        .agg(
            F.stddev(F.col("billed_amount").cast("double")).alias("provider_amount_stddev"),
            F.avg(F.col("billed_amount").cast("double")).alias("provider_amount_mean"),
        )
    )
```

Then join it into `gold_claim_features()`:

```python
provider_amount = spark.read.table("provider_amount_stats")

# ... inside the result chain:
result = (
    base.withColumn("event_date", ...)
    .join(F.broadcast(provider_lifetime), on="provider_id", how="left")
    .join(provider_daily_rolling, on=["provider_id", "event_date"], how="left")
    .join(F.broadcast(provider_amount), on="provider_id", how="left")  # NEW
    .withColumn(
        "claim_amount_anomaly_score",
        F.when(
            F.col("provider_amount_stddev").isNotNull()
            & (F.col("provider_amount_stddev") > 0),
            F.abs(
                F.col("billed_amount").cast("double") - F.col("provider_amount_mean")
            ) / F.col("provider_amount_stddev"),
        ).otherwise(F.lit(None).cast("double")),
    )
```

Finally, add the new column to the `.select()` call at the end of `gold_claim_features()` (around line 325).

### Step 3: Add the feature to `FEATURE_COLUMNS` in `src/ml/__init__.py`

```python
FEATURE_COLUMNS = (
    # ... existing features ...
    "claim_amount_anomaly_score",  # NEW
)
```

### Step 4: Add a fill value to `src/ml/features.py`

```python
DEFAULT_FILL_VALUES: Final[dict[str, float | int]] = {
    # ... existing fill values ...
    "claim_amount_anomaly_score": 0.0,  # NEW
}
```

The fill value is used by `fill_nulls()` in `src/ml/features.py:73-79` to replace nulls before training.

### Step 5: Add to `BOOLEAN_FEATURES` or `NUMERIC_FEATURES`

Since `claim_amount_anomaly_score` is numeric (a z-score), add it to `NUMERIC_FEATURES`:

```python
NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    # ... existing numeric features ...
    "claim_amount_anomaly_score",  # NEW
)
```

### Step 6: Update test sample DataFrames

Find the test files in `tests/` (e.g., `test_gold_contract.py`, `test_ml_contract.py`) and add the new column to any synthetic test DataFrames with a default value. Search for `"claim_features"` in test files to locate the relevant fixtures.
