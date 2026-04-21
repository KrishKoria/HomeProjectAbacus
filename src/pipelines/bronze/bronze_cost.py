"""
Bronze streaming table: healthcare.bronze.cost
===============================================
Ingests raw procedure cost benchmark data from the landing volume into the Bronze Delta layer
using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles).

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest cost benchmark data from CSV sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-05  Provides procedure_code/region benchmarks used in downstream Silver/Gold joins.
FR-DATA-06  Handle schema evolution without pipeline failure (cloudFiles.schemaEvolutionMode).
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.

HIPAA compliance controls
--------------------------
No PHI columns in the cost table — cost benchmarks are non-PHI reference data.
Same three TBLPROPERTIES as all Bronze tables — see bronze_claims.py for rationale.

Role in the denial prevention system
--------------------------------------
The cost table provides regional procedure cost benchmarks. It drives the most important
overbilling denial risk feature in the Gold layer:
  - amount_to_benchmark_ratio = billed_amount / expected_cost
    If ratio > 1.5 → HIGH denial risk (overbilling flag).
    If ratio > 2.0 → ANOMALY (potential fraud flag, triggers clinical review).

Clustering on (procedure_code, region) enables efficient lookups during the Silver→Gold
join: for a given claim, look up expected_cost where procedure_code matches and region
matches the provider's location.

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/cost/
File        : cost.csv
Schema      : procedure_code, average_cost, expected_cost, region
Codes       : PROC1–PROC6
Regions     : Delhi, Mumbai, Bangalore, and others

Output table: healthcare.bronze.cost
Cluster by  : procedure_code, region (composite join key — claims+providers join on both)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.common.bronze_pipeline_config import (
    COMMON_DELTA_TABLE_PROPERTIES,
    PIPELINE_RUN_ID_FORMAT,
    bronze_volume_path,
    csv_autoloader_options,
)
from src.common.observability import MESSAGE_BRONZE_APPEND_ONLY

VOLUME_PATH = bronze_volume_path("cost")

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# ---------------------------------------------------------------------------

# procedure_code IS NOT NULL
# Business rule: every cost record must reference a specific procedure.
# Denial impact: if a claim's procedure_code has no cost entry, the overbilling
#               ratio cannot be computed — the claim cannot be risk-scored.
@dp.expect("procedure_code_not_null", "procedure_code IS NOT NULL")

# average_cost > 0
# Business rule: historical average cost must be a positive number.
# Denial impact: zero/negative average_cost corrupts the overbilling ratio calculation.
@dp.expect("average_cost_positive", "average_cost IS NULL OR average_cost > 0")

# expected_cost > 0
# Business rule: benchmark/expected cost must be a positive number.
# Denial impact: zero/negative expected_cost makes billed_amount / expected_cost
#               either infinite or negative — overbilling detection is unreliable.
@dp.expect("expected_cost_positive", "expected_cost IS NULL OR expected_cost > 0")

# region IS NOT NULL
# Business rule: cost benchmarks are region-specific. A null region cannot be joined
#               to a provider's location, so no regional benchmark applies.
# Denial impact: missing region means overbilling detection falls back to national average
#               which may produce false positives or false negatives.
@dp.expect("region_not_null", "region IS NOT NULL")

# _rescued_data IS NULL
# Business rule: any row Auto Loader could not parse is placed in _rescued_data.
# Action required: investigate the source file for encoding or delimiter issues.
@dp.expect("no_parse_errors", "_rescued_data IS NULL")
@dp.table(
    name="healthcare.bronze.cost",
    cluster_by=["procedure_code", "region"],
    comment=(
        "Raw procedure cost benchmarks ingested from landing volume. Append-only. "
        f"{MESSAGE_BRONZE_APPEND_ONLY} "
        "No PHI columns — cost benchmarks are non-PHI reference data. "
        "Provides average_cost and expected_cost per procedure_code and region. "
        "Key use: amount_to_benchmark_ratio = billed_amount / expected_cost in Gold layer. "
        "Ratio > 1.5 → HIGH denial risk. Ratio > 2.0 → potential fraud flag. "
        "Downstream: healthcare.silver.cost reads this table via Change Data Feed."
    ),
    table_properties=COMMON_DELTA_TABLE_PROPERTIES,
)
def bronze_cost():
    """
    Stream cost benchmark CSV files from the landing volume into healthcare.bronze.cost.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with all source columns plus audit columns:

        Original columns (from CSV):
            procedure_code  str    Primary key (e.g. PROC1). Foreign key in claims table.
            average_cost    int    Historical average procedure cost in INR.
            expected_cost   int    Benchmark/expected cost in INR.
                                   Denominator in the overbilling ratio feature.
                                   Gold: amount_to_benchmark_ratio = billed_amount / expected_cost
            region          str    Geographic region (e.g. Delhi, Mumbai, Bangalore).
                                   Joined with provider.location to apply regional benchmarks.

        Audit columns (added by this pipeline):
            _ingested_at     timestamp  When this row entered the Bronze layer (HIPAA audit).
            _source_file     str        Full volume path of the source file (data lineage).
            _pipeline_run_id str        Pipeline execution timestamp for audit correlation.
            _rescued_data    str?       Raw unparseable content. NULL on clean rows.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**csv_autoloader_options())
        .load(VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_pipeline_run_id",
            F.date_format(F.current_timestamp(), PIPELINE_RUN_ID_FORMAT),
        )
        .drop("_metadata")
    )
