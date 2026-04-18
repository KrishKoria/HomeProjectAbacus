"""
Bronze streaming table: healthcare.bronze.claims
=================================================
Ingests raw healthcare claims from the landing volume into the Bronze Delta layer
using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles).

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest claims data from CSV sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-06  Handle schema evolution without pipeline failure (cloudFiles.schemaEvolutionMode).
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.

HIPAA compliance controls
--------------------------
delta.enableChangeDataFeed         Enables incremental reads downstream so Silver only
                                   processes new records — required for efficient CDC.
delta.logRetentionDuration         6 years — HIPAA 45 CFR § 164.316(b)(2)(i) requires
                                   security documentation retained minimum 6 years.
delta.deletedFileRetentionDuration 6 years — retains physical files after logical deletion
                                   for time-travel audit reconstruction.
_ingested_at                       Records exactly when data entered the system (HIPAA audit).
_source_file                       Records which file the row came from (data lineage).
_pipeline_run_id                   Groups rows by pipeline execution for audit correlation.
_rescued_data                      Preserves malformed rows instead of silently dropping them.

PHI columns (encrypt at rest in production)
--------------------------------------------
patient_id      — direct patient identifier
billed_amount   — financial PHI
diagnosis_code  — medical condition PHI

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/claims/
File        : claims_1000.csv
Schema      : claim_id, patient_id, provider_id, diagnosis_code,
              procedure_code (nullable), billed_amount (nullable), date

Output table: healthcare.bronze.claims
Cluster by  : claim_id (point lookups), date (time-range scans)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/healthcare/bronze/raw_landing/claims/"

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# Violations are counted and logged to the SDP pipeline event log,
# which feeds the HIPAA audit trail. Check the pipeline UI Data Quality tab.
# ---------------------------------------------------------------------------

# claim_id IS NOT NULL
# Business rule: a claim with no identifier cannot be tracked, appealed, or audited.
# Denial impact: claim is un-submittable — immediate rejection.
@dp.expect("claim_id_not_null", "claim_id IS NOT NULL")

# patient_id IS NOT NULL
# Business rule: no patient reference means the claim cannot be tied to a covered person.
# Denial impact: administrative rejection — insurer cannot verify eligibility.
@dp.expect("patient_id_not_null", "patient_id IS NOT NULL")

# provider_id IS NOT NULL
# Business rule: every claim must identify the billing provider.
# Denial impact: administrative rejection — cannot route claim to correct payer contract.
@dp.expect("provider_id_not_null", "provider_id IS NOT NULL")

# billed_amount > 0 (NULL is allowed — flagged separately as a data quality issue)
# Business rule: a zero or negative billed amount is invalid; NULL is handled downstream.
# Denial impact: invalid amount triggers automatic rejection by clearinghouse.
@dp.expect("billed_amount_positive", "billed_amount IS NULL OR billed_amount > 0")

# _rescued_data IS NULL
# Business rule: any row Auto Loader could not parse is placed in _rescued_data.
# Denial impact: malformed rows cannot be validated or submitted.
# Action required: investigate the source file for encoding or delimiter issues.
@dp.expect("no_parse_errors", "_rescued_data IS NULL")
@dp.table(
    name="healthcare.bronze.claims",
    cluster_by=["claim_id", "date"],
    comment=(
        "Raw healthcare claims ingested from landing volume. Append-only. "
        "Do NOT apply transforms or deletes — Bronze is the source-of-truth for HIPAA audit. "
        "PHI columns (encrypt at rest in production): patient_id, billed_amount, diagnosis_code. "
        "Known data quality issues: procedure_code and billed_amount are nullable. "
        "Downstream: healthcare.silver.claims reads this table via Change Data Feed."
    ),
    table_properties={
        # Change Data Feed: enables Silver to read only new rows incrementally (FR-DATA-08).
        "delta.enableChangeDataFeed": "true",
        # HIPAA 45 CFR § 164.316(b)(2)(i) — 6-year security documentation retention.
        "delta.logRetentionDuration": "interval 6 years",
        # Retain physical files after logical deletion for full time-travel audit reconstruction.
        "delta.deletedFileRetentionDuration": "interval 6 years",
    },
)
def bronze_claims():
    """
    Stream claims CSV files from the landing volume into healthcare.bronze.claims.

    Uses Auto Loader (cloudFiles format) which manages checkpoint state and schema
    location automatically — do NOT configure these manually.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with all source columns plus audit columns:

        Original columns (from CSV):
            claim_id        str     Unique claim identifier (e.g. C0001). PHI-adjacent.
            patient_id      str     Patient reference (e.g. P206). PHI — encrypt at rest.
            provider_id     str     Foreign key → healthcare.bronze.providers.provider_id.
            diagnosis_code  str     ICD code reference (e.g. D10). PHI — encrypt at rest.
            procedure_code  str?    CPT code reference (nullable — known data quality issue).
            billed_amount   float?  Amount billed in INR (nullable — known data quality issue).
                                    PHI — encrypt at rest.
            date            date    Claim submission date.

        Audit columns (added by this pipeline):
            _ingested_at    timestamp  When this row entered the Bronze layer (HIPAA audit).
            _source_file    str        Full volume path of the source file (data lineage).
            _pipeline_run_id str       Pipeline execution timestamp for audit correlation.
            _rescued_data   str?       Raw unparseable content if Auto Loader could not parse
                                       a row. NULL on clean rows. Non-null value = data defect.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        # header=true: CSV files have a header row. Without this, the header is ingested
        # as a data record and every column is misnamed — data is silently corrupted.
        .option("header", "true")
        # cloudFiles.inferColumnTypes: infers exact Spark types (IntegerType, DateType, etc.)
        # instead of leaving every column as StringType. Use this, NOT the generic inferSchema.
        .option("cloudFiles.inferColumnTypes", "true")
        # addNewColumns: if the source CSV gains a new column, add it to the table schema
        # instead of failing the pipeline. Prevents outages on upstream schema changes.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # rescuedDataColumn: rows that cannot be parsed are placed into _rescued_data
        # rather than being silently dropped. Critical for Bronze integrity (FR-DATA-02).
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_pipeline_run_id",
            # Timestamp-based run ID groups rows from the same pipeline execution.
            # Format: yyyyMMdd_HHmmss — sortable, human-readable, no external dependency.
            F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
        )
        # Drop the internal _metadata struct after extracting file_path into _source_file.
        # The struct itself is an Auto Loader implementation detail, not a business column.
        .drop("_metadata")
    )
