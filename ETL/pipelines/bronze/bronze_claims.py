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
delta.logRetentionDuration         interval 2190 days — organization retention policy for audit
                                   reconstruction. §164.316(b)(2)(i) mandates 6-year
                                   retention for compliance documentation; Bronze data
                                   supports that trail. Raw PHI table retention is also
                                   governed by applicable state medical records law.
delta.deletedFileRetentionDuration interval 2190 days — retains physical files after logical deletion
                                   for time-travel audit reconstruction.
_ingested_at                       Records exactly when data entered the system.
                                   Audit control per § 164.312(b) — hardware/software
                                   mechanisms that record activity in ePHI systems.
_source_file                       Records which file the row came from (data lineage).
                                   Audit control per § 164.312(b).
_pipeline_run_id                   Groups rows by pipeline execution for audit correlation.
                                   Audit control per § 164.312(b).
_rescued_data                      Preserves malformed rows instead of silently dropping them.
                                   Data integrity per § 164.312(c)(1).

Minimum Necessary justification (§ 164.502(b))
-----------------------------------------------
Bronze ingests ALL columns from the source CSV. This satisfies the Minimum Necessary
standard because the Bronze layer IS the source-of-truth archive required by
§ 164.316(b)(2)(i) — it must be complete and unaltered for audit reconstruction.
Transformations, column masking, and PHI redaction occur at Silver and Gold layers
where only the minimum PHI necessary for each downstream purpose is exposed.

PHI columns — encrypt at rest in production (§ 164.312(a)(2)(iv))
-------------------------------------------------------------------
Encryption is an addressable implementation spec under the Security Rule. In production,
implement AES-256 via Databricks column masking before ingesting real PHI. Any breach
of unencrypted PHI in these columns must be reported to the Covered Entity within
60 days per § 164.410(b).

patient_id      — § 164.514(b)(2)(ii)  direct patient identifier (unique identifying code)
billed_amount   — § 164.514(b)(2)      financial health information
diagnosis_code  — § 164.514(b)(2)(xvi) medical condition linked to patient_id
date            — § 164.514(b)(2)(iv)  claim submission date is a date directly related
                                        to an individual's health event; all date elements
                                        except year are PHI identifiers under HIPAA

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/claims/
File        : claims_1000.csv
Schema      : claim_id, patient_id, provider_id, diagnosis_code,
              procedure_code (nullable), billed_amount (nullable), date,
              synthetic adjudication/payment labels

Output table: healthcare.bronze.claims
Cluster by  : claim_id (point lookups), date (time-range scans)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from ETL.common.bronze_pipeline_config import (
    PIPELINE_RUN_ID_FORMAT,
    bronze_table_name,
    bronze_volume_path,
    csv_autoloader_options,
    table_properties_for_sensitivity,
)
from ETL.common.bronze_sources import BRONZE_SOURCES
from ETL.common.observability import MESSAGE_BRONZE_APPEND_ONLY

TABLE_NAME = bronze_table_name("claims")
VOLUME_PATH = bronze_volume_path("claims")
CLAIMS_PHI_COLUMNS = tuple(sorted(BRONZE_SOURCES["claims"].phi_columns))

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
    name=TABLE_NAME,
    cluster_by=["claim_id", "date"],
    comment=(
        "Raw healthcare claims ingested from landing volume. Append-only. "
        f"{MESSAGE_BRONZE_APPEND_ONLY} "
        "PHI columns (encrypt at rest in production): patient_id, billed_amount, diagnosis_code, claim status, denial/payment labels. "
        "Known data quality issues: procedure_code and billed_amount are nullable. "
        "Downstream: healthcare.silver.claims reads this table via Change Data Feed."
    ),
    table_properties=table_properties_for_sensitivity("PHI", CLAIMS_PHI_COLUMNS),
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
            patient_id      str     Patient reference (e.g. P206).
                                    PHI — § 164.514(b)(2)(ii) unique identifying code.
                                    Encrypt at rest in production (§ 164.312(a)(2)(iv)).
            provider_id     str     Foreign key → healthcare.bronze.providers.provider_id.
                                    Operational — not PHI.
            diagnosis_code  str     ICD code reference (e.g. D10).
                                    PHI — § 164.514(b)(2)(xvi) health condition linked to patient.
                                    Encrypt at rest in production (§ 164.312(a)(2)(iv)).
            procedure_code  str?    CPT code reference (nullable — known data quality issue).
                                    PHI-adjacent — linked to a patient's health event.
            billed_amount   float?  Amount billed in INR (nullable — known data quality issue).
                                    PHI — § 164.514(b)(2) financial health information.
                                    Encrypt at rest in production (§ 164.312(a)(2)(iv)).
            date            date    Claim submission date.
                                    PHI — § 164.514(b)(2)(iv) all date elements except year
                                    for dates directly related to a health event are PHI.
                                    Encrypt at rest in production (§ 164.312(a)(2)(iv)).
            claim_status    str     Synthetic APPROVED/DENIED label for demo ML workflows.
            denial_reason_code str  Synthetic denial reason or NONE for approved rows.
            allowed_amount  decimal Synthetic allowed amount.
            paid_amount     decimal Synthetic paid amount.
            is_denied       bool    Synthetic binary ML label.
            follow_up_required bool Synthetic workflow label.

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
        .options(**csv_autoloader_options())
        .load(VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_pipeline_run_id",
            # Timestamp-based run ID groups rows from the same pipeline execution.
            # Format: yyyyMMdd_HHmmss — sortable, human-readable, no external dependency.
            F.date_format(F.current_timestamp(), PIPELINE_RUN_ID_FORMAT),
        )
        # Drop the internal _metadata struct after extracting file_path into _source_file.
        # The struct itself is an Auto Loader implementation detail, not a business column.
        .drop("_metadata")
    )
