"""
Bronze streaming table: healthcare.bronze.providers
====================================================
Ingests raw provider reference data from the landing volume into the Bronze Delta layer
using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles).

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest provider data from CSV sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-05  Provides provider_id reference used in downstream Silver/Gold joins.
FR-DATA-06  Handle schema evolution without pipeline failure (cloudFiles.schemaEvolutionMode).
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.

HIPAA compliance controls
--------------------------
Same three TBLPROPERTIES as all Bronze tables — see bronze_claims.py for rationale.

PHI classification: this table contains NO PHI columns.
Per 45 CFR § 164.501, PHI is "individually identifiable health information" of PATIENTS.
Provider identity (doctor_name, specialty) and provider business location are operational
data — they describe the care provider, not the patient. HIPAA's Privacy Rule protects
patient health information, not provider credentials or business addresses. No column-level
encryption is required for this table.

Known data quality issues (from dataset analysis — Section 9.2 of product spec)
----------------------------------------------------------------------------------
location (nullable)   Some providers have no location on record. Silver imputes 'Unknown'.
                      Missing location causes administrative rejection of claims from that provider.

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/providers/
File        : providers_1000.csv
Schema      : provider_id, doctor_name, specialty, location (nullable)

Output table: healthcare.bronze.providers
Cluster by  : provider_id (join key — claims join on provider_id)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.bronze_pipeline_config import (
    COMMON_DELTA_TABLE_PROPERTIES,
    stable_pipeline_run_id,
    bronze_table_name,
    bronze_volume_path,
    csv_autoloader_options,
)
from common.observability import MESSAGE_BRONZE_APPEND_ONLY

TABLE_NAME = bronze_table_name("providers")
VOLUME_PATH = bronze_volume_path("providers")

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# ---------------------------------------------------------------------------

# provider_id IS NOT NULL
# Business rule: every provider record must have an identifier.
# Denial impact: claims referencing an unidentifiable provider are rejected.
@dp.expect("provider_id_not_null", "provider_id IS NOT NULL")

# doctor_name IS NOT NULL
# Business rule: provider name is required for credentialing verification by insurers.
# Denial impact: unnamed provider fails payer credentialing check.
@dp.expect("doctor_name_not_null", "doctor_name IS NOT NULL")

# location IS NOT NULL  (warn-only — known nullable field in this dataset)
# Business rule: provider location determines regional cost benchmarks and payer routing.
# Denial impact: missing location causes administrative rejection (dataset analysis §9.2).
# Note: this expectation will fire for known-null rows — that is expected and correct.
#       Silver layer imputes 'Unknown' for missing locations.
@dp.expect("location_present", "location IS NOT NULL")

# _rescued_data IS NULL
# Business rule: any row Auto Loader could not parse is placed in _rescued_data.
# Action required: investigate the source file for encoding or delimiter issues.
@dp.expect("no_parse_errors", "_rescued_data IS NULL")
@dp.table(
    name=TABLE_NAME,
    cluster_by=["provider_id"],
    comment=(
        "Raw provider reference data ingested from landing volume. Append-only. "
        f"{MESSAGE_BRONZE_APPEND_ONLY} "
        "Known data quality issue: location is nullable. "
        "Missing location causes administrative rejection of associated claims. "
        "Silver layer (healthcare.silver.providers) imputes location='Unknown' for nulls. "
        "Downstream: healthcare.silver.providers reads a governed Bronze snapshot for Silver materialization."
    ),
    table_properties=COMMON_DELTA_TABLE_PROPERTIES,
)
def bronze_providers():
    """
    Stream provider CSV files from the landing volume into healthcare.bronze.providers.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with all source columns plus audit columns:

        Original columns (from CSV):
            provider_id  str   Primary key (e.g. PR100). Foreign key in claims table.
                               Operational — not PHI.
            doctor_name  str   Provider full name (e.g. Dr Patel).
                               NOT PHI — 45 CFR § 164.501 defines PHI as health information
                               of PATIENTS. Provider names are business/operational identity.
            specialty    str   Medical specialty (e.g. Neurology, Cardiology, General).
                               NOT PHI — describes the provider's credential, not a patient.
            location     str?  Provider business city (e.g. Bangalore, Mumbai). Nullable.
                               NOT PHI — provider business address, not a patient's address.
                               § 164.514(b)(2)(iii) restricts patient geographic subdivisions;
                               this is provider location used for regional cost benchmarking.

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
            stable_pipeline_run_id(),
        )
        .drop("_metadata")
    )
