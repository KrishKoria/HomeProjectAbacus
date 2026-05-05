"""
Bronze streaming table: healthcare.bronze.dx_px_mapping
========================================================
Ingests the Dx-Px compatibility reference table from the landing volume into the
Bronze Delta layer using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles).

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest Dx-Px mapping reference data from CSV sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-05  Provides diagnosis-procedure compatibility rules used in Gold feature engineering.
FR-DATA-06  Handle schema evolution without pipeline failure (cloudFiles.schemaEvolutionMode).
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.

HIPAA compliance controls
--------------------------
No PHI columns in the dx_px_mapping table — code compatibility rules are non-PHI reference
data. diagnosis_code and procedure_code without patient linkage are medical terminology,
not PHI per § 164.501. Same three TBLPROPERTIES as all Bronze tables.

Role in the denial prevention system
--------------------------------------
The dx_px_mapping table defines which (diagnosis_code, procedure_code) pairs are
clinically compatible and provides a synthetic pair-risk prior per pair.
In Gold, this drives two features:
  - dx_px_compatible           : is this (Dx, Px) pair medically valid?
  - dx_px_pair_risk_prior      : synthetic prior for denial risk by (Dx, Px) pair

In production, this table would be replaced by payer historical aggregates and
coverage-policy reference data.

Clustering on (diagnosis_code, procedure_code) enables efficient broadcast joins
during Gold feature engineering.

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/dx_px_mapping/
File        : dx_px_mapping.csv
Schema      : diagnosis_code, procedure_code, compatible, procedure_category,
              pair_risk_prior
Codes       : 36 rows — full Cartesian product of 6 diagnoses × 6 procedures

Output table: healthcare.bronze.dx_px_mapping
Cluster by  : diagnosis_code, procedure_code (composite join key)
"""

from __future__ import annotations

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

TABLE_NAME = bronze_table_name("dx_px_mapping")
VOLUME_PATH = bronze_volume_path("dx_px_mapping")

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# ---------------------------------------------------------------------------

# diagnosis_code IS NOT NULL
@dp.expect("diagnosis_code_not_null", "diagnosis_code IS NOT NULL")

# procedure_code IS NOT NULL
@dp.expect("procedure_code_not_null", "procedure_code IS NOT NULL")

# compatible IS NOT NULL AND IN (0, 1)
@dp.expect("compatible_valid", "compatible IS NOT NULL AND compatible IN (0, 1)")

# procedure_category IS NOT NULL
@dp.expect("procedure_category_not_null", "procedure_category IS NOT NULL")

# pair_risk_prior in [0, 1]
@dp.expect(
    "pair_risk_prior_valid",
    "pair_risk_prior IS NOT NULL AND pair_risk_prior >= 0.0 AND pair_risk_prior <= 1.0",
)

# _rescued_data IS NULL
@dp.expect("no_parse_errors", "_rescued_data IS NULL")
@dp.table(
    name=TABLE_NAME,
    cluster_by=["diagnosis_code", "procedure_code"],
    comment=(
        "Dx-Px compatibility reference table ingested from landing volume. Append-only. "
        f"{MESSAGE_BRONZE_APPEND_ONLY} "
        "No PHI columns — code mapping rules are non-PHI reference data. "
        "Defines clinically compatible (diagnosis, procedure) pairs and synthetic "
        "pair-risk priors. "
        "Key use: dx_px_compatible and dx_px_pair_risk_prior features in Gold layer. "
        "Downstream: healthcare.silver.dx_px_mapping reads a governed Bronze snapshot."
    ),
    table_properties=COMMON_DELTA_TABLE_PROPERTIES,
)
def bronze_dx_px_mapping():
    """
    Stream Dx-Px mapping CSV files from the landing volume into healthcare.bronze.dx_px_mapping.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with all source columns plus audit columns:

        Original columns (from CSV):
            diagnosis_code       str   Diagnosis code (e.g. D10). Composite primary key.
            procedure_code       str   Procedure code (e.g. PROC2). Composite primary key.
            compatible           int   1 if medically valid pair, 0 otherwise.
            procedure_category   str   Coarse procedure category (Cardiac, General, etc.).
            pair_risk_prior      float Synthetic risk prior [0, 1] for this pair.

        Audit columns (added by this pipeline):
            _ingested_at     timestamp  When this row entered the Bronze layer.
            _source_file     str        Full volume path of the source file.
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
