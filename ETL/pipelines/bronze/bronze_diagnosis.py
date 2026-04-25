"""
Bronze streaming table: healthcare.bronze.diagnosis
====================================================
Ingests raw diagnosis code reference data from the landing volume into the Bronze Delta layer
using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles).

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest diagnosis reference data from CSV sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-05  Provides diagnosis_code reference used in downstream Silver/Gold joins.
FR-DATA-06  Handle schema evolution without pipeline failure (cloudFiles.schemaEvolutionMode).
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.

HIPAA compliance controls
--------------------------
diagnosis_code is NOT PHI in this reference table — see classification note below.
Same three TBLPROPERTIES as all Bronze tables — see bronze_claims.py for rationale.

PHI classification: diagnosis_code in this table is medical terminology, NOT PHI.
Per 45 CFR § 164.501, PHI must be "individually identifiable health information" —
it must be linkable to a specific patient. A standalone lookup table (D10=Heart, High)
contains no patient linkage and cannot identify any individual. diagnosis_code becomes
PHI only when it appears alongside patient_id in the claims table. This reference table
requires standard access control but NOT column-level encryption.

Role in the denial prevention system
--------------------------------------
The diagnosis table maps diagnosis codes (D10–D60) to category and severity.
These are used in Gold layer to derive two key denial risk features:
  - diagnosis_severity_encoded  : High=1, Low=0 (high-severity claims with cheap procedures
                                  trigger clinical review denials)
  - specialty_diagnosis_mismatch: provider specialty vs. diagnosis category mismatch
                                  (e.g. Cardiology provider billing for Bone diagnosis)

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/diagnosis/
File        : diagnosis.csv
Schema      : diagnosis_code, category, severity
Lookup codes: D10=Heart, D20=Bone, D30=Fever, D40=Skin, D50=Diabetes, D60=Cold

Output table: healthcare.bronze.diagnosis
Cluster by  : diagnosis_code (join key — claims join on diagnosis_code)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.bronze_pipeline_config import (
    COMMON_DELTA_TABLE_PROPERTIES,
    PIPELINE_RUN_ID_FORMAT,
    bronze_table_name,
    bronze_volume_path,
    csv_autoloader_options,
)
from common.observability import MESSAGE_BRONZE_APPEND_ONLY

TABLE_NAME = bronze_table_name("diagnosis")
VOLUME_PATH = bronze_volume_path("diagnosis")

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# ---------------------------------------------------------------------------

# diagnosis_code IS NOT NULL
# Business rule: every diagnosis record must have an identifier.
# Denial impact: if a claim references a diagnosis_code that has no entry in this lookup,
#               the diagnosis cannot be validated — clinical review denial.
@dp.expect("diagnosis_code_not_null", "diagnosis_code IS NOT NULL")

# category IS NOT NULL
# Business rule: category (e.g. Heart, Bone) drives the specialty mismatch check.
# Denial impact: without category, specialty-diagnosis mismatch cannot be detected downstream.
@dp.expect("category_not_null", "category IS NOT NULL")

# severity IS NOT NULL
# Business rule: severity (High/Low) drives the diagnosis_severity_encoded Gold feature.
# Denial impact: without severity, ML model cannot compute this feature — prediction quality
#               degrades and high-severity claims may be missed.
@dp.expect("severity_not_null", "severity IS NOT NULL")

# _rescued_data IS NULL
# Business rule: any row Auto Loader could not parse is placed in _rescued_data.
# Action required: investigate the source file for encoding or delimiter issues.
@dp.expect("no_parse_errors", "_rescued_data IS NULL")
@dp.table(
    name=TABLE_NAME,
    cluster_by=["diagnosis_code"],
    comment=(
        "Raw diagnosis code reference data ingested from landing volume. Append-only. "
        f"{MESSAGE_BRONZE_APPEND_ONLY} "
        "PHI classification: diagnosis_code here is medical terminology, NOT PHI (45 CFR § 164.501). "
        "This is a lookup table with no patient linkage — it cannot identify any individual. "
        "diagnosis_code becomes PHI only when combined with patient_id in healthcare.bronze.claims. "
        "Maps diagnosis_code to category and severity for downstream ML feature engineering. "
        "Used to detect: (1) specialty-diagnosis mismatch, (2) high-severity + low-cost procedure. "
        "Downstream: healthcare.silver.diagnosis reads this table via Change Data Feed."
    ),
    table_properties=COMMON_DELTA_TABLE_PROPERTIES,
)
def bronze_diagnosis():
    """
    Stream diagnosis CSV files from the landing volume into healthcare.bronze.diagnosis.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with all source columns plus audit columns:

        Original columns (from CSV):
            diagnosis_code  str  Primary key (e.g. D10). Medical terminology reference code.
                                 NOT PHI in this standalone reference table (45 CFR § 164.501) —
                                 no patient linkage exists here. Becomes PHI only when joined
                                 to patient_id in healthcare.bronze.claims. Standard access
                                 control applies; column-level encryption is NOT required here.
            category        str  Diagnosis category (e.g. Heart, Bone, Fever, Skin, Diabetes, Cold).
                                 Used for specialty-diagnosis mismatch detection in Gold layer.
            severity        str  Risk level: 'High' or 'Low'.
                                 Encoded as 1/0 in Gold ML features. High-severity + low-cost
                                 procedure is a known denial trigger.

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
