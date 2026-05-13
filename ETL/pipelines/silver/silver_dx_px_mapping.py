"""Silver Dx-Px mapping pipeline with trusted/quarantine split and operator diagnostics."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.bronze_pipeline_config import CATALOG_DEFAULT, bronze_table_name
from common.diagnostics import get_silver_diagnostic_id
from common.observability import (
    LOG_CATEGORY_QUARANTINE_AUDIT,
    LOG_CATEGORY_SILVER_PIPELINE,
    MESSAGE_TEMPLATE_QUARANTINE_SUMMARY,
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,
)
from common.silver_cleaning import (
    spark_normalize_code,
    spark_normalize_title,
    spark_quality_flags,
    spark_trim_to_null,
)
from common.silver_pipeline_config import (
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    dedup_window,
    quarantine_table_name,
    read_bronze_snapshot,
    silver_table_name,
    silver_table_properties,
)


BRONZE_DX_PX_MAPPING_TABLE = bronze_table_name("dx_px_mapping")
SILVER_DX_PX_MAPPING_TABLE = silver_table_name(CATALOG_DEFAULT, "dx_px_mapping", SILVER_SCHEMA_DEFAULT)
QUARANTINE_DX_PX_MAPPING_TABLE = quarantine_table_name(CATALOG_DEFAULT, "dx_px_mapping", QUARANTINE_SCHEMA_DEFAULT)


@dp.temporary_view(name="dx_px_mapping_stream")
def _dx_px_mapping_stream():
    """Normalize Dx-Px mapping rows and attach booleans used by the split outputs."""
    duplicate_window = dedup_window("diagnosis_code", "procedure_code")
    return (
        read_bronze_snapshot(spark, BRONZE_DX_PX_MAPPING_TABLE)
        .withColumn("diagnosis_code", spark_normalize_code(F.col("diagnosis_code")))
        .withColumn("procedure_code", spark_normalize_code(F.col("procedure_code")))
        .withColumn("procedure_category", spark_normalize_title(F.col("procedure_category")))
        .withColumn("pair_risk_prior", spark_trim_to_null(F.col("pair_risk_prior")).cast("double"))
        .withColumn("_data_quality_flags", spark_quality_flags({}))
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("missing_diagnosis_code", F.col("diagnosis_code").isNull())
        .withColumn("missing_procedure_code", F.col("procedure_code").isNull())
        .withColumn("missing_procedure_category", F.col("procedure_category").isNull())
        .withColumn(
            "invalid_compatible",
            F.col("compatible").isNull() | ~F.col("compatible").isin(0, 1),
        )
        .withColumn(
            "invalid_pair_risk_prior",
            F.col("pair_risk_prior").isNull()
            | (F.col("pair_risk_prior") < 0.0)
            | (F.col("pair_risk_prior") > 1.0),
        )
    )


@dp.materialized_view(
    name=SILVER_DX_PX_MAPPING_TABLE,
    refresh_policy="incremental",
    comment=(
        MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
            table_name=SILVER_DX_PX_MAPPING_TABLE,
            category=LOG_CATEGORY_SILVER_PIPELINE,
            sensitivity="NON-PHI",
        )
        + " Trusted Silver Dx-Px mapping standardizes code keys, validates compatibility flags,"
        " and quarantines invalid mapping rows."
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def silver_dx_px_mapping():
    """Emit the trusted Silver Dx-Px mapping table."""
    trusted = spark.read.table("dx_px_mapping_stream").where(
        (~F.col("missing_diagnosis_code"))
        & (~F.col("missing_procedure_code"))
        & (~F.col("missing_procedure_category"))
        & (~F.col("invalid_compatible"))
        & (~F.col("invalid_pair_risk_prior"))
        & (F.col("_row_priority") == 1)
    )
    return trusted.drop(
        "_row_priority",
        "missing_diagnosis_code",
        "missing_procedure_code",
        "missing_procedure_category",
        "invalid_compatible",
        "invalid_pair_risk_prior",
    )


@dp.materialized_view(
    name=QUARANTINE_DX_PX_MAPPING_TABLE,
    refresh_policy="incremental",
    comment=(
        MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
            dataset="dx_px_mapping",
            rule_name="critical_row_validation",
            diagnostic_id=get_silver_diagnostic_id("dx_px_mapping", "missing_diagnosis_code"),
            quarantined_records="runtime_count",
        )
        + f" category={LOG_CATEGORY_QUARANTINE_AUDIT}"
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def quarantine_dx_px_mapping():
    """Emit PHI-safe quarantine rows for invalid Dx-Px mapping records."""
    quarantined = (
        spark.read.table("dx_px_mapping_stream")
        .where(
            F.col("missing_diagnosis_code")
            | F.col("missing_procedure_code")
            | F.col("missing_procedure_category")
            | F.col("invalid_compatible")
            | F.col("invalid_pair_risk_prior")
            | (F.col("_row_priority") > 1)
        )
        .withColumn(
            "diagnostic_id",
            F.when(F.col("missing_diagnosis_code"), F.lit(get_silver_diagnostic_id("dx_px_mapping", "missing_diagnosis_code")))
            .when(F.col("missing_procedure_code"), F.lit(get_silver_diagnostic_id("dx_px_mapping", "missing_procedure_code")))
            .when(F.col("missing_procedure_category"), F.lit(get_silver_diagnostic_id("dx_px_mapping", "missing_procedure_category")))
            .when(F.col("invalid_compatible"), F.lit(get_silver_diagnostic_id("dx_px_mapping", "invalid_compatible")))
            .when(F.col("invalid_pair_risk_prior"), F.lit(get_silver_diagnostic_id("dx_px_mapping", "invalid_pair_risk_prior")))
            .otherwise(F.lit(get_silver_diagnostic_id("dx_px_mapping", "duplicate_mapping_key"))),
        )
        .withColumn(
            "rule_name",
            F.when(F.col("missing_diagnosis_code"), F.lit("missing_diagnosis_code"))
            .when(F.col("missing_procedure_code"), F.lit("missing_procedure_code"))
            .when(F.col("missing_procedure_category"), F.lit("missing_procedure_category"))
            .when(F.col("invalid_compatible"), F.lit("invalid_compatible"))
            .when(F.col("invalid_pair_risk_prior"), F.lit("invalid_pair_risk_prior"))
            .otherwise(F.lit("duplicate_mapping_key")),
        )
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Quarantine summary recorded: dataset=dx_px_mapping rule_name="),
                F.col("rule_name"),
                F.lit(" diagnostic_id="),
                F.col("diagnostic_id"),
                F.lit(" quarantined_records=1"),
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("missing_diagnosis_code"), F.lit("diagnosis_code is required for mapping lookups"))
            .when(F.col("missing_procedure_code"), F.lit("procedure_code is required for mapping lookups"))
            .when(F.col("missing_procedure_category"), F.lit("procedure_category is required for downstream feature derivation"))
            .when(F.col("invalid_compatible"), F.lit("compatible must be 0 or 1"))
            .when(F.col("invalid_pair_risk_prior"), F.lit("pair_risk_prior must be between 0.0 and 1.0"))
            .otherwise(F.lit("duplicate diagnosis_code + procedure_code observed")),
        )
    )
    return quarantined.drop("_row_priority")
