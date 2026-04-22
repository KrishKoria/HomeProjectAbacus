"""Silver diagnosis pipeline with normalized lookup values and quarantine diagnostics."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.diagnostics import get_silver_diagnostic_id
from src.common.observability import (
    LOG_CATEGORY_QUARANTINE_AUDIT,
    LOG_CATEGORY_SILVER_PIPELINE,
    MESSAGE_TEMPLATE_QUARANTINE_SUMMARY,
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,
)
from src.common.silver_cleaning import (
    spark_normalize_code,
    spark_normalize_severity,
    spark_normalize_title,
    spark_quality_flags,
)
from src.common.silver_pipeline_config import (
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    read_bronze_cdf,
    silver_table_properties,
)


BRONZE_DIAGNOSIS_TABLE = "healthcare.bronze.diagnosis"
SILVER_DIAGNOSIS_TABLE = f"healthcare.{SILVER_SCHEMA_DEFAULT}.diagnosis"
QUARANTINE_DIAGNOSIS_TABLE = f"healthcare.{QUARANTINE_SCHEMA_DEFAULT}.diagnosis"


def _diagnosis_stream():
    """Normalize diagnosis rows and attach validation flags for trusted/quarantine splits."""
    duplicate_window = Window.partitionBy("diagnosis_code").orderBy(
        F.coalesce(F.col("_commit_timestamp"), F.col("_ingested_at")).desc(),
        F.col("_pipeline_run_id").desc(),
    )
    return (
        read_bronze_cdf(spark, BRONZE_DIAGNOSIS_TABLE)
        .where(F.col("_change_type").isin("insert", "update_postimage"))
        .withColumn("diagnosis_code", spark_normalize_code(F.col("diagnosis_code")))
        .withColumn("category", spark_normalize_title(F.col("category")))
        .withColumn("severity", spark_normalize_severity(F.col("severity")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_data_quality_flags", spark_quality_flags({}))
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("missing_diagnosis_code", F.col("diagnosis_code").isNull())
        .withColumn("missing_category", F.col("category").isNull())
        .withColumn("missing_severity", F.col("severity").isNull())
        .withColumn("invalid_severity", ~F.col("severity").isin("High", "Low"))
    )


@dp.table(
    name=SILVER_DIAGNOSIS_TABLE,
    cluster_by=["diagnosis_code", "severity"],
    comment=(
        MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
            table_name=SILVER_DIAGNOSIS_TABLE,
            category=LOG_CATEGORY_SILVER_PIPELINE,
            sensitivity="NON-PHI",
        )
        + " Trusted Silver diagnosis records standardize category/severity labels and quarantine invalid code rows."
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def silver_diagnosis():
    """Emit the trusted Silver diagnosis lookup table."""
    trusted = _diagnosis_stream().where(
        (~F.col("missing_diagnosis_code"))
        & (~F.col("missing_category"))
        & (~F.col("missing_severity"))
        & (~F.col("invalid_severity"))
        & (F.col("_row_priority") == 1)
    )
    return trusted.drop(
        "_row_priority",
        "missing_diagnosis_code",
        "missing_category",
        "missing_severity",
        "invalid_severity",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    )


@dp.table(
    name=QUARANTINE_DIAGNOSIS_TABLE,
    cluster_by=["diagnosis_code", "diagnostic_id"],
    comment=(
        MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
            dataset="diagnosis",
            rule_name="critical_row_validation",
            diagnostic_id=get_silver_diagnostic_id("diagnosis", "missing_diagnosis_code"),
            quarantined_records="runtime_count",
        )
        + f" category={LOG_CATEGORY_QUARANTINE_AUDIT}"
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def quarantine_diagnosis():
    """Emit PHI-safe quarantine rows for diagnosis records that failed validation."""
    quarantined = (
        _diagnosis_stream()
        .where(
            F.col("missing_diagnosis_code")
            | F.col("missing_category")
            | F.col("missing_severity")
            | F.col("invalid_severity")
            | (F.col("_row_priority") > 1)
        )
        # These branches intentionally prioritize content validation over duplicate
        # detection so the first emitted diagnostic explains the most actionable issue.
        .withColumn(
            "diagnostic_id",
            F.when(F.col("missing_diagnosis_code"), F.lit(get_silver_diagnostic_id("diagnosis", "missing_diagnosis_code")))
            .when(F.col("missing_category"), F.lit(get_silver_diagnostic_id("diagnosis", "missing_category")))
            .when(F.col("missing_severity"), F.lit(get_silver_diagnostic_id("diagnosis", "missing_severity")))
            .when(F.col("invalid_severity"), F.lit(get_silver_diagnostic_id("diagnosis", "invalid_severity")))
            .otherwise(F.lit(get_silver_diagnostic_id("diagnosis", "duplicate_diagnosis_code"))),
        )
        .withColumn(
            "rule_name",
            F.when(F.col("missing_diagnosis_code"), F.lit("missing_diagnosis_code"))
            .when(F.col("missing_category"), F.lit("missing_category"))
            .when(F.col("missing_severity"), F.lit("missing_severity"))
            .when(F.col("invalid_severity"), F.lit("invalid_severity"))
            .otherwise(F.lit("duplicate_diagnosis_code")),
        )
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Quarantine summary recorded: dataset=diagnosis rule_name="),
                F.col("rule_name"),
                F.lit(" diagnostic_id="),
                F.col("diagnostic_id"),
                F.lit(" quarantined_records=1"),
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("missing_diagnosis_code"), F.lit("diagnosis_code is required for lookup validation"))
            .when(F.col("missing_category"), F.lit("category is required for specialty mismatch features"))
            .when(F.col("missing_severity"), F.lit("severity is required for downstream risk features"))
            .when(F.col("invalid_severity"), F.lit("severity must normalize to High or Low"))
            .otherwise(F.lit("duplicate diagnosis_code observed in the silver stream")),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return quarantined.drop("_change_type", "_commit_version", "_commit_timestamp", "_row_priority")
