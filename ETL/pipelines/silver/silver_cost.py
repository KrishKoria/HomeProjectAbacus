"""Silver cost benchmark pipeline with trusted/quarantine split and operator diagnostics."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
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
    spark_decimal_or_null,
    spark_normalize_code,
    spark_normalize_title,
    spark_quality_flags,
)
from common.silver_pipeline_config import (
    MONEY_DECIMAL_PRECISION,
    MONEY_DECIMAL_SCALE,
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    quarantine_table_name,
    read_bronze_cdf,
    silver_table_name,
    silver_table_properties,
)


BRONZE_COST_TABLE = bronze_table_name("cost")
SILVER_COST_TABLE = silver_table_name(CATALOG_DEFAULT, "cost", SILVER_SCHEMA_DEFAULT)
QUARANTINE_COST_TABLE = quarantine_table_name(CATALOG_DEFAULT, "cost", QUARANTINE_SCHEMA_DEFAULT)


def _cost_stream():
    """Normalize regional benchmark rows and attach booleans used by the split outputs."""
    duplicate_window = Window.partitionBy("procedure_code", "region").orderBy(
        # Benchmarks are keyed by procedure + region, so later records replace earlier ones.
        F.col("_ingested_at").desc(),
        F.col("_pipeline_run_id").desc(),
    )
    return (
        read_bronze_cdf(spark, BRONZE_COST_TABLE)
        .withColumn("procedure_code", spark_normalize_code(F.col("procedure_code")))
        .withColumn("average_cost", spark_decimal_or_null(F.col("average_cost"), MONEY_DECIMAL_PRECISION, MONEY_DECIMAL_SCALE))
        .withColumn("expected_cost", spark_decimal_or_null(F.col("expected_cost"), MONEY_DECIMAL_PRECISION, MONEY_DECIMAL_SCALE))
        .withColumn("region", spark_normalize_title(F.col("region")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_data_quality_flags", spark_quality_flags({}))
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("missing_procedure_code", F.col("procedure_code").isNull())
        .withColumn("missing_region", F.col("region").isNull())
        .withColumn("invalid_average_cost", F.col("average_cost").isNull() | (F.col("average_cost") <= F.lit(0)))
        .withColumn("invalid_expected_cost", F.col("expected_cost").isNull() | (F.col("expected_cost") <= F.lit(0)))
    )


@dp.table(
    name=SILVER_COST_TABLE,
    cluster_by=["procedure_code", "region"],
    comment=(
        MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
            table_name=SILVER_COST_TABLE,
            category=LOG_CATEGORY_SILVER_PIPELINE,
            sensitivity="NON-PHI",
        )
        + " Trusted Silver cost benchmarks standardize procedure/region keys and quarantine invalid benchmark rows."
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def silver_cost():
    """Emit the trusted Silver cost benchmark table."""
    trusted = _cost_stream().where(
        (~F.col("missing_procedure_code"))
        & (~F.col("missing_region"))
        & (~F.col("invalid_average_cost"))
        & (~F.col("invalid_expected_cost"))
        & (F.col("_row_priority") == 1)
    )
    return trusted.drop(
        "_row_priority",
        "missing_procedure_code",
        "missing_region",
        "invalid_average_cost",
        "invalid_expected_cost",
    )


@dp.table(
    name=QUARANTINE_COST_TABLE,
    cluster_by=["procedure_code", "diagnostic_id"],
    comment=(
        MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
            dataset="cost",
            rule_name="critical_row_validation",
            diagnostic_id=get_silver_diagnostic_id("cost", "missing_procedure_code"),
            quarantined_records="runtime_count",
        )
        + f" category={LOG_CATEGORY_QUARANTINE_AUDIT}"
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def quarantine_cost():
    """Emit PHI-safe quarantine rows for invalid regional benchmark records."""
    quarantined = (
        _cost_stream()
        .where(
            F.col("missing_procedure_code")
            | F.col("missing_region")
            | F.col("invalid_average_cost")
            | F.col("invalid_expected_cost")
            | (F.col("_row_priority") > 1)
        )
        # The rule order here matches the human-readable reason below so the emitted
        # diagnostic metadata always describes the same primary failure.
        .withColumn(
            "diagnostic_id",
            F.when(F.col("missing_procedure_code"), F.lit(get_silver_diagnostic_id("cost", "missing_procedure_code")))
            .when(F.col("missing_region"), F.lit(get_silver_diagnostic_id("cost", "missing_region")))
            .when(F.col("invalid_average_cost"), F.lit(get_silver_diagnostic_id("cost", "invalid_average_cost")))
            .when(F.col("invalid_expected_cost"), F.lit(get_silver_diagnostic_id("cost", "invalid_expected_cost")))
            .otherwise(F.lit(get_silver_diagnostic_id("cost", "duplicate_cost_key"))),
        )
        .withColumn(
            "rule_name",
            F.when(F.col("missing_procedure_code"), F.lit("missing_procedure_code"))
            .when(F.col("missing_region"), F.lit("missing_region"))
            .when(F.col("invalid_average_cost"), F.lit("invalid_average_cost"))
            .when(F.col("invalid_expected_cost"), F.lit("invalid_expected_cost"))
            .otherwise(F.lit("duplicate_cost_key")),
        )
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Quarantine summary recorded: dataset=cost rule_name="),
                F.col("rule_name"),
                F.lit(" diagnostic_id="),
                F.col("diagnostic_id"),
                F.lit(" quarantined_records=1"),
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("missing_procedure_code"), F.lit("procedure_code is required for benchmark joins"))
            .when(F.col("missing_region"), F.lit("region is required for regional cost benchmarks"))
            .when(F.col("invalid_average_cost"), F.lit("average_cost must parse to a positive decimal"))
            .when(F.col("invalid_expected_cost"), F.lit("expected_cost must parse to a positive decimal"))
            .otherwise(F.lit("duplicate procedure_code + region observed in the silver stream")),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return quarantined.drop("_row_priority")
