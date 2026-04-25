"""Silver providers pipeline with explicit imputation and quarantine diagnostics."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.bronze_pipeline_config import CATALOG_DEFAULT, bronze_table_name
from src.common.diagnostics import get_silver_diagnostic_id
from src.common.observability import (
    LOG_CATEGORY_QUARANTINE_AUDIT,
    LOG_CATEGORY_SILVER_PIPELINE,
    MESSAGE_TEMPLATE_QUARANTINE_SUMMARY,
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,
)
from src.common.silver_cleaning import spark_normalize_code, spark_normalize_title, spark_quality_flags
from src.common.silver_pipeline_config import (
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    quarantine_table_name,
    read_bronze_cdf,
    silver_table_name,
    silver_table_properties,
)


BRONZE_PROVIDERS_TABLE = bronze_table_name("providers")
SILVER_PROVIDERS_TABLE = silver_table_name(CATALOG_DEFAULT, "providers", SILVER_SCHEMA_DEFAULT)
QUARANTINE_PROVIDERS_TABLE = quarantine_table_name(CATALOG_DEFAULT, "providers", QUARANTINE_SCHEMA_DEFAULT)


def _providers_stream():
    """Normalize provider records and attach validation booleans used downstream."""
    duplicate_window = Window.partitionBy("provider_id").orderBy(
        F.col("_ingested_at").desc(),
        F.col("_pipeline_run_id").desc(),
    )
    cleaned = (
        read_bronze_cdf(spark, BRONZE_PROVIDERS_TABLE)
        .withColumn("provider_id", spark_normalize_code(F.col("provider_id")))
        .withColumn("doctor_name", spark_normalize_title(F.col("doctor_name")))
        .withColumn("specialty", spark_normalize_title(F.col("specialty")))
        .withColumn("location", spark_normalize_title(F.col("location")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn(
            "_data_quality_flags",
            spark_quality_flags({"provider_location_unknown": F.col("location").isNull()}),
        )
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("missing_provider_id", F.col("provider_id").isNull())
        .withColumn("missing_doctor_name", F.col("doctor_name").isNull())
    )
    # Missing location is survivable for provider lookups, so keep the row trusted
    # and surface the gap through a quality flag instead of quarantining it.
    return cleaned.withColumn("location", F.coalesce(F.col("location"), F.lit("Unknown")))


@dp.table(
    name=SILVER_PROVIDERS_TABLE,
    cluster_by=["provider_id", "specialty"],
    comment=(
        MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
            table_name=SILVER_PROVIDERS_TABLE,
            category=LOG_CATEGORY_SILVER_PIPELINE,
            sensitivity="NON-PHI",
        )
        + " Trusted Silver providers impute missing location to 'Unknown' and quarantine only missing business keys."
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def silver_providers():
    """Emit the trusted Silver providers table."""
    trusted = _providers_stream().where(
        F.col("missing_provider_id").eqNullSafe(False)
        & F.col("missing_doctor_name").eqNullSafe(False)
        & (F.col("_row_priority") == 1)
    )
    return trusted.drop(
        "_row_priority",
        "missing_provider_id",
        "missing_doctor_name",
    )


@dp.table(
    name=QUARANTINE_PROVIDERS_TABLE,
    cluster_by=["provider_id", "diagnostic_id"],
    comment=(
        MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
            dataset="providers",
            rule_name="critical_row_validation",
            diagnostic_id=get_silver_diagnostic_id("providers", "missing_provider_id"),
            quarantined_records="runtime_count",
        )
        + f" category={LOG_CATEGORY_QUARANTINE_AUDIT}"
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def quarantine_providers():
    """Emit PHI-safe quarantine rows for provider records that cannot be trusted."""
    diagnostics = (
        _providers_stream()
        .where(F.col("missing_provider_id") | F.col("missing_doctor_name") | (F.col("_row_priority") > 1))
        .withColumn(
            "diagnostic_id",
            F.when(F.col("missing_provider_id"), F.lit(get_silver_diagnostic_id("providers", "missing_provider_id")))
            .when(F.col("missing_doctor_name"), F.lit(get_silver_diagnostic_id("providers", "missing_doctor_name")))
            .otherwise(F.lit(get_silver_diagnostic_id("providers", "duplicate_provider_id"))),
        )
        .withColumn(
            "rule_name",
            F.when(F.col("missing_provider_id"), F.lit("missing_provider_id"))
            .when(F.col("missing_doctor_name"), F.lit("missing_doctor_name"))
            .otherwise(F.lit("duplicate_provider_id")),
        )
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Quarantine summary recorded: dataset=providers rule_name="),
                F.col("rule_name"),
                F.lit(" diagnostic_id="),
                F.col("diagnostic_id"),
                F.lit(" quarantined_records=1"),
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("missing_provider_id"), F.lit("provider_id is required for a trusted provider record"))
            .when(F.col("missing_doctor_name"), F.lit("doctor_name is required for credential verification"))
            .otherwise(F.lit("duplicate provider_id observed in the silver stream")),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return diagnostics.drop("_row_priority")
