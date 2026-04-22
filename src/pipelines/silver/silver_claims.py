"""Silver claims pipeline with PHI-safe quarantine diagnostics.

This module keeps the trusted claims surface readable by isolating the non-obvious
bits: normalization, critical-row quarantine, and duplicate resolution.
quality signals are emitted as flags and PHI-safe status strings, never as raw
patient values in logs.
"""

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
    spark_date_or_null,
    spark_decimal_or_null,
    spark_normalize_code,
    spark_quality_flags,
)
from src.common.silver_pipeline_config import (
    MONEY_DECIMAL_PRECISION,
    MONEY_DECIMAL_SCALE,
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    read_bronze_cdf,
    silver_table_properties,
)


BRONZE_CLAIMS_TABLE = "healthcare.bronze.claims"
BRONZE_PROVIDERS_TABLE = "healthcare.bronze.providers"
BRONZE_DIAGNOSIS_TABLE = "healthcare.bronze.diagnosis"

SILVER_CLAIMS_TABLE = f"healthcare.{SILVER_SCHEMA_DEFAULT}.claims"
QUARANTINE_CLAIMS_TABLE = f"healthcare.{QUARANTINE_SCHEMA_DEFAULT}.claims"

_TRUSTED_COMMENT = MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
    table_name=SILVER_CLAIMS_TABLE,
    category=LOG_CATEGORY_SILVER_PIPELINE,
    sensitivity="PHI",
)
_QUARANTINE_COMMENT = MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
    dataset="claims",
    rule_name="critical_row_validation",
    diagnostic_id=get_silver_diagnostic_id("claims", "missing_claim_id"),
    quarantined_records="{runtime_count}",
).replace("{runtime_count}", "runtime_count")


def _normalized_provider_lookup():
    """Return the provider lookup used to validate claims without pulling PHI into logs."""
    return (
        spark.table(BRONZE_PROVIDERS_TABLE)
        .select(
            spark_normalize_code(F.col("provider_id")).alias(
                "provider_id_lookup"),
            F.lit(True).alias("provider_exists"),
        )
        .dropna()
        .dropDuplicates(["provider_id_lookup"])
    )


def _normalized_diagnosis_lookup():
    """Return the diagnosis lookup used to validate claims without logging row values."""
    return (
        spark.table(BRONZE_DIAGNOSIS_TABLE)
        .select(
            spark_normalize_code(F.col("diagnosis_code")).alias(
                "diagnosis_code_lookup"),
            F.lit(True).alias("diagnosis_exists"),
        )
        .dropna()
        .dropDuplicates(["diagnosis_code_lookup"])
    )


def _claims_stream():
    """Build the shared normalized claims stream used by trusted and quarantine outputs."""
    claims = read_bronze_cdf(spark, BRONZE_CLAIMS_TABLE)
    providers = _normalized_provider_lookup()
    diagnosis = _normalized_diagnosis_lookup()

    cleaned = (
        claims.where(F.col("_change_type").isin("insert", "update_postimage"))
        .withColumn("claim_id", spark_normalize_code(F.col("claim_id")))
        .withColumn("patient_id", spark_normalize_code(F.col("patient_id")))
        .withColumn("provider_id", spark_normalize_code(F.col("provider_id")))
        .withColumn("diagnosis_code", spark_normalize_code(F.col("diagnosis_code")))
        .withColumn("procedure_code", spark_normalize_code(F.col("procedure_code")))
        .withColumn(
            "billed_amount",
            spark_decimal_or_null(F.col("billed_amount"),
                                  MONEY_DECIMAL_PRECISION, MONEY_DECIMAL_SCALE),
        )
        .withColumn("date", spark_date_or_null(F.col("date")))
        .join(providers, F.col("provider_id") == F.col("provider_id_lookup"), "left")
        .join(diagnosis, F.col("diagnosis_code") == F.col("diagnosis_code_lookup"), "left")
    )

    quality_flags = {
        "missing_procedure_code": F.col("procedure_code").isNull(),
        "missing_billed_amount": F.col("billed_amount").isNull(),
    }

    duplicate_window = Window.partitionBy("claim_id").orderBy(
        # Keep the latest image when the same claim_id appears multiple times.
        F.coalesce(F.col("_commit_timestamp"), F.col("_ingested_at")).desc(),
        F.col("_pipeline_run_id").desc(),
    )

    with_flags = (
        cleaned.withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_data_quality_flags", spark_quality_flags(quality_flags))
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("missing_claim_id", F.col("claim_id").isNull())
        .withColumn("missing_patient_id", F.col("patient_id").isNull())
        .withColumn("missing_provider_id", F.col("provider_id").isNull())
        .withColumn("missing_diagnosis_code", F.col("diagnosis_code").isNull())
        .withColumn("invalid_claim_date", F.col("date").isNull())
        .withColumn(
            "unknown_provider_reference",
            F.col("provider_id").isNotNull() & F.col(
                "provider_exists").isNull(),
        )
        .withColumn(
            "unknown_diagnosis_reference",
            F.col("diagnosis_code").isNotNull() & F.col(
                "diagnosis_exists").isNull(),
        )
    )

    # Keep quarantine_reason, diagnostic_id, and rule_name in the same precedence
    # order so operators see one consistent "first failing rule" per quarantined row.
    quarantine_reason = (
        F.when(F.col("missing_claim_id"), F.lit(
            "claim_id is required for a trusted silver record"))
        .when(F.col("missing_patient_id"), F.lit("patient_id is required for a trusted silver record"))
        .when(F.col("missing_provider_id"), F.lit("provider_id is required for a trusted silver record"))
        .when(F.col("missing_diagnosis_code"), F.lit("diagnosis_code is required for a trusted silver record"))
        .when(F.col("invalid_claim_date"), F.lit("claim date could not be parsed into yyyy-MM-dd"))
        .when(F.col("unknown_provider_reference"), F.lit("provider_id does not match a known provider reference row"))
        .when(F.col("unknown_diagnosis_reference"), F.lit("diagnosis_code does not match a known diagnosis reference row"))
        .when(F.col("_row_priority") > 1, F.lit("duplicate claim_id observed in the silver stream"))
        .otherwise(F.lit(None))
    )

    diagnostic_id = (
        F.when(F.col("missing_claim_id"), F.lit(
            get_silver_diagnostic_id("claims", "missing_claim_id")))
        .when(F.col("missing_patient_id"), F.lit(get_silver_diagnostic_id("claims", "missing_patient_id")))
        .when(F.col("missing_provider_id"), F.lit(get_silver_diagnostic_id("claims", "missing_provider_id")))
        .when(F.col("missing_diagnosis_code"), F.lit(get_silver_diagnostic_id("claims", "missing_diagnosis_code")))
        .when(F.col("invalid_claim_date"), F.lit(get_silver_diagnostic_id("claims", "invalid_claim_date")))
        .when(F.col("unknown_provider_reference"), F.lit(get_silver_diagnostic_id("claims", "unknown_provider_reference")))
        .when(F.col("unknown_diagnosis_reference"), F.lit(get_silver_diagnostic_id("claims", "unknown_diagnosis_reference")))
        .when(F.col("_row_priority") > 1, F.lit(get_silver_diagnostic_id("claims", "duplicate_claim_id")))
        .otherwise(F.lit(None))
    )

    rule_name = (
        F.when(F.col("missing_claim_id"), F.lit("missing_claim_id"))
        .when(F.col("missing_patient_id"), F.lit("missing_patient_id"))
        .when(F.col("missing_provider_id"), F.lit("missing_provider_id"))
        .when(F.col("missing_diagnosis_code"), F.lit("missing_diagnosis_code"))
        .when(F.col("invalid_claim_date"), F.lit("invalid_claim_date"))
        .when(F.col("unknown_provider_reference"), F.lit("unknown_provider_reference"))
        .when(F.col("unknown_diagnosis_reference"), F.lit("unknown_diagnosis_reference"))
        .when(F.col("_row_priority") > 1, F.lit("duplicate_claim_id"))
        .otherwise(F.lit(None))
    )

    return with_flags.withColumn("diagnostic_id", diagnostic_id).withColumn("rule_name", rule_name).withColumn(
        "quarantine_reason", quarantine_reason
    )


@dp.table(
    name=SILVER_CLAIMS_TABLE,
    cluster_by=["claim_id", "date"],
    comment=(
        f"{_TRUSTED_COMMENT} "
        "Trusted Silver claims retain nullable procedure_code and billed_amount as quality flags, "
        "while critical identity/date defects are redirected into healthcare.quarantine.claims."
    ),
    table_properties=silver_table_properties(
        "PHI",
        ("patient_id", "diagnosis_code", "billed_amount", "date"),
    ),
)
def silver_claims():
    """Emit the trusted Silver claims table."""
    trusted = _claims_stream().where(F.col("diagnostic_id").isNull()
                                     ).where(F.col("_row_priority") == 1)
    return trusted.drop(
        "_row_priority",
        "missing_claim_id",
        "missing_patient_id",
        "missing_provider_id",
        "missing_diagnosis_code",
        "invalid_claim_date",
        "unknown_provider_reference",
        "unknown_diagnosis_reference",
        "diagnostic_id",
        "rule_name",
        "quarantine_reason",
        "provider_exists",
        "diagnosis_exists",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    )


@dp.table(
    name=QUARANTINE_CLAIMS_TABLE,
    cluster_by=["claim_id", "diagnostic_id"],
    comment=(
        f"{_QUARANTINE_COMMENT} "
        "PHI-safe quarantine stream for claim rows that cannot enter trusted silver. "
        "Messages carry dataset/rule identifiers only and never interpolate patient values."
    ),
    table_properties=silver_table_properties(
        "PHI",
        ("patient_id", "diagnosis_code", "billed_amount", "date"),
    ),
)
def quarantine_claims():
    """Emit PHI-safe quarantine rows for claims that failed trusted-row validation."""
    quarantined = _claims_stream().where(F.col("diagnostic_id").isNotNull())
    return quarantined.withColumn(
        "status_message",
        F.concat(
            F.lit("Quarantine summary recorded: dataset=claims rule_name="),
            F.col("rule_name"),
            F.lit(" diagnostic_id="),
            F.col("diagnostic_id"),
            F.lit(" quarantined_records=1"),
        ),
    ).withColumn("_quarantined_at", F.current_timestamp()).drop(
        "_row_priority",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "missing_claim_id",
        "missing_patient_id",
        "missing_provider_id",
        "missing_diagnosis_code",
        "invalid_claim_date",
        "unknown_provider_reference",
        "unknown_diagnosis_reference",
        "provider_exists",
        "diagnosis_exists",
    )
