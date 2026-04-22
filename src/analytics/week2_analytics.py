from __future__ import annotations

from typing import Final

from src.common.observability import (
    LOG_CATEGORY_ANALYTICS_BUILD,
    MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY,
)


ANALYTICS_SCHEMA_DEFAULT: Final[str] = "analytics"
HIGH_COST_THRESHOLD_RATIO: Final[float] = 1.5

DASHBOARD_SOURCE_TABLES: Final[tuple[str, ...]] = (
    "claims_provider_joined",
    "claims_diagnosis_joined",
    "claims_by_specialty_summary",
    "claims_by_region_summary",
    "high_cost_claims_summary",
    "week2_dashboard_summary",
)


def analytics_table_name(catalog: str, analytics_schema: str, table_name: str) -> str:
    """Return a fully-qualified analytics table name."""
    return f"{catalog}.{analytics_schema}.{table_name}"


def bronze_table_name(catalog: str, bronze_schema: str, table_name: str) -> str:
    """Return a fully-qualified Bronze table name."""
    return f"{catalog}.{bronze_schema}.{table_name}"


def _log_dataset_ready(table_name: str, sensitivity: str) -> str:
    return MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY.format(
        table_name=table_name,
        category=LOG_CATEGORY_ANALYTICS_BUILD,
        sensitivity=sensitivity,
    )


def ensure_analytics_schema(spark, catalog: str, analytics_schema: str) -> None:
    """Create the analytics schema if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{analytics_schema}")


def build_claims_provider_joined(spark, catalog: str, bronze_schema: str):
    """Join claims to providers without cleaning nulls or deduplicating records."""
    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    providers = spark.table(bronze_table_name(catalog, bronze_schema, "providers")).alias("providers")
    return claims.join(providers, on="provider_id", how="left")


def build_claims_diagnosis_joined(spark, catalog: str, bronze_schema: str):
    """Join claims to diagnosis reference data without altering Bronze values."""
    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    diagnosis = spark.table(bronze_table_name(catalog, bronze_schema, "diagnosis")).alias("diagnosis")
    return claims.join(diagnosis, on="diagnosis_code", how="left")


def build_claims_by_specialty_summary(spark, catalog: str, bronze_schema: str):
    """Aggregate claim activity by provider specialty."""
    from pyspark.sql import functions as F

    joined = build_claims_provider_joined(spark, catalog, bronze_schema)
    return (
        joined.groupBy("specialty")
        .agg(
            F.count("*").alias("claim_count"),
            F.countDistinct("provider_id").alias("provider_count"),
            F.avg("billed_amount").alias("avg_billed_amount"),
            F.sum("billed_amount").alias("total_billed_amount"),
        )
        .orderBy(F.desc("claim_count"), F.asc("specialty"))
    )


def build_claims_by_region_summary(spark, catalog: str, bronze_schema: str):
    """Aggregate claim activity by provider region."""
    from pyspark.sql import functions as F

    joined = build_claims_provider_joined(spark, catalog, bronze_schema)
    return (
        joined.groupBy(F.col("location").alias("region"))
        .agg(
            F.count("*").alias("claim_count"),
            F.countDistinct("provider_id").alias("provider_count"),
            F.avg("billed_amount").alias("avg_billed_amount"),
            F.sum("billed_amount").alias("total_billed_amount"),
        )
        .orderBy(F.desc("claim_count"), F.asc("region"))
    )


def build_high_cost_claims_summary(
    spark,
    catalog: str,
    bronze_schema: str,
    threshold_ratio: float = HIGH_COST_THRESHOLD_RATIO,
):
    """Return claim-level overbilling summary using Bronze claims, providers, and cost."""
    from pyspark.sql import functions as F

    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    providers = spark.table(bronze_table_name(catalog, bronze_schema, "providers")).alias("providers")
    cost = spark.table(bronze_table_name(catalog, bronze_schema, "cost")).alias("cost")

    return (
        claims.join(providers, on="provider_id", how="left")
        # The benchmark table is keyed by procedure and provider region, so both sides
        # are needed before a claim can be compared to an expected cost.
        .join(
            cost,
            on=[
                claims["procedure_code"] == cost["procedure_code"],
                providers["location"] == cost["region"],
            ],
            how="left",
        )
        .select(
            claims["claim_id"],
            claims["provider_id"],
            providers["doctor_name"],
            providers["specialty"],
            providers["location"].alias("region"),
            claims["procedure_code"],
            claims["billed_amount"],
            cost["expected_cost"],
            (
                F.col("billed_amount").cast("double") / F.col("expected_cost").cast("double")
            ).alias("amount_to_benchmark_ratio"),
        )
        # Claims without a regional benchmark stay visible in the broader EDA tables,
        # but they cannot participate in the high-cost ratio analysis.
        .where(F.col("expected_cost").isNotNull())
        .where(F.col("amount_to_benchmark_ratio") >= F.lit(threshold_ratio))
        .orderBy(F.desc("amount_to_benchmark_ratio"), F.asc("claim_id"))
    )


def build_week2_dashboard_summary(spark, catalog: str, bronze_schema: str):
    """Create a date-grain dashboard table for total claims, trends, and anomalies."""
    from pyspark.sql import functions as F

    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    providers = spark.table(bronze_table_name(catalog, bronze_schema, "providers")).alias("providers")
    cost = spark.table(bronze_table_name(catalog, bronze_schema, "cost")).alias("cost")

    enriched = (
        claims.join(providers, on="provider_id", how="left")
        .join(
            cost,
            on=[
                claims["procedure_code"] == cost["procedure_code"],
                providers["location"] == cost["region"],
            ],
            how="left",
        )
        .withColumn(
            "amount_to_benchmark_ratio",
            F.col("billed_amount").cast("double") / F.col("expected_cost").cast("double"),
        )
        .withColumn(
            # Keep this as an integer flag so the daily rollup can sum anomalies directly.
            "is_high_cost_claim",
            F.when(F.col("amount_to_benchmark_ratio") >= F.lit(HIGH_COST_THRESHOLD_RATIO), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    return (
        enriched.groupBy(F.col("date").alias("claim_date"))
        .agg(
            F.count("*").alias("total_claims"),
            F.countDistinct("provider_id").alias("active_provider_count"),
            F.sum("billed_amount").alias("total_billed_amount"),
            F.avg("billed_amount").alias("avg_billed_amount"),
            F.sum("is_high_cost_claim").alias("high_cost_claim_count"),
        )
        .orderBy("claim_date")
    )


def write_managed_table(dataframe, table_name: str) -> str:
    """Persist a DataFrame as a managed Delta table with overwrite semantics."""
    dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return table_name


def build_and_persist_week2_assets(
    spark,
    catalog: str = "healthcare",
    bronze_schema: str = "bronze",
    analytics_schema: str = ANALYTICS_SCHEMA_DEFAULT,
) -> dict[str, str]:
    """Build all Week 2 analytics outputs and persist them to Delta tables."""
    ensure_analytics_schema(spark, catalog, analytics_schema)

    outputs = {
        "claims_provider_joined": build_claims_provider_joined(spark, catalog, bronze_schema),
        "claims_diagnosis_joined": build_claims_diagnosis_joined(spark, catalog, bronze_schema),
        "claims_by_specialty_summary": build_claims_by_specialty_summary(spark, catalog, bronze_schema),
        "claims_by_region_summary": build_claims_by_region_summary(spark, catalog, bronze_schema),
        "high_cost_claims_summary": build_high_cost_claims_summary(spark, catalog, bronze_schema),
        "week2_dashboard_summary": build_week2_dashboard_summary(spark, catalog, bronze_schema),
    }

    persisted: dict[str, str] = {}
    for table_key, dataframe in outputs.items():
        table_fqn = analytics_table_name(catalog, analytics_schema, table_key)
        write_managed_table(dataframe, table_fqn)
        persisted[table_key] = _log_dataset_ready(table_fqn, "MINIMUM-NECESSARY")

    return persisted


__all__ = [
    "ANALYTICS_SCHEMA_DEFAULT",
    "DASHBOARD_SOURCE_TABLES",
    "HIGH_COST_THRESHOLD_RATIO",
    "analytics_table_name",
    "bronze_table_name",
    "build_and_persist_week2_assets",
    "build_claims_by_region_summary",
    "build_claims_by_specialty_summary",
    "build_claims_diagnosis_joined",
    "build_claims_provider_joined",
    "build_high_cost_claims_summary",
    "build_week2_dashboard_summary",
]
