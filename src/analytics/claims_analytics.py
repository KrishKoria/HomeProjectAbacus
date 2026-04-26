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
    "claims_dashboard_summary",
    "claims_adjudication_summary",
    "claims_denial_reason_summary",
    "claims_revenue_daily_summary",
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


def _cache_if_available(dataframe):
    return dataframe.cache() if hasattr(dataframe, "cache") else dataframe


def _binary_flag(column_name: str):
    from pyspark.sql import functions as F

    return F.when(F.col(column_name).cast("int") == F.lit(1), F.lit(1)).otherwise(F.lit(0))


def _double_amount(column_name: str):
    from pyspark.sql import functions as F

    return F.col(column_name).cast("double")


def _percent_or_null(numerator, denominator):
    from pyspark.sql import functions as F

    return F.when((denominator.isNull()) | (denominator == F.lit(0.0)), F.lit(None)).otherwise(
        F.round((numerator / denominator) * F.lit(100.0), 2)
    )


def build_claims_provider_joined(spark, catalog: str, bronze_schema: str):
    """Join claims to providers with a persistable, non-duplicated schema."""
    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    providers = spark.table(bronze_table_name(catalog, bronze_schema, "providers")).alias("providers")
    return claims.join(providers, on="provider_id", how="left").select(
        claims["claim_id"],
        claims["patient_id"],
        claims["provider_id"],
        claims["diagnosis_code"],
        claims["procedure_code"],
        claims["billed_amount"],
        claims["date"],
        providers["doctor_name"],
        providers["specialty"],
        providers["location"],
        claims["_ingested_at"].alias("claim_ingested_at"),
        claims["_source_file"].alias("claim_source_file"),
        claims["_pipeline_run_id"].alias("claim_pipeline_run_id"),
        providers["_ingested_at"].alias("provider_ingested_at"),
        providers["_source_file"].alias("provider_source_file"),
        providers["_pipeline_run_id"].alias("provider_pipeline_run_id"),
    )


def build_claims_diagnosis_joined(spark, catalog: str, bronze_schema: str):
    """Join claims to diagnosis reference data with a persistable schema."""
    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).alias("claims")
    diagnosis = spark.table(bronze_table_name(catalog, bronze_schema, "diagnosis")).alias("diagnosis")
    return claims.join(diagnosis, on="diagnosis_code", how="left").select(
        claims["claim_id"],
        claims["patient_id"],
        claims["provider_id"],
        claims["diagnosis_code"],
        claims["procedure_code"],
        claims["billed_amount"],
        claims["date"],
        diagnosis["category"],
        diagnosis["severity"],
        claims["_ingested_at"].alias("claim_ingested_at"),
        claims["_source_file"].alias("claim_source_file"),
        claims["_pipeline_run_id"].alias("claim_pipeline_run_id"),
        diagnosis["_ingested_at"].alias("diagnosis_ingested_at"),
        diagnosis["_source_file"].alias("diagnosis_source_file"),
        diagnosis["_pipeline_run_id"].alias("diagnosis_pipeline_run_id"),
    )


def build_claims_by_specialty_summary(spark, catalog: str, bronze_schema: str, joined=None):
    """Aggregate claim activity by provider specialty."""
    from pyspark.sql import functions as F

    joined = joined if joined is not None else build_claims_provider_joined(spark, catalog, bronze_schema)
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


def build_claims_by_region_summary(spark, catalog: str, bronze_schema: str, joined=None):
    """Aggregate claim activity by provider region."""
    from pyspark.sql import functions as F

    joined = joined if joined is not None else build_claims_provider_joined(spark, catalog, bronze_schema)
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


def _build_claims_provider_cost_enriched(spark, catalog: str, bronze_schema: str):
    """Join claims, providers, and cost with unambiguous post-join column names."""
    from pyspark.sql import functions as F

    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims")).select(
        "claim_id",
        "provider_id",
        "date",
        F.col("procedure_code").alias("claim_procedure_code"),
        "billed_amount",
    )
    providers = spark.table(bronze_table_name(catalog, bronze_schema, "providers")).select(
        "provider_id",
        "doctor_name",
        "specialty",
        F.col("location").alias("provider_region"),
    )
    cost = spark.table(bronze_table_name(catalog, bronze_schema, "cost")).select(
        F.col("procedure_code").alias("cost_procedure_code"),
        F.col("region").alias("cost_region"),
        "expected_cost",
    )

    return (
        claims.join(providers, on="provider_id", how="left")
        .join(
            cost,
            on=[
                F.col("claim_procedure_code") == F.col("cost_procedure_code"),
                F.col("provider_region") == F.col("cost_region"),
            ],
            how="left",
        )
        .withColumn(
            "amount_to_benchmark_ratio",
            F.col("billed_amount").cast("double") / F.col("expected_cost").cast("double"),
        )
    )


def build_high_cost_claims_summary(
    spark,
    catalog: str,
    bronze_schema: str,
    threshold_ratio: float = HIGH_COST_THRESHOLD_RATIO,
    enriched=None,
):
    """Return claim-level overbilling summary using Bronze claims, providers, and cost."""
    from pyspark.sql import functions as F

    enriched = enriched if enriched is not None else _build_claims_provider_cost_enriched(spark, catalog, bronze_schema)
    return (
        enriched.select(
            "claim_id",
            "provider_id",
            "doctor_name",
            "specialty",
            F.col("provider_region").alias("region"),
            F.col("claim_procedure_code").alias("procedure_code"),
            "billed_amount",
            "expected_cost",
            "amount_to_benchmark_ratio",
        )
        # Claims without a regional benchmark stay visible in the broader EDA tables,
        # but they cannot participate in the high-cost ratio analysis.
        .where(F.col("expected_cost").isNotNull())
        .where(F.col("amount_to_benchmark_ratio") >= F.lit(threshold_ratio))
        .orderBy(F.desc("amount_to_benchmark_ratio"), F.asc("claim_id"))
    )


def build_claims_dashboard_summary(spark, catalog: str, bronze_schema: str, enriched=None):
    """Create a date-grain dashboard table for total claims, trends, and anomalies."""
    from pyspark.sql import functions as F

    enriched = (
        enriched if enriched is not None else _build_claims_provider_cost_enriched(spark, catalog, bronze_schema)
    )
    enriched = (
        enriched
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


def build_claims_adjudication_summary(spark, catalog: str, bronze_schema: str, claims=None):
    """Aggregate top-line adjudication and reimbursement KPIs from Bronze claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(bronze_table_name(catalog, bronze_schema, "claims"))
    denied_flag = _binary_flag("is_denied")
    follow_up_flag = _binary_flag("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.agg(
            F.count("*").alias("total_claims"),
            F.sum(F.when(denied_flag == F.lit(0), F.lit(1)).otherwise(F.lit(0))).alias("approved_claims"),
            F.sum(denied_flag).alias("denied_claims"),
            F.sum(follow_up_flag).alias("follow_up_required_claims"),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
        )
        .withColumn("denial_rate_pct", _percent_or_null(F.col("denied_claims"), F.col("total_claims")))
        .withColumn("allowed_rate_pct", _percent_or_null(F.col("total_allowed_amount"), F.col("total_billed_amount")))
        .withColumn("paid_rate_pct", _percent_or_null(F.col("total_paid_amount"), F.col("total_billed_amount")))
    )


def build_claims_denial_reason_summary(spark, catalog: str, bronze_schema: str, claims=None):
    """Aggregate denial reason counts and financial impact from Bronze claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(bronze_table_name(catalog, bronze_schema, "claims"))
    denied_flag = _binary_flag("is_denied")
    follow_up_flag = _binary_flag("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.groupBy(F.coalesce(F.col("denial_reason_code"), F.lit("UNKNOWN")).alias("denial_reason_code"))
        .agg(
            F.count("*").alias("claim_count"),
            F.sum(denied_flag).alias("denied_claims"),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
            F.round(F.sum(F.when(denied_flag == F.lit(1), billed_amount).otherwise(F.lit(0.0))), 2).alias(
                "denied_billed_amount"
            ),
            F.sum(follow_up_flag).alias("follow_up_required_claims"),
        )
        .orderBy(F.desc("denied_claims"), F.desc("denied_billed_amount"), F.asc("denial_reason_code"))
    )


def build_claims_revenue_daily_summary(spark, catalog: str, bronze_schema: str, claims=None):
    """Aggregate date-grain adjudication and reimbursement metrics from Bronze claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(bronze_table_name(catalog, bronze_schema, "claims"))
    denied_flag = _binary_flag("is_denied")
    follow_up_flag = _binary_flag("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.groupBy(F.col("date").alias("claim_date"))
        .agg(
            F.count("*").alias("total_claims"),
            F.sum(F.when(denied_flag == F.lit(0), F.lit(1)).otherwise(F.lit(0))).alias("approved_claims"),
            F.sum(denied_flag).alias("denied_claims"),
            F.sum(follow_up_flag).alias("follow_up_required_claims"),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
        )
        .withColumn("denial_rate_pct", _percent_or_null(F.col("denied_claims"), F.col("total_claims")))
        .orderBy("claim_date")
    )


def write_managed_table(dataframe, table_name: str) -> str:
    """Persist a DataFrame as a managed Delta table with overwrite semantics."""
    dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return table_name


def build_and_persist_claims_assets(
    spark,
    catalog: str = "healthcare",
    bronze_schema: str = "bronze",
    analytics_schema: str = ANALYTICS_SCHEMA_DEFAULT,
) -> dict[str, str]:
    """Build all Week 2 analytics outputs and persist them to Delta tables."""
    ensure_analytics_schema(spark, catalog, analytics_schema)

    claims_provider_joined = _cache_if_available(build_claims_provider_joined(spark, catalog, bronze_schema))
    claims_provider_cost_enriched = _cache_if_available(_build_claims_provider_cost_enriched(spark, catalog, bronze_schema))
    claims = _cache_if_available(spark.table(bronze_table_name(catalog, bronze_schema, "claims")))

    outputs = {
        "claims_provider_joined": claims_provider_joined,
        "claims_diagnosis_joined": build_claims_diagnosis_joined(spark, catalog, bronze_schema),
        "claims_by_specialty_summary": build_claims_by_specialty_summary(spark, catalog, bronze_schema, claims_provider_joined),
        "claims_by_region_summary": build_claims_by_region_summary(spark, catalog, bronze_schema, claims_provider_joined),
        "high_cost_claims_summary": build_high_cost_claims_summary(
            spark,
            catalog,
            bronze_schema,
            enriched=claims_provider_cost_enriched,
        ),
        "claims_dashboard_summary": build_claims_dashboard_summary(
            spark,
            catalog,
            bronze_schema,
            enriched=claims_provider_cost_enriched,
        ),
        "claims_adjudication_summary": build_claims_adjudication_summary(spark, catalog, bronze_schema, claims),
        "claims_denial_reason_summary": build_claims_denial_reason_summary(spark, catalog, bronze_schema, claims),
        "claims_revenue_daily_summary": build_claims_revenue_daily_summary(spark, catalog, bronze_schema, claims),
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
    "build_and_persist_claims_assets",
    "build_claims_by_region_summary",
    "build_claims_by_specialty_summary",
    "build_claims_adjudication_summary",
    "build_claims_diagnosis_joined",
    "build_claims_denial_reason_summary",
    "build_claims_provider_joined",
    "build_claims_revenue_daily_summary",
    "build_high_cost_claims_summary",
    "build_claims_dashboard_summary",
    "_build_claims_provider_cost_enriched",
]
