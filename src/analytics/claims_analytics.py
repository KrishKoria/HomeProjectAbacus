from __future__ import annotations

from typing import Final

from src.common.bronze_pipeline_config import (
    BRONZE_SCHEMA_DEFAULT,
    bronze_table_name as _config_bronze_table_name,
)
from src.common.observability import (
    LOG_CATEGORY_ANALYTICS_BUILD,
    MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY,
)
from src.common.silver_pipeline_config import (
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    quarantine_table_name,
    silver_table_name,
)


ANALYTICS_SCHEMA_DEFAULT: Final[str] = "analytics"
HIGH_COST_THRESHOLD_RATIO: Final[float] = 1.5
TRUE_VALUES: Final[tuple[str, ...]] = ("1", "TRUE", "YES", "Y")
FALSE_VALUES: Final[tuple[str, ...]] = ("0", "FALSE", "NO", "N")
REFERENCE_TABLES: Final[tuple[str, ...]] = ("claims", "providers", "diagnosis", "cost", "policies")

DASHBOARD_SOURCE_TABLES: Final[tuple[str, ...]] = (
    "claims_provider_joined",
    "claims_diagnosis_joined",
    "claims_by_specialty_summary",
    "claims_by_region_summary",
    "claims_by_diagnosis_summary",
    "claims_provider_specialty_mismatch",
    "high_cost_claims_summary",
    "claims_dashboard_summary",
    "claims_adjudication_summary",
    "claims_denial_reason_summary",
    "claims_revenue_daily_summary",
    "bronze_pipeline_audit",
    "ops_data_freshness",
    "silver_claims_cost_enriched",
    "silver_claim_lineage",
)


def analytics_table_name(catalog: str, analytics_schema: str, table_name: str) -> str:
    """Return a fully-qualified analytics table name."""
    return f"{catalog}.{analytics_schema}.{table_name}"


def trusted_table_name(catalog: str, table_name: str, silver_schema: str = SILVER_SCHEMA_DEFAULT) -> str:
    """Return the trusted Silver source table for analytics reads."""
    return silver_table_name(catalog, table_name, silver_schema)


def raw_bronze_table_name(catalog: str, bronze_schema: str, table_name: str) -> str:
    """Return a raw Bronze table name for audit-only analytics."""
    return _config_bronze_table_name(table_name, catalog=catalog, schema=bronze_schema)


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
    """Cache a DataFrame when the runtime supports it; keep serverless paths portable."""
    if not hasattr(dataframe, "cache"):
        return dataframe
    try:
        return dataframe.cache()
    except Exception as exc:
        message = str(exc)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in message or "PERSIST TABLE is not supported" in message:
            return dataframe
        raise


def _boolean_or_null(column_name: str):
    from pyspark.sql import functions as F

    normalized = F.upper(F.trim(F.col(column_name).cast("string")))
    return (
        F.when(normalized.isin(*TRUE_VALUES), F.lit(True))
        .when(normalized.isin(*FALSE_VALUES), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _double_amount(column_name: str):
    from pyspark.sql import functions as F

    return F.col(column_name).cast("double")


def _percent_or_null(numerator, denominator):
    from pyspark.sql import functions as F

    return F.when((denominator.isNull()) | (denominator == F.lit(0.0)), F.lit(None)).otherwise(
        F.round((numerator / denominator) * F.lit(100.0), 2)
    )


def build_claims_provider_joined(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
):
    """Join trusted Silver claims to providers with a persistable schema."""
    claims = spark.table(trusted_table_name(catalog, "claims", silver_schema)).alias("claims")
    providers = spark.table(trusted_table_name(catalog, "providers", silver_schema)).alias("providers")
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


def build_claims_diagnosis_joined(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
):
    """Join trusted Silver claims to diagnosis reference data with a persistable schema."""
    claims = spark.table(trusted_table_name(catalog, "claims", silver_schema)).alias("claims")
    diagnosis = spark.table(trusted_table_name(catalog, "diagnosis", silver_schema)).alias("diagnosis")
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


def build_claims_by_specialty_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, joined=None):
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


def build_claims_by_region_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, joined=None):
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


def build_claims_by_diagnosis_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, joined=None):
    """Aggregate trusted claims by diagnosis category and severity."""
    from pyspark.sql import functions as F

    joined = joined if joined is not None else build_claims_diagnosis_joined(spark, catalog, bronze_schema)
    return (
        joined.groupBy("category", "severity")
        .agg(
            F.count("*").alias("claim_count"),
            F.countDistinct("diagnosis_code").alias("diagnosis_code_count"),
            F.avg("billed_amount").alias("avg_billed_amount"),
            F.sum("billed_amount").alias("total_billed_amount"),
        )
        .orderBy(F.desc("claim_count"), F.asc("category"), F.asc("severity"))
    )


def _build_claims_provider_cost_enriched(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
):
    """Join trusted claims, providers, and cost with unambiguous post-join names."""
    from pyspark.sql import functions as F

    claims = spark.table(silver_table_name(catalog, "claims", silver_schema)).select(
        "claim_id",
        "provider_id",
        "diagnosis_code",
        "date",
        F.col("procedure_code").alias("claim_procedure_code"),
        "billed_amount",
    )
    providers = spark.table(trusted_table_name(catalog, "providers", silver_schema)).select(
        "provider_id",
        "doctor_name",
        "specialty",
        F.col("location").alias("provider_region"),
    )
    cost = spark.table(trusted_table_name(catalog, "cost", silver_schema)).select(
        F.col("procedure_code").alias("cost_procedure_code"),
        F.col("region").alias("cost_region"),
        "expected_cost",
    )

    return (
        claims.join(F.broadcast(providers), on="provider_id", how="left")
        .join(
            F.broadcast(cost),
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


def build_silver_claims_cost_enriched(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT):
    """Expose the trusted Silver claims-provider-cost enrichment as a managed asset."""
    return _build_claims_provider_cost_enriched(spark, catalog, bronze_schema)


def build_high_cost_claims_summary(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    threshold_ratio: float = HIGH_COST_THRESHOLD_RATIO,
    enriched=None,
):
    """Return claim-level overbilling summary using trusted Silver inputs."""
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
        .where(F.col("expected_cost").isNotNull())
        .where(F.col("amount_to_benchmark_ratio") >= F.lit(threshold_ratio))
        .orderBy(F.desc("amount_to_benchmark_ratio"), F.asc("claim_id"))
    )


def build_claims_dashboard_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, enriched=None):
    """Create a trusted date-grain dashboard table for trends and anomalies."""
    from pyspark.sql import functions as F

    enriched = enriched if enriched is not None else _build_claims_provider_cost_enriched(spark, catalog, bronze_schema)
    enriched = enriched.withColumn(
        "is_high_cost_claim",
        F.when(F.col("amount_to_benchmark_ratio") >= F.lit(HIGH_COST_THRESHOLD_RATIO), F.lit(1)).otherwise(F.lit(0)),
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


def build_claims_adjudication_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, claims=None):
    """Aggregate top-line adjudication and reimbursement KPIs from trusted claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(silver_table_name(catalog, "claims", SILVER_SCHEMA_DEFAULT))
    denied = _boolean_or_null("is_denied")
    follow_up = _boolean_or_null("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.agg(
            F.count("*").alias("total_claims"),
            F.sum(F.when(denied.eqNullSafe(False), F.lit(1)).otherwise(F.lit(0))).alias("approved_claims"),
            F.sum(F.when(denied.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias("denied_claims"),
            F.sum(F.when(denied.isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                "unknown_adjudication_status_claims"
            ),
            F.sum(F.when(follow_up.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias(
                "follow_up_required_claims"
            ),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
        )
        .withColumn("denial_rate_pct", _percent_or_null(F.col("denied_claims"), F.col("total_claims")))
        .withColumn("allowed_rate_pct", _percent_or_null(F.col("total_allowed_amount"), F.col("total_billed_amount")))
        .withColumn("paid_rate_pct", _percent_or_null(F.col("total_paid_amount"), F.col("total_billed_amount")))
    )


def build_claims_denial_reason_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, claims=None):
    """Aggregate denial reason counts and financial impact from trusted claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(silver_table_name(catalog, "claims", SILVER_SCHEMA_DEFAULT))
    denied = _boolean_or_null("is_denied")
    follow_up = _boolean_or_null("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.groupBy(F.coalesce(F.col("denial_reason_code"), F.lit("UNKNOWN")).alias("denial_reason_code"))
        .agg(
            F.count("*").alias("claim_count"),
            F.sum(F.when(denied.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias("denied_claims"),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
            F.round(F.sum(F.when(denied.eqNullSafe(True), billed_amount).otherwise(F.lit(0.0))), 2).alias(
                "denied_billed_amount"
            ),
            F.sum(F.when(follow_up.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias(
                "follow_up_required_claims"
            ),
        )
        .orderBy(F.desc("denied_claims"), F.desc("denied_billed_amount"), F.asc("denial_reason_code"))
    )


def build_claims_revenue_daily_summary(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT, claims=None):
    """Aggregate date-grain adjudication and reimbursement metrics from trusted claims."""
    from pyspark.sql import functions as F

    claims = claims if claims is not None else spark.table(silver_table_name(catalog, "claims", SILVER_SCHEMA_DEFAULT))
    denied = _boolean_or_null("is_denied")
    follow_up = _boolean_or_null("follow_up_required")
    billed_amount = _double_amount("billed_amount")
    allowed_amount = _double_amount("allowed_amount")
    paid_amount = _double_amount("paid_amount")

    return (
        claims.groupBy(F.col("date").alias("claim_date"))
        .agg(
            F.count("*").alias("total_claims"),
            F.sum(F.when(denied.eqNullSafe(False), F.lit(1)).otherwise(F.lit(0))).alias("approved_claims"),
            F.sum(F.when(denied.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias("denied_claims"),
            F.sum(F.when(denied.isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                "unknown_adjudication_status_claims"
            ),
            F.sum(F.when(follow_up.eqNullSafe(True), F.lit(1)).otherwise(F.lit(0))).alias(
                "follow_up_required_claims"
            ),
            F.round(F.sum(billed_amount), 2).alias("total_billed_amount"),
            F.round(F.sum(allowed_amount), 2).alias("total_allowed_amount"),
            F.round(F.sum(paid_amount), 2).alias("total_paid_amount"),
        )
        .withColumn("denial_rate_pct", _percent_or_null(F.col("denied_claims"), F.col("total_claims")))
        .orderBy("claim_date")
    )


def build_claims_provider_specialty_mismatch(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    enriched=None,
    diagnosis_joined=None,
):
    """Flag claims where provider specialty does not align with diagnosis category."""
    from pyspark.sql import functions as F

    enriched = enriched if enriched is not None else _build_claims_provider_cost_enriched(spark, catalog, bronze_schema)
    diagnosis_joined = (
        diagnosis_joined if diagnosis_joined is not None else build_claims_diagnosis_joined(spark, catalog, bronze_schema)
    )
    diagnosis = diagnosis_joined.select("claim_id", "category", "severity")
    return (
        enriched.join(F.broadcast(diagnosis), on="claim_id", how="left")
        .withColumn(
            "specialty_diagnosis_mismatch",
            F.when(F.col("specialty").isNull() | F.col("category").isNull(), F.lit(None).cast("boolean")).otherwise(
                F.lower(F.col("specialty")) != F.lower(F.col("category"))
            ),
        )
        .select(
            "claim_id",
            "provider_id",
            "doctor_name",
            "specialty",
            "diagnosis_code",
            "category",
            "severity",
            "specialty_diagnosis_mismatch",
        )
    )


def build_bronze_pipeline_audit(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT):
    """Summarize raw Bronze row lineage by dataset and pipeline run."""
    from pyspark.sql import functions as F

    frames = []
    for dataset in REFERENCE_TABLES:
        frame = (
            spark.table(raw_bronze_table_name(catalog, bronze_schema, dataset))
            .groupBy("_pipeline_run_id")
            .agg(
                F.count("*").alias("row_count"),
                F.min("_ingested_at").alias("first_ingested_at"),
                F.max("_ingested_at").alias("last_ingested_at"),
                F.countDistinct("_source_file").alias("source_file_count"),
            )
            .withColumn("dataset", F.lit(dataset))
        )
        frames.append(frame)

    audit = frames[0]
    for frame in frames[1:]:
        audit = audit.unionByName(frame)
    return audit.select("dataset", "_pipeline_run_id", "row_count", "source_file_count", "first_ingested_at", "last_ingested_at")


def build_ops_data_freshness(spark, catalog: str, bronze_schema: str = BRONZE_SCHEMA_DEFAULT):
    """Track data freshness from Bronze ingestion timestamps."""
    from pyspark.sql import functions as F

    frames = []
    for dataset in REFERENCE_TABLES:
        frame = spark.table(raw_bronze_table_name(catalog, bronze_schema, dataset)).agg(
            F.lit(dataset).alias("dataset"),
            F.max("_ingested_at").alias("latest_ingested_at"),
            F.current_timestamp().alias("observed_at"),
        )
        frames.append(frame.withColumn("freshness_lag_hours", F.round((F.unix_timestamp("observed_at") - F.unix_timestamp("latest_ingested_at")) / F.lit(3600.0), 2)))

    freshness = frames[0]
    for frame in frames[1:]:
        freshness = freshness.unionByName(frame)
    return freshness


def build_silver_claim_lineage(
    spark,
    catalog: str,
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
    quarantine_schema: str = QUARANTINE_SCHEMA_DEFAULT,
):
    """Map claim IDs to their trusted/quarantine Silver processing status."""
    from pyspark.sql import functions as F

    trusted = spark.table(silver_table_name(catalog, "claims", silver_schema)).select(
        "claim_id",
        F.col("_ingested_at").alias("bronze_ingested_at"),
        F.col("_silver_processed_at").alias("silver_processed_at"),
        F.lit("trusted").alias("quarantine_status"),
        F.lit(None).cast("string").alias("diagnostic_id"),
        F.lit(None).cast("string").alias("rule_name"),
    )
    quarantined = spark.table(quarantine_table_name(catalog, "claims", quarantine_schema)).select(
        "claim_id",
        F.col("_ingested_at").alias("bronze_ingested_at"),
        F.col("_quarantined_at").alias("silver_processed_at"),
        F.lit("quarantined").alias("quarantine_status"),
        "diagnostic_id",
        "rule_name",
    )
    return trusted.unionByName(quarantined)


def write_managed_table(dataframe, table_name: str) -> str:
    """Persist a DataFrame as a managed Delta table with overwrite semantics."""
    dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return table_name


def build_and_persist_claims_assets(
    spark,
    catalog: str = "healthcare",
    bronze_schema: str = BRONZE_SCHEMA_DEFAULT,
    analytics_schema: str = ANALYTICS_SCHEMA_DEFAULT,
) -> dict[str, str]:
    """Build analytics, operational, and feature outputs and persist them to Delta tables."""
    ensure_analytics_schema(spark, catalog, analytics_schema)

    claims_provider_joined = _cache_if_available(build_claims_provider_joined(spark, catalog, bronze_schema))
    claims_diagnosis_joined = _cache_if_available(build_claims_diagnosis_joined(spark, catalog, bronze_schema))
    claims_provider_cost_enriched = _cache_if_available(_build_claims_provider_cost_enriched(spark, catalog, bronze_schema))
    claims = _cache_if_available(spark.table(silver_table_name(catalog, "claims", SILVER_SCHEMA_DEFAULT)))
    mismatch = _cache_if_available(
        build_claims_provider_specialty_mismatch(
            spark,
            catalog,
            bronze_schema,
            enriched=claims_provider_cost_enriched,
            diagnosis_joined=claims_diagnosis_joined,
        )
    )

    outputs = {
        "claims_provider_joined": claims_provider_joined,
        "claims_diagnosis_joined": claims_diagnosis_joined,
        "claims_by_specialty_summary": build_claims_by_specialty_summary(
            spark, catalog, bronze_schema, claims_provider_joined
        ),
        "claims_by_region_summary": build_claims_by_region_summary(spark, catalog, bronze_schema, claims_provider_joined),
        "claims_by_diagnosis_summary": build_claims_by_diagnosis_summary(
            spark, catalog, bronze_schema, claims_diagnosis_joined
        ),
        "claims_provider_specialty_mismatch": mismatch,
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
        "bronze_pipeline_audit": build_bronze_pipeline_audit(spark, catalog, bronze_schema),
        "ops_data_freshness": build_ops_data_freshness(spark, catalog, bronze_schema),
        "silver_claims_cost_enriched": claims_provider_cost_enriched,
        "silver_claim_lineage": build_silver_claim_lineage(spark, catalog, bronze_schema),
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
    "build_and_persist_claims_assets",
    "build_bronze_pipeline_audit",
    "build_claims_adjudication_summary",
    "build_claims_by_diagnosis_summary",
    "build_claims_by_region_summary",
    "build_claims_by_specialty_summary",
    "build_claims_dashboard_summary",
    "build_claims_denial_reason_summary",
    "build_claims_diagnosis_joined",
    "build_claims_provider_joined",
    "build_claims_provider_specialty_mismatch",
    "build_claims_revenue_daily_summary",
    "build_high_cost_claims_summary",
    "build_ops_data_freshness",
    "build_silver_claim_lineage",
    "build_silver_claims_cost_enriched",
    "raw_bronze_table_name",
    "trusted_table_name",
    "_build_claims_provider_cost_enriched",
]
