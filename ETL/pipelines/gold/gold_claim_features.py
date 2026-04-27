from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.bronze_pipeline_config import CATALOG_DEFAULT
from common.gold_pipeline_config import (
    GOLD_SCHEMA_DEFAULT,
    PHI_COLUMNS_GOLD,
    gold_table_name,
    gold_table_properties,
    read_silver_snapshot,
)
from common.observability import (
    LOG_CATEGORY_GOLD_PIPELINE,
    MESSAGE_TEMPLATE_GOLD_TABLE_READY,
)
from common.silver_pipeline_config import SILVER_SCHEMA_DEFAULT


GOLD_CLAIM_FEATURES_TABLE = gold_table_name(CATALOG_DEFAULT, "claim_features", GOLD_SCHEMA_DEFAULT)

CLAIMS_PHI_COLUMNS = PHI_COLUMNS_GOLD

_THRESHOLD_RATIO: float = 1.5

_GOLD_TABLE_READY_COMMENT = MESSAGE_TEMPLATE_GOLD_TABLE_READY.format(
    table_name=GOLD_CLAIM_FEATURES_TABLE,
    category=LOG_CATEGORY_GOLD_PIPELINE,
    sensitivity="SENSITIVE",
)

_silver_claims = f"{CATALOG_DEFAULT}.{SILVER_SCHEMA_DEFAULT}.claims"
_silver_providers = f"{CATALOG_DEFAULT}.{SILVER_SCHEMA_DEFAULT}.providers"
_silver_diagnosis = f"{CATALOG_DEFAULT}.{SILVER_SCHEMA_DEFAULT}.diagnosis"
_silver_cost = f"{CATALOG_DEFAULT}.{SILVER_SCHEMA_DEFAULT}.cost"


@dp.materialized_view(
    name="claims_feature_base",
    private=True,
    comment=(
        "Private intermediate: Silver claims joined with provider, diagnosis, "
        "and cost reference data. Serves as the shared base for feature engineering "
        "and the final Gold output."
    ),
    table_properties=gold_table_properties("SENSITIVE", CLAIMS_PHI_COLUMNS),
)
def _claims_feature_base():
    claims = read_silver_snapshot(spark, _silver_claims)
    providers = read_silver_snapshot(spark, _silver_providers)
    diagnosis = read_silver_snapshot(spark, _silver_diagnosis)
    cost = read_silver_snapshot(spark, _silver_cost)

    claims_provider = claims.join(
        F.broadcast(providers.select("provider_id", "specialty", "location")),
        on="provider_id",
        how="left",
    )

    claims_provider_diag = claims_provider.join(
        F.broadcast(diagnosis.select("diagnosis_code", "category", "severity")),
        on="diagnosis_code",
        how="left",
    )

    claim_cost = claims_provider_diag.join(
        F.broadcast(
            cost.select(
                F.col("procedure_code").alias("cost_procedure_code"),
                F.col("region").alias("cost_region"),
                "average_cost",
                "expected_cost",
            )
        ),
        on=[
            F.col("procedure_code") == F.col("cost_procedure_code"),
            F.col("location") == F.col("cost_region"),
        ],
        how="left",
    )

    return (
        claim_cost
        .withColumn("is_procedure_missing", F.col("procedure_code").isNull())
        .withColumn("is_amount_missing", F.col("billed_amount").isNull())
        .withColumn(
            "amount_to_benchmark_ratio",
            F.when(
                F.col("expected_cost").isNotNull() & (F.col("expected_cost") > 0),
                F.col("billed_amount").cast("double") / F.col("expected_cost").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "billed_vs_avg_cost",
            F.when(
                F.col("average_cost").isNotNull() & (F.col("average_cost") > 0),
                F.col("billed_amount").cast("double") / F.col("average_cost").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "high_cost_flag",
            F.when(
                F.col("amount_to_benchmark_ratio").isNotNull()
                & (F.col("amount_to_benchmark_ratio") >= F.lit(_THRESHOLD_RATIO)),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "severity_procedure_mismatch",
            F.when(
                (F.col("severity") == F.lit("High"))
                & (F.col("expected_cost").isNotNull())
                & (F.col("expected_cost").cast("double") < F.lit(5000.0)),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "specialty_diagnosis_mismatch",
            F.when(
                F.col("specialty").isNull() | F.col("category").isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(
                F.lower(F.col("specialty")) != F.lower(F.col("category")),
            ),
        )
        .withColumn("provider_location_missing", F.col("location").isNull())
        .withColumn(
            "diagnosis_severity_encoded",
            F.when(F.col("severity") == F.lit("High"), F.lit(1))
            .when(F.col("severity") == F.lit("Low"), F.lit(0))
            .otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "denial_label",
            F.when(F.col("is_denied") == F.lit(True), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    ).drop(
        "cost_procedure_code",
        "cost_region",
    )


@dp.materialized_view(
    name="provider_aggregations",
    private=True,
    comment=(
        "Private intermediate: provider-level claim counts and denial rates "
        "for window-based features."
    ),
    table_properties=gold_table_properties("SENSITIVE", CLAIMS_PHI_COLUMNS),
)
def _provider_aggregations():
    claims = read_silver_snapshot(spark, _silver_claims)
    return (
        claims.filter(F.col("provider_id").isNotNull())
        .withColumn("is_denied_int", F.when(F.col("is_denied") == F.lit(True), F.lit(1)).otherwise(F.lit(0)))
        .groupBy("provider_id")
        .agg(
            F.count("*").alias("provider_claim_count"),
            F.sum("is_denied_int").alias("provider_denied_count"),
        )
        .withColumn(
            "provider_risk_score",
            F.when(
                F.col("provider_claim_count") > 0,
                F.col("provider_denied_count").cast("double") / F.col("provider_claim_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("provider_denied_count")
    )


@dp.table(
    name=GOLD_CLAIM_FEATURES_TABLE,
    cluster_by=["claim_id"],
    comment=(
        _GOLD_TABLE_READY_COMMENT
        + " Gold claim features table: 13 engineered risk features joined from "
        "Silver claims, providers, diagnosis, and cost. Ready for ML training."
    ),
    table_properties=gold_table_properties("SENSITIVE", CLAIMS_PHI_COLUMNS),
)
def gold_claim_features():
    base = spark.read.table("claims_feature_base")
    provider_agg = spark.read.table("provider_aggregations")

    claim_window_30d = (
        Window.partitionBy("provider_id")
        .orderBy(F.col("date").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )

    diagnosis_window = Window.partitionBy("provider_id", "diagnosis_code")

    result = (
        base.join(
            F.broadcast(provider_agg),
            on="provider_id",
            how="left",
        )
        .withColumn(
            "provider_claim_count_30d",
            F.when(
                F.col("date").isNotNull() & F.col("provider_id").isNotNull(),
                F.count("*").over(claim_window_30d),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "diagnosis_count",
            F.when(
                F.col("diagnosis_code").isNotNull(),
                F.approx_count_distinct("diagnosis_code").over(diagnosis_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
            ).otherwise(F.lit(None).cast("int")),
        )
    )

    return result.select(
        "claim_id",
        "patient_id",
        "provider_id",
        "diagnosis_code",
        "procedure_code",
        "billed_amount",
        "date",
        "specialty",
        "location",
        "category",
        "severity",
        "average_cost",
        "expected_cost",
        "is_denied",
        "is_procedure_missing",
        "is_amount_missing",
        "amount_to_benchmark_ratio",
        "billed_vs_avg_cost",
        "high_cost_flag",
        "severity_procedure_mismatch",
        "specialty_diagnosis_mismatch",
        "provider_location_missing",
        "diagnosis_severity_encoded",
        "diagnosis_count",
        "provider_claim_count",
        "provider_claim_count_30d",
        "provider_risk_score",
        "denial_label",
        "_gold_processed_at",
        "_ingested_at",
        "_source_file",
        "_pipeline_run_id",
        "_silver_processed_at",
        "_data_quality_flags",
    )