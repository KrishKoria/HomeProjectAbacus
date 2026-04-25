from __future__ import annotations

from typing import Final

from pyspark.sql import functions as F

from src.common.observability import (
    LOG_CATEGORY_QUARANTINE_AUDIT,
    LOG_CATEGORY_SILVER_PIPELINE,
)
from src.common.log_messages import (
    render_silver_table_ready,
)
from src.common.silver_pipeline_config import (
    ANALYTICS_SCHEMA_DEFAULT,
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    create_required_schemas,
)


QUALITY_OBSERVABILITY_TABLES: Final[tuple[str, str]] = (
    "ops_silver_table_status",
    "ops_quarantine_summary",
)
WEEK3_DATASETS: Final[tuple[str, ...]] = (
    "claims",
    "providers",
    "diagnosis",
    "cost",
    "policy_chunks",
)


def _silver_fqn(catalog: str, dataset: str, schema: str) -> str:
    """Return a fully-qualified table name for the requested schema split."""
    return f"{catalog}.{schema}.{dataset}"


def build_silver_table_status(
    spark,
    catalog: str,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
    quarantine_schema: str = QUARANTINE_SCHEMA_DEFAULT,
):
    """Build one status row per week 3 trusted/quarantine table pair."""
    rows = []
    for dataset in WEEK3_DATASETS:
        silver_table = _silver_fqn(catalog, dataset, silver_schema)
        quarantine_table = _silver_fqn(catalog, dataset, quarantine_schema)
        silver_count = spark.table(silver_table).count()
        quarantine_count = spark.table(quarantine_table).count()
        rows.append(
            {
                "dataset": dataset,
                "silver_table": silver_table,
                "silver_row_count": silver_count,
                "quarantine_table": quarantine_table,
                "quarantine_row_count": quarantine_count,
                "status_message": render_silver_table_ready(
                    table_name=silver_table,
                    category=LOG_CATEGORY_SILVER_PIPELINE,
                    sensitivity="PHI" if dataset == "claims" else "NON-PHI",
                ),
            }
        )
    return spark.createDataFrame(rows)


def build_quarantine_summary(
    spark,
    catalog: str,
    quarantine_schema: str = QUARANTINE_SCHEMA_DEFAULT,
):
    """Aggregate quarantine rows into operator-friendly, PHI-safe diagnostics."""
    frames = []
    for dataset in WEEK3_DATASETS:
        quarantine_table = _silver_fqn(catalog, dataset, quarantine_schema)
        frames.append(
            spark.table(quarantine_table)
            .groupBy("rule_name", "diagnostic_id")
            .count()
            .withColumn("dataset", F.lit(dataset))
        .withColumnRenamed("count", "quarantined_records")
        )

    summary = frames[0]
    for frame in frames[1:]:
        summary = summary.unionByName(frame)

    return summary.withColumn(
        "status_message",
        F.concat(
            F.lit("Quarantine summary recorded: dataset="),
            F.col("dataset"),
            F.lit(" rule_name="),
            F.col("rule_name"),
            F.lit(" diagnostic_id="),
            F.col("diagnostic_id"),
            F.lit(" quarantined_records="),
            F.col("quarantined_records").cast("string"),
        ),
    ).withColumn("category", F.lit(LOG_CATEGORY_QUARANTINE_AUDIT))


def write_quality_assets(
    spark,
    catalog: str = "healthcare",
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
    quarantine_schema: str = QUARANTINE_SCHEMA_DEFAULT,
    analytics_schema: str = ANALYTICS_SCHEMA_DEFAULT,
) -> dict[str, str]:
    """Persist week 3 operator-facing status tables into the analytics schema."""
    create_required_schemas(
        spark,
        catalog,
        silver_schema=silver_schema,
        quarantine_schema=quarantine_schema,
        analytics_schema=analytics_schema,
    )

    outputs = {
        "ops_silver_table_status": build_silver_table_status(
            spark,
            catalog,
            silver_schema=silver_schema,
            quarantine_schema=quarantine_schema,
        ),
        "ops_quarantine_summary": build_quarantine_summary(
            spark,
            catalog,
            quarantine_schema=quarantine_schema,
        ),
    }

    persisted: dict[str, str] = {}
    for table_name, dataframe in outputs.items():
        table_fqn = f"{catalog}.{analytics_schema}.{table_name}"
        dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_fqn)
        persisted[table_name] = table_fqn
    return persisted


__all__ = [
    "QUALITY_OBSERVABILITY_TABLES",
    "WEEK3_DATASETS",
    "build_quarantine_summary",
    "build_silver_table_status",
    "write_quality_assets",
]
