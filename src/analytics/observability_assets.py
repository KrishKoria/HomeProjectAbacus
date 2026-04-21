from __future__ import annotations

from typing import Final

from src.common.bronze_pipeline_config import format_claimops_diagnostic_id
from src.common.observability import MESSAGE_EVENT_LOG_SQL_BRIDGE


ANALYTICS_OBSERVABILITY_TABLES: Final[tuple[str, ...]] = (
    "ops_pipeline_updates",
    "ops_expectation_metrics",
    "ops_user_actions",
    "ops_latest_failures",
)

FAILURE_DIAGNOSTIC_IDS: Final[dict[str, str]] = {
    "claims": format_claimops_diagnostic_id("OBS", 401),
    "providers": format_claimops_diagnostic_id("OBS", 402),
    "diagnosis": format_claimops_diagnostic_id("OBS", 403),
    "cost": format_claimops_diagnostic_id("OBS", 404),
    "policies": format_claimops_diagnostic_id("OBS", 405),
}


def event_log_bridge_sql(pipeline_id: str) -> str:
    """Return the minimal SQL bridge used when event_log() must be queried via SQL."""
    return f"SELECT * FROM event_log('{pipeline_id}')"


def latest_failure_diagnostic_id(dataset: str) -> str:
    """Return the diagnostic ID used for latest-failure rollups."""
    return FAILURE_DIAGNOSTIC_IDS.get(dataset, format_claimops_diagnostic_id("OBS", 499))


def load_event_log_dataframe(spark, pipeline_id: str | None = None, published_event_log_table: str | None = None):
    """Load the pipeline event log using either a published Delta table or a minimal SQL bridge."""
    if published_event_log_table:
        return spark.table(published_event_log_table)
    if not pipeline_id:
        raise ValueError("Provide either published_event_log_table or pipeline_id.")
    return spark.sql(event_log_bridge_sql(pipeline_id))


def build_pipeline_updates(dataframe):
    """Shape update lifecycle records from the raw event log."""
    from pyspark.sql import functions as F

    return (
        dataframe.where(F.col("event_type").isin("create_update", "update_progress", "flow_progress"))
        .select(
            F.col("timestamp"),
            F.col("event_type"),
            F.coalesce(F.col("origin.update_id"), F.col("details.update_id")).alias("update_id"),
            F.coalesce(F.col("origin.pipeline_id"), F.col("details.pipeline_id")).alias("pipeline_id"),
            F.coalesce(F.col("details.state"), F.col("message")).alias("update_state"),
            F.coalesce(F.col("origin.user_name"), F.col("user_identity")).alias("actor"),
        )
    )


def build_expectation_metrics(dataframe):
    """Shape expectation metrics from the raw event log."""
    from pyspark.sql import functions as F

    return (
        dataframe.where(F.col("event_type") == "expectations")
        .select(
            F.col("timestamp"),
            F.coalesce(F.col("origin.update_id"), F.col("details.update_id")).alias("update_id"),
            F.coalesce(F.col("details.dataset"), F.col("details.flow_name")).alias("dataset"),
            F.col("details.name").alias("expectation"),
            F.col("details.passed_records").alias("passed_records"),
            F.col("details.failed_records").alias("failed_records"),
        )
    )


def build_user_actions(dataframe):
    """Shape user-facing audit actions from the raw event log."""
    from pyspark.sql import functions as F

    return (
        dataframe.where(F.col("event_type").isin("user_action", "create_update"))
        .select(
            F.col("timestamp"),
            F.coalesce(F.col("origin.user_name"), F.col("user_identity")).alias("actor"),
            F.col("event_type"),
            F.coalesce(F.col("details.user_action"), F.col("message")).alias("action"),
            F.coalesce(F.col("origin.pipeline_id"), F.col("details.pipeline_id")).alias("pipeline_id"),
        )
    )


def build_latest_failures(dataframe):
    """Shape the most recent failure rows with stable diagnostic IDs."""
    from pyspark.sql import functions as F

    failure_rows = (
        dataframe.where(F.col("level").isin("ERROR", "WARN"))
        .select(
            F.col("timestamp"),
            F.coalesce(F.col("details.dataset"), F.col("details.flow_name")).alias("dataset"),
            F.coalesce(F.col("origin.update_id"), F.col("details.update_id")).alias("update_id"),
            F.coalesce(F.col("error.error_code"), F.col("details.error_code")).alias("platform_error_code"),
            F.coalesce(F.col("error.message"), F.col("message")).alias("error_message"),
        )
    )

    mapping_expr = F.create_map(*[F.lit(item) for kv in FAILURE_DIAGNOSTIC_IDS.items() for item in kv])
    return failure_rows.withColumn("diagnostic_id", F.coalesce(mapping_expr[F.col("dataset")], F.lit(latest_failure_diagnostic_id("unknown"))))


def write_observability_tables(
    spark,
    pipeline_id: str | None = None,
    published_event_log_table: str | None = None,
    catalog: str = "healthcare",
    analytics_schema: str = "analytics",
) -> dict[str, str]:
    """Build and persist Databricks-native observability tables."""
    event_log_df = load_event_log_dataframe(
        spark,
        pipeline_id=pipeline_id,
        published_event_log_table=published_event_log_table,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{analytics_schema}")
    outputs = {
        "ops_pipeline_updates": build_pipeline_updates(event_log_df),
        "ops_expectation_metrics": build_expectation_metrics(event_log_df),
        "ops_user_actions": build_user_actions(event_log_df),
        "ops_latest_failures": build_latest_failures(event_log_df),
    }

    persisted: dict[str, str] = {}
    for table_name, dataframe in outputs.items():
        table_fqn = f"{catalog}.{analytics_schema}.{table_name}"
        dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_fqn)
        persisted[table_name] = MESSAGE_EVENT_LOG_SQL_BRIDGE if not published_event_log_table else table_fqn

    return persisted


__all__ = [
    "ANALYTICS_OBSERVABILITY_TABLES",
    "FAILURE_DIAGNOSTIC_IDS",
    "MESSAGE_EVENT_LOG_SQL_BRIDGE",
    "build_expectation_metrics",
    "build_latest_failures",
    "build_pipeline_updates",
    "build_user_actions",
    "event_log_bridge_sql",
    "latest_failure_diagnostic_id",
    "load_event_log_dataframe",
    "write_observability_tables",
]
