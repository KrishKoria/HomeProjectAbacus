from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
    if pipeline_id and published_event_log_table:
        raise ValueError("Provide only one of published_event_log_table or pipeline_id.")
    if published_event_log_table:
        return spark.table(published_event_log_table)
    if not pipeline_id:
        raise ValueError("Provide either published_event_log_table or pipeline_id.")
    return spark.sql(event_log_bridge_sql(pipeline_id))


def _cache_if_available(dataframe):
    """Cache the shared event log input when the runtime supports it."""
    if not hasattr(dataframe, "cache"):
        return dataframe
    try:
        return dataframe.cache()
    except Exception as exc:
        message = str(exc)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in message or "PERSIST TABLE is not supported" in message:
            return dataframe
        raise


def _unpersist_if_available(dataframe) -> None:
    if not hasattr(dataframe, "unpersist"):
        return
    try:
        dataframe.unpersist()
    except Exception:
        return


def _nested_event_log_path(dataframe, root: str, path: str):
    """Read an event-log nested path when Databricks exposes it as a struct or JSON string."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructType

    path_type = _event_log_path_type(dataframe, root, path)
    if path_type is not None and not isinstance(dataframe.schema[root].dataType, StringType):
        return F.col(f"{root}.{path}")
    if isinstance(dataframe.schema[root].dataType, StringType) if root in dataframe.columns else False:
        return F.get_json_object(F.col(root), f"$.{path}")
    return F.lit(None).cast("string")


def _event_log_path_type(dataframe, root: str, path: str):
    """Return the Spark type for a nested event-log path when it is available."""
    from pyspark.sql.types import StringType, StructType

    root_type = dataframe.schema[root].dataType if root in dataframe.columns else None
    if isinstance(root_type, StructType):
        current_type = root_type
        for part in path.split("."):
            if not isinstance(current_type, StructType) or part not in current_type.names:
                return None
            current_type = current_type[part].dataType
        return current_type
    if isinstance(root_type, StringType):
        return StringType()
    return None


def _nested_event_log_field(dataframe, root: str, field: str):
    """Read a first-level event-log nested field from a struct or JSON string."""
    return _nested_event_log_path(dataframe, root, field)


def _event_log_column(dataframe, column: str):
    """Read an optional top-level event-log column without assuming every source shape has it."""
    from pyspark.sql import functions as F

    if column in dataframe.columns:
        return F.col(column)
    return F.lit(None).cast("string")


def build_pipeline_updates(dataframe):
    """Shape update lifecycle records from the raw event log."""
    from pyspark.sql import functions as F

    origin_update_id = _nested_event_log_field(dataframe, "origin", "update_id")
    origin_pipeline_id = _nested_event_log_field(dataframe, "origin", "pipeline_id")
    origin_user_name = _nested_event_log_field(dataframe, "origin", "user_name")
    details_update_id = _nested_event_log_field(dataframe, "details", "update_id")
    details_pipeline_id = _nested_event_log_field(dataframe, "details", "pipeline_id")
    details_state = _nested_event_log_field(dataframe, "details", "state")
    update_progress_state = _nested_event_log_path(dataframe, "details", "update_progress.state")
    flow_progress_status = _nested_event_log_path(dataframe, "details", "flow_progress.status")

    return (
        dataframe.where(F.col("event_type").isin("create_update", "update_progress", "flow_progress"))
        .select(
            F.col("timestamp"),
            F.col("event_type"),
            F.coalesce(origin_update_id, details_update_id).alias("update_id"),
            F.coalesce(origin_pipeline_id, details_pipeline_id).alias("pipeline_id"),
            F.coalesce(update_progress_state, flow_progress_status, details_state, _event_log_column(dataframe, "message")).alias("update_state"),
            F.coalesce(origin_user_name, _event_log_column(dataframe, "user_identity")).alias("actor"),
        )
    )


def build_expectation_metrics(dataframe):
    """Shape expectation metrics from the raw event log."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

    origin_update_id = _nested_event_log_field(dataframe, "origin", "update_id")
    details_update_id = _nested_event_log_field(dataframe, "details", "update_id")
    expectations = _nested_event_log_path(dataframe, "details", "flow_progress.data_quality.expectations")
    expectation_type = _event_log_path_type(dataframe, "details", "flow_progress.data_quality.expectations")
    expectation_schema = ArrayType(
        StructType(
            [
                StructField("name", StringType(), True),
                StructField("dataset", StringType(), True),
                StructField("passed_records", LongType(), True),
                StructField("failed_records", LongType(), True),
            ]
        )
    )
    if isinstance(expectation_type, ArrayType):
        expectation_rows = F.explode_outer(expectations)
    else:
        expectation_rows = F.explode_outer(
            F.when(
                expectations.cast("string").isNotNull(),
                F.from_json(expectations.cast("string"), expectation_schema),
            )
        )

    return (
        dataframe.where(F.col("event_type") == "flow_progress")
        .withColumn("_expectation", expectation_rows)
        .where(F.col("_expectation").isNotNull())
        .select(
            F.col("timestamp"),
            F.coalesce(origin_update_id, details_update_id).alias("update_id"),
            F.col("_expectation.dataset").alias("dataset"),
            F.col("_expectation.name").alias("expectation"),
            F.col("_expectation.passed_records").alias("passed_records"),
            F.col("_expectation.failed_records").alias("failed_records"),
        )
    )


def build_user_actions(dataframe):
    """Shape user-facing audit actions from the raw event log."""
    from pyspark.sql import functions as F

    origin_update_id = _nested_event_log_field(dataframe, "origin", "update_id")
    details_update_id = _nested_event_log_field(dataframe, "details", "update_id")
    origin_user_name = _nested_event_log_field(dataframe, "origin", "user_name")
    origin_pipeline_id = _nested_event_log_field(dataframe, "origin", "pipeline_id")
    details_user_action = _nested_event_log_field(dataframe, "details", "user_action")
    user_action_action = _nested_event_log_path(dataframe, "details", "user_action.action")
    user_action_user_name = _nested_event_log_path(dataframe, "details", "user_action.user_name")
    details_pipeline_id = _nested_event_log_field(dataframe, "details", "pipeline_id")

    return (
        dataframe.where(F.col("event_type").isin("user_action", "create_update"))
        .select(
            F.col("timestamp"),
            F.coalesce(origin_update_id, details_update_id).alias("update_id"),
            F.coalesce(origin_user_name, _event_log_column(dataframe, "user_identity"), user_action_user_name).alias("actor"),
            F.col("event_type"),
            F.coalesce(user_action_action, details_user_action, _event_log_column(dataframe, "message")).alias("action_name"),
            F.coalesce(origin_pipeline_id, details_pipeline_id).alias("pipeline_id"),
        )
    )


def build_latest_failures(dataframe):
    """Shape the most recent failure rows with stable diagnostic IDs."""
    from pyspark.sql import functions as F

    details_dataset = _nested_event_log_field(dataframe, "details", "dataset")
    details_flow_name = _nested_event_log_field(dataframe, "details", "flow_name")
    flow_progress_dataset = _nested_event_log_path(dataframe, "details", "flow_progress.metrics.dataset")
    origin_update_id = _nested_event_log_field(dataframe, "origin", "update_id")
    details_update_id = _nested_event_log_field(dataframe, "details", "update_id")
    error_code = _nested_event_log_field(dataframe, "error", "error_code")
    error_message = _nested_event_log_field(dataframe, "error", "message")
    details_error_code = _nested_event_log_field(dataframe, "details", "error_code")

    failure_rows = (
        dataframe.where(F.col("level").isin("ERROR", "WARN"))
        .select(
            F.col("timestamp"),
            F.coalesce(details_dataset, details_flow_name, flow_progress_dataset).alias("dataset"),
            F.coalesce(origin_update_id, details_update_id).alias("update_id"),
            F.coalesce(error_code, details_error_code).alias("platform_error_code"),
            F.coalesce(error_message, _event_log_column(dataframe, "message")).alias("error_message"),
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
    parallel_writes: bool = True,
    max_parallel_writes: int = 4,
) -> dict[str, str]:
    """Build and persist Databricks-native observability tables."""
    event_log_df = _cache_if_available(
        load_event_log_dataframe(
            spark,
            pipeline_id=pipeline_id,
            published_event_log_table=published_event_log_table,
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{analytics_schema}")
    outputs = {
        "ops_pipeline_updates": build_pipeline_updates(event_log_df),
        "ops_expectation_metrics": build_expectation_metrics(event_log_df),
        "ops_user_actions": build_user_actions(event_log_df),
        "ops_latest_failures": build_latest_failures(event_log_df),
    }

    def persist_one(item: tuple[str, object]) -> tuple[str, str]:
        table_name, dataframe = item
        table_fqn = f"{catalog}.{analytics_schema}.{table_name}"
        dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_fqn)
        return table_name, MESSAGE_EVENT_LOG_SQL_BRIDGE if not published_event_log_table else table_fqn

    try:
        if parallel_writes and len(outputs) > 1:
            worker_count = max(1, min(max_parallel_writes, len(outputs)))
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                persisted = dict(executor.map(persist_one, outputs.items()))
        else:
            persisted = dict(persist_one(item) for item in outputs.items())
    finally:
        _unpersist_if_available(event_log_df)

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
