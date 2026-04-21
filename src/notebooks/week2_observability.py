# Databricks notebook source
# COMMAND ----------
from src.analytics.observability_assets import write_observability_tables


dbutils.widgets.text("catalog", "healthcare", "Catalog")
dbutils.widgets.text("analytics_schema", "analytics", "Analytics schema")
dbutils.widgets.text("pipeline_id", "", "Pipeline ID")
dbutils.widgets.text("published_event_log_table", "", "Published event-log table")

CATALOG = dbutils.widgets.get("catalog").strip()
ANALYTICS_SCHEMA = dbutils.widgets.get("analytics_schema").strip()
PIPELINE_ID = dbutils.widgets.get("pipeline_id").strip()
PUBLISHED_EVENT_LOG_TABLE = dbutils.widgets.get("published_event_log_table").strip()

if not PIPELINE_ID and not PUBLISHED_EVENT_LOG_TABLE:
    raise ValueError("Provide either pipeline_id or published_event_log_table.")

print(f"Building observability assets into {CATALOG}.{ANALYTICS_SCHEMA}")

# COMMAND ----------
persisted_tables = write_observability_tables(
    spark,
    pipeline_id=PIPELINE_ID or None,
    published_event_log_table=PUBLISHED_EVENT_LOG_TABLE or None,
    catalog=CATALOG,
    analytics_schema=ANALYTICS_SCHEMA,
)

display(
    spark.createDataFrame(
        [{"table_name": table_name, "status": status} for table_name, status in persisted_tables.items()]
    )
)

# COMMAND ----------
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_pipeline_updates"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_expectation_metrics"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_user_actions"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_latest_failures"))
