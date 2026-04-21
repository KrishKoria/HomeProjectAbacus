# Databricks notebook source
# COMMAND ----------
from src.analytics.week3_quality_assets import write_week3_quality_assets


dbutils.widgets.text("catalog", "healthcare", "Catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("quarantine_schema", "quarantine", "Quarantine schema")
dbutils.widgets.text("analytics_schema", "analytics", "Analytics schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
QUARANTINE_SCHEMA = dbutils.widgets.get("quarantine_schema").strip()
ANALYTICS_SCHEMA = dbutils.widgets.get("analytics_schema").strip()

print(
    "Week 3 silver validation namespace: "
    f"silver={CATALOG}.{SILVER_SCHEMA} "
    f"quarantine={CATALOG}.{QUARANTINE_SCHEMA} "
    f"analytics={CATALOG}.{ANALYTICS_SCHEMA}"
)

# COMMAND ----------
persisted_assets = write_week3_quality_assets(
    spark,
    catalog=CATALOG,
    silver_schema=SILVER_SCHEMA,
    quarantine_schema=QUARANTINE_SCHEMA,
    analytics_schema=ANALYTICS_SCHEMA,
)

display(
    spark.createDataFrame(
        [{"table_name": table_name, "status": status} for table_name, status in persisted_assets.items()]
    )
)

# COMMAND ----------
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_silver_table_status"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.ops_quarantine_summary"))
