# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

from src.analytics.week2_analytics import (
    ANALYTICS_SCHEMA_DEFAULT,
    build_and_persist_week2_assets,
    bronze_table_name,
)


dbutils.widgets.text("catalog", "healthcare", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("analytics_schema", ANALYTICS_SCHEMA_DEFAULT, "Analytics schema")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
ANALYTICS_SCHEMA = dbutils.widgets.get("analytics_schema").strip()

print(f"Week 2 exploration namespace: bronze={CATALOG}.{BRONZE_SCHEMA} analytics={CATALOG}.{ANALYTICS_SCHEMA}")

# COMMAND ----------
claims = spark.table(bronze_table_name(CATALOG, BRONZE_SCHEMA, "claims"))
providers = spark.table(bronze_table_name(CATALOG, BRONZE_SCHEMA, "providers"))
diagnosis = spark.table(bronze_table_name(CATALOG, BRONZE_SCHEMA, "diagnosis"))
cost = spark.table(bronze_table_name(CATALOG, BRONZE_SCHEMA, "cost"))

display(claims.limit(10))
display(providers.limit(10))
display(diagnosis.limit(10))
display(cost.limit(10))

# COMMAND ----------
display(
    claims.groupBy("diagnosis_code")
    .count()
    .orderBy(F.desc("count"), F.asc("diagnosis_code"))
)

display(
    claims.groupBy("provider_id")
    .count()
    .orderBy(F.desc("count"), F.asc("provider_id"))
)

display(
    claims.select(
        F.count("*").alias("total_claims"),
        F.avg("billed_amount").alias("avg_billed_amount"),
        F.sum(F.when(F.col("procedure_code").isNull(), 1).otherwise(0)).alias("missing_procedure_code_rows"),
        F.sum(F.when(F.col("billed_amount").isNull(), 1).otherwise(0)).alias("missing_billed_amount_rows"),
    )
)

# COMMAND ----------
persisted_assets = build_and_persist_week2_assets(
    spark,
    catalog=CATALOG,
    bronze_schema=BRONZE_SCHEMA,
    analytics_schema=ANALYTICS_SCHEMA,
)

display(
    spark.createDataFrame(
        [{"table_key": table_key, "status": status} for table_key, status in persisted_assets.items()]
    )
)

# COMMAND ----------
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.claims_by_specialty_summary"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.claims_by_region_summary"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.high_cost_claims_summary"))
display(spark.table(f"{CATALOG}.{ANALYTICS_SCHEMA}.week2_dashboard_summary"))
