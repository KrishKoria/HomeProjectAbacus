from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.bronze_pipeline_config import CATALOG_DEFAULT
from common.gold_pipeline_config import (
    GOLD_SCHEMA_DEFAULT,
    gold_table_name,
    gold_table_properties,
    read_silver_snapshot,
)

GOLD_POLICY_CHUNKS_TABLE = gold_table_name(CATALOG_DEFAULT, "policy_chunks")
SILVER_POLICY_CHUNKS_TABLE = "healthcare.silver.policy_chunks"
EMBEDDING_MODEL = "databricks-gte-large-en"
EMBEDDING_DIM = 768


@dp.materialized_view(
    name=GOLD_POLICY_CHUNKS_TABLE,
    refresh_policy="incremental",
    comment=(
        "Gold policy chunks table with populated embedding vectors from "
        f"Databricks GTE ({EMBEDDING_MODEL}). Vectors are 768-dimensional "
        "float64 arrays stored via array<double>. The table is clustered by "
        "chunk_id for efficient delta-sync to the Vector Search index. "
        "Embedding status tracks incremental processing state: COMPLETED "
        "for successful embeddings, FAILED for persistent errors."
    ),
    table_properties=gold_table_properties("NON-PHI"),
)
def gold_policy_embeddings():
    import datetime

    silver = read_silver_snapshot(
        spark,
        SILVER_POLICY_CHUNKS_TABLE,
    ).where(F.col("chunk_text").isNotNull() & (F.trim(F.col("chunk_text")) != ""))

    now = datetime.datetime.now(datetime.timezone.utc)

    result = (
        silver.withColumn(
            "embedding_vector",
            F.expr(f"ai_query('{EMBEDDING_MODEL}', chunk_text)"),
        )
        .withColumn(
            "embedding_status",
            F.when(
                F.col("embedding_vector").isNotNull(),
                F.lit("COMPLETED"),
            ).otherwise(F.lit("FAILED")),
        )
        .withColumn("embedding_model", F.lit(EMBEDDING_MODEL))
        .withColumn("embedded_at", F.lit(now))
    )

    return result.select(
        "chunk_id",
        "document_path",
        "chunk_index",
        "chunk_text",
        "token_count",
        "embedding_vector",
        "embedding_status",
        "embedding_model",
        "embedded_at",
    )
