from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


def _embed_chunk(text: str | None) -> list[float] | None:
    """Call Databricks GTE endpoint for a single chunk text.

    On Databricks this uses ``ai_query``; outside Databricks returns None.
    """
    if not text or not text.strip():
        return None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        result = spark.sql(
            "SELECT ai_query('{model}', '{text}') AS embedding".format(
                model=EMBEDDING_MODEL, text=text.replace("'", "''")
            )
        ).collect()
        if result and result[0] and result[0][0]:
            return list(result[0][0])
    except Exception:
        pass
    return None


_embed_udf = F.udf(_embed_chunk, T.ArrayType(T.DoubleType()))


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
    silver = read_silver_snapshot(
        spark,
        SILVER_POLICY_CHUNKS_TABLE,
    ).where(F.col("chunk_text").isNotNull() & (F.trim(F.col("chunk_text")) != ""))

    existing = spark.read.table(GOLD_POLICY_CHUNKS_TABLE).select("chunk_id").where(
        F.col("embedding_status") == F.lit("COMPLETED")
    )

    new_chunks = silver.join(
        existing,
        on="chunk_id",
        how="left_anti",
    )

    result = (
        new_chunks.withColumn(
            "embedding_vector",
            _embed_udf(F.col("chunk_text")),
        )
        .withColumn(
            "embedding_status",
            F.when(
                F.col("embedding_vector").isNotNull(),
                F.lit("COMPLETED"),
            ).otherwise(F.lit("FAILED")),
        )
        .withColumn("embedding_model", F.lit(EMBEDDING_MODEL))
        .withColumn("embedded_at", F.current_timestamp())
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
