from __future__ import annotations

from typing import Final

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.bronze_pipeline_config import CATALOG_DEFAULT
from common.gold_pipeline_config import (
    GOLD_SCHEMA_DEFAULT,
    gold_table_name,
    gold_table_properties,
    read_silver_snapshot,
)
from common.silver_pipeline_config import SILVER_SCHEMA_DEFAULT, silver_table_name

GOLD_POLICY_CHUNKS_TABLE: Final[str] = gold_table_name(CATALOG_DEFAULT, "policy_chunks")
SILVER_POLICY_CHUNKS_TABLE: Final[str] = silver_table_name(CATALOG_DEFAULT, "policy_chunks", SILVER_SCHEMA_DEFAULT)
EMBEDDING_MODEL: Final[str] = "databricks-gte-large-en"
EMBEDDING_DIM: Final[int] = 768
_PIPELINE_CHANNEL_PREVIEW: Final[str] = "PREVIEW"


def _embedding_mv_table_properties() -> dict[str, str]:
    properties = gold_table_properties("NON-PHI")
    properties["pipelines.channel"] = _PIPELINE_CHANNEL_PREVIEW
    return properties


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
    table_properties=_embedding_mv_table_properties(),
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
            "_embedding_result",
            F.expr(f"ai_query('{EMBEDDING_MODEL}', chunk_text, failOnError => false)"),
        )
        .withColumn("embedding_vector", F.col("_embedding_result.response"))
        .withColumn(
            "embedding_status",
            F.when(
                F.col("_embedding_result.errorStatus").isNull(),
                F.lit("COMPLETED"),
            ).otherwise(F.lit("FAILED")),
        )
        .withColumn("embedding_model", F.lit(EMBEDDING_MODEL))
        .withColumn("embedded_at", F.lit(now))
        .drop("_embedding_result")
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
