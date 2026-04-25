"""Silver policy chunk pipeline with extraction comments and PHI-safe diagnostics."""

from __future__ import annotations

from io import BytesIO

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from common.bronze_pipeline_config import CATALOG_DEFAULT, bronze_table_name
from common.diagnostics import get_silver_diagnostic_id
from common.observability import (
    LOG_CATEGORY_POLICY_CHUNKING,
    LOG_CATEGORY_QUARANTINE_AUDIT,
    MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY,
    MESSAGE_TEMPLATE_QUARANTINE_SUMMARY,
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,
)
from common.silver_pipeline_config import (
    NON_PHI_TABLE_PROPERTIES,
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    quarantine_table_name,
    read_bronze_cdf,
    silver_table_name,
    silver_table_properties,
)


BRONZE_POLICIES_TABLE = bronze_table_name("policies")
SILVER_POLICY_CHUNKS_TABLE = silver_table_name(CATALOG_DEFAULT, "policy_chunks", SILVER_SCHEMA_DEFAULT)
QUARANTINE_POLICY_CHUNKS_TABLE = quarantine_table_name(CATALOG_DEFAULT, "policy_chunks", QUARANTINE_SCHEMA_DEFAULT)

_CHUNK_SCHEMA = ArrayType(
    StructType(
        [
            StructField("chunk_index", IntegerType(), False),
            StructField("chunk_text", StringType(), False),
            StructField("token_count", IntegerType(), False),
        ]
    )
)

_TEXT_SCHEMA = StructType(
    [
        StructField("policy_text", StringType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)


def _extract_policy_text(pdf_bytes):
    """Wrap pdfplumber extraction so the Spark UDF returns structured status values."""
    try:
        if not pdf_bytes:
            policy_text = None
        else:
            import pdfplumber

            page_text = []
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    extracted = page.extract_text() or ""
                    normalized = extracted.strip()
                    if normalized:
                        page_text.append(normalized)
            policy_text = "\n".join(page_text) if page_text else None
    except Exception as exc:  # pragma: no cover - Spark UDF runtime path
        return (None, "UNREADABLE_PDF", str(exc))
    if policy_text is None:
        return (None, "EMPTY_PDF_TEXT", None)
    return (policy_text, "OK", None)


def _chunk_policy_text(policy_text, chunk_size_tokens: int = 512, overlap_tokens: int = 64):
    """Chunk normalized policy text into the fixed token windows used by RAG."""
    if policy_text is None:
        return []

    normalized = policy_text
    for delimiter in ("\r", "\n", "\t"):
        normalized = normalized.replace(delimiter, " ")
    normalized = " ".join(normalized.split())
    if not normalized:
        return []

    tokens = normalized.split(" ")
    step = max(1, chunk_size_tokens - overlap_tokens)
    chunks = []
    chunk_index = 0
    for start_index in range(0, len(tokens), step):
        token_slice = tokens[start_index:start_index + chunk_size_tokens]
        if not token_slice:
            continue
        chunks.append(
            {
                "chunk_index": chunk_index,
                "chunk_text": " ".join(token_slice),
                "token_count": len(token_slice),
            }
        )
        chunk_index += 1
        if start_index + chunk_size_tokens >= len(tokens):
            break
    return chunks


_extract_policy_text_udf = F.udf(_extract_policy_text, _TEXT_SCHEMA)
_chunk_policy_text_udf = F.udf(_chunk_policy_text, _CHUNK_SCHEMA)


def _policy_documents_stream():
    """Build the shared document stream for trusted chunks and quarantined PDFs."""
    duplicate_window = Window.partitionBy("path").orderBy(
        # Policies are versioned by source path; only the freshest copy should fan out
        # into chunks, while older copies are preserved for quarantine diagnostics.
        F.coalesce(F.col("_ingested_at"), F.col("modificationTime")).desc(),
        F.col("_pipeline_run_id").desc(),
    )
    extracted = (
        read_bronze_cdf(spark, BRONZE_POLICIES_TABLE)
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("extract_result", _extract_policy_text_udf(F.col("content")))
        .withColumn("policy_text", F.col("extract_result.policy_text"))
        .withColumn("extraction_status", F.col("extract_result.status"))
        .withColumn("extraction_error_message", F.col("extract_result.error_message"))
        .withColumn(
            "_data_quality_flags",
            F.array_remove(
                F.array(
                    F.when(F.col("extraction_status") == F.lit("UNREADABLE_PDF"), F.lit("unreadable_pdf")),
                    F.when(F.col("extraction_status") == F.lit("EMPTY_PDF_TEXT"), F.lit("empty_pdf_text")),
                ),
                F.lit(None),
            ),
        )
    )
    return extracted.drop("extract_result")


@dp.table(
    name=SILVER_POLICY_CHUNKS_TABLE,
    cluster_by=["document_path", "chunk_id"],
    comment=(
        MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
            table_name=SILVER_POLICY_CHUNKS_TABLE,
            category=LOG_CATEGORY_POLICY_CHUNKING,
            sensitivity="NON-PHI",
        )
        + " Trusted Silver policy chunks contain extracted policy text only; unreadable or empty PDFs are quarantined."
    ),
    table_properties=silver_table_properties("NON-PHI"),
)
def silver_policy_chunks():
    """Emit trusted policy chunks for downstream retrieval/indexing."""
    trusted_docs = (
        _policy_documents_stream()
        .where(F.col("extraction_status") == F.lit("OK"))
        .where(F.col("_row_priority") == 1)
        .withColumn("chunks", _chunk_policy_text_udf(F.col("policy_text")))
        .withColumn("chunk", F.explode(F.col("chunks")))
        .withColumn("document_path", F.col("path"))
        .withColumn("chunk_index", F.col("chunk.chunk_index"))
        .withColumn("chunk_text", F.col("chunk.chunk_text"))
        .withColumn("token_count", F.col("chunk.token_count"))
        .withColumn(
            # A deterministic hash keeps chunk IDs stable across reruns as long as the
            # document path and chunk position do not change.
            "chunk_id",
            F.sha2(F.concat_ws("::", F.col("path"), F.col("chunk.chunk_index").cast("string")), 256),
        )
    )
    return trusted_docs.select(
        "chunk_id",
        "document_path",
        "chunk_index",
        "chunk_text",
        "token_count",
        "_silver_processed_at",
        "_data_quality_flags",
        "_source_file",
        "_pipeline_run_id",
    )


@dp.table(
    name=QUARANTINE_POLICY_CHUNKS_TABLE,
    cluster_by=["path", "diagnostic_id"],
    comment=(
        MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
            dataset="policy_chunks",
            rule_name="pdf_extraction",
            diagnostic_id=get_silver_diagnostic_id("policy_chunks", "unreadable_pdf"),
            quarantined_records="runtime_count",
        )
        + f" category={LOG_CATEGORY_QUARANTINE_AUDIT}"
    ),
    table_properties=NON_PHI_TABLE_PROPERTIES,
)
def quarantine_policy_chunks():
    """Emit PHI-safe quarantine rows for unreadable or empty policy documents."""
    quarantined = (
        _policy_documents_stream()
        .where((F.col("extraction_status") != F.lit("OK")) | (F.col("_row_priority") > 1))
        .withColumn(
            "diagnostic_id",
            F.when(F.col("extraction_status") == F.lit("UNREADABLE_PDF"), F.lit(get_silver_diagnostic_id("policy_chunks", "unreadable_pdf")))
            .when(F.col("extraction_status") == F.lit("EMPTY_PDF_TEXT"), F.lit(get_silver_diagnostic_id("policy_chunks", "empty_pdf_text")))
            .otherwise(F.lit(get_silver_diagnostic_id("policy_chunks", "duplicate_policy_path"))),
        )
        .withColumn(
            "rule_name",
            F.when(F.col("extraction_status") == F.lit("UNREADABLE_PDF"), F.lit("unreadable_pdf"))
            .when(F.col("extraction_status") == F.lit("EMPTY_PDF_TEXT"), F.lit("empty_pdf_text"))
            .otherwise(F.lit("duplicate_policy_path")),
        )
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("extraction_status") == F.lit("UNREADABLE_PDF"),
                F.lit("pdfplumber could not extract policy text from the binary document"),
            )
            .when(
                F.col("extraction_status") == F.lit("EMPTY_PDF_TEXT"),
                F.lit("policy document produced no extractable text"),
            )
            .otherwise(F.lit("duplicate policy path observed in the silver stream")),
        )
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Policy chunk extraction recorded: document_path="),
                F.col("path"),
                F.lit(" chunk_count=0 diagnostic_id="),
                F.col("diagnostic_id"),
            ),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return quarantined.drop("_row_priority")
