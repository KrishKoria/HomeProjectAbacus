"""Silver policy chunk pipeline with extraction comments and PHI-safe diagnostics."""

from __future__ import annotations

from io import BytesIO
from typing import Final

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
    MAX_CHUNK_COUNT,
    MAX_EXTRACTED_TEXT_LENGTH,
    MAX_PDF_PAGE_COUNT,
    MAX_PDF_SIZE_BYTES,
    MAX_PDF_TOKEN_COUNT,
    NON_PHI_TABLE_PROPERTIES,
    QUARANTINE_SCHEMA_DEFAULT,
    SILVER_SCHEMA_DEFAULT,
    quarantine_table_name,
    read_bronze_snapshot,
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

_EXTRACTION_STATUS_META: Final[dict[str, tuple[str, str]]] = {
    "UNREADABLE_PDF": ("unreadable_pdf", "pdfplumber could not extract policy text from the binary document"),
    "EMPTY_PDF_TEXT": ("empty_pdf_text", "policy document produced no extractable text"),
    "OVERSIZED_PDF_FILE": ("oversized_pdf_file", "policy PDF file size exceeds the maximum allowed size"),
    "OVERSIZED_PDF_PAGES": ("oversized_pdf_pages", "policy PDF page count exceeds the maximum allowed pages"),
    "OVERSIZED_PDF_TEXT": ("oversized_pdf_text", "extracted policy text length exceeds the maximum allowed size"),
}

_STATUS_RULE_MAP: Final[dict[str, str]] = {s: m[0] for s, m in _EXTRACTION_STATUS_META.items()}
_STATUS_REASON_MAP: Final[dict[str, str]] = {s: m[1] for s, m in _EXTRACTION_STATUS_META.items()}
_STATUS_DIAGNOSTIC_MAP: Final[dict[str, str]] = {
    s: get_silver_diagnostic_id("policy_chunks", m[0]) for s, m in _EXTRACTION_STATUS_META.items()
}


def _status_chain(value_map: dict[str, str], default: str):
    """Build a WHEN/OTHERWISE chain mapping extraction_status to target values."""
    items = list(value_map.items())
    result = F.when(F.col("extraction_status") == F.lit(items[0][0]), F.lit(items[0][1]))
    for status, value in items[1:]:
        result = result.when(F.col("extraction_status") == F.lit(status), F.lit(value))
    return result.otherwise(F.lit(default))


def _extract_policy_text(pdf_bytes):
    """Wrap pdfplumber extraction so the Spark UDF returns structured status values."""
    try:
        if not pdf_bytes:
            policy_text = None
        else:
            if len(pdf_bytes) > MAX_PDF_SIZE_BYTES:
                return (None, "OVERSIZED_PDF_FILE", f"File size {len(pdf_bytes)} exceeds {MAX_PDF_SIZE_BYTES}")

            import pdfplumber

            page_text = []
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for i, page in enumerate(pdf.pages):
                    if i >= MAX_PDF_PAGE_COUNT:
                        return (None, "OVERSIZED_PDF_PAGES", f"Page count exceeds {MAX_PDF_PAGE_COUNT}")
                    extracted = page.extract_text() or ""
                    normalized = extracted.strip()
                    if normalized:
                        page_text.append(normalized)
            policy_text = "\n".join(page_text) if page_text else None
    except Exception as exc:  # pragma: no cover - Spark UDF runtime path
        return (None, "UNREADABLE_PDF", str(exc))
    if policy_text is None:
        return (None, "EMPTY_PDF_TEXT", None)
    if len(policy_text) > MAX_EXTRACTED_TEXT_LENGTH:
        return (None, "OVERSIZED_PDF_TEXT", f"Extracted text length {len(policy_text)} exceeds {MAX_EXTRACTED_TEXT_LENGTH}")
    return (policy_text, "OK", None)


def _chunk_policy_text(policy_text, chunk_size_tokens: int = 512, overlap_tokens: int = 64):
    """Chunk normalized policy text into the fixed token windows used by RAG."""
    if policy_text is None:
        return []

    normalized = " ".join(policy_text.split())
    if not normalized:
        return []

    tokens = normalized.split(" ")
    if len(tokens) > MAX_PDF_TOKEN_COUNT:
        tokens = tokens[:MAX_PDF_TOKEN_COUNT]

    step = max(1, chunk_size_tokens - overlap_tokens)
    chunks = []
    chunk_index = 0
    for start_index in range(0, len(tokens), step):
        if chunk_index >= MAX_CHUNK_COUNT:
            break
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


@dp.temporary_view(name="policy_documents_stream")
def _policy_documents_stream():
    """Build the shared document stream for trusted chunks and quarantined PDFs."""
    duplicate_window = Window.partitionBy("path").orderBy(
        # Policies are versioned by source path; only the freshest copy should fan out
        # into chunks, while older copies are preserved for quarantine diagnostics.
        F.coalesce(F.col("_ingested_at"), F.col("modificationTime")).desc(),
        F.col("_pipeline_run_id").desc(),
        F.col("_source_file").desc(),
    )
    extracted = (
        read_bronze_snapshot(spark, BRONZE_POLICIES_TABLE)
        .withColumn("_row_priority", F.row_number().over(duplicate_window))
        .withColumn("extract_result", _extract_policy_text_udf(F.col("content")))
        .withColumn("policy_text", F.col("extract_result.policy_text"))
        .withColumn("extraction_status", F.col("extract_result.status"))
        .withColumn("extraction_error_message", F.col("extract_result.error_message"))
        .withColumn(
            "_data_quality_flags",
            F.filter(
                F.array(*[
                    F.when(F.col("extraction_status") == F.lit(s), F.lit(r))
                    for s, (r, _) in _EXTRACTION_STATUS_META.items()
                ]),
                lambda flag: flag.isNotNull(),
            ),
        )
    )
    return extracted.drop("extract_result")


@dp.materialized_view(
    name=SILVER_POLICY_CHUNKS_TABLE,
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
        spark.read.table("policy_documents_stream")
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
        "_data_quality_flags",
        "_source_file",
        "_pipeline_run_id",
    )


@dp.materialized_view(
    name=QUARANTINE_POLICY_CHUNKS_TABLE,
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
        spark.read.table("policy_documents_stream")
        .where((F.col("extraction_status") != F.lit("OK")) | (F.col("_row_priority") > 1))
        .withColumn("diagnostic_id", _status_chain(_STATUS_DIAGNOSTIC_MAP, get_silver_diagnostic_id("policy_chunks", "duplicate_policy_path")))
        .withColumn("rule_name", _status_chain(_STATUS_RULE_MAP, "duplicate_policy_path"))
        .withColumn("quarantine_reason", _status_chain(_STATUS_REASON_MAP, "duplicate policy path observed in the silver stream"))
        .withColumn(
            "status_message",
            F.concat(
                F.lit("Policy chunk extraction recorded: document_path="),
                F.col("path"),
                F.lit(" chunk_count=0 diagnostic_id="),
                F.col("diagnostic_id"),
            ),
        )
    )
    return quarantined.drop("_row_priority")
