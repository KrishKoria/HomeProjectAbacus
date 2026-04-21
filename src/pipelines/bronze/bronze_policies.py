"""
Bronze streaming table: healthcare.bronze.policies
====================================================
Ingests raw insurance policy PDF documents from the landing volume into the Bronze
Delta layer using Spark Declarative Pipelines (SDP) with Auto Loader (cloudFiles)
in binaryFile format.

Product requirements satisfied
-------------------------------
FR-DATA-01  Ingest policy documents from volume sources.
FR-DATA-02  Preserve ALL raw data in Bronze with ingestion timestamp and source metadata.
FR-DATA-06  Handle schema evolution without pipeline failure — binaryFile schema is fixed,
            so no schema evolution is needed; new files are appended automatically.
FR-DATA-07  Maintain full data lineage from source file to table (_source_file column).
FR-DATA-08  Support incremental processing — Auto Loader checkpoint prevents reprocessing.
FR-RAG-01   Provides raw source corpus for policy retrieval (RAG indexing pipeline).

HIPAA compliance controls
--------------------------
delta.enableChangeDataFeed         Enables incremental reads downstream so Silver only
                                   processes new policy documents — required for efficient CDC.
delta.logRetentionDuration         6 years — organization retention policy for audit
                                   reconstruction, consistent with §164.316(b)(2)(i)
                                   compliance documentation standards.
delta.deletedFileRetentionDuration 6 years — retains physical files after logical deletion
                                   for time-travel audit reconstruction.
_ingested_at                       Records exactly when data entered the system.
                                   Audit control per § 164.312(b).
_source_file                       Records which file the document came from (data lineage).
                                   Audit control per § 164.312(b).
_pipeline_run_id                   Groups rows by pipeline execution for audit correlation.
                                   Audit control per § 164.312(b).

PHI classification — this table contains NO PHI
-------------------------------------------------
Policy documents are insurance policy text describing billing rules, reimbursement
schedules, and coverage guidelines. They contain no patient-identifiable information.
Per architecture Assumption A-04 and HIPAA § 164.501, PHI is "individually identifiable
health information" of patients — policy text is not. No column-level encryption is
required for this table.

This classification is also the basis for the PHI Firewall in the RAG query path
(architecture §14.2): the Vector Search index built from this table contains only
policy text, never patient data, which is what makes it safe to pass to the LLM
Foundation Model endpoint.

binaryFile format differences from CSV pipelines
-------------------------------------------------
Unlike the four CSV Bronze tables, this pipeline uses cloudFiles.format=binaryFile.
Key differences:
  - Schema is fixed: path, modificationTime, length, content — no inferColumnTypes needed.
  - No cloudFiles.schemaEvolutionMode — binaryFile schema never changes.
  - No cloudFiles.rescuedDataColumn — binary files cannot produce parse errors.
  - pathGlobFilter=*.pdf — only ingest PDF files; reject other file types.
  - `path` column (from the binaryFile schema) IS the source file path.
    _source_file is set from `path` directly rather than from _metadata.file_path.
  - Text extraction (pdfplumber) and chunking happen in Silver, NOT here.
    Bronze stores raw bytes as-is — no transformation, no text parsing.

Source
------
Volume path : /Volumes/healthcare/bronze/raw_landing/policies/
File type   : PDF (*.pdf)
Schema      : path (string), modificationTime (timestamp), length (long), content (binary)

Output table: healthcare.bronze.policies
Cluster by  : path (document-level lookups by file path)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.common.bronze_pipeline_config import (
    PIPELINE_RUN_ID_FORMAT,
    binary_file_autoloader_options,
    bronze_volume_path,
    table_properties_for_sensitivity,
)
from src.common.observability import MESSAGE_BRONZE_APPEND_ONLY

VOLUME_PATH = bronze_volume_path("policies")

# ---------------------------------------------------------------------------
# Data quality expectations — ALL are warn-only at Bronze.
# Bronze NEVER drops raw records (FR-DATA-02: preserve all raw data).
# Violations are counted and logged to the SDP pipeline event log.
# ---------------------------------------------------------------------------

# path IS NOT NULL
# Business rule: every document must have a resolvable file path for lineage.
# RAG impact: a pathless document cannot be cited in a policy explanation.
@dp.expect("pdf_path_not_null", "path IS NOT NULL")

# content IS NOT NULL
# Business rule: a NULL binary content means the file could not be read.
# RAG impact: an empty content column will produce an empty text chunk in Silver,
# which degrades RAG retrieval quality.
@dp.expect("pdf_content_not_null", "content IS NOT NULL")

# length > 0
# Business rule: a zero-byte PDF contains no policy text and cannot be parsed.
# RAG impact: Silver pdfplumber parsing will produce zero chunks from an empty file.
@dp.expect("pdf_size_positive", "length > 0")
@dp.table(
    name="healthcare.bronze.policies",
    cluster_by=["path"],
    comment=(
        "Raw insurance policy PDF documents ingested from landing volume. Append-only. "
        f"Binary content (raw PDF bytes) preserved as-is. {MESSAGE_BRONZE_APPEND_ONLY} "
        "No PHI — policy documents are insurance billing policy text (Assumption A-04). "
        "pathGlobFilter=*.pdf: only PDF files are ingested; other file types are ignored. "
        "Downstream: healthcare.silver.policy_chunks reads this table via Change Data Feed "
        "and applies pdfplumber text extraction + 512-token chunking for RAG indexing."
    ),
    table_properties=table_properties_for_sensitivity("NON-PHI"),
)
def bronze_policies():
    """
    Stream insurance policy PDF documents from the landing volume into
    healthcare.bronze.policies.

    Uses Auto Loader (cloudFiles) with binaryFile format, which manages checkpoint
    state automatically — do NOT configure checkpoint paths manually.

    Returns
    -------
    pyspark.sql.DataFrame
        Streaming DataFrame with the fixed binaryFile schema plus audit columns:

        binaryFile schema columns (fixed — no inference needed):
            path              str        Full volume path of the PDF file.
                                         Also serves as the document's primary key.
                                         Example: /Volumes/.../policies/cms_policy_v2.pdf
            modificationTime  timestamp  Last modification time of the source file.
                                         Useful for detecting updated policy versions.
            length            long       File size in bytes. Zero-length = empty file
                                         (will trigger pdf_size_positive expectation).
            content           binary     Raw PDF bytes. pdfplumber in Silver extracts
                                         text from this column. Store as-is at Bronze.

        Audit columns (added by this pipeline):
            _ingested_at      timestamp  When this document entered the Bronze layer.
                                         HIPAA audit control per § 164.312(b).
            _source_file      str        Full volume path of the source PDF.
                                         Set from `path` (binaryFile schema column).
                                         HIPAA audit control per § 164.312(b).
            _pipeline_run_id  str        Pipeline execution timestamp for audit correlation.
                                         Format: yyyyMMdd_HHmmss.
                                         HIPAA audit control per § 164.312(b).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**binary_file_autoloader_options())
        .load(VOLUME_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        # For binaryFile format, `path` is a top-level schema column that contains
        # the full source file path — use it directly instead of _metadata.file_path.
        .withColumn("_source_file", F.col("path"))
        .withColumn(
            "_pipeline_run_id",
            # Timestamp-based run ID groups documents from the same pipeline execution.
            # Format: yyyyMMdd_HHmmss — sortable, human-readable, no external dependency.
            F.date_format(F.current_timestamp(), PIPELINE_RUN_ID_FORMAT),
        )
        # Drop the internal _metadata struct — file_path is already captured in `path`.
        .drop("_metadata")
    )
