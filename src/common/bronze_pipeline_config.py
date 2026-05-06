from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Final, Iterable

logger = logging.getLogger(__name__)

from src.common.diagnostics import CLAIMOPS_DOMAINS, DIAGNOSTIC_DOMAIN_BRONZE, format_claimops_diagnostic_id


AUDIT_COLUMNS: Final[tuple[str, str, str]] = (
    "_ingested_at",
    "_source_file",
    "_pipeline_run_id",
)
RESCUED_DATA_COLUMN: Final[str] = "_rescued_data"
PIPELINE_RUN_ID_FORMAT: Final[str] = "yyyyMMdd_HHmmss"
PIPELINE_RUN_ID_CONF: Final[str] = "claimops.pipeline_run_id"
_DEFAULT_PIPELINE_RUN_ID: Final[str] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
CATALOG_DEFAULT: Final[str] = "healthcare"
BRONZE_SCHEMA_DEFAULT: Final[str] = "bronze"
BRONZE_VOLUME_DEFAULT: Final[str] = "raw_landing"
BRONZE_VOLUME_ROOT: Final[str] = "/Volumes/healthcare/bronze/raw_landing"

COMMON_DELTA_TABLE_PROPERTIES: Final[dict[str, str]] = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.logRetentionDuration": "interval 2190 days",
    "delta.deletedFileRetentionDuration": "interval 2190 days",
}


def table_name(catalog: str, schema: str, table: str) -> str:
    """Return a fully-qualified Unity Catalog table name."""
    return f"{catalog}.{schema}.{table}"


def bronze_table_name(
    table: str,
    *,
    catalog: str = CATALOG_DEFAULT,
    schema: str = BRONZE_SCHEMA_DEFAULT,
) -> str:
    """Return a fully-qualified Bronze table name."""
    return table_name(catalog, schema, table)


def bronze_volume_root(
    *,
    catalog: str = CATALOG_DEFAULT,
    schema: str = BRONZE_SCHEMA_DEFAULT,
    volume: str = BRONZE_VOLUME_DEFAULT,
) -> str:
    """Return the Unity Catalog volume root for Bronze source landing."""
    return f"/Volumes/{catalog}/{schema}/{volume}"


def bronze_volume_path(
    dataset_key: str,
    *,
    catalog: str = CATALOG_DEFAULT,
    schema: str = BRONZE_SCHEMA_DEFAULT,
    volume: str = BRONZE_VOLUME_DEFAULT,
) -> str:
    """Return the canonical Bronze volume path for a dataset folder."""
    return f"{bronze_volume_root(catalog=catalog, schema=schema, volume=volume)}/{dataset_key}/"


def csv_autoloader_options() -> dict[str, str]:
    """Shared Auto Loader defaults for CSV Bronze ingestion."""
    return {
        "cloudFiles.format": "csv",
        "header": "true",
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": RESCUED_DATA_COLUMN,
    }


def stable_pipeline_run_id():
    """Return a Spark literal run ID stable across micro-batches in one module load."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    configured = ""
    spark_session = SparkSession.getActiveSession()
    if spark_session is not None:
        try:
            configured = spark_session.conf.get(PIPELINE_RUN_ID_CONF, "")
        except Exception:
            logger.warning(
                "[%s] Could not read pipeline_run_id from SparkConf; using default.",
                format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_BRONZE, 101),
                exc_info=True,
            )
            configured = ""
    return F.lit(configured or _DEFAULT_PIPELINE_RUN_ID)


def cache_if_available(dataframe):
    """Cache a DataFrame when the runtime supports it; keep serverless paths portable."""
    if not hasattr(dataframe, "cache"):
        return dataframe
    try:
        return dataframe.cache()
    except Exception as exc:
        message = str(exc)
        if "NOT_SUPPORTED_WITH_SERVERLESS" in message or "PERSIST TABLE is not supported" in message:
            return dataframe
        raise


def unpersist_if_available(dataframe) -> None:
    """Unpersist a DataFrame when the runtime supports it."""
    if not hasattr(dataframe, "unpersist"):
        return
    try:
        dataframe.unpersist()
    except Exception:
        logger.warning(
            "[%s] Could not unpersist DataFrame cache; continuing.",
            format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_BRONZE, 103),
            exc_info=True,
        )
        return


def binary_file_autoloader_options(path_glob_filter: str = "*.pdf") -> dict[str, str]:
    """Shared Auto Loader defaults for binary file Bronze ingestion."""
    return {
        "cloudFiles.format": "binaryFile",
        "pathGlobFilter": path_glob_filter,
    }


def table_properties_for_sensitivity(
    sensitivity: str,
    phi_columns: Iterable[str] = (),
) -> dict[str, str]:
    """Return shared table properties plus PHI metadata for the given sensitivity."""
    properties = dict(COMMON_DELTA_TABLE_PROPERTIES)
    properties["hipaa.phi_columns"] = ",".join(phi_columns)
    properties["hipaa.data_sensitivity"] = sensitivity
    return properties


_VALID_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate a Unity Catalog unquoted identifier. Returns the validated name or raises ValueError."""
    if not _VALID_IDENTIFIER_RE.match(name):
        diag_id = format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_BRONZE, 102)
        raise ValueError(
            f"[{diag_id}] Invalid {label}: {name!r}. "
            f"Must match {_VALID_IDENTIFIER_RE.pattern}"
        )
    return name


def escape_backtick_identifier(name: str) -> str:
    """Return a backtick-quoted identifier with embedded backticks doubled."""
    return f"`{name.replace('`', '``')}`"


__all__ = [
    "AUDIT_COLUMNS",
    "BRONZE_SCHEMA_DEFAULT",
    "BRONZE_VOLUME_DEFAULT",
    "BRONZE_VOLUME_ROOT",
    "CATALOG_DEFAULT",
    "CLAIMOPS_DOMAINS",
    "COMMON_DELTA_TABLE_PROPERTIES",
    "PIPELINE_RUN_ID_CONF",
    "PIPELINE_RUN_ID_FORMAT",
    "RESCUED_DATA_COLUMN",
    "binary_file_autoloader_options",
    "bronze_table_name",
    "bronze_volume_path",
    "bronze_volume_root",
    "cache_if_available",
    "csv_autoloader_options",
    "escape_backtick_identifier",
    "format_claimops_diagnostic_id",
    "stable_pipeline_run_id",
    "table_name",
    "table_properties_for_sensitivity",
    "unpersist_if_available",
    "validate_identifier",
]
