from __future__ import annotations

from typing import Final, Iterable

from common.diagnostics import CLAIMOPS_DOMAINS, format_claimops_diagnostic_id


AUDIT_COLUMNS: Final[tuple[str, str, str]] = (
    "_ingested_at",
    "_source_file",
    "_pipeline_run_id",
)
RESCUED_DATA_COLUMN: Final[str] = "_rescued_data"
PIPELINE_RUN_ID_FORMAT: Final[str] = "yyyyMMdd_HHmmss"
CATALOG_DEFAULT: Final[str] = "healthcare"
BRONZE_SCHEMA_DEFAULT: Final[str] = "bronze"
BRONZE_VOLUME_DEFAULT: Final[str] = "raw_landing"
BRONZE_VOLUME_ROOT: Final[str] = "/Volumes/healthcare/bronze/raw_landing"

COMMON_DELTA_TABLE_PROPERTIES: Final[dict[str, str]] = {
    "delta.enableChangeDataFeed": "true",
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
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": RESCUED_DATA_COLUMN,
    }


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


__all__ = [
    "AUDIT_COLUMNS",
    "BRONZE_SCHEMA_DEFAULT",
    "BRONZE_VOLUME_DEFAULT",
    "BRONZE_VOLUME_ROOT",
    "CATALOG_DEFAULT",
    "CLAIMOPS_DOMAINS",
    "COMMON_DELTA_TABLE_PROPERTIES",
    "PIPELINE_RUN_ID_FORMAT",
    "RESCUED_DATA_COLUMN",
    "binary_file_autoloader_options",
    "bronze_table_name",
    "bronze_volume_root",
    "bronze_volume_path",
    "csv_autoloader_options",
    "format_claimops_diagnostic_id",
    "table_name",
    "table_properties_for_sensitivity",
]
