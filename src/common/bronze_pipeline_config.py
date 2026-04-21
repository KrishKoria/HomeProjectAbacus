from __future__ import annotations

from typing import Final, Iterable


AUDIT_COLUMNS: Final[tuple[str, str, str]] = (
    "_ingested_at",
    "_source_file",
    "_pipeline_run_id",
)
RESCUED_DATA_COLUMN: Final[str] = "_rescued_data"
PIPELINE_RUN_ID_FORMAT: Final[str] = "yyyyMMdd_HHmmss"
BRONZE_VOLUME_ROOT: Final[str] = "/Volumes/healthcare/bronze/raw_landing"

COMMON_DELTA_TABLE_PROPERTIES: Final[dict[str, str]] = {
    "delta.enableChangeDataFeed": "true",
    "delta.logRetentionDuration": "interval 6 years",
    "delta.deletedFileRetentionDuration": "interval 6 years",
}

CLAIMOPS_DOMAINS: Final[frozenset[str]] = frozenset({"BRZ", "ANL", "HIPAA", "OBS"})


def format_claimops_diagnostic_id(domain: str, number: int) -> str:
    """Return a stable diagnostic identifier for logs and dashboards."""
    normalized_domain = domain.upper()
    if normalized_domain not in CLAIMOPS_DOMAINS:
        raise ValueError(f"Unsupported diagnostic domain: {domain}")
    if number < 0 or number > 999:
        raise ValueError("Diagnostic number must be between 0 and 999.")
    return f"CLAIMOPS-{normalized_domain}-{number:03d}"


def bronze_volume_path(dataset_key: str) -> str:
    """Return the canonical Bronze volume path for a dataset folder."""
    return f"{BRONZE_VOLUME_ROOT}/{dataset_key}/"


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
    "BRONZE_VOLUME_ROOT",
    "CLAIMOPS_DOMAINS",
    "COMMON_DELTA_TABLE_PROPERTIES",
    "PIPELINE_RUN_ID_FORMAT",
    "RESCUED_DATA_COLUMN",
    "binary_file_autoloader_options",
    "bronze_volume_path",
    "csv_autoloader_options",
    "format_claimops_diagnostic_id",
    "table_properties_for_sensitivity",
]
