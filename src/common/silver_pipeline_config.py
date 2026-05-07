from __future__ import annotations

from typing import Final, Iterable

from src.common.bronze_pipeline_config import table_properties_for_sensitivity


SILVER_SCHEMA_DEFAULT: Final[str] = "silver"
QUARANTINE_SCHEMA_DEFAULT: Final[str] = "quarantine"
ANALYTICS_SCHEMA_DEFAULT: Final[str] = "analytics"

SILVER_AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "_data_quality_flags",
)
QUARANTINE_AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "diagnostic_id",
    "rule_name",
    "quarantine_reason",
)

POLICY_CHUNK_SIZE_TOKENS: Final[int] = 512
POLICY_CHUNK_OVERLAP_TOKENS: Final[int] = 64
MONEY_DECIMAL_PRECISION: Final[int] = 18
MONEY_DECIMAL_SCALE: Final[int] = 2

NON_PHI_TABLE_PROPERTIES: Final[dict[str, str]] = table_properties_for_sensitivity("NON-PHI")
PHI_TABLE_PROPERTIES: Final[dict[str, str]] = table_properties_for_sensitivity("PHI")
SENSITIVE_TABLE_PROPERTIES: Final[dict[str, str]] = table_properties_for_sensitivity("SENSITIVE")

MAX_PDF_SIZE_BYTES: Final[int] = 50_000_000
MAX_PDF_PAGE_COUNT: Final[int] = 2000
MAX_EXTRACTED_TEXT_LENGTH: Final[int] = 5_000_000
MAX_CHUNK_COUNT: Final[int] = 5000
MAX_PDF_TOKEN_COUNT: Final[int] = 500_000


def silver_table_name(catalog: str, table_name: str, schema: str = SILVER_SCHEMA_DEFAULT) -> str:
    """Return a fully-qualified Silver table name."""
    return f"{catalog}.{schema}.{table_name}"


def quarantine_table_name(
    catalog: str,
    table_name: str,
    schema: str = QUARANTINE_SCHEMA_DEFAULT,
) -> str:
    """Return a fully-qualified quarantine table name."""
    return f"{catalog}.{schema}.{table_name}"


def create_required_schemas(
    spark,
    catalog: str,
    silver_schema: str = SILVER_SCHEMA_DEFAULT,
    quarantine_schema: str = QUARANTINE_SCHEMA_DEFAULT,
    analytics_schema: str = ANALYTICS_SCHEMA_DEFAULT,
) -> None:
    """Ensure all week 3 schemas exist before writing outputs."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{quarantine_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{analytics_schema}")


def silver_table_properties(
    sensitivity: str,
    phi_columns: Iterable[str] = (),
) -> dict[str, str]:
    """Return Silver/Quarantine Delta table properties with layer metadata."""
    properties = table_properties_for_sensitivity(sensitivity, phi_columns)
    properties["claimops.layer"] = "silver"
    return properties


def read_bronze_snapshot(spark, table_name: str):
    """Return a batch snapshot of a Bronze table for Silver materialization."""
    return spark.read.table(table_name)


__all__ = [
    "ANALYTICS_SCHEMA_DEFAULT",
    "MAX_CHUNK_COUNT",
    "MAX_EXTRACTED_TEXT_LENGTH",
    "MAX_PDF_PAGE_COUNT",
    "MAX_PDF_SIZE_BYTES",
    "MAX_PDF_TOKEN_COUNT",
    "MONEY_DECIMAL_PRECISION",
    "MONEY_DECIMAL_SCALE",
    "NON_PHI_TABLE_PROPERTIES",
    "PHI_TABLE_PROPERTIES",
    "POLICY_CHUNK_OVERLAP_TOKENS",
    "POLICY_CHUNK_SIZE_TOKENS",
    "QUARANTINE_AUDIT_COLUMNS",
    "QUARANTINE_SCHEMA_DEFAULT",
    "SENSITIVE_TABLE_PROPERTIES",
    "SILVER_AUDIT_COLUMNS",
    "SILVER_SCHEMA_DEFAULT",
    "create_required_schemas",
    "quarantine_table_name",
    "read_bronze_snapshot",
    "silver_table_name",
    "silver_table_properties",
]
