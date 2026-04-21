from __future__ import annotations

from typing import Final, Iterable

from src.common.bronze_pipeline_config import COMMON_DELTA_TABLE_PROPERTIES, table_properties_for_sensitivity


SILVER_SCHEMA_DEFAULT: Final[str] = "silver"
QUARANTINE_SCHEMA_DEFAULT: Final[str] = "quarantine"
ANALYTICS_SCHEMA_DEFAULT: Final[str] = "analytics"

SILVER_AUDIT_COLUMNS: Final[tuple[str, str]] = (
    "_silver_processed_at",
    "_data_quality_flags",
)
QUARANTINE_AUDIT_COLUMNS: Final[tuple[str, str, str, str]] = (
    "_quarantined_at",
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
    properties = dict(COMMON_DELTA_TABLE_PROPERTIES)
    properties.update(table_properties_for_sensitivity(sensitivity, phi_columns))
    properties["claimops.layer"] = "silver"
    return properties


def read_bronze_cdf(spark, table_name: str):
    """Return a streaming DataFrame that reads Bronze changes through Delta CDF."""
    return spark.readStream.option("readChangeFeed", "true").table(table_name)


__all__ = [
    "ANALYTICS_SCHEMA_DEFAULT",
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
    "read_bronze_cdf",
    "silver_table_name",
    "silver_table_properties",
]
