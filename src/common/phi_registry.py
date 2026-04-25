"""
HIPAA PHI Column Registry - Abacus Insights Bronze Layer.

Machine-readable inventory of Protected Health Information (PHI) columns
across Bronze Delta tables.

Compliance basis:
- 45 CFR 164.308(a)(1)(ii)(A): Risk Analysis
- 45 CFR 164.312(a)(2)(iv): Encryption and Decryption

The default registry preserves the current `healthcare.bronze.*` contract, while
the builder functions allow the same source manifest to be projected into a
different Databricks catalog/schema for staging or future S3-backed workspaces.
"""

from __future__ import annotations

from typing import Final

from src.common.bronze_pipeline_config import (
    BRONZE_SCHEMA_DEFAULT,
    CATALOG_DEFAULT,
    bronze_table_name,
)
from src.common.bronze_sources import BRONZE_SOURCES


SENSITIVE_COLUMNS_BY_DATASET: Final[dict[str, frozenset[str]]] = {
    "claims": frozenset(
        {
            "claim_id",
            "provider_id",
            "procedure_code",
        }
    ),
    "diagnosis": frozenset(),
    "providers": frozenset(),
    "cost": frozenset(),
}


def build_phi_columns_registry(
    *,
    catalog: str = CATALOG_DEFAULT,
    schema: str = BRONZE_SCHEMA_DEFAULT,
) -> dict[str, frozenset[str]]:
    """Build a PHI column registry for a Bronze namespace."""
    return {
        bronze_table_name(dataset, catalog=catalog, schema=schema): source.phi_columns
        for dataset, source in BRONZE_SOURCES.items()
    }


def build_sensitive_columns_registry(
    *,
    catalog: str = CATALOG_DEFAULT,
    schema: str = BRONZE_SCHEMA_DEFAULT,
) -> dict[str, frozenset[str]]:
    """Build a PHI-adjacent column registry for a Bronze namespace."""
    return {
        bronze_table_name(dataset, catalog=catalog, schema=schema): SENSITIVE_COLUMNS_BY_DATASET.get(
            dataset,
            frozenset(),
        )
        for dataset in BRONZE_SOURCES
    }


PHI_COLUMNS: Final[dict[str, frozenset[str]]] = build_phi_columns_registry()
SENSITIVE_COLUMNS: Final[dict[str, frozenset[str]]] = build_sensitive_columns_registry()


def get_phi_columns(table_name: str) -> frozenset[str]:
    """Return PHI columns for a fully-qualified Bronze table name."""
    return PHI_COLUMNS.get(table_name, frozenset())


def get_sensitive_columns(table_name: str) -> frozenset[str]:
    """Return PHI-adjacent columns for a fully-qualified Bronze table name."""
    return SENSITIVE_COLUMNS.get(table_name, frozenset())


def is_phi_column(table_name: str, column_name: str) -> bool:
    """Return True if the given column is PHI in the specified table."""
    return column_name in PHI_COLUMNS.get(table_name, frozenset())


__all__ = [
    "PHI_COLUMNS",
    "SENSITIVE_COLUMNS",
    "SENSITIVE_COLUMNS_BY_DATASET",
    "build_phi_columns_registry",
    "build_sensitive_columns_registry",
    "get_phi_columns",
    "get_sensitive_columns",
    "is_phi_column",
]
