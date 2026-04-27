from __future__ import annotations

from typing import Final, Iterable

from src.common.bronze_pipeline_config import COMMON_DELTA_TABLE_PROPERTIES, table_properties_for_sensitivity


GOLD_SCHEMA_DEFAULT: Final[str] = "gold"

GOLD_AUDIT_COLUMNS: Final[tuple[str, ...]] = (
    "_gold_processed_at",
)

PHI_COLUMNS_GOLD: Final[tuple[str, ...]] = (
    "billed_amount",
    "date",
    "diagnosis_code",
    "is_denied",
    "patient_id",
)


def gold_table_name(catalog: str, table_name: str, schema: str = GOLD_SCHEMA_DEFAULT) -> str:
    """Return a fully-qualified Gold table name."""
    return f"{catalog}.{schema}.{table_name}"


def gold_table_properties(
    sensitivity: str,
    phi_columns: Iterable[str] = (),
) -> dict[str, str]:
    """Return Gold Delta table properties with layer metadata and HIPAA flags.

    Gold tables are derived from Silver and carry SENSITIVE classification because
    they join PHI-adjacent columns (patient_id, billed_amount, diagnosis_code)
    with engineered risk features.
    """
    properties = dict(COMMON_DELTA_TABLE_PROPERTIES)
    properties.update(table_properties_for_sensitivity(sensitivity, phi_columns))
    properties["claimops.layer"] = "gold"
    return properties


def read_silver_snapshot(spark, table_name: str):
    """Return a batch snapshot of a Silver table for Gold materialization.

    Gold pipelines read from Silver tables using batch reads (not streaming)
    because Gold materializations operate on complete snapshots of the trusted
    Silver layer, not incremental change streams.
    """
    return spark.read.table(table_name)


__all__ = [
    "GOLD_AUDIT_COLUMNS",
    "GOLD_SCHEMA_DEFAULT",
    "PHI_COLUMNS_GOLD",
    "gold_table_name",
    "gold_table_properties",
    "read_silver_snapshot",
]