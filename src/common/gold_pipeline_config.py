from __future__ import annotations

from typing import Final, Iterable

from src.common.bronze_pipeline_config import table_properties_for_sensitivity


GOLD_SCHEMA_DEFAULT: Final[str] = "gold"

GOLD_AUDIT_COLUMNS: Final[tuple[str, ...]] = ()

PHI_COLUMNS_GOLD: Final[tuple[str, ...]] = (
    "billed_amount",
    "date",
    "diagnosis_code",
    "is_denied",
    "patient_id",
)

HIGH_COST_RATIO_THRESHOLD: Final[float] = 1.5
HIGH_SEVERITY_EXPECTED_COST_FLOOR: Final[float] = 5000.0
PROVIDER_LOOKBACK_WINDOW_DAYS: Final[int] = 30
PROVIDER_LOOKBACK_WINDOW_60D: Final[int] = 60
PROVIDER_LOOKBACK_WINDOW_90D: Final[int] = 90
MIN_PROVIDER_RISK_COUNT: Final[int] = 5

# Dx-Px mapping defaults — used when a (diagnosis, procedure) pair is missing
# from the mapping table or when either code is NULL on the claim.
DX_PX_COMPATIBLE_DEFAULT: Final[int] = 0
DX_PX_PAIR_RISK_PRIOR_DEFAULT: Final[float] = 0.15


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
    properties = table_properties_for_sensitivity(sensitivity, phi_columns)
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
    "DX_PX_COMPATIBLE_DEFAULT",
    "DX_PX_PAIR_RISK_PRIOR_DEFAULT",
    "GOLD_AUDIT_COLUMNS",
    "GOLD_SCHEMA_DEFAULT",
    "HIGH_COST_RATIO_THRESHOLD",
    "HIGH_SEVERITY_EXPECTED_COST_FLOOR",
    "MIN_PROVIDER_RISK_COUNT",
    "PHI_COLUMNS_GOLD",
    "PROVIDER_LOOKBACK_WINDOW_60D",
    "PROVIDER_LOOKBACK_WINDOW_90D",
    "PROVIDER_LOOKBACK_WINDOW_DAYS",
    "gold_table_name",
    "gold_table_properties",
    "read_silver_snapshot",
]
