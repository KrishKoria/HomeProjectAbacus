from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from src.common.bronze_sources import BRONZE_SOURCES


PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_ROOT: Final[Path] = PROJECT_ROOT / "datasets"
DEFAULT_DATA_ROOT: Final[Path] = PROJECT_ROOT / "data"

BRONZE_DATASET_KEYS: Final[tuple[str, ...]] = tuple(BRONZE_SOURCES)
ANALYTICS_ARTIFACT_KEYS: Final[tuple[str, ...]] = (
    "claims_provider_joined",
    "claims_diagnosis_joined",
    "claims_by_specialty_summary",
    "claims_by_region_summary",
    "high_cost_claims_summary",
    "week2_dashboard_summary",
)
OPS_ARTIFACT_KEYS: Final[tuple[str, ...]] = (
    "artifact_inventory",
    "analytics_runs",
    "dataset_row_counts",
    "ingest_runs",
    "latest_failures",
    "latest_warnings",
    "quality_metrics",
)


def bronze_root(data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return data_root / "bronze"


def analytics_root(data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return data_root / "analytics"


def ops_root(data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return data_root / "ops"


def bronze_artifact_file(dataset_key: str, data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return bronze_root(data_root) / dataset_key / f"{dataset_key}.parquet"


def analytics_artifact_file(artifact_key: str, data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return analytics_root(data_root) / artifact_key / f"{artifact_key}.parquet"


def ops_artifact_file(artifact_key: str, data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    return ops_root(data_root) / f"{artifact_key}.parquet"


def ensure_local_directories(data_root: Path = DEFAULT_DATA_ROOT) -> None:
    bronze_root(data_root).mkdir(parents=True, exist_ok=True)
    analytics_root(data_root).mkdir(parents=True, exist_ok=True)
    ops_root(data_root).mkdir(parents=True, exist_ok=True)


def new_pipeline_run_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc)
    return f"{prefix}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}"


def utc_now_timestamp() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "ANALYTICS_ARTIFACT_KEYS",
    "BRONZE_DATASET_KEYS",
    "DEFAULT_DATA_ROOT",
    "DEFAULT_SOURCE_ROOT",
    "OPS_ARTIFACT_KEYS",
    "PROJECT_ROOT",
    "analytics_artifact_file",
    "analytics_root",
    "bronze_artifact_file",
    "bronze_root",
    "ensure_local_directories",
    "new_pipeline_run_id",
    "ops_artifact_file",
    "ops_root",
    "utc_now_timestamp",
]
