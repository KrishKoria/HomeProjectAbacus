from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st

from src.local_pipeline.ops_artifacts import (
    ANALYTICS_RUN_COLUMNS,
    ARTIFACT_INVENTORY_COLUMNS,
    DATASET_ROW_COUNT_COLUMNS,
    FAILURE_COLUMNS,
    INGEST_RUN_COLUMNS,
    QUALITY_METRIC_COLUMNS,
    WARNING_COLUMNS,
)
from src.local_pipeline.paths import (
    DEFAULT_DATA_ROOT,
    analytics_artifact_file,
    ops_artifact_file,
)


OPS_COLUMNS_BY_ARTIFACT: dict[str, tuple[str, ...]] = {
    "analytics_runs": ANALYTICS_RUN_COLUMNS,
    "artifact_inventory": ARTIFACT_INVENTORY_COLUMNS,
    "dataset_row_counts": DATASET_ROW_COUNT_COLUMNS,
    "ingest_runs": INGEST_RUN_COLUMNS,
    "latest_failures": FAILURE_COLUMNS,
    "latest_warnings": WARNING_COLUMNS,
    "quality_metrics": QUALITY_METRIC_COLUMNS,
}


@dataclass(frozen=True, slots=True)
class ArtifactMissingError(FileNotFoundError):
    artifact_name: str
    artifact_path: Path

    def __str__(self) -> str:
        return f"Expected artifact '{self.artifact_name}' at {self.artifact_path}"


def _normalize_data_root(data_root: Path | str | None) -> Path:
    if data_root is None:
        return DEFAULT_DATA_ROOT
    return Path(data_root)


def _read_required_parquet(artifact_name: str, artifact_path: Path) -> pd.DataFrame:
    if not artifact_path.exists():
        raise ArtifactMissingError(artifact_name=artifact_name, artifact_path=artifact_path)
    return pd.read_parquet(artifact_path)


def load_analytics_artifact(artifact_name: str, data_root: Path | str | None = None) -> pd.DataFrame:
    root = _normalize_data_root(data_root)
    return _read_required_parquet(artifact_name, analytics_artifact_file(artifact_name, root))


def load_optional_ops_artifact(artifact_name: str, data_root: Path | str | None = None) -> pd.DataFrame:
    root = _normalize_data_root(data_root)
    artifact_path = ops_artifact_file(artifact_name, root)
    if not artifact_path.exists():
        return pd.DataFrame(columns=OPS_COLUMNS_BY_ARTIFACT[artifact_name])
    return pd.read_parquet(artifact_path).reindex(columns=OPS_COLUMNS_BY_ARTIFACT[artifact_name])


@st.cache_data(show_spinner=False)
def cached_load_analytics_artifact(artifact_name: str, data_root: str = str(DEFAULT_DATA_ROOT)) -> pd.DataFrame:
    return load_analytics_artifact(artifact_name, data_root)


@st.cache_data(show_spinner=False)
def cached_load_optional_ops_artifact(artifact_name: str, data_root: str = str(DEFAULT_DATA_ROOT)) -> pd.DataFrame:
    return load_optional_ops_artifact(artifact_name, data_root)


def filter_frame_by_date(
    frame: pd.DataFrame,
    column: str,
    start_date,
    end_date,
) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return frame
    normalized = frame.copy()
    normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if start_date is not None:
        normalized = normalized[normalized[column] >= pd.Timestamp(start_date)]
    if end_date is not None:
        normalized = normalized[normalized[column] <= pd.Timestamp(end_date)]
    return normalized


def available_dimension_values(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    values = frame[column].dropna().astype(str).unique().tolist()
    return sorted(value for value in values if value)


def latest_record(frame: pd.DataFrame, sort_column: str = "timestamp") -> pd.Series | None:
    if frame.empty or sort_column not in frame.columns:
        return None
    ordered = frame.sort_values(sort_column, ascending=False, kind="stable")
    return ordered.iloc[0]


__all__ = [
    "ArtifactMissingError",
    "available_dimension_values",
    "cached_load_analytics_artifact",
    "cached_load_optional_ops_artifact",
    "filter_frame_by_date",
    "latest_record",
    "load_analytics_artifact",
    "load_optional_ops_artifact",
]
