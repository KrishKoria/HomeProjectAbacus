from __future__ import annotations

from pathlib import Path
from typing import Final, Iterable

import pandas as pd


INGEST_RUN_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "status",
    "dataset_count",
    "source_root",
    "started_at",
    "completed_at",
)
ANALYTICS_RUN_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "status",
    "artifact_count",
    "started_at",
    "completed_at",
)
DATASET_ROW_COUNT_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "layer",
    "dataset",
    "source_row_count",
    "row_count",
    "warning_count",
)
QUALITY_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "dataset",
    "metric_group",
    "metric_name",
    "metric_value",
)
WARNING_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "dataset",
    "warning_code",
    "warning_message",
    "affected_records",
)
FAILURE_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "dataset",
    "update_id",
    "platform_error_code",
    "diagnostic_id",
    "error_message",
)
ARTIFACT_INVENTORY_COLUMNS: Final[tuple[str, ...]] = (
    "timestamp",
    "pipeline_run_id",
    "artifact_type",
    "artifact_name",
    "artifact_path",
    "row_count",
    "status",
)


def frame_from_records(
    records: Iterable[dict[str, object]],
    columns: tuple[str, ...],
) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    if frame.empty:
        return pd.DataFrame(columns=columns)
    return frame.reindex(columns=columns)


def read_parquet_if_exists(path: Path, columns: tuple[str, ...]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_parquet(path)
    return frame.reindex(columns=columns)


def write_snapshot(frame: pd.DataFrame, destination: Path) -> pd.DataFrame:
    destination.parent.mkdir(parents=True, exist_ok=True)
    normalized = frame.copy()
    normalized.to_parquet(destination, index=False)
    return normalized


def append_records(
    destination: Path,
    records: Iterable[dict[str, object]],
    columns: tuple[str, ...],
    sort_by: str = "timestamp",
    keep_last: int | None = 250,
) -> pd.DataFrame:
    existing = read_parquet_if_exists(destination, columns)
    incoming = frame_from_records(records, columns)
    if existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True)
    if not combined.empty and sort_by in combined.columns:
        combined = combined.sort_values(sort_by, kind="stable")
    if keep_last is not None and len(combined) > keep_last:
        combined = combined.tail(keep_last)
    return write_snapshot(combined.reindex(columns=columns), destination)


def artifact_inventory_record(
    *,
    timestamp: object,
    pipeline_run_id: str,
    artifact_type: str,
    artifact_name: str,
    artifact_path: Path,
    row_count: int,
    status: str = "SUCCESS",
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "pipeline_run_id": pipeline_run_id,
        "artifact_type": artifact_type,
        "artifact_name": artifact_name,
        "artifact_path": str(artifact_path),
        "row_count": row_count,
        "status": status,
    }


__all__ = [
    "ANALYTICS_RUN_COLUMNS",
    "ARTIFACT_INVENTORY_COLUMNS",
    "DATASET_ROW_COUNT_COLUMNS",
    "FAILURE_COLUMNS",
    "INGEST_RUN_COLUMNS",
    "QUALITY_METRIC_COLUMNS",
    "WARNING_COLUMNS",
    "append_records",
    "artifact_inventory_record",
    "frame_from_records",
    "read_parquet_if_exists",
    "write_snapshot",
]
