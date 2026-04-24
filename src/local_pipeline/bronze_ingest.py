from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from src.analytics.observability_assets import latest_failure_diagnostic_id
from src.common.bronze_sources import BRONZE_SOURCES, BronzeSource
from src.common.silver_cleaning import (
    normalize_code_value,
    normalize_nullable_string,
    normalize_severity_value,
    normalize_title_value,
    parse_date_value,
    parse_decimal_value,
)
from src.local_pipeline.ops_artifacts import (
    ARTIFACT_INVENTORY_COLUMNS,
    DATASET_ROW_COUNT_COLUMNS,
    FAILURE_COLUMNS,
    INGEST_RUN_COLUMNS,
    QUALITY_METRIC_COLUMNS,
    WARNING_COLUMNS,
    append_records,
    artifact_inventory_record,
)
from src.local_pipeline.paths import (
    DEFAULT_DATA_ROOT,
    DEFAULT_SOURCE_ROOT,
    bronze_artifact_file,
    ensure_local_directories,
    new_pipeline_run_id,
    ops_artifact_file,
    utc_now_timestamp,
)


DATASET_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "claims": ("claim_id", "patient_id", "provider_id", "diagnosis_code", "procedure_code", "billed_amount", "date"),
    "providers": ("provider_id", "doctor_name", "specialty", "location"),
    "diagnosis": ("diagnosis_code", "category", "severity"),
    "cost": ("procedure_code", "average_cost", "expected_cost", "region"),
}


class DatasetSourceMissingError(FileNotFoundError):
    def __init__(self, dataset: str, source_path: Path) -> None:
        super().__init__(f"Missing source file for dataset '{dataset}': {source_path}")
        self.dataset = dataset
        self.source_path = source_path


class DatasetSchemaError(ValueError):
    def __init__(self, dataset: str, missing_columns: list[str]) -> None:
        super().__init__(f"Dataset '{dataset}' is missing required columns: {', '.join(missing_columns)}")
        self.dataset = dataset
        self.missing_columns = tuple(missing_columns)


@dataclass(frozen=True, slots=True)
class DatasetIngestResult:
    dataset: str
    artifact_path: Path
    source_row_count: int
    landed_row_count: int
    warning_records: list[dict[str, object]]
    quality_metric_records: list[dict[str, object]]


def _decimal_to_float(value) -> float | None:
    return float(value) if value is not None else None


def _coerce_text_series(raw: pd.Series, normalizer: Callable[[object], object]) -> pd.Series:
    return raw.map(normalizer).astype("string")


def _coerce_decimal_series(raw: pd.Series) -> pd.Series:
    values = raw.map(parse_decimal_value).map(_decimal_to_float)
    return pd.Series(values, dtype="float64")


def _coerce_date_series(raw: pd.Series) -> pd.Series:
    parsed = raw.map(parse_date_value)
    return pd.to_datetime(parsed, errors="coerce")


def _invalid_parse_count(raw: pd.Series, parsed: pd.Series) -> int:
    nonblank = raw.map(normalize_nullable_string).notna()
    return int((nonblank & parsed.isna()).sum())


def _build_metric(
    *,
    timestamp: object,
    pipeline_run_id: str,
    dataset: str,
    metric_group: str,
    metric_name: str,
    metric_value: int,
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "pipeline_run_id": pipeline_run_id,
        "dataset": dataset,
        "metric_group": metric_group,
        "metric_name": metric_name,
        "metric_value": metric_value,
    }


def _build_warning(
    *,
    timestamp: object,
    pipeline_run_id: str,
    dataset: str,
    warning_code: str,
    warning_message: str,
    affected_records: int,
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "pipeline_run_id": pipeline_run_id,
        "dataset": dataset,
        "warning_code": warning_code,
        "warning_message": warning_message,
        "affected_records": affected_records,
    }


def _read_raw_csv(source_path: Path, dataset: str) -> pd.DataFrame:
    if not source_path.exists():
        raise DatasetSourceMissingError(dataset, source_path)
    frame = pd.read_csv(source_path, dtype=str, keep_default_na=False)
    missing_columns = [column for column in DATASET_REQUIRED_COLUMNS[dataset] if column not in frame.columns]
    if missing_columns:
        raise DatasetSchemaError(dataset, missing_columns)
    return frame


def _coerce_claims(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "claim_id": _coerce_text_series(raw["claim_id"], normalize_code_value),
            "patient_id": _coerce_text_series(raw["patient_id"], normalize_code_value),
            "provider_id": _coerce_text_series(raw["provider_id"], normalize_code_value),
            "diagnosis_code": _coerce_text_series(raw["diagnosis_code"], normalize_code_value),
            "procedure_code": _coerce_text_series(raw["procedure_code"], normalize_code_value),
            "billed_amount": _coerce_decimal_series(raw["billed_amount"]),
            "date": _coerce_date_series(raw["date"]),
        }
    )


def _coerce_providers(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "provider_id": _coerce_text_series(raw["provider_id"], normalize_code_value),
            "doctor_name": _coerce_text_series(raw["doctor_name"], normalize_title_value),
            "specialty": _coerce_text_series(raw["specialty"], normalize_title_value),
            "location": _coerce_text_series(raw["location"], normalize_title_value),
        }
    )


def _coerce_diagnosis(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "diagnosis_code": _coerce_text_series(raw["diagnosis_code"], normalize_code_value),
            "category": _coerce_text_series(raw["category"], normalize_title_value),
            "severity": _coerce_text_series(raw["severity"], normalize_severity_value),
        }
    )


def _coerce_cost(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "procedure_code": _coerce_text_series(raw["procedure_code"], normalize_code_value),
            "average_cost": _coerce_decimal_series(raw["average_cost"]),
            "expected_cost": _coerce_decimal_series(raw["expected_cost"]),
            "region": _coerce_text_series(raw["region"], normalize_title_value),
        }
    )


COERCION_BY_DATASET: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "claims": _coerce_claims,
    "providers": _coerce_providers,
    "diagnosis": _coerce_diagnosis,
    "cost": _coerce_cost,
}


def _collect_quality_signals(
    dataset: str,
    source: BronzeSource,
    raw: pd.DataFrame,
    coerced: pd.DataFrame,
    *,
    timestamp: object,
    pipeline_run_id: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    metrics: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []

    if len(raw) != source.expected_row_count:
        warnings.append(
            _build_warning(
                timestamp=timestamp,
                pipeline_run_id=pipeline_run_id,
                dataset=dataset,
                warning_code="expected_row_count_mismatch",
                warning_message=f"Expected {source.expected_row_count} rows but found {len(raw)} rows in source CSV.",
                affected_records=abs(len(raw) - source.expected_row_count),
            )
        )

    for column in DATASET_REQUIRED_COLUMNS[dataset]:
        missing_count = int(coerced[column].isna().sum())
        metrics.append(
            _build_metric(
                timestamp=timestamp,
                pipeline_run_id=pipeline_run_id,
                dataset=dataset,
                metric_group="missing",
                metric_name=f"missing_{column}",
                metric_value=missing_count,
            )
        )

    if dataset == "claims":
        invalid_billed = _invalid_parse_count(raw["billed_amount"], coerced["billed_amount"])
        invalid_date = _invalid_parse_count(raw["date"], coerced["date"])
        invalid_map = {
            "invalid_billed_amount": invalid_billed,
            "invalid_date": invalid_date,
        }
    elif dataset == "cost":
        invalid_average = _invalid_parse_count(raw["average_cost"], coerced["average_cost"])
        invalid_expected = _invalid_parse_count(raw["expected_cost"], coerced["expected_cost"])
        invalid_map = {
            "invalid_average_cost": invalid_average,
            "invalid_expected_cost": invalid_expected,
        }
    else:
        invalid_map = {}

    for metric_name, metric_value in invalid_map.items():
        metrics.append(
            _build_metric(
                timestamp=timestamp,
                pipeline_run_id=pipeline_run_id,
                dataset=dataset,
                metric_group="invalid",
                metric_name=metric_name,
                metric_value=metric_value,
            )
        )
        if metric_value:
            warnings.append(
                _build_warning(
                    timestamp=timestamp,
                    pipeline_run_id=pipeline_run_id,
                    dataset=dataset,
                    warning_code=metric_name,
                    warning_message=f"{metric_name.replace('_', ' ')} values were coerced to null.",
                    affected_records=metric_value,
                )
            )

    return metrics, warnings


def ingest_bronze_dataset(
    dataset: str,
    source: BronzeSource,
    *,
    source_root: Path,
    data_root: Path,
    pipeline_run_id: str,
    timestamp: object,
) -> DatasetIngestResult:
    source_path = source_root / source.local_filename
    raw = _read_raw_csv(source_path, dataset)
    coerced = COERCION_BY_DATASET[dataset](raw)
    coerced["_ingested_at"] = timestamp
    coerced["_source_file"] = str(source_path)
    coerced["_pipeline_run_id"] = pipeline_run_id

    quality_metrics, warning_records = _collect_quality_signals(
        dataset,
        source,
        raw,
        coerced,
        timestamp=timestamp,
        pipeline_run_id=pipeline_run_id,
    )

    artifact_path = bronze_artifact_file(dataset, data_root)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    coerced.to_parquet(artifact_path, index=False)

    return DatasetIngestResult(
        dataset=dataset,
        artifact_path=artifact_path,
        source_row_count=len(raw),
        landed_row_count=len(coerced),
        warning_records=warning_records,
        quality_metric_records=quality_metrics,
    )


def run_local_bronze_ingest(
    *,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    data_root: Path = DEFAULT_DATA_ROOT,
) -> dict[str, object]:
    ensure_local_directories(data_root)

    pipeline_run_id = new_pipeline_run_id("bronze")
    started_at = utc_now_timestamp()
    timestamp = pd.Timestamp(started_at)

    row_count_records: list[dict[str, object]] = []
    quality_metric_records: list[dict[str, object]] = []
    warning_records: list[dict[str, object]] = []
    failure_records: list[dict[str, object]] = []
    artifact_records: list[dict[str, object]] = []
    results: list[DatasetIngestResult] = []
    status = "SUCCESS"

    try:
        for dataset, source in BRONZE_SOURCES.items():
            result = ingest_bronze_dataset(
                dataset,
                source,
                source_root=source_root,
                data_root=data_root,
                pipeline_run_id=pipeline_run_id,
                timestamp=timestamp,
            )
            results.append(result)
            quality_metric_records.extend(result.quality_metric_records)
            warning_records.extend(result.warning_records)
            row_count_records.append(
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "layer": "bronze",
                    "dataset": dataset,
                    "source_row_count": result.source_row_count,
                    "row_count": result.landed_row_count,
                    "warning_count": len(result.warning_records),
                }
            )
            artifact_records.append(
                artifact_inventory_record(
                    timestamp=timestamp,
                    pipeline_run_id=pipeline_run_id,
                    artifact_type="bronze",
                    artifact_name=dataset,
                    artifact_path=result.artifact_path,
                    row_count=result.landed_row_count,
                )
            )
    except Exception as exc:
        status = "FAILED"
        dataset = getattr(exc, "dataset", "bronze_ingest")
        failure_records.append(
            {
                "timestamp": timestamp,
                "dataset": dataset,
                "update_id": pipeline_run_id,
                "platform_error_code": "LOCAL-RUNTIME",
                "diagnostic_id": latest_failure_diagnostic_id(dataset),
                "error_message": str(exc),
            }
        )
        raise
    finally:
        completed_at = utc_now_timestamp()
        append_records(
            ops_artifact_file("ingest_runs", data_root),
            [
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "status": status,
                    "dataset_count": len(results),
                    "source_root": str(source_root),
                    "started_at": started_at,
                    "completed_at": completed_at,
                }
            ],
            INGEST_RUN_COLUMNS,
        )
        append_records(
            ops_artifact_file("dataset_row_counts", data_root),
            row_count_records,
            DATASET_ROW_COUNT_COLUMNS,
            keep_last=1000,
        )
        append_records(
            ops_artifact_file("quality_metrics", data_root),
            quality_metric_records,
            QUALITY_METRIC_COLUMNS,
            keep_last=2000,
        )
        append_records(
            ops_artifact_file("latest_warnings", data_root),
            warning_records,
            WARNING_COLUMNS,
            keep_last=500,
        )
        append_records(
            ops_artifact_file("latest_failures", data_root),
            failure_records,
            FAILURE_COLUMNS,
            keep_last=250,
        )
        append_records(
            ops_artifact_file("artifact_inventory", data_root),
            artifact_records,
            ARTIFACT_INVENTORY_COLUMNS,
            keep_last=1000,
        )

    return {
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "datasets_processed": len(results),
        "warnings_logged": len(warning_records),
    }


__all__ = [
    "DATASET_REQUIRED_COLUMNS",
    "DatasetIngestResult",
    "DatasetSchemaError",
    "DatasetSourceMissingError",
    "ingest_bronze_dataset",
    "run_local_bronze_ingest",
]
