from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.analytics.observability_assets import latest_failure_diagnostic_id
from src.analytics.week2_analytics import HIGH_COST_THRESHOLD_RATIO
from src.local_pipeline.ops_artifacts import (
    ANALYTICS_RUN_COLUMNS,
    ARTIFACT_INVENTORY_COLUMNS,
    DATASET_ROW_COUNT_COLUMNS,
    FAILURE_COLUMNS,
    QUALITY_METRIC_COLUMNS,
    WARNING_COLUMNS,
    append_records,
    artifact_inventory_record,
)
from src.local_pipeline.paths import (
    ANALYTICS_ARTIFACT_KEYS,
    DEFAULT_DATA_ROOT,
    analytics_artifact_file,
    bronze_artifact_file,
    ensure_local_directories,
    new_pipeline_run_id,
    ops_artifact_file,
    utc_now_timestamp,
)


DISPLAY_UNKNOWN = "Unknown"


class BronzeArtifactMissingError(FileNotFoundError):
    def __init__(self, dataset: str, artifact_path: Path) -> None:
        super().__init__(f"Missing Bronze artifact for dataset '{dataset}': {artifact_path}")
        self.dataset = dataset
        self.artifact_path = artifact_path


@dataclass(frozen=True, slots=True)
class AnalyticsArtifactResult:
    artifact_name: str
    artifact_path: Path
    row_count: int


def _load_bronze_dataset(dataset: str, data_root: Path) -> pd.DataFrame:
    artifact_path = bronze_artifact_file(dataset, data_root)
    if not artifact_path.exists():
        raise BronzeArtifactMissingError(dataset, artifact_path)
    return pd.read_parquet(artifact_path)


def _display_dimension(series: pd.Series) -> pd.Series:
    normalized = series.astype("string")
    return normalized.fillna(DISPLAY_UNKNOWN)


def _select_claim_fields(claims: pd.DataFrame) -> pd.DataFrame:
    return claims[["claim_id", "provider_id", "diagnosis_code", "procedure_code", "billed_amount", "date"]].rename(
        columns={"date": "claim_date"}
    )


def _select_provider_fields(providers: pd.DataFrame) -> pd.DataFrame:
    return providers[["provider_id", "doctor_name", "specialty", "location"]].rename(columns={"location": "region"})


def _select_diagnosis_fields(diagnosis: pd.DataFrame) -> pd.DataFrame:
    return diagnosis[["diagnosis_code", "category", "severity"]]


def _select_cost_fields(cost: pd.DataFrame) -> pd.DataFrame:
    return cost[["procedure_code", "region", "average_cost", "expected_cost"]]


def _build_claims_cost_enriched(
    claims: pd.DataFrame,
    providers: pd.DataFrame,
    cost: pd.DataFrame,
) -> pd.DataFrame:
    enriched = (
        _select_claim_fields(claims)
        .merge(_select_provider_fields(providers), on="provider_id", how="left")
        .merge(_select_cost_fields(cost), on=["procedure_code", "region"], how="left")
    )
    enriched["specialty"] = _display_dimension(enriched["specialty"])
    enriched["region"] = _display_dimension(enriched["region"])
    enriched["doctor_name"] = _display_dimension(enriched["doctor_name"])
    enriched["amount_to_benchmark_ratio"] = enriched["billed_amount"] / enriched["expected_cost"]
    return enriched


def build_claims_provider_joined(claims: pd.DataFrame, providers: pd.DataFrame) -> pd.DataFrame:
    joined = _select_claim_fields(claims).merge(_select_provider_fields(providers), on="provider_id", how="left")
    joined["specialty"] = _display_dimension(joined["specialty"])
    joined["region"] = _display_dimension(joined["region"])
    joined["doctor_name"] = _display_dimension(joined["doctor_name"])
    return joined


def build_claims_diagnosis_joined(claims: pd.DataFrame, diagnosis: pd.DataFrame) -> pd.DataFrame:
    joined = _select_claim_fields(claims).merge(_select_diagnosis_fields(diagnosis), on="diagnosis_code", how="left")
    joined["category"] = _display_dimension(joined["category"])
    joined["severity"] = _display_dimension(joined["severity"])
    return joined


def build_claims_by_specialty_summary(claims_provider_joined: pd.DataFrame) -> pd.DataFrame:
    summary = (
        claims_provider_joined.groupby("specialty", dropna=False)
        .agg(
            claim_count=("claim_id", "size"),
            provider_count=("provider_id", "nunique"),
            avg_billed_amount=("billed_amount", "mean"),
            total_billed_amount=("billed_amount", "sum"),
        )
        .reset_index()
        .sort_values(["claim_count", "specialty"], ascending=[False, True], kind="stable")
    )
    return summary


def build_claims_by_region_summary(claims_provider_joined: pd.DataFrame) -> pd.DataFrame:
    summary = (
        claims_provider_joined.groupby("region", dropna=False)
        .agg(
            claim_count=("claim_id", "size"),
            provider_count=("provider_id", "nunique"),
            avg_billed_amount=("billed_amount", "mean"),
            total_billed_amount=("billed_amount", "sum"),
        )
        .reset_index()
        .sort_values(["claim_count", "region"], ascending=[False, True], kind="stable")
    )
    return summary


def build_high_cost_claims_summary(
    claims: pd.DataFrame,
    providers: pd.DataFrame,
    cost: pd.DataFrame,
    *,
    threshold_ratio: float = HIGH_COST_THRESHOLD_RATIO,
) -> pd.DataFrame:
    enriched = _build_claims_cost_enriched(claims, providers, cost)
    summary = enriched[enriched["expected_cost"].notna()].copy()
    summary = summary[summary["amount_to_benchmark_ratio"] >= threshold_ratio]
    summary = summary[
        [
            "claim_id",
            "provider_id",
            "doctor_name",
            "specialty",
            "region",
            "procedure_code",
            "claim_date",
            "billed_amount",
            "expected_cost",
            "amount_to_benchmark_ratio",
        ]
    ].sort_values(["amount_to_benchmark_ratio", "claim_id"], ascending=[False, True], kind="stable")
    return summary


def build_week2_dashboard_summary(
    claims: pd.DataFrame,
    providers: pd.DataFrame,
    cost: pd.DataFrame,
) -> pd.DataFrame:
    enriched = _build_claims_cost_enriched(claims, providers, cost)
    enriched["is_high_cost_claim"] = (
        enriched["amount_to_benchmark_ratio"].fillna(0).ge(HIGH_COST_THRESHOLD_RATIO).astype("int64")
    )
    summary = (
        enriched[enriched["claim_date"].notna()]
        .groupby("claim_date", dropna=False)
        .agg(
            total_claims=("claim_id", "size"),
            active_provider_count=("provider_id", "nunique"),
            total_billed_amount=("billed_amount", "sum"),
            avg_billed_amount=("billed_amount", "mean"),
            high_cost_claim_count=("is_high_cost_claim", "sum"),
        )
        .reset_index()
        .sort_values("claim_date", kind="stable")
    )
    return summary


def _write_analytics_artifact(artifact_name: str, frame: pd.DataFrame, data_root: Path) -> AnalyticsArtifactResult:
    artifact_path = analytics_artifact_file(artifact_name, data_root)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(artifact_path, index=False)
    return AnalyticsArtifactResult(artifact_name=artifact_name, artifact_path=artifact_path, row_count=len(frame))


def run_local_analytics_build(*, data_root: Path = DEFAULT_DATA_ROOT) -> dict[str, object]:
    ensure_local_directories(data_root)

    pipeline_run_id = new_pipeline_run_id("analytics")
    started_at = utc_now_timestamp()
    timestamp = pd.Timestamp(started_at)
    artifact_records: list[dict[str, object]] = []
    row_count_records: list[dict[str, object]] = []
    quality_metric_records: list[dict[str, object]] = []
    warning_records: list[dict[str, object]] = []
    failure_records: list[dict[str, object]] = []
    written_artifacts: list[AnalyticsArtifactResult] = []
    status = "SUCCESS"

    try:
        claims = _load_bronze_dataset("claims", data_root)
        providers = _load_bronze_dataset("providers", data_root)
        diagnosis = _load_bronze_dataset("diagnosis", data_root)
        cost = _load_bronze_dataset("cost", data_root)

        claims_provider_joined = build_claims_provider_joined(claims, providers)
        claims_diagnosis_joined = build_claims_diagnosis_joined(claims, diagnosis)
        claims_by_specialty_summary = build_claims_by_specialty_summary(claims_provider_joined)
        claims_by_region_summary = build_claims_by_region_summary(claims_provider_joined)
        claims_cost_enriched = _build_claims_cost_enriched(claims, providers, cost)
        high_cost_claims_summary = build_high_cost_claims_summary(claims, providers, cost)
        week2_dashboard_summary = build_week2_dashboard_summary(claims, providers, cost)

        artifacts = {
            "claims_provider_joined": claims_provider_joined,
            "claims_diagnosis_joined": claims_diagnosis_joined,
            "claims_by_specialty_summary": claims_by_specialty_summary,
            "claims_by_region_summary": claims_by_region_summary,
            "high_cost_claims_summary": high_cost_claims_summary,
            "week2_dashboard_summary": week2_dashboard_summary,
        }

        benchmark_join_missing = int(
            (
                claims_cost_enriched["procedure_code"].notna()
                & claims_cost_enriched["region"].ne(DISPLAY_UNKNOWN)
                & claims_cost_enriched["expected_cost"].isna()
            ).sum()
        )
        quality_metric_records.extend(
            [
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "dataset": "claims_provider_joined",
                    "metric_group": "quality",
                    "metric_name": "missing_region",
                    "metric_value": int(claims_provider_joined["region"].eq(DISPLAY_UNKNOWN).sum()),
                },
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "dataset": "high_cost_claims_summary",
                    "metric_group": "quality",
                    "metric_name": "benchmark_join_missing",
                    "metric_value": benchmark_join_missing,
                },
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "dataset": "week2_dashboard_summary",
                    "metric_group": "quality",
                    "metric_name": "claims_missing_claim_date",
                    "metric_value": int(claims["date"].isna().sum()),
                },
            ]
        )
        if benchmark_join_missing:
            warning_records.append(
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "dataset": "high_cost_claims_summary",
                    "warning_code": "benchmark_join_missing",
                    "warning_message": "Claims without a usable benchmark were excluded from ratio-based anomaly analysis.",
                    "affected_records": benchmark_join_missing,
                }
            )

        for artifact_name, frame in artifacts.items():
            sanitized = frame.drop(columns=["patient_id"], errors="ignore")
            written = _write_analytics_artifact(artifact_name, sanitized, data_root)
            written_artifacts.append(written)
            row_count_records.append(
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "layer": "analytics",
                    "dataset": artifact_name,
                    "source_row_count": None,
                    "row_count": written.row_count,
                    "warning_count": 0,
                }
            )
            artifact_records.append(
                artifact_inventory_record(
                    timestamp=timestamp,
                    pipeline_run_id=pipeline_run_id,
                    artifact_type="analytics",
                    artifact_name=artifact_name,
                    artifact_path=written.artifact_path,
                    row_count=written.row_count,
                )
            )
    except Exception as exc:
        status = "FAILED"
        dataset = getattr(exc, "dataset", "analytics_build")
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
            ops_artifact_file("analytics_runs", data_root),
            [
                {
                    "timestamp": timestamp,
                    "pipeline_run_id": pipeline_run_id,
                    "status": status,
                    "artifact_count": len(written_artifacts),
                    "started_at": started_at,
                    "completed_at": completed_at,
                }
            ],
            ANALYTICS_RUN_COLUMNS,
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
        "artifacts_written": len(written_artifacts),
        "artifact_names": [artifact.artifact_name for artifact in written_artifacts],
    }


__all__ = [
    "ANALYTICS_ARTIFACT_KEYS",
    "AnalyticsArtifactResult",
    "BronzeArtifactMissingError",
    "build_claims_by_region_summary",
    "build_claims_by_specialty_summary",
    "build_claims_diagnosis_joined",
    "build_claims_provider_joined",
    "build_high_cost_claims_summary",
    "build_week2_dashboard_summary",
    "run_local_analytics_build",
]
