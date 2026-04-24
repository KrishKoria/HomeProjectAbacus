import pandas as pd
from pathlib import Path

from src.local_pipeline.analytics_builders import (
    ANALYTICS_ARTIFACT_KEYS,
    build_high_cost_claims_summary,
    run_local_analytics_build,
)
from src.local_pipeline.bronze_ingest import run_local_bronze_ingest
from src.local_pipeline.paths import analytics_artifact_file, bronze_artifact_file, ops_artifact_file


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_run_local_bronze_ingest_writes_bronze_artifacts(tmp_path) -> None:
    data_root = tmp_path / "data"
    summary = run_local_bronze_ingest(source_root=PROJECT_ROOT / "datasets", data_root=data_root)

    assert summary["status"] == "SUCCESS"
    claims_path = bronze_artifact_file("claims", data_root)
    assert claims_path.exists()

    claims_frame = pd.read_parquet(claims_path)
    assert {"_ingested_at", "_source_file", "_pipeline_run_id"}.issubset(claims_frame.columns)
    assert len(claims_frame) == 1000


def test_run_local_analytics_build_writes_phi_safe_outputs(tmp_path) -> None:
    data_root = tmp_path / "data"
    run_local_bronze_ingest(source_root=PROJECT_ROOT / "datasets", data_root=data_root)
    summary = run_local_analytics_build(data_root=data_root)

    assert summary["status"] == "SUCCESS"
    assert summary["artifacts_written"] == len(ANALYTICS_ARTIFACT_KEYS)

    for artifact_name in ANALYTICS_ARTIFACT_KEYS:
        artifact_path = analytics_artifact_file(artifact_name, data_root)
        assert artifact_path.exists()
        artifact_frame = pd.read_parquet(artifact_path)
        assert "patient_id" not in artifact_frame.columns

    quality_metrics = pd.read_parquet(ops_artifact_file("quality_metrics", data_root))
    assert not quality_metrics[quality_metrics["metric_name"] == "benchmark_join_missing"].empty


def test_build_high_cost_claims_summary_applies_threshold() -> None:
    claims = pd.DataFrame(
        {
            "claim_id": ["C1", "C2"],
            "provider_id": ["PR1", "PR1"],
            "diagnosis_code": ["D10", "D10"],
            "procedure_code": ["PROC1", "PROC1"],
            "billed_amount": [20000.0, 4000.0],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        }
    )
    providers = pd.DataFrame(
        {
            "provider_id": ["PR1"],
            "doctor_name": ["Dr Patel"],
            "specialty": ["Cardiology"],
            "location": ["Delhi"],
        }
    )
    cost = pd.DataFrame(
        {
            "procedure_code": ["PROC1"],
            "average_cost": [4000.0],
            "expected_cost": [5000.0],
            "region": ["Delhi"],
        }
    )

    result = build_high_cost_claims_summary(claims, providers, cost, threshold_ratio=1.5)

    assert list(result["claim_id"]) == ["C1"]
    assert float(result.iloc[0]["amount_to_benchmark_ratio"]) == 4.0
