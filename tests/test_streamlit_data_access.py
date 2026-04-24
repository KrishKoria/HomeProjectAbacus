from pathlib import Path

import pandas as pd
import pytest

from src.local_pipeline.analytics_builders import run_local_analytics_build
from src.local_pipeline.bronze_ingest import run_local_bronze_ingest
from streamlit_app.lib.data_access import ArtifactMissingError, load_analytics_artifact, load_optional_ops_artifact


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_load_analytics_artifact_raises_when_missing(tmp_path) -> None:
    with pytest.raises(ArtifactMissingError):
        load_analytics_artifact("week2_dashboard_summary", tmp_path / "data")


def test_streamlit_data_access_loads_generated_artifacts(tmp_path) -> None:
    data_root = tmp_path / "data"
    run_local_bronze_ingest(source_root=PROJECT_ROOT / "datasets", data_root=data_root)
    run_local_analytics_build(data_root=data_root)

    trend_frame = load_analytics_artifact("week2_dashboard_summary", data_root)
    failure_frame = load_optional_ops_artifact("latest_failures", data_root)

    assert not trend_frame.empty
    assert "patient_id" not in trend_frame.columns
    assert list(failure_frame.columns) == [
        "timestamp",
        "dataset",
        "update_id",
        "platform_error_code",
        "diagnostic_id",
        "error_message",
    ]
