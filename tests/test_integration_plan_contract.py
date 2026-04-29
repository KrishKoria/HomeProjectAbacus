from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class FrameworkContractTests(unittest.TestCase):
    def test_health_check_result_summary_line_formats_state(self) -> None:
        from src.framework import HealthCheckResult

        self.assertEqual(
            HealthCheckResult("bronze", True, "tables=5").summary_line(),
            "OK: bronze - tables=5",
        )
        self.assertEqual(
            HealthCheckResult("bronze", False, "missing required tables").summary_line(),
            "FAIL: bronze - missing required tables",
        )

    def test_service_registry_and_manifests_exist(self) -> None:
        expected_paths = (
            PROJECT_ROOT / "services" / "manifest.yml",
            PROJECT_ROOT / "services" / "etl" / "bronze" / "service.yml",
            PROJECT_ROOT / "services" / "etl" / "silver" / "service.yml",
            PROJECT_ROOT / "services" / "etl" / "gold" / "service.yml",
            PROJECT_ROOT / "services" / "ml" / "training" / "service.yml",
            PROJECT_ROOT / "services" / "infrastructure" / "setup" / "service.yml",
            PROJECT_ROOT / "services" / "infrastructure" / "load_sample_data" / "service.yml",
        )
        for path in expected_paths:
            with self.subTest(path=path):
                self.assertTrue(path.exists())


class TrainingContractTests(unittest.TestCase):
    def test_entrypoint_argv_defaults_to_tune(self) -> None:
        from scripts import train_denial_model

        with mock.patch.object(sys, "argv", ["train_denial_model.py"]):
            self.assertEqual(train_denial_model._entrypoint_argv(), ["--tune"])

    def test_entrypoint_argv_passes_through_databricks_parameters(self) -> None:
        from scripts import train_denial_model

        argv = [
            "train_denial_model.py",
            "--tune",
            "--catalog",
            "healthcare",
            "--gold-table",
            "healthcare.gold.claim_features",
            "--registered-model-name",
            "healthcare.ml.claim_denial_model",
        ]
        with mock.patch.object(sys, "argv", argv):
            self.assertEqual(train_denial_model._entrypoint_argv(), argv[1:])

    def test_train_with_mlflow_logs_training_metadata(self) -> None:
        from src.ml.train import train_with_mlflow

        fake_mlflow = mock.MagicMock()
        fake_mlflow.start_run.return_value.__enter__.return_value = SimpleNamespace(
            info=SimpleNamespace(run_id="run-123")
        )
        fake_mlflow.active_run.return_value = SimpleNamespace(info=SimpleNamespace(run_id="run-123"))
        fake_mlflow.sklearn.log_model.return_value = SimpleNamespace(registered_model_version="7")

        with (
            mock.patch("src.ml.train.mlflow", fake_mlflow),
            mock.patch("src.ml.train._configure_registry_for_runtime"),
            mock.patch("src.ml.train._resolve_experiment_name", return_value="exp"),
        ):
            run_id = train_with_mlflow(
                model=object(),
                model_name="xgboost",
                params={"max_depth": 6},
                metrics={"accuracy": 0.9},
                training_metadata={
                    "training_row_count": 1000,
                    "gold_table_name": "healthcare.gold.claim_features",
                    "gold_table_version": 3,
                    "training_data_fingerprint": "abc123",
                    "feature_columns": ["a", "b"],
                    "target_column": "denial_label",
                    "release_gate_passed": True,
                },
            )

        self.assertEqual(run_id, "run-123")
        fake_mlflow.log_dict.assert_called_once_with({"columns": ["a", "b"]}, "feature_columns.json")
        fake_mlflow.log_params.assert_any_call({"max_depth": 6})
        logged_metadata = fake_mlflow.log_params.call_args_list[1].args[0]
        self.assertEqual(logged_metadata["training_row_count"], "1000")
        self.assertEqual(logged_metadata["training_data_fingerprint"], "abc123")


class RetrainGateTests(unittest.TestCase):
    class _FakeRow(dict):
        def asDict(self, recursive: bool = True):
            return dict(self)

    class _FakeFrame:
        def __init__(self, rows):
            self.rows = [dict(row) for row in rows]

        def count(self):
            return len(self.rows)

        def select(self, *columns):
            return RetrainGateTests._FakeFrame(
                [{column: row[column] for column in columns} for row in self.rows]
            )

        def withColumn(self, column_name, _expr):
            rows = []
            for row in self.rows:
                copied = dict(row)
                copied[column_name] = "||".join(str(value) for value in row.values())
                rows.append(copied)
            return RetrainGateTests._FakeFrame(rows)

        def orderBy(self, *_args, **_kwargs):
            return RetrainGateTests._FakeFrame(
                sorted(self.rows, key=lambda row: tuple(str(value) for value in row.values()))
            )

        def limit(self, count):
            return RetrainGateTests._FakeFrame(self.rows[:count])

        def drop(self, column_name):
            return RetrainGateTests._FakeFrame(
                [{key: value for key, value in row.items() if key != column_name} for row in self.rows]
            )

        def collect(self):
            return [RetrainGateTests._FakeRow(row) for row in self.rows]

    class _FakeSpark:
        def __init__(self, rows):
            self.rows = rows

        def table(self, _name):
            frame = mock.MagicMock()
            frame.count.return_value = len(self.rows)
            return frame

        def sql(self, _query):
            result = mock.MagicMock()
            result.collect.return_value = [{"version": 5}]
            return result

    def test_retrain_decision_summary_line(self) -> None:
        from src.ml.retrain_gate import RetrainDecision

        decision = RetrainDecision.should_retrain_true(
            reason="data fingerprint changed",
            current_row_count=95000,
            current_gold_version=3,
            current_fingerprint="abc123",
            champion_run_id="run-1",
        )
        self.assertIn("RETRAIN", decision.summary_line())

    def test_decide_retrain_returns_true_when_no_champion_exists(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = mock.MagicMock()
        fake_spark.table.return_value.count.return_value = 10
        fake_spark.sql.return_value.collect.return_value = [{"version": 5}]

        with mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="abc123"):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a", "b"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=mock.MagicMock(get_model_version_by_alias=mock.MagicMock(side_effect=RuntimeError("missing"))),
            )

        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "no champion model found")

    def test_decide_retrain_raises_on_zero_rows(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = mock.MagicMock()
        fake_spark.table.return_value.count.return_value = 0

        with self.assertRaises(ValueError):
            decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=mock.MagicMock(),
            )

    def test_decide_retrain_detects_fingerprint_drift(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "old"})
        )
        with (
            mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="new"),
            mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=["a"]),
        ):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=fake_client,
            )

        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "data fingerprint changed")

    def test_decide_retrain_detects_feature_column_changes(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same"})
        )
        with (
            mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="same"),
            mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=["a", "b"]),
        ):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=fake_client,
            )

        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "feature columns changed")

    def test_decide_retrain_requires_retrain_when_champion_feature_metadata_is_missing(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same"})
        )
        with (
            mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="same"),
            mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=[]),
        ):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=fake_client,
            )

        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "champion feature_columns metadata missing")

    def test_decide_retrain_skips_when_metadata_matches(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same"})
        )
        with (
            mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="same"),
            mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=["a"]),
        ):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=fake_client,
            )

        self.assertFalse(decision.should_retrain)
        self.assertEqual(decision.reason, "no data changes")

    def test_compute_fingerprint_is_deterministic_and_data_sensitive(self) -> None:
        from src.ml.retrain_gate import compute_fingerprint

        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        altered_rows = [{"a": 1, "b": 2}, {"a": 3, "b": 5}]
        fake_frame = self._FakeFrame(rows)
        altered_frame = self._FakeFrame(altered_rows)
        fake_spark = mock.MagicMock()
        fake_spark.table.side_effect = [fake_frame, fake_frame, altered_frame]

        first = compute_fingerprint(fake_spark, "healthcare.gold.claim_features", ["a", "b"])
        second = compute_fingerprint(fake_spark, "healthcare.gold.claim_features", ["a", "b"])
        third = compute_fingerprint(fake_spark, "healthcare.gold.claim_features", ["a", "b"])

        self.assertEqual(first, second)
        self.assertNotEqual(first, third)


class BundleContractTests(unittest.TestCase):
    def test_databricks_bundle_includes_expected_service_patterns(self) -> None:
        source = (PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8")

        self.assertIn("services/*/resources/*.yml", source)
        self.assertIn("services/*/*/resources/*.yml", source)
        self.assertIn("services/*/*/*/resources/*.yml", source)
        self.assertIn("model_version: \"1\"", source)

    def test_training_job_contains_condition_task_and_event_log_fanout(self) -> None:
        source = (
            PROJECT_ROOT
            / "services"
            / "ml"
            / "training"
            / "resources"
            / "training.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("condition_task:", source)
        self.assertIn("EQUAL_TO", source)
        self.assertIn("observe_bronze", source)
        self.assertIn("observe_silver", source)
        self.assertIn("observe_gold", source)

    def test_observability_source_supports_pipeline_stage_append_mode(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "analytics" / "observability_assets.py"
        ).read_text(encoding="utf-8")

        self.assertIn("pipeline_stage: str | None = None", source)
        self.assertIn('dataframe.write.mode("append")', source)
        self.assertIn('withColumn("pipeline_stage"', source)


if __name__ == "__main__":
    unittest.main()
