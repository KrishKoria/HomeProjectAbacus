from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import ModuleType
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
            PROJECT_ROOT / "app.yaml",
            PROJECT_ROOT / "services" / "etl" / "service.yml",
            PROJECT_ROOT / "services" / "etl" / "file_arrival.service.yml",
            PROJECT_ROOT / "services" / "etl" / "fast_dev.service.yml",
            PROJECT_ROOT / "services" / "etl" / "analytics_observability.service.yml",
            PROJECT_ROOT / "services" / "ml" / "training" / "service.yml",
            PROJECT_ROOT / "services" / "rag" / "vector_index" / "service.yml",
            PROJECT_ROOT / "services" / "infrastructure" / "setup" / "service.yml",
            PROJECT_ROOT / "services" / "frontend" / "service.yml",
        )
        for path in expected_paths:
            with self.subTest(path=path):
                self.assertTrue(path.exists())


class TrainingContractTests(unittest.TestCase):
    def test_entrypoint_argv_defaults_to_tune(self) -> None:
        from src.scripts import train_denial_model

        with mock.patch.object(sys, "argv", ["train_denial_model.py"]):
            self.assertEqual(train_denial_model._entrypoint_argv(), ["--tune"])

    def test_entrypoint_argv_passes_through_databricks_parameters(self) -> None:
        from src.scripts import train_denial_model

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

        decision = RetrainDecision.retrain(
            reason="data fingerprint changed",
            current_row_count=95000,
            current_gold_version=3,
            current_fingerprint="abc123",
            champion_run_id="run-1",
        )
        self.assertIn("RETRAIN", decision.summary_line())
        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)

    def test_decide_retrain_returns_retrain_when_no_champion_exists(self) -> None:
        from mlflow.exceptions import MlflowException
        from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST

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
                mlflow_client=mock.MagicMock(
                    get_model_version_by_alias=mock.MagicMock(
                        side_effect=MlflowException("Not found", RESOURCE_DOES_NOT_EXIST)
                    )
                ),
            )

        self.assertEqual(decision.decision_status, "retrain")
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
            data=SimpleNamespace(params={"training_data_fingerprint": "old", "training_row_count": "1000"})
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

        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "data fingerprint changed")

    def test_decide_retrain_detects_feature_column_changes(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same", "training_row_count": "1000"})
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

        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "feature columns changed")

    def test_decide_retrain_requires_retrain_when_champion_feature_metadata_is_missing(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same", "training_row_count": "1000"})
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

        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.reason, "champion feature_columns metadata missing")

    def test_decide_retrain_skips_when_metadata_matches(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same", "training_row_count": "1000"})
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

        self.assertEqual(decision.decision_status, "skip")
        self.assertFalse(decision.should_retrain)
        self.assertEqual(decision.reason, "no data changes")

    def test_current_gold_version_skips_describe_history_for_views(self) -> None:
        from src.ml.retrain_gate import _current_gold_version

        fake_table_type = mock.MagicMock()
        fake_table_type.collect.return_value = [{"table_type": "VIEW"}]
        fake_spark = mock.MagicMock()
        fake_spark.sql.return_value = fake_table_type

        version = _current_gold_version(fake_spark, "healthcare.gold.claim_features")

        self.assertEqual(version, -1)
        fake_spark.sql.assert_called_once()
        self.assertIn("information_schema.tables", fake_spark.sql.call_args.args[0])

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

    def test_compute_fingerprint_raises_when_spark_aggregate_fails(self) -> None:
        from src.ml.retrain_gate import compute_fingerprint

        class _Expr:
            def alias(self, _name: str):
                return self

            def cast(self, _dtype: str):
                return self

        class _AggregateFrame:
            def collect(self):
                raise RuntimeError("aggregate failed")

        class _SparkFrame:
            def __init__(self):
                self.collect = mock.MagicMock(return_value=[])

            def count(self):
                return 2

            def select(self, *_args, **_kwargs):
                return self

            def agg(self, *_args, **_kwargs):
                return _AggregateFrame()

        class _FakeFunctions:
            @staticmethod
            def sha2(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def concat_ws(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def coalesce(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def col(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def lit(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def sort_array(*_args, **_kwargs):
                return _Expr()

            @staticmethod
            def collect_list(*_args, **_kwargs):
                return _Expr()

        fake_pyspark = ModuleType("pyspark")
        fake_sql = ModuleType("pyspark.sql")
        fake_sql.functions = _FakeFunctions
        fake_pyspark.sql = fake_sql

        frame = _SparkFrame()
        fake_spark = mock.MagicMock()
        fake_spark.table.return_value = frame

        with mock.patch.dict(
            sys.modules,
            {
                "pyspark": fake_pyspark,
                "pyspark.sql": fake_sql,
                "pyspark.sql.functions": _FakeFunctions,
            },
        ):
            with self.assertRaises(RuntimeError):
                compute_fingerprint(fake_spark, "healthcare.gold.claim_features", ["a", "b"])

        frame.collect.assert_not_called()

    def test_decide_retrain_returns_error_on_mlflow_transport_failure(self) -> None:
        from mlflow.exceptions import MlflowException
        from mlflow.protos.databricks_pb2 import INTERNAL_ERROR

        from src.ml.retrain_gate import decide_retrain

        fake_spark = mock.MagicMock()
        fake_spark.table.return_value.count.return_value = 10
        fake_spark.sql.return_value.collect.return_value = [{"version": 5}]

        with mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="abc123"):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=mock.MagicMock(
                    get_model_version_by_alias=mock.MagicMock(
                        side_effect=MlflowException("Backend error", INTERNAL_ERROR)
                    )
                ),
            )

        self.assertEqual(decision.decision_status, "error")
        self.assertIsNone(decision.should_retrain)
        self.assertIsNotNone(decision.error_detail)

    def test_decide_retrain_skips_when_fingerprint_changes_below_row_count_threshold(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        # 970 rows vs 1000 previous: delta = 30 < max(100, 5% × 1000) = 100 → skip
        fake_spark = self._FakeSpark([{"a": i} for i in range(970)])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "old", "training_row_count": "1000"})
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

        # abs(970 - 1000) = 30 < max(100, 50) = 100 → skip
        self.assertEqual(decision.decision_status, "skip")
        self.assertIn("row_count delta below retrain threshold", decision.reason)

    def test_decide_retrain_retrains_when_fingerprint_changes_above_row_count_threshold(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": i} for i in range(200)])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "old", "training_row_count": "1000"})
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

        # abs(200 - 1000) = 800 >= max(100, 50) = 100 → retrain
        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)

    def test_decide_retrain_retrains_when_fingerprint_changes_same_row_count(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        # Same row count, different fingerprint = reference data shift -> retrain
        fake_spark = self._FakeSpark([{"a": i} for i in range(1000)])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "old", "training_row_count": "1000"})
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

        # 1000 == 1000, fingerprint changed -> reference data shift -> retrain
        self.assertEqual(decision.decision_status, "retrain")
        self.assertIn("reference data shift", decision.reason)

    def test_decide_retrain_retrains_when_feature_columns_changed_regardless_of_row_count(self) -> None:
        from src.ml.retrain_gate import decide_retrain

        fake_spark = self._FakeSpark([{"a": 1}])
        fake_client = mock.MagicMock()
        fake_client.get_model_version_by_alias.return_value = SimpleNamespace(run_id="run-1")
        fake_client.get_run.return_value = SimpleNamespace(
            data=SimpleNamespace(params={"training_data_fingerprint": "same", "training_row_count": "1000"})
        )
        with (
            mock.patch("src.ml.retrain_gate.compute_fingerprint", return_value="same"),
            mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=["x", "y"]),
        ):
            decision = decide_retrain(
                fake_spark,
                gold_table="healthcare.gold.claim_features",
                feature_columns=["a"],
                registered_model_name="healthcare.ml.claim_denial_model",
                champion_alias="champion",
                mlflow_client=fake_client,
            )

        self.assertEqual(decision.decision_status, "retrain")
        self.assertEqual(decision.reason, "feature columns changed")

    def test_current_gold_object_metadata_uses_asdict_for_spark_row(self) -> None:
        from src.ml.retrain_gate import _current_gold_object_metadata

        fake_spark = mock.MagicMock()
        fake_row = mock.MagicMock()
        fake_row.asDict.return_value = {"table_type": "MATERIALIZED_VIEW", "last_altered": "2025-01-15T12:00:00Z"}
        fake_result = mock.MagicMock()
        fake_result.collect.return_value = [fake_row]
        fake_spark.sql.return_value = fake_result

        obj_type, last_altered = _current_gold_object_metadata(
            fake_spark, "healthcare.gold.claim_features"
        )

        self.assertEqual(obj_type, "MATERIALIZED_VIEW")
        self.assertEqual(last_altered, "2025-01-15T12:00:00Z")
        fake_row.asDict.assert_called_once_with(recursive=True)

    def test_current_gold_object_metadata_returns_fallback_on_missing_row(self) -> None:
        from src.ml.retrain_gate import _current_gold_object_metadata

        fake_spark = mock.MagicMock()
        fake_result = mock.MagicMock()
        fake_result.collect.return_value = []
        fake_spark.sql.return_value = fake_result

        obj_type, last_altered = _current_gold_object_metadata(
            fake_spark, "healthcare.gold.claim_features"
        )

        self.assertEqual(obj_type, "unknown")
        self.assertIsNone(last_altered)


class BundleContractTests(unittest.TestCase):
    def test_databricks_bundle_includes_expected_service_patterns(self) -> None:
        source = (PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8")

        self.assertIn("services/*/resources/*.yml", source)
        self.assertIn("services/*/*/resources/*.yml", source)
        self.assertIn("services/*/*/*/resources/*.yml", source)
        self.assertIn("model_version: \"1\"", source)
        self.assertIn("vector_search_endpoint_name", source)
        self.assertIn("vector_search_index_name", source)
        self.assertIn("vector_search_query_model_endpoint_name", source)
        self.assertIn("app_sql_warehouse_id", source)
        self.assertIn("app_sql_http_path", source)
        self.assertIn("app_claim_features_table", source)
        self.assertIn("app_model_registry_name", source)
        self.assertIn("app_model_alias", source)

    def test_frontend_app_bundle_resource_and_manifest_are_wired(self) -> None:
        manifest_source = (PROJECT_ROOT / "services" / "manifest.yml").read_text(encoding="utf-8")
        service_source = (
            PROJECT_ROOT / "services" / "frontend" / "service.yml"
        ).read_text(encoding="utf-8")
        resource_source = (
            PROJECT_ROOT / "services" / "frontend" / "resources" / "frontend.app.yml"
        ).read_text(encoding="utf-8")
        app_yaml = (PROJECT_ROOT / "app.yaml").read_text(encoding="utf-8")

        self.assertIn("frontend_app:", manifest_source)
        self.assertIn("manifest: services/frontend/service.yml", manifest_source)
        self.assertIn("depends_on:", manifest_source)
        self.assertIn("resource_key: claim_ops_app", service_source)
        self.assertIn("resource_type: apps", service_source)
        self.assertIn("apps:", resource_source)
        self.assertIn("claim_ops_app:", resource_source)
        self.assertIn("source_code_path: ../../../", resource_source)
        self.assertIn("app-sql-warehouse", resource_source)
        self.assertIn("${var.app_sql_warehouse_id}", resource_source)
        self.assertIn("app-policy-vector-index", resource_source)
        self.assertIn("securable_full_name: ${var.vector_search_index_name}", resource_source)
        self.assertIn("securable_type: TABLE", resource_source)
        self.assertIn("permission: SELECT", resource_source)
        self.assertIn("app-claim-denial-model", resource_source)
        self.assertIn("securable_full_name: ${var.app_model_registry_name}", resource_source)
        self.assertIn("securable_type: FUNCTION", resource_source)
        self.assertIn("permission: EXECUTE", resource_source)
        self.assertIn("command:", app_yaml)
        self.assertIn("streamlit", app_yaml)
        self.assertIn("app_streamlit.py", app_yaml)

    def test_app_yaml_defines_required_runtime_envs(self) -> None:
        source = (PROJECT_ROOT / "app.yaml").read_text(encoding="utf-8")

        self.assertIn("CLAIMOPS_SQL_WAREHOUSE_ID", source)
        self.assertIn("CLAIMOPS_SQL_HTTP_PATH", source)
        self.assertIn("CLAIMOPS_GOLD_TABLE", source)
        self.assertIn("CLAIMOPS_MODEL_NAME", source)
        self.assertIn("CLAIMOPS_MODEL_ALIAS", source)
        self.assertIn("CLAIMOPS_VECTOR_INDEX_NAME", source)

    def test_streamlit_frontend_uses_sql_connector_not_spark_session(self) -> None:
        source = (PROJECT_ROOT / "app_streamlit.py").read_text(encoding="utf-8")

        self.assertIn("from databricks import sql", source)
        self.assertIn("oauth_service_principal", source)
        self.assertIn("DATABRICKS_CLIENT_ID", source)
        self.assertIn("DATABRICKS_CLIENT_SECRET", source)
        self.assertIn("WHERE claim_id = ?", source)
        self.assertIn("LIMIT 1", source)
        self.assertIn("Sample Claims", source)
        self.assertIn("degraded", source)
        self.assertIn("WorkspaceClient", source)
        self.assertIn("vector_search_indexes.get_index", source)
        self.assertNotIn("SparkSession", source)
        self.assertIn("_DEFAULT_MODEL_NAME", source)
        self.assertNotIn('_env("CLAIMOPS_MODEL_NAME", DEFAULT_MODEL_NAME)', source)

    def test_retrain_job_is_decoupled_from_file_arrival_etl(self) -> None:
        source = (
            PROJECT_ROOT
            / "services"
            / "ml"
            / "training"
            / "resources"
            / "training.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("ml_retrain_job:", source)
        self.assertIn("maybe_retrain_model.py", source)
        self.assertNotIn("file_arrival:", source)
        self.assertNotIn("run_bronze_pipeline", source)

    def test_job_clusters_use_unity_catalog_access_mode(self) -> None:
        job_files = (
            PROJECT_ROOT / "services" / "infrastructure" / "setup" / "resources" / "setup_infrastructure.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl_file_arrival.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl_fast_dev.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "analytics_observability.job.yml",
            PROJECT_ROOT / "services" / "ml" / "training" / "resources" / "training.job.yml",
            PROJECT_ROOT / "services" / "rag" / "vector_index" / "resources" / "vector_index.job.yml",
        )
        for path in job_files:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertTrue(
                    "environment_key: default" in source
                    or (
                        "data_security_mode: SINGLE_USER" in source
                        and "single_user_name: ${workspace.current_user.userName}" in source
                    )
                    or "job_clusters:" in source
                )

    def test_sample_data_load_is_optional_setup_task_not_separate_job(self) -> None:
        manifest = (PROJECT_ROOT / "services" / "manifest.yml").read_text(encoding="utf-8")
        setup_job = (
            PROJECT_ROOT
            / "services"
            / "infrastructure"
            / "setup"
            / "resources"
            / "setup_infrastructure.job.yml"
        ).read_text(encoding="utf-8")

        self.assertNotIn("load_sample_data:", manifest)
        self.assertIn("name: load_sample_data", setup_job)
        self.assertIn('default: "false"', setup_job)
        self.assertIn("{{job.parameters.load_sample_data}}", setup_job)
        self.assertIn("task_key: load_sample_data", setup_job)
        self.assertFalse(
            (PROJECT_ROOT / "services" / "infrastructure" / "load_sample_data" / "resources" / "load_sample_data.job.yml").exists()
        )

    def test_gcp_job_clusters_attach_local_ssd_for_supported_n2_compute(self) -> None:
        job_files = (
            PROJECT_ROOT / "services" / "infrastructure" / "setup" / "resources" / "setup_infrastructure.job.yml",
            PROJECT_ROOT / "services" / "ml" / "training" / "resources" / "training.job.yml",
        )
        for path in job_files:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertTrue(
                    "environment_key: default" in source
                    or "local_ssd_count: 1" in source
                    or "job_clusters:" in source
                )

    def test_consolidated_etl_pipeline_uses_serverless_compute(self) -> None:
        pipeline_files = (
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl.pipeline.yml",
        )
        for path in pipeline_files:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertIn("serverless: true", source)
                self.assertIn("etl_pipeline_event_log", source)
                self.assertIn("ETL/pipelines/bronze", source)
                self.assertIn("ETL/pipelines/silver", source)
                self.assertIn("ETL/pipelines/gold", source)

    def test_prod_lakeflow_pipelines_use_serverless_compute(self) -> None:
        source = (PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8")

        self.assertIn("prod:", source)
        self.assertNotIn("serverless: false", source)
        self.assertNotIn("local_ssd_count", source)
        self.assertNotIn("pipelines.clusterShutdown.delay", source)

    def test_file_arrival_job_contains_only_pipeline_verify_and_launcher(self) -> None:
        source = (
            PROJECT_ROOT
            / "services" / "etl"
            / "resources"
            / "etl_file_arrival.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("file_arrival:", source)
        self.assertIn("run_etl_pipeline", source)
        self.assertIn("verify_etl_light", source)
        self.assertIn("sync_policy_vector_index", source)
        self.assertIn("launch_analytics_observability", source)
        self.assertIn("run_if: ALL_DONE", source)
        self.assertIn("--pipeline-result", source)
        self.assertIn("{{tasks.run_etl_pipeline.result_state}}", source)
        self.assertIn("{{tasks.verify_etl_light.result_state}}", source)
        self.assertIn("job_id: ${resources.jobs.rag_vector_index_job.id}", source)
        self.assertNotIn("build_analytics", source)
        self.assertNotIn("build_observability", source)
        self.assertNotIn("train_denial_model.py", source)

    def test_analytics_observability_job_is_parameterized_and_not_periodic(self) -> None:
        source = (
            PROJECT_ROOT
            / "services" / "etl"
            / "resources"
            / "analytics_observability.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("parameters:", source)
        self.assertIn("name: upstream_status", source)
        self.assertIn("name: parent_job_name", source)
        self.assertIn("name: parent_run_id", source)
        self.assertIn("name: pipeline_stage", source)
        self.assertNotIn("periodic:", source)

    def test_analytics_observability_job_has_deterministic_task_order_and_gating(self) -> None:
        source = (
            PROJECT_ROOT
            / "services" / "etl"
            / "resources"
            / "analytics_observability.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("task_key: build_observability", source)
        self.assertIn(
            "task_key: build_analytics\n          depends_on:\n            - task_key: build_observability",
            source,
        )
        self.assertIn(
            "task_key: build_quality_assets\n          depends_on:\n            - task_key: build_analytics",
            source,
        )
        self.assertIn("--upstream-status", source)

    def test_check_new_data_notebook_exits_nonzero_on_error_status(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "notebooks" / "check_new_data.ipynb"
        ).read_text(encoding="utf-8")

        self.assertIn('decision.decision_status == \\"error\\"', source)
        self.assertIn("sys.exit(1)", source)

    def test_check_new_data_notebook_uses_explicit_insert_column_list(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "notebooks" / "check_new_data.ipynb"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "decided_at, decision_status, should_retrain, reason, error_detail",
            source,
            "INSERT INTO must use explicit target column list to avoid "
            "positional mismatch with migrated table schemas",
        )
        self.assertIn(
            "INSERT INTO",
            source,
        )

    def test_batch_silver_gold_outputs_use_materialized_view(self) -> None:
        incremental_files = (
            "ETL/pipelines/silver/silver_claims.py",
            "ETL/pipelines/silver/silver_providers.py",
            "ETL/pipelines/silver/silver_diagnosis.py",
            "ETL/pipelines/silver/silver_cost.py",
            "ETL/pipelines/gold/gold_claim_features.py",
        )
        for relpath in incremental_files:
            source = (PROJECT_ROOT / relpath).read_text(encoding="utf-8")
            with self.subTest(path=relpath):
                self.assertIn(
                    "@dp.materialized_view(\n",
                    source,
                )
                self.assertIn('refresh_policy="incremental"', source)

        # policy_chunks uses UDFs + explode — incompatible with incremental refresh.
        policy_source = (
            PROJECT_ROOT / "ETL/pipelines/silver/silver_policy_chunks.py"
        ).read_text(encoding="utf-8")
        self.assertIn("@dp.materialized_view(\n", policy_source)
        self.assertNotIn('refresh_policy="incremental"', policy_source)

    def test_no_current_timestamp_in_silver_gold_incremental_mv_rows(self) -> None:
        for relpath in (
            "ETL/pipelines/silver/silver_claims.py",
            "ETL/pipelines/silver/silver_providers.py",
            "ETL/pipelines/silver/silver_diagnosis.py",
            "ETL/pipelines/silver/silver_cost.py",
            "ETL/pipelines/silver/silver_policy_chunks.py",
            "ETL/pipelines/gold/gold_claim_features.py",
        ):
            source = (PROJECT_ROOT / relpath).read_text(encoding="utf-8")
            with self.subTest(path=relpath):
                self.assertNotIn(
                    "F.current_timestamp()",
                    source,
                    f"{relpath} contains current_timestamp() which is "
                    f"non-deterministic and breaks incremental MV refresh",
                )

    def test_bundle_keeps_unity_catalog_schema_names_unprefixed(self) -> None:
        source = (PROJECT_ROOT / "databricks.yml").read_text(encoding="utf-8")

        self.assertIn("experimental:", source)
        self.assertIn("skip_name_prefix_for_schema: true", source)

    def test_observability_source_supports_pipeline_stage_append_mode(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "analytics" / "observability_assets.py"
        ).read_text(encoding="utf-8")

        self.assertIn("pipeline_stage: str | None = None", source)
        self.assertIn('dataframe.write.mode("append")', source)
        self.assertIn('withColumn("pipeline_stage"', source)
        self.assertNotIn("ThreadPoolExecutor", source)

    def test_spark_python_entrypoints_rely_on_editable_install_not_sys_path(self) -> None:
        """Scripts under src/scripts/ must NOT manually inject PROJECT_ROOT into
        sys.path.  Package resolution is handled uniformly by the editable install
        (``--editable ${workspace.file_path}``) declared in every job/pipeline
        environment spec, so the old boilerplate is both redundant and inconsistent.

        Note: load_sample_data.py is exempt from the _SCRIPT_PATH check because it
        legitimately uses PROJECT_ROOT to locate fixture dataset files on disk, not
        for sys.path manipulation."""
        for path in sorted((PROJECT_ROOT / "src" / "scripts").glob("*.py")):
            source = path.read_text(encoding="utf-8")
            if "from src." not in source and "import src" not in source:
                continue

            with self.subTest(path=path.name):
                self.assertNotIn("sys.path.insert(0, str(PROJECT_ROOT))", source)
                if path.name != "load_sample_data.py":
                    self.assertNotIn("_SCRIPT_PATH.parents[2]", source)

    def test_lakeflow_pipelines_install_project_editable_for_common_imports(self) -> None:
        pipeline_yml_files = (
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl.pipeline.yml",
        )
        for path in pipeline_yml_files:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertIn("environment:", source)
                self.assertIn("dependencies:", source)
                self.assertIn("--editable ${workspace.file_path}", source)

        for path in sorted((PROJECT_ROOT / "ETL" / "pipelines").rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            if "from common." not in source and "import common" not in source:
                continue
            with self.subTest(path=path.relative_to(PROJECT_ROOT)):
                self.assertNotIn("sys.path.insert(0, str(_path))", source)
                self.assertNotIn("_PIPELINE_PATH", source)

    def test_job_environments_install_project_editable(self) -> None:
        """Jobs use either environment dependencies or explicit cluster config."""
        job_yml_files = (
            PROJECT_ROOT / "services" / "ml" / "training" / "resources" / "training.job.yml",
            PROJECT_ROOT / "services" / "infrastructure" / "setup" / "resources" / "setup_infrastructure.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl_file_arrival.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "etl_fast_dev.job.yml",
            PROJECT_ROOT / "services" / "etl" / "resources" / "analytics_observability.job.yml",
            PROJECT_ROOT / "services" / "rag" / "vector_index" / "resources" / "vector_index.job.yml",
        )
        for path in job_yml_files:
            source = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertTrue(
                    ("dependencies:" in source and "--editable ${workspace.file_path}" in source)
                    or "job_clusters:" in source
                )

    def test_rag_vector_index_job_parameters_are_bundle_driven(self) -> None:
        source = (
            PROJECT_ROOT / "services" / "rag" / "vector_index" / "resources" / "vector_index.job.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("rag_vector_index_job:", source)
        self.assertIn("create_or_sync_policy_vector_index", source)
        self.assertIn("create_vector_index.py", source)
        self.assertIn("--mv-source-table", source)
        self.assertIn("${var.catalog}.${var.gold_schema}.policy_chunks", source)
        self.assertIn("--source-table", source)
        self.assertIn("${var.catalog}.${var.gold_schema}.policy_chunks_vs", source)
        self.assertIn("${var.vector_search_endpoint_name}", source)
        self.assertIn("${var.vector_search_index_name}", source)
        self.assertIn("--query-model-endpoint", source)
        self.assertIn("${var.vector_search_query_model_endpoint_name}", source)
        self.assertIn("databricks-vectorsearch", source)

    def test_setup_entrypoint_imports_cleanly_without_file_global(self) -> None:
        """setup_retrain_decisions.py must not rely on __file__ for sys.path
        manipulation.  The editable install provides the package resolution, so
        exec'ing the source without __file__ in the namespace must succeed and
        make HealthCheckResult importable."""
        path = PROJECT_ROOT / "src" / "scripts" / "setup_retrain_decisions.py"
        source = path.read_text(encoding="utf-8")
        namespace: dict[str, object] = {"__name__": "databricks_exec_test"}

        exec(compile(source, str(path), "exec"), namespace)

        self.assertIn("HealthCheckResult", namespace)

    def test_setup_entrypoint_does_not_raise_system_exit_on_success(self) -> None:
        path = PROJECT_ROOT / "src" / "scripts" / "setup_retrain_decisions.py"
        source = path.read_text(encoding="utf-8")
        fake_spark = mock.MagicMock()
        # DESCRIBE TABLE returns all columns already present — no migration needed.
        describe_result = mock.MagicMock()
        describe_result.collect.return_value = [
            {"col_name": col}
            for col in ["decided_at", "decision_status", "should_retrain", "reason",
                         "error_detail", "current_row_count", "current_gold_version",
                         "current_gold_object_type", "current_gold_last_altered",
                         "current_fingerprint", "champion_run_id",
                         "previous_training_row_count", "row_count_delta", "row_count_delta_pct"]
        ]
        fake_spark.sql.return_value = describe_result
        fake_session = ModuleType("pyspark.sql")
        fake_session.SparkSession = SimpleNamespace(
            builder=SimpleNamespace(getOrCreate=mock.MagicMock(return_value=fake_spark))
        )
        fake_pyspark = ModuleType("pyspark")

        with (
            mock.patch.dict(sys.modules, {"pyspark": fake_pyspark, "pyspark.sql": fake_session}),
            mock.patch.object(sys, "argv", ["setup_retrain_decisions.py"]),
        ):
            exec(compile(source, str(path), "exec"), {"__name__": "__main__"})

        self.assertGreaterEqual(fake_spark.sql.call_count, 2)

    def test_setup_entrypoint_migrates_missing_columns_idempotently(self) -> None:
        path = PROJECT_ROOT / "src" / "scripts" / "setup_retrain_decisions.py"
        source = path.read_text(encoding="utf-8")
        fake_spark = mock.MagicMock()
        # Old schema — missing all 7 migration columns.
        old_schema = mock.MagicMock()
        old_schema.collect.return_value = [
            {"col_name": col}
            for col in ["decided_at", "should_retrain", "reason",
                         "current_row_count", "current_gold_version",
                         "current_fingerprint", "champion_run_id"]
        ]
        fake_spark.sql.return_value = old_schema
        fake_session = ModuleType("pyspark.sql")
        fake_session.SparkSession = SimpleNamespace(
            builder=SimpleNamespace(getOrCreate=mock.MagicMock(return_value=fake_spark))
        )
        fake_pyspark = ModuleType("pyspark")

        with (
            mock.patch.dict(sys.modules, {"pyspark": fake_pyspark, "pyspark.sql": fake_session}),
            mock.patch.object(sys, "argv", ["setup_retrain_decisions.py"]),
        ):
            exec(compile(source, str(path), "exec"), {"__name__": "__main__"})

        sql_calls = [call[0][0] for call in fake_spark.sql.call_args_list]
        alter_calls = [c for c in sql_calls if "ALTER TABLE" in c]
        self.assertEqual(len(alter_calls), 1)
        alter_sql = alter_calls[0]
        self.assertIn("ADD COLUMNS", alter_sql)
        for col in ["decision_status", "error_detail", "previous_training_row_count",
                     "row_count_delta", "row_count_delta_pct",
                     "current_gold_object_type", "current_gold_last_altered"]:
            with self.subTest(column=col):
                self.assertIn(col, alter_sql)

    def test_setup_entrypoint_rerun_is_idempotent_no_alter(self) -> None:
        path = PROJECT_ROOT / "src" / "scripts" / "setup_retrain_decisions.py"
        source = path.read_text(encoding="utf-8")
        fake_spark = mock.MagicMock()
        # Full schema — all columns already present.
        full_schema = mock.MagicMock()
        full_schema.collect.return_value = [
            {"col_name": col}
            for col in ["decided_at", "decision_status", "should_retrain", "reason",
                         "error_detail", "current_row_count", "current_gold_version",
                         "current_gold_object_type", "current_gold_last_altered",
                         "current_fingerprint", "champion_run_id",
                         "previous_training_row_count", "row_count_delta", "row_count_delta_pct"]
        ]
        fake_spark.sql.return_value = full_schema
        fake_session = ModuleType("pyspark.sql")
        fake_session.SparkSession = SimpleNamespace(
            builder=SimpleNamespace(getOrCreate=mock.MagicMock(return_value=fake_spark))
        )
        fake_pyspark = ModuleType("pyspark")

        with (
            mock.patch.dict(sys.modules, {"pyspark": fake_pyspark, "pyspark.sql": fake_session}),
            mock.patch.object(sys, "argv", ["setup_retrain_decisions.py"]),
        ):
            exec(compile(source, str(path), "exec"), {"__name__": "__main__"})

        sql_calls = [call[0][0] for call in fake_spark.sql.call_args_list]
        alter_calls = [c for c in sql_calls if "ALTER TABLE" in c]
        self.assertEqual(len(alter_calls), 0, "Rerun with full schema must not ALTER")


if __name__ == "__main__":
    unittest.main()
