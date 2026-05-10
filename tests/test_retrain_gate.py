from __future__ import annotations

import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from src.ml.retrain_gate import RetrainDecision, compute_fingerprint, decide_retrain


class TestRetrainDecision(unittest.TestCase):
    def test_retrain_factory(self) -> None:
        decision = RetrainDecision.retrain(
            reason="row count changed",
            current_row_count=200,
            current_gold_version=3,
            current_fingerprint="abc123",
            champion_run_id="run_1",
            previous_training_row_count=100,
        )
        self.assertEqual(decision.decision_status, "retrain")
        self.assertTrue(decision.should_retrain)
        self.assertEqual(decision.current_row_count, 200)
        self.assertEqual(decision.current_gold_version, 3)
        self.assertEqual(decision.current_fingerprint, "abc123")
        self.assertEqual(decision.champion_run_id, "run_1")

    def test_skip_factory(self) -> None:
        decision = RetrainDecision.skip(
            reason="no data changes",
            current_row_count=100,
            current_gold_version=2,
            current_fingerprint="abc123",
            champion_run_id="run_1",
            previous_training_row_count=100,
        )
        self.assertEqual(decision.decision_status, "skip")
        self.assertFalse(decision.should_retrain)
        self.assertEqual(decision.current_row_count, 100)
        self.assertEqual(decision.current_gold_version, 2)
        self.assertEqual(decision.current_fingerprint, "abc123")

    def test_error_factory(self) -> None:
        decision = RetrainDecision.error(
            reason="mlflow champion alias lookup failed",
            error_detail="Connection refused",
            current_row_count=0,
            current_gold_version=0,
            current_fingerprint="",
            champion_run_id=None,
        )
        self.assertEqual(decision.decision_status, "error")
        self.assertIsNone(decision.should_retrain)
        self.assertIn("Connection refused", decision.error_detail)

    def test_summary_line_format(self) -> None:
        decision = RetrainDecision.skip(
            reason="no data changes",
            current_row_count=100,
            current_gold_version=2,
            current_fingerprint="abc",
            champion_run_id=None,
        )
        summary = decision.summary_line()
        self.assertIn("SKIP", summary)
        self.assertIn("no data changes", summary)
        self.assertIn("100", summary)


class TestComputeFingerprint(unittest.TestCase):
    def test_same_data_returns_same_fingerprint(self) -> None:
        mock_agg = MagicMock()
        mock_agg.collect.return_value = [{"row_count": 100, "hash_sum": 123456789}]
        mock_select = MagicMock()
        mock_select.agg.return_value = mock_agg
        mock_table = MagicMock()
        mock_table.select.return_value = mock_select
        mock_spark = MagicMock()
        mock_spark.table.return_value = mock_table

        with patch("hashlib.sha256") as mock_sha:
            mock_sha.return_value.hexdigest.return_value = "same_fingerprint"
            count_1, fp_1 = compute_fingerprint(mock_spark, "gold_table", ["col_a", "col_b"])
            mock_sha.reset_mock()
            mock_sha.return_value.hexdigest.return_value = "same_fingerprint"
            count_2, fp_2 = compute_fingerprint(mock_spark, "gold_table", ["col_a", "col_b"])

        self.assertEqual(count_1, count_2)
        self.assertEqual(fp_1, fp_2)

    def test_different_data_returns_different_fingerprint(self) -> None:
        mock_agg_1 = MagicMock()
        mock_agg_1.collect.return_value = [{"row_count": 100, "hash_sum": 111111}]
        mock_digest_1 = MagicMock()
        mock_digest_1.agg.return_value = mock_agg_1
        mock_select_1 = MagicMock()
        mock_select_1.select.return_value = mock_digest_1
        mock_table_1 = MagicMock()
        mock_table_1.select.return_value = mock_select_1
        mock_spark_1 = MagicMock()
        mock_spark_1.table.return_value = mock_table_1

        mock_agg_2 = MagicMock()
        mock_agg_2.collect.return_value = [{"row_count": 200, "hash_sum": 222222}]
        mock_digest_2 = MagicMock()
        mock_digest_2.agg.return_value = mock_agg_2
        mock_select_2 = MagicMock()
        mock_select_2.select.return_value = mock_digest_2
        mock_table_2 = MagicMock()
        mock_table_2.select.return_value = mock_select_2
        mock_spark_2 = MagicMock()
        mock_spark_2.table.return_value = mock_table_2

        with patch("src.ml.retrain_gate.sha256") as mock_sha:
            mock_sha.return_value.hexdigest.side_effect = [
                "content_a", "fp_a", "content_b", "fp_b"
            ]
            count_1, fp_1 = compute_fingerprint(mock_spark_1, "gold_table", ["col_a"])
            count_2, fp_2 = compute_fingerprint(mock_spark_2, "gold_table", ["col_a"])

        self.assertNotEqual(fp_1, fp_2)


class TestDecideRetrain(unittest.TestCase):
    def setUp(self) -> None:
        self.gold_table = "healthcare.gold.claim_features"
        self.feature_columns = ["col_a", "col_b"]
        self.model_name = "healthcare.ml.claim_denial_model"
        self.champion_alias = "champion"

    def test_raises_on_zero_rows(self) -> None:
        mock_spark = MagicMock()
        mock_spark.table.return_value.count.return_value = 0

        with self.assertRaises(ValueError):
            decide_retrain(
                mock_spark,
                self.gold_table,
                self.feature_columns,
                self.model_name,
                self.champion_alias,
            )

    def test_error_on_mlflow_client_failure(self) -> None:
        mock_spark = MagicMock()
        mock_spark.table.return_value.count.return_value = 100
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.side_effect = Exception("MLflow API unreachable")

        decision = decide_retrain(
            mock_spark,
            self.gold_table,
            self.feature_columns,
            self.model_name,
            self.champion_alias,
            mlflow_client=mock_client,
        )

        self.assertEqual(decision.decision_status, "error")
        self.assertIn("MLflow API unreachable", decision.error_detail)

    def test_retrain_when_no_champion(self) -> None:
        mock_spark = MagicMock()
        mock_spark.table.return_value.count.return_value = 100
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value = None

        decision = decide_retrain(
            mock_spark,
            self.gold_table,
            self.feature_columns,
            self.model_name,
            self.champion_alias,
            mlflow_client=mock_client,
        )

        self.assertEqual(decision.decision_status, "retrain")
        self.assertEqual(decision.reason, "no champion model found")

    @mock.patch("src.ml.retrain_gate._feature_columns_from_run", return_value=["col_a", "col_b"])
    @mock.patch("src.ml.retrain_gate._current_gold_version", return_value=2)
    def test_skip_when_no_data_changes(self, mock_gold_version, mock_feature_cols) -> None:
        mock_spark = MagicMock()
        mock_spark.table.return_value.count.return_value = 100
        mock_version = MagicMock()
        mock_version.run_id = "run_1"
        mock_client = MagicMock()
        mock_client.get_model_version_by_alias.return_value = mock_version
        mock_run = MagicMock()
        mock_run.data.params = {
            "training_row_count": "100",
            "gold_table_version": "2",
            "training_data_fingerprint": "abc123",
        }
        mock_client.get_run.return_value = mock_run

        decision = decide_retrain(
            mock_spark,
            self.gold_table,
            self.feature_columns,
            self.model_name,
            self.champion_alias,
            mlflow_client=mock_client,
        )

        self.assertEqual(decision.decision_status, "skip")


if __name__ == "__main__":
    unittest.main()
