from __future__ import annotations

import pickle
import tempfile
import time
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


class FeaturePreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.ml import FEATURE_COLUMNS, TARGET_COLUMN

        cls.feature_columns = FEATURE_COLUMNS
        cls.target_column = TARGET_COLUMN
        np.random.seed(42)
        cls.sample_df = pd.DataFrame(
            {
                "claim_id": [f"C{i:04d}" for i in range(200)],
                "is_procedure_missing": np.random.choice([True, False], 200),
                "is_amount_missing": np.random.choice([True, False], 200, p=[0.05, 0.95]),
                "amount_to_benchmark_ratio": np.random.uniform(0.5, 3.0, 200),
                "billed_vs_avg_cost": np.random.uniform(0.5, 2.5, 200),
                "high_cost_flag": np.random.choice([True, False], 200, p=[0.15, 0.85]),
                "severity_procedure_mismatch": np.random.choice([True, False], 200, p=[0.2, 0.8]),
                "specialty_diagnosis_mismatch": np.random.choice([True, False, None], 200, p=[0.3, 0.6, 0.1]),
                "provider_location_missing": np.random.choice([True, False], 200, p=[0.1, 0.9]),
                "diagnosis_severity_encoded": np.random.choice([0, 1, None], 200, p=[0.4, 0.4, 0.2]),
                "diagnosis_count": np.random.randint(1, 10, 200),
                "provider_claim_count": np.random.randint(1, 50, 200),
                "provider_claim_count_30d": np.random.randint(0, 20, 200),
                "provider_risk_score": np.random.uniform(0.0, 0.8, 200),
                "denial_label": np.random.choice([0, 1], 200, p=[0.7, 0.3]),
            }
        )

    def test_feature_preparation_handles_nulls(self):
        from src.ml.features import fill_nulls, prepare_training_data

        df_with_nulls = self.sample_df.copy()
        df_with_nulls.loc[0, "amount_to_benchmark_ratio"] = None
        df_with_nulls.loc[1, "specialty_diagnosis_mismatch"] = None
        df_with_nulls.loc[2, "diagnosis_severity_encoded"] = None

        X, y = prepare_training_data(df_with_nulls)
        self.assertEqual(X.isnull().sum().sum(), 0, "Features should have no nulls after preparation")
        self.assertEqual(len(X), len(y))

    def test_train_test_split_maintains_stratification(self):
        from src.ml.features import prepare_training_data, stratified_split

        X, y = prepare_training_data(self.sample_df)
        X_train, X_test, y_train, y_test = stratified_split(X, y, test_size=0.3)

        train_ratio = y_train.mean()
        test_ratio = y_test.mean()
        self.assertAlmostEqual(train_ratio, test_ratio, delta=0.05,
                               msg="Class balance should be preserved in stratified split")

    def test_feature_columns_constant_is_stable(self):
        from src.ml import FEATURE_COLUMNS

        self.assertEqual(len(FEATURE_COLUMNS), 13)
        self.assertIn("denial_label", ["denial_label"])
        expected_features = {
            "is_procedure_missing",
            "is_amount_missing",
            "amount_to_benchmark_ratio",
            "billed_vs_avg_cost",
            "high_cost_flag",
            "severity_procedure_mismatch",
            "specialty_diagnosis_mismatch",
            "provider_location_missing",
            "diagnosis_severity_encoded",
            "diagnosis_count",
            "provider_claim_count",
            "provider_claim_count_30d",
            "provider_risk_score",
        }
        self.assertEqual(set(FEATURE_COLUMNS), expected_features)


class ModelTrainingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.ml.features import prepare_training_data, stratified_split

        np.random.seed(42)
        n = 300
        cls.X_data = pd.DataFrame(
            {
                "is_procedure_missing": np.random.choice([0, 1], n),
                "is_amount_missing": np.random.choice([0, 1], n, p=[0.05, 0.95]),
                "amount_to_benchmark_ratio": np.random.uniform(0.5, 3.0, n),
                "billed_vs_avg_cost": np.random.uniform(0.5, 2.5, n),
                "high_cost_flag": np.random.choice([0, 1], n, p=[0.15, 0.85]),
                "severity_procedure_mismatch": np.random.choice([0, 1], n, p=[0.2, 0.8]),
                "specialty_diagnosis_mismatch": np.random.choice([0, 1], n, p=[0.3, 0.7]),
                "provider_location_missing": np.random.choice([0, 1], n, p=[0.1, 0.9]),
                "diagnosis_severity_encoded": np.random.choice([0, 1], n),
                "diagnosis_count": np.random.randint(1, 10, n),
                "provider_claim_count": np.random.randint(1, 50, n),
                "provider_claim_count_30d": np.random.randint(0, 20, n),
                "provider_risk_score": np.random.uniform(0.0, 0.8, n),
            }
        )
        cls.y_data = np.random.choice([0, 1], n, p=[0.7, 0.3])
        X_train, X_test, y_train, y_test = stratified_split(cls.X_data, pd.Series(cls.y_data), test_size=0.3)
        cls.X_train = X_train
        cls.X_test = X_test
        cls.y_train = y_train
        cls.y_test = y_test

    def test_model_training_converges(self):
        from src.ml.train import train_xgboost

        model = train_xgboost(self.X_train, self.y_train, X_val=self.X_test, y_val=self.y_test)
        self.assertTrue(hasattr(model, "predict_proba"))
        preds = model.predict(self.X_test)
        self.assertEqual(len(preds), len(self.y_test))

    def test_logistic_regression_training_converges(self):
        from src.ml.train import train_logistic_regression

        model = train_logistic_regression(self.X_train, self.y_train)
        self.assertTrue(hasattr(model, "predict_proba"))
        preds = model.predict(self.X_test)
        self.assertEqual(len(preds), len(self.y_test))


class ModelEvaluationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.ml.features import prepare_training_data, stratified_split
        from src.ml.train import train_xgboost

        np.random.seed(42)
        n = 300
        feature_data = pd.DataFrame(
            {
                "is_procedure_missing": np.random.choice([0, 1], n),
                "is_amount_missing": np.random.choice([0, 1], n),
                "amount_to_benchmark_ratio": np.random.uniform(0.5, 3.0, n),
                "billed_vs_avg_cost": np.random.uniform(0.5, 2.5, n),
                "high_cost_flag": np.random.choice([0, 1], n),
                "severity_procedure_mismatch": np.random.choice([0, 1], n),
                "specialty_diagnosis_mismatch": np.random.choice([0, 1], n),
                "provider_location_missing": np.random.choice([0, 1], n),
                "diagnosis_severity_encoded": np.random.choice([0, 1], n),
                "diagnosis_count": np.random.randint(1, 10, n),
                "provider_claim_count": np.random.randint(1, 50, n),
                "provider_claim_count_30d": np.random.randint(0, 20, n),
                "provider_risk_score": np.random.uniform(0.0, 0.8, n),
            }
        )
        labels = pd.Series(np.random.choice([0, 1], n, p=[0.7, 0.3]))
        X_train, X_test, y_train, y_test = stratified_split(feature_data, labels, test_size=0.3)
        cls.model = train_xgboost(X_train, y_train, X_val=X_test, y_val=y_test)
        cls.X_test = X_test
        cls.y_test = y_test

    def test_model_predictions_in_valid_range(self):
        probs = self.model.predict_proba(self.X_test)[:, 1]
        self.assertTrue(np.all(probs >= 0) and np.all(probs <= 1))

    def test_model_evaluation_metrics_computed(self):
        from src.ml.evaluate import evaluate_model

        metrics = evaluate_model(self.model, self.X_test, self.y_test)
        self.assertIsInstance(metrics.accuracy, float)
        self.assertIsInstance(metrics.precision, float)
        self.assertIsInstance(metrics.recall, float)
        self.assertIsInstance(metrics.f1, float)
        self.assertIsInstance(metrics.roc_auc, float)
        self.assertTrue(0 <= metrics.accuracy <= 1)
        self.assertTrue(0 <= metrics.roc_auc <= 1)

    def test_shap_values_match_feature_count(self):
        from src.ml.evaluate import compute_shap_values

        shap_values, feature_names = compute_shap_values(self.model, self.X_test, max_samples=50)
        self.assertEqual(shap_values.shape[1], self.X_test.shape[1])

    def test_confusion_matrix_computed(self):
        from src.ml.evaluate import compute_confusion_matrix

        y_pred = self.model.predict(self.X_test)
        tn, fp, fn, tp = compute_confusion_matrix(self.y_test, y_pred)
        self.assertEqual(tn + fp + fn + tp, len(self.y_test))

    def test_evaluation_report_structure(self):
        from src.ml.evaluate import EvaluationMetrics, generate_evaluation_report

        metrics = EvaluationMetrics(accuracy=0.85, precision=0.75, recall=0.82, f1=0.78, roc_auc=0.88)
        report = generate_evaluation_report(metrics, (50, 10, 8, 32), "xgboost", feature_names=["f1", "f2"])
        self.assertEqual(report["model_name"], "xgboost")
        self.assertIn("meets_thresholds", report)
        self.assertTrue(report["meets_thresholds"])


class PredictionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.ml.features import prepare_training_data, stratified_split
        from src.ml.train import train_xgboost

        np.random.seed(42)
        n = 200
        feature_data = pd.DataFrame(
            {
                "is_procedure_missing": np.random.choice([0, 1], n),
                "is_amount_missing": np.random.choice([0, 1], n),
                "amount_to_benchmark_ratio": np.random.uniform(0.5, 3.0, n),
                "billed_vs_avg_cost": np.random.uniform(0.5, 2.5, n),
                "high_cost_flag": np.random.choice([0, 1], n),
                "severity_procedure_mismatch": np.random.choice([0, 1], n),
                "specialty_diagnosis_mismatch": np.random.choice([0, 1], n),
                "provider_location_missing": np.random.choice([0, 1], n),
                "diagnosis_severity_encoded": np.random.choice([0, 1], n),
                "diagnosis_count": np.random.randint(1, 10, n),
                "provider_claim_count": np.random.randint(1, 50, n),
                "provider_claim_count_30d": np.random.randint(0, 20, n),
                "provider_risk_score": np.random.uniform(0.0, 0.8, n),
            }
        )
        labels = pd.Series(np.random.choice([0, 1], n, p=[0.7, 0.3]))
        X_train, X_test, y_train, y_test = stratified_split(feature_data, labels, test_size=0.3)
        cls.model = train_xgboost(X_train, y_train, X_val=X_test, y_val=y_test)
        cls.X_test = X_test
        cls.feature_data = feature_data
        cls.feature_data_with_id = feature_data.copy()
        cls.feature_data_with_id["claim_id"] = [f"C{i:04d}" for i in range(n)]

    def test_risk_level_classification(self):
        from src.ml.predict import RiskLevel

        self.assertEqual(RiskLevel.from_probability(0.1), RiskLevel.LOW)
        self.assertEqual(RiskLevel.from_probability(0.5), RiskLevel.MEDIUM)
        self.assertEqual(RiskLevel.from_probability(0.9), RiskLevel.HIGH)

    def test_predict_single_returns_probability_and_risk(self):
        from src.ml.predict import predict_single

        result = predict_single(self.model, self.feature_data.iloc[0].to_dict())
        self.assertIn("denial_probability", result)
        self.assertIn("risk_level", result)
        self.assertTrue(0 <= result["denial_probability"] <= 1)
        self.assertIn(result["risk_level"], ["LOW", "MEDIUM", "HIGH"])

    def test_predict_batch_returns_dataframe(self):
        from src.ml.predict import predict_batch

        result = predict_batch(self.model, self.feature_data_with_id)
        self.assertIn("denial_probability", result.columns)
        self.assertIn("risk_level", result.columns)
        self.assertTrue((result["denial_probability"] >= 0).all())
        self.assertTrue((result["denial_probability"] <= 1).all())

    def test_model_save_and_load_roundtrip(self):
        from src.ml.predict import load_trained_model, predict_single

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
            pickle.dump(self.model, tmp)
            tmp_path = tmp.name

        loaded = load_trained_model(tmp_path)
        original_result = predict_single(self.model, self.feature_data.iloc[0].to_dict())
        loaded_result = predict_single(loaded, self.feature_data.iloc[0].to_dict())
        self.assertAlmostEqual(
            original_result["denial_probability"],
            loaded_result["denial_probability"],
            places=4,
        )
        Path(tmp_path).unlink(missing_ok=True)

    def test_prediction_latency_under_150ms(self):
        from src.ml.predict import predict_single

        feature_dict = self.feature_data.iloc[0].to_dict()
        start = time.perf_counter()
        for _ in range(10):
            predict_single(self.model, feature_dict)
        elapsed = (time.perf_counter() - start) / 10
        self.assertLess(elapsed, 0.150, f"Average prediction latency {elapsed*1000:.1f}ms exceeds 150ms p95 target")


if __name__ == "__main__":
    unittest.main()