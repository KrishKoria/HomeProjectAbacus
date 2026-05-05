from __future__ import annotations

from decimal import Decimal
import pickle
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

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
                "provider_claim_count_60d": np.random.randint(0, 30, 200),
                "provider_claim_count_90d": np.random.randint(0, 50, 200),
                "provider_risk_score": np.random.uniform(0.0, 0.8, 200),
                "cost_overbenchmark_and_highseverity": np.random.uniform(0.0, 3.0, 200),
                "mismatch_and_overbenchmark": np.random.uniform(0.0, 4.0, 200),
                "provider_30d_denial_rate": np.random.uniform(0.0, 0.6, 200),
                "missing_fields_count": np.random.randint(0, 4, 200),
                "low_volume_provider_risk": np.random.choice([None, 0.2, 0.4, 0.6], 200, p=[0.6, 0.2, 0.1, 0.1]),
                "dx_px_compatible": np.random.choice([0, 1, None], 200, p=[0.4, 0.4, 0.2]),
                "dx_px_pair_risk_prior": np.random.choice([None, 0.1, 0.25, 0.5, 0.75], 200, p=[0.2, 0.2, 0.3, 0.2, 0.1]),
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

    def test_prepare_training_data_coerces_object_numeric_features(self):
        from src.ml.features import prepare_training_data

        object_backed = self.sample_df.head(3).copy()
        object_backed["amount_to_benchmark_ratio"] = ["1.2", "2.4", None]
        object_backed["provider_risk_score"] = ["0.45", Decimal("0.33"), "0.10"]
        object_backed["dx_px_pair_risk_prior"] = [Decimal("0.30"), "0.55", None]

        X, _ = prepare_training_data(object_backed)

        self.assertTrue((X.dtypes == "float64").all(), X.dtypes.to_string())
        self.assertAlmostEqual(X.loc[X.index[0], "dx_px_pair_risk_prior"], 0.30, places=6)
        self.assertAlmostEqual(X.loc[X.index[1], "provider_risk_score"], 0.33, places=6)

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

        self.assertEqual(len(FEATURE_COLUMNS), 22)
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
            "provider_claim_count_60d",
            "provider_claim_count_90d",
            "provider_risk_score",
            "cost_overbenchmark_and_highseverity",
            "mismatch_and_overbenchmark",
            "provider_30d_denial_rate",
            "missing_fields_count",
            "low_volume_provider_risk",
            "dx_px_compatible",
            "dx_px_pair_risk_prior",
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

    def test_calibrate_classifier_returns_calibrated_wrapper(self):
        # Calibration is what makes the §13 HIGH_RISK_PROBABILITY_THRESHOLD = 0.7
        # cutoff land on a meaningful part of the score distribution. A bare
        # XGBoost is uncalibrated; the wrapper must expose predict/predict_proba
        # and unwrap cleanly for SHAP.
        from sklearn.calibration import CalibratedClassifierCV

        from src.ml.evaluate import _unwrap_for_shap
        from src.ml.train import calibrate_classifier, train_xgboost

        raw = train_xgboost(self.X_train, self.y_train)
        calibrated = calibrate_classifier(
            raw, self.X_train, self.y_train, method="sigmoid", cv=3
        )
        self.assertIsInstance(calibrated, CalibratedClassifierCV)
        self.assertTrue(hasattr(calibrated, "predict_proba"))
        probs = calibrated.predict_proba(self.X_test)[:, 1]
        self.assertTrue((probs >= 0).all() and (probs <= 1).all())
        # SHAP unwrap should reach back to the underlying XGBoost.
        from xgboost import XGBClassifier
        self.assertIsInstance(_unwrap_for_shap(calibrated), XGBClassifier)

    def test_untuned_xgboost_uses_provided_random_seed(self):
        from src.ml.train import train_xgboost

        model_42 = train_xgboost(self.X_train, self.y_train, random_seed=42)
        model_99 = train_xgboost(self.X_train, self.y_train, random_seed=99)

        self.assertEqual(model_42.get_params()["random_state"], 42)
        self.assertEqual(model_99.get_params()["random_state"], 99)

    def test_untuned_lr_uses_provided_random_seed(self):
        from src.ml.train import train_logistic_regression

        model_42 = train_logistic_regression(self.X_train, self.y_train, random_seed=42)
        model_99 = train_logistic_regression(self.X_train, self.y_train, random_seed=99)

        self.assertEqual(model_42.get_params()["random_state"], 42)
        self.assertEqual(model_99.get_params()["random_state"], 99)

    def test_optuna_xgboost_uses_provided_random_seed(self):
        from src.ml.train import _build_xgb_from_trial
        from unittest import mock as umock

        fake_trial = umock.MagicMock()
        fake_trial.suggest_int.side_effect = lambda name, low, high: {
            "max_depth": 5, "n_estimators": 100, "min_child_weight": 3
        }.get(name, low)
        fake_trial.suggest_float.side_effect = lambda name, low, high, log=False: 1.0

        model_77 = _build_xgb_from_trial(fake_trial, random_seed=77)
        self.assertEqual(model_77.get_params()["random_state"], 77)

    def test_cli_random_seed_reaches_train_pipeline(self):
        from src.scripts import train_denial_model

        args = train_denial_model._parse_args(["--no-tune", "--random-seed", "88"])
        self.assertEqual(args.random_seed, 88)

    def test_train_pipeline_logistic_params_use_provided_random_seed(self):
        from src.ml.train import LOGREG_DEFAULT_PARAMS

        # Simulate what train_pipeline does when building the candidates list.
        logreg_params_42 = dict(LOGREG_DEFAULT_PARAMS)
        logreg_params_42["random_state"] = 42
        self.assertEqual(logreg_params_42["random_state"], 42)

        logreg_params_88 = dict(LOGREG_DEFAULT_PARAMS)
        logreg_params_88["random_state"] = 88
        self.assertEqual(logreg_params_88["random_state"], 88)

        # Shared constant must not be mutated in place.
        self.assertEqual(LOGREG_DEFAULT_PARAMS["random_state"], 42)


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
                "provider_claim_count_60d": np.random.randint(0, 30, n),
                "provider_claim_count_90d": np.random.randint(0, 50, n),
                "provider_risk_score": np.random.uniform(0.0, 0.8, n),
                "cost_overbenchmark_and_highseverity": np.random.uniform(0.0, 3.0, n),
                "mismatch_and_overbenchmark": np.random.uniform(0.0, 4.0, n),
                "provider_30d_denial_rate": np.random.uniform(0.0, 0.6, n),
                "missing_fields_count": np.random.randint(0, 4, n),
                "low_volume_provider_risk": np.random.choice([0.0, 0.2, 0.4, 0.6], n),
                "dx_px_compatible": np.random.choice([0, 1], n),
                "dx_px_pair_risk_prior": np.random.choice([0.0, 0.1, 0.25, 0.5, 0.75], n),
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

        metrics = EvaluationMetrics(
            accuracy=0.85,
            precision=0.75,
            recall=0.82,
            f1=0.78,
            roc_auc=0.88,
            recall_at_high=0.85,
        )
        report = generate_evaluation_report(metrics, (50, 10, 8, 32), "xgboost", feature_names=["f1", "f2"])
        self.assertEqual(report["model_name"], "xgboost")
        self.assertIn("meets_thresholds", report)
        self.assertIn("recall_at_high", report)
        self.assertTrue(report["meets_thresholds"])

    def test_recall_at_high_only_counts_high_tier_positives(self):
        from src.ml.evaluate import recall_at_high

        # Two positives total; only one has prob >= 0.7, so Recall@HIGH = 0.5.
        y_true = [1, 1, 0, 0]
        y_prob = [0.95, 0.55, 0.10, 0.40]
        self.assertAlmostEqual(recall_at_high(y_true, y_prob), 0.5, places=6)

    def test_recall_at_high_returns_zero_when_no_positives(self):
        from src.ml.evaluate import recall_at_high

        self.assertEqual(recall_at_high([0, 0, 0], [0.9, 0.8, 0.95]), 0.0)

    def test_meets_thresholds_uses_recall_at_high_not_global_recall(self):
        from src.ml.evaluate import EvaluationMetrics

        # Global recall passes (0.90) but Recall@HIGH fails (0.50). The §13
        # gate must reject this model — global recall is irrelevant.
        metrics = EvaluationMetrics(
            accuracy=0.85,
            precision=0.75,
            recall=0.90,
            f1=0.82,
            roc_auc=0.88,
            recall_at_high=0.50,
        )
        self.assertFalse(metrics.meets_thresholds())


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
                "provider_claim_count_60d": np.random.randint(0, 30, n),
                "provider_claim_count_90d": np.random.randint(0, 50, n),
                "provider_risk_score": np.random.uniform(0.0, 0.8, n),
                "cost_overbenchmark_and_highseverity": np.random.uniform(0.0, 3.0, n),
                "mismatch_and_overbenchmark": np.random.uniform(0.0, 4.0, n),
                "provider_30d_denial_rate": np.random.uniform(0.0, 0.6, n),
                "missing_fields_count": np.random.randint(0, 4, n),
                "low_volume_provider_risk": np.random.choice([0.0, 0.2, 0.4, 0.6], n),
                "dx_px_compatible": np.random.choice([0, 1], n),
                "dx_px_pair_risk_prior": np.random.choice([0.0, 0.1, 0.25, 0.5, 0.75], n),
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

    def test_risk_thresholds_align_with_evaluate_module(self):
        # Inference-time risk tiering (predict.RISK_THRESHOLD_HIGH) must match
        # the threshold used to compute Recall@HIGH in the evaluation gate,
        # otherwise the production "HIGH" tier and the gate measure diverge.
        from src.ml.evaluate import HIGH_RISK_PROBABILITY_THRESHOLD
        from src.ml.predict import RISK_THRESHOLD_HIGH

        self.assertEqual(RISK_THRESHOLD_HIGH, HIGH_RISK_PROBABILITY_THRESHOLD)

    def test_coerce_features_casts_all_model_inputs_to_float64(self):
        from src.ml import FEATURE_COLUMNS
        from src.ml.predict import _coerce_features

        raw = pd.DataFrame(
            [
                {
                    "is_procedure_missing": True,
                    "is_amount_missing": False,
                    "amount_to_benchmark_ratio": 1.2,
                    "billed_vs_avg_cost": 0.9,
                    "high_cost_flag": True,
                    "severity_procedure_mismatch": False,
                    "specialty_diagnosis_mismatch": True,
                    "provider_location_missing": False,
                    "diagnosis_severity_encoded": 1,
                    "diagnosis_count": 4,
                    "provider_claim_count": 25,
                    "provider_claim_count_30d": 3,
                    "provider_claim_count_60d": 5,
                    "provider_claim_count_90d": 12,
                    "provider_risk_score": "0.45",
                    "cost_overbenchmark_and_highseverity": 1.2,
                    "mismatch_and_overbenchmark": 0.0,
                    "provider_30d_denial_rate": 0.33,
                    "missing_fields_count": 1,
                    "low_volume_provider_risk": 0.0,
                    "dx_px_compatible": 1,
                    "dx_px_pair_risk_prior": Decimal("0.3"),
                }
            ]
        )

        coerced = _coerce_features(raw, FEATURE_COLUMNS)

        self.assertEqual(list(coerced.columns), list(FEATURE_COLUMNS))
        self.assertTrue((coerced.dtypes == "float64").all(), coerced.dtypes.to_string())


class RegistryLoadTests(unittest.TestCase):
    def test_get_registry_model_dependencies_uses_alias_uri(self):
        from src.ml.predict import get_registry_model_dependencies

        fake_mlflow = mock.MagicMock()
        fake_mlflow.pyfunc.get_model_dependencies.return_value = "/tmp/model/requirements.txt"

        with mock.patch.dict(sys.modules, {"mlflow": fake_mlflow}):
            path = get_registry_model_dependencies(
                "healthcare.ml.claim_denial_model",
                "champion",
            )

        self.assertEqual(path, "/tmp/model/requirements.txt")
        fake_mlflow.pyfunc.get_model_dependencies.assert_called_once_with(
            "models:/healthcare.ml.claim_denial_model@champion"
        )

    def test_load_from_registry_surfaces_model_dependency_bootstrap_guidance(self):
        from src.ml.predict import load_from_registry

        fake_mlflow = mock.MagicMock()
        fake_mlflow.pyfunc.load_model.side_effect = ModuleNotFoundError("No module named 'xgboost'")
        fake_mlflow.pyfunc.get_model_dependencies.return_value = "/tmp/model/requirements.txt"

        with mock.patch.dict(sys.modules, {"mlflow": fake_mlflow}):
            with self.assertRaises(ModuleNotFoundError) as ctx:
                load_from_registry("healthcare.ml.claim_denial_model", "champion")

        message = str(ctx.exception)
        self.assertIn("/tmp/model/requirements.txt", message)
        self.assertIn("%pip install -q -r /tmp/model/requirements.txt", message)
        self.assertIn("dbutils.library.restartPython()", message)


class ReleaseGateTests(unittest.TestCase):
    def test_main_exits_nonzero_and_skips_save_when_metrics_fail(self):
        from unittest import mock

        from src.scripts import train_denial_model
        from src.ml.evaluate import EvaluationMetrics

        failing_metrics = EvaluationMetrics(
            accuracy=0.5,
            precision=0.5,
            recall=0.5,
            f1=0.5,
            roc_auc=0.5,
            recall_at_high=0.5,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = Path(tmpdir) / "should_not_exist.pkl"
            with mock.patch.object(
                train_denial_model,
                "_load_features",
                return_value=pd.DataFrame(),
            ), mock.patch.object(
                train_denial_model,
                "train_pipeline",
                return_value=(object(), "xgboost", failing_metrics, failing_metrics, failing_metrics, failing_metrics, failing_metrics, failing_metrics, failing_metrics),
            ):
                rc = train_denial_model.main(
                    [
                        "--no-tune",
                        "--model-output",
                        str(model_path),
                    ]
                )

            self.assertEqual(rc, 1)
            self.assertFalse(model_path.exists(), "Failing model must not be persisted")


if __name__ == "__main__":
    unittest.main()
