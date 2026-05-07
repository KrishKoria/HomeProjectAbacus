from __future__ import annotations

import sys
import unittest
from types import ModuleType
from unittest import mock

import numpy as np
import xgboost as xgb

from src.ml import FEATURE_COLUMNS
from src.xai import explainer as xai_explainer
from src.xai.explainer import explain
from src.xai.feature_reasons import FEATURE_REASONS


class FeatureReasonsTests(unittest.TestCase):
    def test_feature_mapping_has_entry_for_every_feature_column(self) -> None:
        """All 20 FEATURE_COLUMNS must have a business-language reason."""
        for feature in FEATURE_COLUMNS:
            with self.subTest(feature=feature):
                self.assertIn(feature, FEATURE_REASONS)
                self.assertIsInstance(FEATURE_REASONS[feature], str)
                self.assertGreater(len(FEATURE_REASONS[feature].strip()), 0)

    def test_feature_mapping_has_no_extra_entries(self) -> None:
        """FEATURE_REASONS must not contain entries for removed features."""
        self.assertEqual(set(FEATURE_REASONS.keys()), set(FEATURE_COLUMNS))

    def test_reason_text_contains_no_phi(self) -> None:
        """Business-language reason templates must never embed raw claim values."""
        phi_markers = ("patient_id", "claim_id", "$", "XXX-XX", "billed_amount")
        for feature, reason in FEATURE_REASONS.items():
            with self.subTest(feature=feature):
                lower = reason.lower()
                for marker in phi_markers:
                    self.assertNotIn(marker, lower)


class ShapExplanationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        n_samples = 200
        n_features = len(FEATURE_COLUMNS)
        rng = np.random.RandomState(42)
        cls.X_train = rng.randn(n_samples, n_features)
        cls.y_train = (rng.rand(n_samples) > 0.7).astype(int)
        cls.feature_names = list(FEATURE_COLUMNS)

        cls.model = xgb.XGBClassifier(
            n_estimators=10,
            max_depth=3,
            random_state=42,
            verbosity=0,
        )
        cls.model.fit(cls.X_train, cls.y_train)

    def tearDown(self) -> None:
        xai_explainer._EXPLAINER_CACHE.clear()

    def test_explain_returns_correct_structure(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        for entry in result:
            self.assertIn("feature", entry)
            self.assertIn("importance", entry)
            self.assertIn("shap_value", entry)
            self.assertIn("reason", entry)
            self.assertIn("direction", entry)

    def test_explain_returns_top_n_by_default(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        self.assertEqual(len(result), 5)

    def test_explain_respects_custom_top_n(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
        self.assertEqual(len(result), 3)

    def test_explain_sorts_by_descending_importance(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        importances = [r["importance"] for r in result]
        self.assertEqual(importances, sorted(importances, reverse=True))

    def test_explain_direction_matches_shap_sign(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        for entry in result:
            if entry["shap_value"] > 0:
                self.assertEqual(entry["direction"], "increases_risk")
            elif entry["shap_value"] < 0:
                self.assertEqual(entry["direction"], "decreases_risk")
            else:
                self.assertEqual(entry["direction"], "neutral")

    def test_explain_fewer_features_than_top_n_returns_all(self) -> None:
        """When top_n exceeds the feature count, all features are returned."""
        result = explain(self.model, self.X_train[:1], self.feature_names, top_n=50)
        self.assertEqual(len(result), len(self.feature_names))

    def test_explain_all_features_when_top_n_equals_count(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names, top_n=len(self.feature_names))
        self.assertEqual(len(result), len(self.feature_names))

    def test_explain_batch_returns_one_row_per_claim(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        self.assertEqual(len(result), 5)

    def test_explain_rejects_multi_row_input(self) -> None:
        with self.assertRaisesRegex(ValueError, "supports one claim only"):
            explain(self.model, self.X_train[:2], self.feature_names)

    def test_explain_accepts_1d_single_sample_input(self) -> None:
        result = explain(self.model, self.X_train[0], self.feature_names)
        self.assertEqual(len(result), 5)

    def test_explain_feature_length_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Feature names length does not match"):
            explain(self.model, self.X_train[:1], self.feature_names[:-1])

    def test_explain_phi_absence_in_output(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        output_text = " ".join(r["reason"] for r in result)
        for phi_marker in ("patient_id", "XXX-XX", "billed_amount", "@"):
            self.assertNotIn(phi_marker, output_text.lower())

    def test_explain_zero_shap_values_map_to_neutral(self) -> None:
        class FakeTreeExplainer:
            def __init__(self, _model) -> None:
                pass

            def shap_values(self, X):
                return np.zeros((1, X.shape[1]), dtype=float)

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            result = explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
        self.assertEqual([entry["direction"] for entry in result], ["neutral", "neutral", "neutral"])

    def test_explain_legacy_list_output_uses_positive_class(self) -> None:
        class FakeTreeExplainer:
            def __init__(self, _model) -> None:
                pass

            def shap_values(self, X):
                neg = np.full((1, X.shape[1]), -9.0, dtype=float)
                pos = np.full((1, X.shape[1]), 3.0, dtype=float)
                return [neg, pos]

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            result = explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
        self.assertEqual([entry["shap_value"] for entry in result], [3.0, 3.0, 3.0])
        self.assertEqual([entry["direction"] for entry in result], ["increases_risk"] * 3)

    def test_explain_rejects_legacy_list_non_binary_outputs(self) -> None:
        class FakeTreeExplainer:
            def __init__(self, _model) -> None:
                pass

            def shap_values(self, X):
                one = np.zeros((1, X.shape[1]), dtype=float)
                return [one, one, one]

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            with self.assertRaisesRegex(ValueError, "only supported for binary outputs"):
                explain(self.model, self.X_train[:1], self.feature_names, top_n=3)

    def test_explain_3d_binary_output_uses_positive_class_axis(self) -> None:
        class FakeTreeExplainer:
            def __init__(self, _model) -> None:
                pass

            def shap_values(self, X):
                out = np.zeros((1, X.shape[1], 2), dtype=float)
                out[0, :, 1] = 2.5
                return out

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            result = explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
        self.assertEqual([entry["shap_value"] for entry in result], [2.5, 2.5, 2.5])

    def test_explain_rejects_unsupported_3d_output_count(self) -> None:
        class FakeTreeExplainer:
            def __init__(self, _model) -> None:
                pass

            def shap_values(self, X):
                return np.zeros((1, X.shape[1], 3), dtype=float)

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            with self.assertRaisesRegex(ValueError, "only supported for binary outputs"):
                explain(self.model, self.X_train[:1], self.feature_names, top_n=3)

    def test_explainer_cache_reuses_tree_explainer_for_same_model(self) -> None:
        class FakeTreeExplainer:
            init_calls = 0

            def __init__(self, _model) -> None:
                type(self).init_calls += 1

            def shap_values(self, X):
                return np.ones((1, X.shape[1]), dtype=float)

        fake_shap = ModuleType("shap")
        fake_shap.TreeExplainer = FakeTreeExplainer

        xai_explainer._EXPLAINER_CACHE.clear()
        with mock.patch.dict(sys.modules, {"shap": fake_shap}):
            explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
            explain(self.model, self.X_train[:1], self.feature_names, top_n=3)
        self.assertEqual(FakeTreeExplainer.init_calls, 1)


if __name__ == "__main__":
    unittest.main()
