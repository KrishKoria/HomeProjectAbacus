from __future__ import annotations

import unittest

import numpy as np
import xgboost as xgb

from src.ml import FEATURE_COLUMNS
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
                # Zero SHAP value: direction is arbitrary but must be one of the two
                self.assertIn(entry["direction"], {"increases_risk", "decreases_risk"})

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

    def test_explain_phi_absence_in_output(self) -> None:
        result = explain(self.model, self.X_train[:1], self.feature_names)
        output_text = " ".join(r["reason"] for r in result)
        for phi_marker in ("patient_id", "XXX-XX", "billed_amount", "@"):
            self.assertNotIn(phi_marker, output_text.lower())


if __name__ == "__main__":
    unittest.main()
