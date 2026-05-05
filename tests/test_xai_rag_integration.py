"""End-to-end integration: XAI SHAP explanations → RAG retrieval → combined output."""
from __future__ import annotations

import unittest

import numpy as np
import xgboost as xgb

from src.ml import FEATURE_COLUMNS
from src.rag.retriever import retrieve_and_explain
from src.rag.vector_search import PolicyRetriever
from src.xai.explainer import explain


class XaiRagIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        n_samples = 200
        n_features = len(FEATURE_COLUMNS)
        rng = np.random.RandomState(99)
        cls.X_train = rng.randn(n_samples, n_features)
        cls.y_train = (rng.rand(n_samples) > 0.7).astype(int)
        cls.feature_names = list(FEATURE_COLUMNS)

        cls.model = xgb.XGBClassifier(
            n_estimators=10,
            max_depth=3,
            random_state=99,
            verbosity=0,
        )
        cls.model.fit(cls.X_train, cls.y_train)

    def test_end_to_end_shap_to_rag_combined_output(self) -> None:
        """claim features → SHAP explanation → RAG retrieval → combined dict."""
        X_row = self.X_train[:1]
        shap_explanations = explain(self.model, X_row, self.feature_names, top_n=5)
        self.assertEqual(len(shap_explanations), 5)

        retriever = PolicyRetriever()
        result = retrieve_and_explain(
            shap_reasons=shap_explanations,
            retriever=retriever,
            top_k=3,
        )

        # Verify combined output structure
        self.assertIsInstance(result, dict)
        self.assertIn("explanations", result)
        self.assertIn("policy_chunks", result)
        self.assertIn("narrative", result)
        self.assertIn("policy_citations", result)
        self.assertIn("source", result)

        self.assertEqual(result["explanations"], shap_explanations)
        self.assertIsInstance(result["policy_chunks"], list)
        self.assertGreater(len(result["narrative"]), 0)

    def test_end_to_end_empty_chunks_still_produces_explanations(self) -> None:
        """When no policies match, explanations still surface."""
        X_row = self.X_train[:1]
        shap_explanations = explain(self.model, X_row, self.feature_names, top_n=3)

        result = retrieve_and_explain(
            shap_reasons=shap_explanations,
            retriever=PolicyRetriever(),
            top_k=3,
        )

        self.assertEqual(len(result["explanations"]), 3)
        self.assertEqual(len(result["policy_chunks"]), 0)
        self.assertIn("No specific policy documents", result["narrative"])

    def test_phi_absence_across_full_pipeline(self) -> None:
        """Verify no PHI markers leak through any stage of the pipeline."""
        X_row = self.X_train[:1]
        shap_explanations = explain(self.model, X_row, self.feature_names, top_n=5)

        result = retrieve_and_explain(
            shap_reasons=shap_explanations,
            retriever=PolicyRetriever(),
            top_k=3,
        )

        output_text = " ".join(
            [result["narrative"]]
            + [str(r) for r in result["explanations"]]
            + [str(c) for c in result["policy_chunks"]]
        )
        phi_markers = ("patient_id", "XXX-XX-XXXX", "$42,500")
        for marker in phi_markers:
            self.assertNotIn(marker, output_text.lower())

    def test_all_features_get_reasons(self) -> None:
        """Every FEATURE_COLUMN must have a business reason in the XAI mapping."""
        from src.xai.feature_reasons import FEATURE_REASONS

        for feature in FEATURE_COLUMNS:
            with self.subTest(feature=feature):
                self.assertIn(feature, FEATURE_REASONS)
                self.assertGreater(len(FEATURE_REASONS[feature]), 10)

    def test_rag_handles_multiple_reasons_without_error(self) -> None:
        X_row = self.X_train[:1]
        shap_explanations = explain(self.model, X_row, self.feature_names, top_n=5)

        # Multiple calls with same retriever should not error
        retriever = PolicyRetriever()
        for _ in range(3):
            result = retrieve_and_explain(
                shap_reasons=shap_explanations,
                retriever=retriever,
            )
            self.assertIn("narrative", result)


if __name__ == "__main__":
    unittest.main()
