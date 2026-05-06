from __future__ import annotations

import unittest

import app_streamlit


class StreamlitLatencySummaryTests(unittest.TestCase):
    def test_summarize_latency_calculates_composite_metrics(self) -> None:
        summary = app_streamlit._summarize_latency(
            feature_lookup_ms=120.0,
            risk_inference_ms=35.0,
            shap_ms=40.0,
            policy_retrieval_ms=300.0,
            narrative_ms=500.0,
            total_ms=1005.0,
        )

        self.assertEqual(summary["risk_path_ms"], 155.0)
        self.assertEqual(summary["explanation_ms"], 840.0)
        self.assertEqual(summary["total_ms"], 1005.0)
        self.assertGreaterEqual(summary["total_ms"], summary["risk_path_ms"])


if __name__ == "__main__":
    unittest.main()
