from __future__ import annotations

import unittest
from unittest import mock

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


class StreamlitPolicyGuidanceTests(unittest.TestCase):
    def test_render_policy_guidance_uses_readable_policy_title_and_excerpt(self) -> None:
        rag_result = {
            "source": "llm",
            "narrative": "Short policy narrative.",
            "policy_chunks": [
                {
                    "document_path": "dbfs:/Volumes/healthcare/bronze/raw_landing/policies/claim_submission_completeness_policy.pdf",
                    "chunk_index": 0,
                    "relevance_score": 0.91,
                    "chunk_text": "A" * 380,
                }
            ],
        }

        with (
            mock.patch.object(app_streamlit.st, "markdown") as markdown_mock,
            mock.patch.object(app_streamlit.st, "info") as info_mock,
        ):
            app_streamlit._render_policy_guidance(rag_result)

        info_mock.assert_not_called()
        rendered_html = markdown_mock.call_args.args[0]
        self.assertIn("Claim Submission Completeness Policy", rendered_html)
        self.assertIn("Excerpt 1", rendered_html)
        self.assertIn("Match 91%", rendered_html)
        self.assertIn("Show full excerpt", rendered_html)
        self.assertNotIn("dbfs:/Volumes/healthcare/bronze/raw_landing/policies", rendered_html)

    def test_render_policy_guidance_hides_score_chip_when_unavailable(self) -> None:
        rag_result = {
            "source": "template",
            "narrative": "Template narrative.",
            "policy_chunks": [
                {
                    "document_path": "dbfs:/Volumes/healthcare/bronze/raw_landing/policies/missing_data_field_triage_policy.pdf",
                    "chunk_index": 2,
                    "relevance_score": None,
                    "chunk_text": "Short chunk text.",
                }
            ],
        }

        with (
            mock.patch.object(app_streamlit.st, "markdown") as markdown_mock,
            mock.patch.object(app_streamlit.st, "info") as info_mock,
        ):
            app_streamlit._render_policy_guidance(rag_result)

        info_mock.assert_not_called()
        rendered_html = markdown_mock.call_args.args[0]
        self.assertIn("Missing Data Field Triage Policy", rendered_html)
        self.assertIn("Excerpt 3", rendered_html)
        self.assertNotIn("Match ", rendered_html)
        self.assertNotIn("Score ", rendered_html)

    def test_policy_relevance_label_helper(self) -> None:
        self.assertEqual(app_streamlit._format_policy_relevance_label(0.73), "Match 73%")
        self.assertEqual(app_streamlit._format_policy_relevance_label(1.25), "Score 1.25")
        self.assertIsNone(app_streamlit._format_policy_relevance_label(None))
        self.assertIsNone(app_streamlit._format_policy_relevance_label("bad"))

    def test_policy_relevance_label_helper_formats_raw_vector_score(self) -> None:
        self.assertEqual(
            app_streamlit._format_policy_relevance_label(0.0042, "raw"),
            "Score 0.0042",
        )

    def test_render_policy_guidance_uses_raw_score_label_for_vector_scores(self) -> None:
        rag_result = {
            "source": "llm",
            "narrative": "Short policy narrative.",
            "policy_chunks": [
                {
                    "document_path": "dbfs:/Volumes/healthcare/bronze/raw_landing/policies/medical_necessity_by_diagnosis_policy.pdf",
                    "chunk_index": 0,
                    "relevance_score": 0.0042,
                    "relevance_score_kind": "raw",
                    "chunk_text": "Policy text.",
                }
            ],
        }

        with (
            mock.patch.object(app_streamlit.st, "markdown") as markdown_mock,
            mock.patch.object(app_streamlit.st, "info") as info_mock,
        ):
            app_streamlit._render_policy_guidance(rag_result)

        info_mock.assert_not_called()
        rendered_html = markdown_mock.call_args.args[0]
        self.assertIn("Score 0.0042", rendered_html)
        self.assertNotIn("Match 0%", rendered_html)

    def test_render_policy_guidance_formats_narrative_as_summary_and_list(self) -> None:
        rag_result = {
            "source": "llm",
            "narrative": (
                "The claim is at risk of denial due to missing required fields. "
                "Claim Submission Completeness Policy requires procedure_code and billed_amount. "
                "Resolve missing data before payer submission."
            ),
            "policy_chunks": [],
        }

        with (
            mock.patch.object(app_streamlit.st, "markdown") as markdown_mock,
            mock.patch.object(app_streamlit.st, "info") as info_mock,
        ):
            app_streamlit._render_policy_guidance(rag_result)

        info_mock.assert_not_called()
        rendered_html = markdown_mock.call_args.args[0]
        self.assertIn("policy-narrative-summary", rendered_html)
        self.assertIn("policy-narrative-list", rendered_html)
        self.assertIn("<li>Claim Submission Completeness Policy requires procedure_code and billed_amount.</li>", rendered_html)
        self.assertIn("<li>Resolve missing data before payer submission.</li>", rendered_html)


if __name__ == "__main__":
    unittest.main()
