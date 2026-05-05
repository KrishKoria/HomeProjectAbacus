from __future__ import annotations

import unittest

from src.rag.retriever import _scrub_phi, retrieve_and_explain
from src.rag.synthesizer import _synthesize_via_template, synthesize
from src.rag.vector_search import PolicyRetriever


class PhiScrubbingTests(unittest.TestCase):
    def test_scrub_social_security_pattern(self) -> None:
        self.assertNotIn("123-45-6789", _scrub_phi("SSN: 123-45-6789 for patient"))

    def test_scrub_date_iso_pattern(self) -> None:
        self.assertNotIn("2024-02-22", _scrub_phi("Claim date 2024-02-22 processed"))

    def test_scrub_date_us_pattern(self) -> None:
        self.assertNotIn("02/22/2024", _scrub_phi("DOB 02/22/2024 in claim"))

    def test_scrub_dollar_amount(self) -> None:
        self.assertNotIn("$42,500", _scrub_phi("Billed $42,500 for procedure"))

    def test_scrub_patient_id_prefix(self) -> None:
        self.assertNotIn("PAT-12345", _scrub_phi("Reference PAT-12345 in record"))

    def test_scrub_mrn_prefix(self) -> None:
        cleaned = _scrub_phi("MRN 987654 in chart")
        self.assertNotIn("987654", cleaned)

    def test_non_phi_text_passes_unchanged(self) -> None:
        original = "Claim amount exceeds expected cost for procedure"
        self.assertEqual(_scrub_phi(original), original)


class PolicyRetrieverTests(unittest.TestCase):
    def test_empty_query_returns_empty_list(self) -> None:
        retriever = PolicyRetriever()
        result = retriever.search("")
        self.assertEqual(result, [])

    def test_whitespace_only_query_returns_empty_list(self) -> None:
        retriever = PolicyRetriever()
        result = retriever.search("   ")
        self.assertEqual(result, [])

    def test_normalize_results_fills_missing_keys(self) -> None:
        raw = [{"chunk_text": "Policy rule A", "score": 0.95}]
        normalized = PolicyRetriever._normalize_results(raw)
        self.assertEqual(len(normalized), 1)
        entry = normalized[0]
        self.assertEqual(entry["chunk_text"], "Policy rule A")
        self.assertEqual(entry["document_path"], "")
        self.assertEqual(entry["chunk_index"], 0)
        self.assertEqual(entry["relevance_score"], 0.0)

    def test_search_handles_no_databricks_sdk_gracefully(self) -> None:
        retriever = PolicyRetriever()
        result = retriever.search("medical necessity criteria")
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])


class SynthesizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.shap_reasons = [
            {
                "feature": "amount_to_benchmark_ratio",
                "importance": 0.35,
                "reason": "The billed amount relative to benchmark was elevated.",
                "direction": "increases_risk",
            },
            {
                "feature": "severity_procedure_mismatch",
                "importance": 0.28,
                "reason": "Diagnosis severity did not align with billed procedure.",
                "direction": "increases_risk",
            },
        ]
        self.policy_chunks = [
            {
                "chunk_text": "Section 4.2: Claims exceeding 150% of benchmark require medical review.",
                "document_path": "/policies/cms_guidelines_2024.pdf",
                "chunk_index": 3,
                "relevance_score": 0.92,
            },
        ]

    def test_template_synthesis_includes_reasons(self) -> None:
        result = _synthesize_via_template(self.shap_reasons, self.policy_chunks)
        self.assertIn("increases_risk", result["narrative"])
        self.assertEqual(result["source"], "template")
        self.assertGreater(len(result["policy_citations"]), 0)

    def test_template_synthesis_without_chunks(self) -> None:
        result = _synthesize_via_template(self.shap_reasons, [])
        self.assertIn("No specific policy documents", result["narrative"])
        self.assertEqual(len(result["policy_citations"]), 0)
        self.assertEqual(result["source"], "template")

    def test_synthesize_falls_back_when_llm_unavailable(self) -> None:
        """Without databricks-sdk, synthesize() must fall back to template."""
        result = synthesize(
            shap_reasons=self.shap_reasons,
            policy_chunks=self.policy_chunks,
            model_endpoint="databricks-meta-llama-3-3-70b-instruct",
        )
        self.assertIn("source", result)
        self.assertIn("narrative", result)
        self.assertIn("policy_citations", result)
        # On non-Databricks, this should fall back to template
        self.assertGreater(len(result["narrative"]), 0)

    def test_synthesize_empty_inputs(self) -> None:
        result = synthesize([], [])
        self.assertIn("Insufficient information", result["narrative"])
        self.assertEqual(result["source"], "none")


class RetrieveAndExplainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.shap_reasons = [
            {
                "feature": "high_cost_flag",
                "importance": 0.42,
                "reason": "Claim exceeded the high-cost threshold.",
                "direction": "increases_risk",
            },
        ]

    def test_empty_reasons_returns_sentinel_result(self) -> None:
        retriever = PolicyRetriever()
        result = retrieve_and_explain([], retriever=retriever)
        self.assertIn("No SHAP explanations", result["narrative"])
        self.assertEqual(result["source"], "none")
        self.assertEqual(result["policy_chunks"], [])

    def test_full_flow_with_no_databricks(self) -> None:
        """End-to-end flow succeeds even without Databricks runtime."""
        retriever = PolicyRetriever()
        result = retrieve_and_explain(
            shap_reasons=self.shap_reasons,
            retriever=retriever,
            top_k=3,
        )
        self.assertIsInstance(result, dict)
        self.assertIn("narrative", result)
        self.assertIn("policy_chunks", result)
        self.assertIn("policy_citations", result)
        self.assertIn("source", result)
        self.assertEqual(result["explanations"], self.shap_reasons)

    def test_query_phi_scrubbed_before_retrieval(self) -> None:
        reasons_with_phi = [
            {
                "feature": "amount",
                "importance": 0.5,
                "reason": "Claim for PAT-12345 billed $42,500 on 2024-02-22.",
                "direction": "increases_risk",
            },
        ]
        retriever = PolicyRetriever()
        result = retrieve_and_explain(
            shap_reasons=reasons_with_phi,
            retriever=retriever,
            top_k=3,
        )
        narrative = result["narrative"].lower()
        self.assertNotIn("pat-12345", narrative)
        self.assertNotIn("$42,500", narrative)
        self.assertNotIn("2024-02-22", narrative)


if __name__ == "__main__":
    unittest.main()
