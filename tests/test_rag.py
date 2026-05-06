from __future__ import annotations

import argparse
import json
import sys
import unittest
from types import ModuleType
from types import SimpleNamespace
from unittest import mock

from src.rag.retriever import _scrub_phi, retrieve_and_explain
from src.rag.synthesizer import _synthesize_via_template, synthesize
from src.rag.vector_search import PolicyRetriever
from src.scripts import create_vector_index


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

    def test_vector_search_client_uses_service_principal_when_available(self) -> None:
        from src.rag.vector_search import _vector_search_client

        fake_constructor = mock.Mock(return_value=object())
        fake_module = mock.Mock(VectorSearchClient=fake_constructor)

        with (
            mock.patch.dict("sys.modules", {"databricks.vector_search.client": fake_module}),
            mock.patch.dict(
                "os.environ",
                {
                    "DATABRICKS_HOST": "https://dbc-test.cloud.databricks.com",
                    "DATABRICKS_CLIENT_ID": "client-id",
                    "DATABRICKS_CLIENT_SECRET": "client-secret",
                },
                clear=False,
            ),
        ):
            _vector_search_client()

        fake_constructor.assert_called_once_with(
            workspace_url="https://dbc-test.cloud.databricks.com",
            service_principal_client_id="client-id",
            service_principal_client_secret="client-secret",
            disable_notice=True,
        )

    def test_workspace_query_index_uses_default_databricks_app_auth(self) -> None:
        from src.rag.vector_search import _workspace_query_index

        fake_indexes = mock.MagicMock()
        fake_indexes.query_index.return_value = SimpleNamespace(
            as_dict=lambda: {
                "manifest": {
                    "columns": [
                        {"name": "chunk_id"},
                        {"name": "chunk_text"},
                        {"name": "document_path"},
                        {"name": "chunk_index"},
                        {"name": "score"},
                    ]
                },
                "result": {
                    "data_array": [
                        ["chunk-1", "Policy text", "/policy.pdf", 3, 0.91],
                    ]
                },
            }
        )
        fake_client = mock.MagicMock()
        fake_client.vector_search_indexes = fake_indexes
        fake_sdk = ModuleType("databricks.sdk")
        fake_sdk.WorkspaceClient = mock.Mock(return_value=fake_client)

        with mock.patch.dict(sys.modules, {"databricks.sdk": fake_sdk}):
            rows = _workspace_query_index(
                "healthcare.gold.policy_chunks_index",
                "medical necessity",
                5,
            )

        fake_sdk.WorkspaceClient.assert_called_once_with()
        fake_indexes.query_index.assert_called_once_with(
            index_name="healthcare.gold.policy_chunks_index",
            columns=["chunk_id", "chunk_text", "document_path", "chunk_index"],
            query_text="medical necessity",
            num_results=5,
        )
        self.assertEqual(rows[0]["chunk_id"], "chunk-1")
        self.assertEqual(rows[0]["chunk_text"], "Policy text")
        self.assertEqual(rows[0]["relevance_score"], 0.91)


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
        self.assertEqual(result["timing"]["retrieval_ms"], 0.0)
        self.assertEqual(result["timing"]["synthesis_ms"], 0.0)

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
        self.assertIn("timing", result)
        self.assertIn("retrieval_ms", result["timing"])
        self.assertIn("synthesis_ms", result["timing"])
        self.assertGreaterEqual(result["timing"]["retrieval_ms"], 0.0)
        self.assertGreaterEqual(result["timing"]["synthesis_ms"], 0.0)
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


class VectorIndexScriptTests(unittest.TestCase):
    def test_dry_run_payload_is_deterministic(self) -> None:
        args = argparse.Namespace(
            source_table="healthcare.gold.policy_chunks_vs",
            mv_source_table="healthcare.gold.policy_chunks",
            endpoint_name="endpoint_a",
            index_name="healthcare.gold.policy_chunks_index",
            query_model_endpoint="databricks-gte-large-en",
            embedding_column="embedding_vector",
            primary_key="chunk_id",
            dry_run=True,
        )
        payload = create_vector_index._dry_run_output(create_vector_index._configure_index(args))
        parsed = json.loads(payload)

        self.assertEqual(parsed["endpoint_name"], "endpoint_a")
        self.assertEqual(parsed["index_name"], "healthcare.gold.policy_chunks_index")
        self.assertEqual(parsed["source_table"], "healthcare.gold.policy_chunks_vs")
        self.assertEqual(parsed["mv_source_table"], "healthcare.gold.policy_chunks")
        self.assertEqual(parsed["query_model_endpoint"], "databricks-gte-large-en")
        self.assertEqual(parsed["embedding_column"], "embedding_vector")
        self.assertEqual(parsed["primary_key"], "chunk_id")
        self.assertEqual(parsed["pipeline_type"], "TRIGGERED")

    def test_existing_index_triggers_sync_instead_of_recreate(self) -> None:
        fake_index = mock.MagicMock()
        fake_client = mock.MagicMock()
        fake_endpoint = mock.MagicMock()
        fake_endpoint.name = "endpoint_a"
        fake_client.list_endpoints.return_value = [fake_endpoint]
        fake_client.get_index.return_value = fake_index

        args = argparse.Namespace(
            source_table="healthcare.gold.policy_chunks_vs",
            mv_source_table="healthcare.gold.policy_chunks",
            endpoint_name="endpoint_a",
            index_name="healthcare.gold.policy_chunks_index",
            query_model_endpoint="databricks-gte-large-en",
            embedding_column="embedding_vector",
            primary_key="chunk_id",
            dry_run=False,
        )

        fake_module = mock.Mock(VectorSearchClient=mock.Mock(return_value=fake_client))
        with (
            mock.patch.dict("sys.modules", {"databricks.vector_search.client": fake_module}),
            mock.patch.object(create_vector_index, "_ensure_cdf_delta_source"),
        ):
            create_vector_index.create_vector_index(args)

        fake_client.create_endpoint.assert_not_called()
        fake_client.create_delta_sync_index.assert_not_called()
        fake_index.sync.assert_called_once()

    def test_missing_index_creates_then_syncs(self) -> None:
        fake_created_index = mock.MagicMock()
        fake_client = mock.MagicMock()
        fake_client.index_exists.return_value = False
        fake_client.get_index.return_value = fake_created_index

        args = argparse.Namespace(
            source_table="healthcare.gold.policy_chunks_vs",
            mv_source_table="healthcare.gold.policy_chunks",
            endpoint_name="endpoint_a",
            index_name="healthcare.gold.policy_chunks_index",
            query_model_endpoint="databricks-gte-large-en",
            embedding_column="embedding_vector",
            primary_key="chunk_id",
            dry_run=False,
        )

        fake_module = mock.Mock(VectorSearchClient=mock.Mock(return_value=fake_client))
        with (
            mock.patch.dict("sys.modules", {"databricks.vector_search.client": fake_module}),
            mock.patch.object(create_vector_index, "_ensure_cdf_delta_source"),
        ):
            create_vector_index.create_vector_index(args)

        fake_client.create_delta_sync_index.assert_called_once_with(
            endpoint_name="endpoint_a",
            source_table_name="healthcare.gold.policy_chunks_vs",
            index_name="healthcare.gold.policy_chunks_index",
            primary_key="chunk_id",
            embedding_dimension=1024,
            embedding_vector_column="embedding_vector",
            model_endpoint_name_for_query="databricks-gte-large-en",
            pipeline_type="TRIGGERED",
        )
        fake_client.get_index.assert_called_with(index_name="healthcare.gold.policy_chunks_index")
        fake_created_index.sync.assert_called_once()

    def test_mirror_sql_enforces_cdf_and_deletes_removed_rows(self) -> None:
        fake_spark = mock.MagicMock()
        fake_spark.table.return_value.columns = ["chunk_id", "chunk_text", "embedding_vector"]
        fake_spark_session = mock.MagicMock()
        fake_spark_session.getActiveSession.return_value = fake_spark
        fake_sql_module = mock.MagicMock(SparkSession=fake_spark_session)

        with mock.patch.dict("sys.modules", {"pyspark.sql": fake_sql_module}):
            create_vector_index._ensure_cdf_delta_source(
                mv_table="healthcare.gold.policy_chunks",
                delta_table="healthcare.gold.policy_chunks_vs",
                primary_key="chunk_id",
            )

        executed_sql = [call.args[0] for call in fake_spark.sql.call_args_list]
        self.assertTrue(
            any("ALTER TABLE `healthcare`.`gold`.`policy_chunks_vs`" in sql for sql in executed_sql)
        )
        self.assertTrue(
            any("delta.enableChangeDataFeed" in sql for sql in executed_sql)
        )
        self.assertTrue(
            any("WHEN NOT MATCHED BY SOURCE THEN DELETE" in sql for sql in executed_sql)
        )


if __name__ == "__main__":
    unittest.main()
