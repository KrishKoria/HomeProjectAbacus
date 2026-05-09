from __future__ import annotations

import argparse
import json
import sys
import unittest
from types import ModuleType
from types import SimpleNamespace
from unittest import mock

from src.rag.embeddings import EmbeddingProvider
from src.rag.policy_labels import policy_display_name, policy_excerpt_label, policy_reference_label
from src.rag.policy_labels import _scrub_phi
from src.rag.retriever import retrieve_and_explain
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
    def setUp(self) -> None:
        from src.rag import vector_search
        vector_search._reset_workspace_client()

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
        self.assertEqual(entry["relevance_score"], 0.95)
        self.assertEqual(entry["relevance_score_kind"], "raw")
        self.assertEqual(entry["policy_name"], "Unknown Policy")

    def test_normalize_results_uses_none_for_missing_or_invalid_score(self) -> None:
        raw = [
            {"chunk_text": "Policy rule A"},
            {"chunk_text": "Policy rule B", "score": "bad"},
        ]
        normalized = PolicyRetriever._normalize_results(raw)
        self.assertIsNone(normalized[0]["relevance_score"])
        self.assertIsNone(normalized[1]["relevance_score"])

    def test_normalize_results_prefers_relevance_score_field(self) -> None:
        raw = [{"chunk_text": "Policy rule A", "relevance_score": 0.77, "score": 0.95}]
        normalized = PolicyRetriever._normalize_results(raw)
        self.assertEqual(normalized[0]["relevance_score"], 0.77)
        self.assertEqual(normalized[0]["relevance_score_kind"], "normalized")

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
        self.assertEqual(rows[0]["relevance_score_kind"], "raw")
        self.assertEqual(rows[0]["policy_name"], "Policy")

    def test_workspace_query_index_uses_trailing_score_when_manifest_omits_score(self) -> None:
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
                    ]
                },
                "result": {
                    "data_array": [
                        ["chunk-1", "Policy text", "dbfs:/Volumes/x/policies/my_policy.pdf", 0, 0.73],
                    ]
                },
            }
        )
        fake_client = mock.MagicMock()
        fake_client.vector_search_indexes = fake_indexes
        fake_sdk = ModuleType("databricks.sdk")
        fake_sdk.WorkspaceClient = mock.Mock(return_value=fake_client)

        with mock.patch.dict(sys.modules, {"databricks.sdk": fake_sdk}):
            rows = _workspace_query_index("healthcare.gold.policy_chunks_index", "missing fields", 5)

        self.assertEqual(rows[0]["relevance_score"], 0.73)
        self.assertEqual(rows[0]["relevance_score_kind"], "raw")
        self.assertEqual(rows[0]["policy_name"], "My Policy")

    def test_workspace_query_index_caches_query_vector_requirement(self) -> None:
        from src.rag import vector_search as vector_search_module

        class InvalidParameterValue(Exception):
            pass

        response = SimpleNamespace(
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
                "result": {"data_array": [["chunk-1", "Policy text", "/policy.pdf", 0, 0.88]]},
            }
        )
        fake_indexes = mock.MagicMock()
        fake_indexes.query_index.side_effect = [
            InvalidParameterValue("Please provide query vector"),
            response,
            response,
        ]
        fake_client = mock.MagicMock()
        fake_client.vector_search_indexes = fake_indexes
        fake_sdk = ModuleType("databricks.sdk")
        fake_sdk.WorkspaceClient = mock.Mock(return_value=fake_client)
        fake_platform_errors = ModuleType("databricks.sdk.errors.platform")
        fake_platform_errors.InvalidParameterValue = InvalidParameterValue

        with (
            mock.patch.dict(
                sys.modules,
                {
                    "databricks.sdk": fake_sdk,
                    "databricks.sdk.errors.platform": fake_platform_errors,
                },
            ),
            mock.patch.object(vector_search_module, "_QUERY_TEXT_SUPPORT_CACHE", {}),
            mock.patch.object(vector_search_module, "_generate_query_embedding", return_value=[0.1, 0.2]),
        ):
            rows_one = vector_search_module._workspace_query_index("idx_a", "medical necessity", 5)
            rows_two = vector_search_module._workspace_query_index("idx_a", "eligibility rules", 5)

        self.assertEqual(rows_one[0]["relevance_score"], 0.88)
        self.assertEqual(rows_two[0]["relevance_score"], 0.88)
        self.assertEqual(fake_indexes.query_index.call_count, 3)
        first_call = fake_indexes.query_index.call_args_list[0].kwargs
        second_call = fake_indexes.query_index.call_args_list[1].kwargs
        third_call = fake_indexes.query_index.call_args_list[2].kwargs
        self.assertIn("query_text", first_call)
        self.assertIn("query_vector", second_call)
        self.assertIn("query_vector", third_call)

    def test_vector_sdk_fallback_caches_query_vector_requirement(self) -> None:
        from src.rag import vector_search as vector_search_module

        fake_module = ModuleType("databricks.vector_search.client")
        fake_module.VectorSearchClient = mock.Mock()
        fake_endpoint = mock.MagicMock()
        fake_endpoint.similarity_search.side_effect = [
            Exception("Index requires query vector"),
            {"result": {"data_array": [["chunk-1", "Policy text", "/policy.pdf", 0, 0.79]]}},
            {"result": {"data_array": [["chunk-2", "Policy text 2", "/policy2.pdf", 1, 0.78]]}},
        ]
        fake_client = mock.MagicMock()
        fake_client.get_index.return_value = fake_endpoint

        retriever = PolicyRetriever(index_name="idx_fallback")
        with (
            mock.patch.dict(sys.modules, {"databricks.vector_search.client": fake_module}),
            mock.patch.object(vector_search_module, "_workspace_query_index", side_effect=ImportError("no sdk")),
            mock.patch.object(vector_search_module, "_vector_search_client", return_value=fake_client),
            mock.patch.object(vector_search_module, "_generate_query_embedding", return_value=[0.3, 0.4]),
            mock.patch.object(vector_search_module, "_QUERY_TEXT_SUPPORT_CACHE", {}),
        ):
            rows_one = retriever._query_index("medical necessity", 3)
            rows_two = retriever._query_index("eligibility rules", 3)

        self.assertEqual(rows_one[0]["chunk_id"], "chunk-1")
        self.assertEqual(rows_two[0]["chunk_id"], "chunk-2")
        self.assertEqual(fake_endpoint.similarity_search.call_count, 3)
        first_call = fake_endpoint.similarity_search.call_args_list[0].kwargs
        second_call = fake_endpoint.similarity_search.call_args_list[1].kwargs
        third_call = fake_endpoint.similarity_search.call_args_list[2].kwargs
        self.assertIn("query_text", first_call)
        self.assertIn("query_vector", second_call)
        self.assertIn("query_vector", third_call)


class EmbeddingProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        from src.rag import embeddings
        embeddings._reset_workspace_client()

    def test_call_endpoint_sends_batch_and_maps_using_index(self) -> None:
        query_mock = mock.Mock(
            return_value=SimpleNamespace(
                data=[
                    SimpleNamespace(index=1, embedding=[2.0, 2.5]),
                    SimpleNamespace(index=0, embedding=[1.0, 1.5]),
                ]
            )
        )
        fake_client = SimpleNamespace(serving_endpoints=SimpleNamespace(query=query_mock))
        fake_sdk = ModuleType("databricks.sdk")
        fake_sdk.WorkspaceClient = mock.Mock(return_value=fake_client)

        with mock.patch.dict(sys.modules, {"databricks.sdk": fake_sdk}):
            provider = EmbeddingProvider(embedding_dim=2)
            vectors = provider._call_endpoint(["first", "second"])

        fake_sdk.WorkspaceClient.assert_called_once_with()
        query_mock.assert_called_once_with(
            name="databricks-gte-large-en",
            input=["first", "second"],
        )
        self.assertEqual(vectors, [[1.0, 1.5], [2.0, 2.5]])

    def test_call_endpoint_uses_zero_vectors_for_empty_or_missing_embeddings(self) -> None:
        query_mock = mock.Mock(
            return_value=SimpleNamespace(
                data=[
                    SimpleNamespace(index=0, embedding=[]),
                    SimpleNamespace(embedding=[9.0, 9.5]),
                ]
            )
        )
        fake_client = SimpleNamespace(serving_endpoints=SimpleNamespace(query=query_mock))
        fake_sdk = ModuleType("databricks.sdk")
        fake_sdk.WorkspaceClient = mock.Mock(return_value=fake_client)

        with mock.patch.dict(sys.modules, {"databricks.sdk": fake_sdk}):
            provider = EmbeddingProvider(embedding_dim=2)
            vectors = provider._call_endpoint(["a", "b", "c"])

        self.assertEqual(vectors[0], [0.0, 0.0])
        self.assertEqual(vectors[1], [9.0, 9.5])
        self.assertEqual(vectors[2], [0.0, 0.0])

    def test_embed_batch_skips_backoff_when_sdk_is_unavailable(self) -> None:
        from src.rag import embeddings as embeddings_module

        provider = EmbeddingProvider(embedding_dim=2, max_retries=3, base_delay=1.0)
        with (
            mock.patch.object(
                provider,
                "_call_endpoint",
                side_effect=embeddings_module._SdkUnavailableError("sdk missing"),
            ),
            mock.patch.object(embeddings_module.time, "sleep") as sleep_mock,
        ):
            vectors = provider.embed_batch(["hello"])

        sleep_mock.assert_not_called()
        self.assertEqual(vectors, [[0.0, 0.0]])

    def test_embed_batch_retries_transient_rate_limit_errors(self) -> None:
        from src.rag import embeddings as embeddings_module

        provider = EmbeddingProvider(embedding_dim=2, max_retries=2, base_delay=0.5)
        with (
            mock.patch.object(
                provider,
                "_call_endpoint",
                side_effect=[embeddings_module._RateLimitError("429"), [[1.0, 2.0]]],
            ),
            mock.patch.object(embeddings_module.time, "sleep") as sleep_mock,
        ):
            vectors = provider.embed_batch(["hello"])

        sleep_mock.assert_called_once_with(0.5)
        self.assertEqual(vectors, [[1.0, 2.0]])


class PolicyLabelTests(unittest.TestCase):
    def test_policy_display_name_from_dbfs_path(self) -> None:
        label = policy_display_name("dbfs:/Volumes/healthcare/bronze/raw_landing/policies/missing_data_field_triage_policy.pdf")
        self.assertEqual(label, "Missing Data Field Triage Policy")

    def test_policy_display_name_handles_unknown(self) -> None:
        self.assertEqual(policy_display_name(""), "Unknown Policy")
        self.assertEqual(policy_display_name(None), "Unknown Policy")

    def test_policy_reference_label_uses_excerpt_index(self) -> None:
        self.assertEqual(policy_excerpt_label(0), "Excerpt 1")
        self.assertEqual(policy_excerpt_label("2"), "Excerpt 3")
        self.assertEqual(policy_reference_label("/tmp/claim_submission_completeness_policy.pdf", 1), "Claim Submission Completeness Policy, Excerpt 2")


class SynthesizerTests(unittest.TestCase):
    def setUp(self) -> None:
        from src.rag import synthesizer
        synthesizer._reset_workspace_client()
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

    def test_synthesize_skips_llm_when_no_policy_chunks(self) -> None:
        expected = {
            "narrative": "template narrative",
            "policy_citations": [],
            "source": "template",
        }
        with (
            mock.patch("src.rag.synthesizer._synthesize_via_llm") as llm_mock,
            mock.patch(
                "src.rag.synthesizer._synthesize_via_template",
                return_value=expected,
            ) as template_mock,
        ):
            result = synthesize(self.shap_reasons, [])

        llm_mock.assert_not_called()
        template_mock.assert_called_once()
        self.assertEqual(result, expected)


class RetrieveAndExplainTests(unittest.TestCase):
    def setUp(self) -> None:
        from src.rag import vector_search
        vector_search._reset_workspace_client()
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
