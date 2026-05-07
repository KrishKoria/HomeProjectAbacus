from __future__ import annotations

import ast
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_EMBEDDING_PATH = (
    PROJECT_ROOT / "ETL" / "pipelines" / "gold" / "gold_policy_embeddings.py"
)


class GoldPolicyEmbeddingsContractTests(unittest.TestCase):
    @classmethod
    def _parse_pipeline(cls):
        return ast.parse(GOLD_EMBEDDING_PATH.read_text(encoding="utf-8"))

    def test_pipeline_defines_materialized_view(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("@dp.materialized_view(", source)
        self.assertIn("gold_policy_embeddings", source)
        self.assertIn('refresh_policy="incremental"', source)

    def test_pipeline_reads_from_silver_policy_chunks(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("silver_table_name", source)
        self.assertIn("SILVER_SCHEMA_DEFAULT", source)
        self.assertNotIn('SILVER_POLICY_CHUNKS_TABLE = "healthcare.silver.policy_chunks"', source)
        self.assertIn("read_silver_snapshot", source)

    def test_pipeline_writes_to_gold_policy_chunks(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("policy_chunks", source)

    def test_embedding_dimension_is_1024(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("EMBEDDING_DIM: Final[int] = 1024", source)

    def test_embedding_model_is_gte_large_en(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn('databricks-gte-large-en', source)

    def test_embedding_status_tracks_completed_and_failed(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("COMPLETED", source)
        self.assertIn("FAILED", source)

    def test_output_columns_match_spec(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        required_columns = [
            "chunk_id",
            "document_path",
            "chunk_index",
            "chunk_text",
            "token_count",
            "embedding_vector",
            "embedding_status",
            "embedding_model",
            "embedded_at",
        ]
        for col in required_columns:
            with self.subTest(column=col):
                self.assertIn(f'"{col}"', source)

    def test_pipeline_does_not_read_its_own_gold_target(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")

        self.assertNotIn("spark.read.table(GOLD_POLICY_CHUNKS_TABLE)", source)
        self.assertNotIn("read.table(GOLD_POLICY_CHUNKS_TABLE)", source)
        self.assertNotIn("left_anti", source)

    def test_skips_null_and_whitespace_only_chunks(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("chunk_text", source)
        self.assertIn("isNotNull", source)

    def test_table_properties_non_phi(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn('"NON-PHI"', source)
        self.assertIn('"pipelines.channel"', source)
        self.assertIn('"PREVIEW"', source)

    def test_ai_query_uses_row_tolerant_error_handling(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("failOnError => false", source)
        self.assertIn("_embedding_result.result", source)
        self.assertIn("_embedding_result.errorMessage", source)

    def test_gold_config_clustering(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertIn("chunk_id", source)

    def test_pipeline_has_no_self_read_fallback(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        self.assertNotIn("except Exception as exc:", source)
        self.assertNotIn("_is_table_missing_exception", source)
        self.assertNotIn("logger.warning(", source)

    def test_embedding_freshness_is_driven_by_silver_chunk_identity(self) -> None:
        source = GOLD_EMBEDDING_PATH.read_text(encoding="utf-8")
        silver_source = (
            PROJECT_ROOT / "ETL" / "pipelines" / "silver" / "silver_policy_chunks.py"
        ).read_text(encoding="utf-8")

        self.assertIn("ai_query", source)
        self.assertIn('F.col("chunk.chunk_text")', silver_source)


if __name__ == "__main__":
    unittest.main()
