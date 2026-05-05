from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PolicyRetriever:
    """Query the Databricks Vector Search index for relevant policy chunks.

    Wraps ``databricks-vector-sdk`` for delta-sync index access with
    cosine similarity on ``healthcare.gold.policy_chunks``.
    """

    def __init__(
        self,
        index_name: str = "healthcare.gold.policy_chunks_index",
        default_top_k: int = 5,
    ) -> None:
        self.index_name = index_name
        self.default_top_k = default_top_k

    def search(
        self,
        query_text: str,
        top_k: int | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve top-K policy chunks matching *query_text*.

        Returns a list of dicts with keys: chunk_text, document_path,
        chunk_index, relevance_score.
        """
        if not query_text.strip():
            return []

        k = top_k if top_k is not None else self.default_top_k

        try:
            results = self._query_index(query_text, k)
            return self._normalize_results(results)
        except Exception:
            logger.exception(
                "Vector Search query failed for index %s; returning empty results",
                self.index_name,
            )
            return []

    def _query_index(
        self, query_text: str, top_k: int
    ) -> list[dict[str, Any]]:
        """Call Databricks Vector Search index endpoint.

        Uses ``databricks-vector-sdk`` when available; falls back to an
        empty result set outside Databricks.
        """
        try:
            from databricks.vector_search.client import VectorSearchClient
        except ImportError:
            logger.debug("databricks-vector-sdk not installed; no-op retrieval")
            return []

        client = VectorSearchClient(disable_notice=True)
        endpoint = client.get_index(self.index_name)
        if endpoint is None:
            logger.warning("Vector Search index %s not found", self.index_name)
            return []

        raw = endpoint.similarity_search(
            query_text=query_text,
            columns=["chunk_id", "chunk_text", "document_path", "chunk_index"],
            num_results=top_k,
        )
        result = raw.get("result", {})
        data = result.get("data_array", [])
        return [
            {
                "chunk_id": row[0],
                "chunk_text": row[1],
                "document_path": row[2],
                "chunk_index": row[3],
                "relevance_score": float(row[4]) if len(row) > 4 else 0.0,
            }
            for row in data
        ]

    @staticmethod
    def _normalize_results(
        raw: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Ensure every result dict has the expected keys."""
        normalized = []
        for item in raw:
            normalized.append(
                {
                    "chunk_text": str(item.get("chunk_text", "")),
                    "document_path": str(item.get("document_path", "")),
                    "chunk_index": int(item.get("chunk_index", 0)),
                    "relevance_score": float(item.get("relevance_score", 0.0)),
                }
            )
        return normalized
