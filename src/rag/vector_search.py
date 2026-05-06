from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_RESULT_COLUMNS = ["chunk_id", "chunk_text", "document_path", "chunk_index"]


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _workspace_url() -> str:
    host = _env("DATABRICKS_HOST")
    if not host:
        hostname = _env("DATABRICKS_SERVER_HOSTNAME")
        if hostname:
            host = f"https://{hostname}"
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    return host.rstrip("/") if host else ""


def _vector_search_client():
    from databricks.vector_search.client import VectorSearchClient

    workspace_url = _workspace_url()
    personal_access_token = _env("DATABRICKS_TOKEN")
    if workspace_url and personal_access_token:
        return VectorSearchClient(
            workspace_url=workspace_url,
            personal_access_token=personal_access_token,
            disable_notice=True,
        )

    client_id = _env("DATABRICKS_CLIENT_ID")
    client_secret = _env("DATABRICKS_CLIENT_SECRET")
    if workspace_url and client_id and client_secret:
        return VectorSearchClient(
            workspace_url=workspace_url,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
            disable_notice=True,
        )

    logger.warning(
        "VectorSearchClient is unconfigured: set DATABRICKS_HOST "
        "with DATABRICKS_TOKEN or (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)"
    )
    return VectorSearchClient(disable_notice=True)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        return value.as_dict()
    return {}


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _manifest_column_names(manifest: Any) -> list[str]:
    columns = _field(manifest, "columns", []) or []
    names: list[str] = []
    for column in columns:
        name = _field(column, "name")
        if name:
            names.append(str(name))
    return names


_EMBEDDING_ENDPOINT = "databricks-gte-large-en"
_EMBEDDING_DIM = 1024


def _generate_query_embedding(query_text: str) -> list[float]:
    """Generate a 1024-dim embedding vector for the given query text.

    Uses the Databricks GTE Foundation Model endpoint so that callers
    can pass ``query_vector`` when the Vector Search index does not have
    a query-time model endpoint wired.
    """
    from src.rag.embeddings import EmbeddingProvider

    provider = EmbeddingProvider(
        endpoint_name=_EMBEDDING_ENDPOINT, embedding_dim=_EMBEDDING_DIM
    )
    embeddings = provider.embed_batch([query_text])
    if not embeddings:
        logger.error("GTE embedding returned empty result for query text")
        return []
    return embeddings[0]

def _workspace_query_index(index_name: str, query_text: str, top_k: int) -> list[dict[str, Any]]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors.platform import InvalidParameterValue

    w = WorkspaceClient()

    def _do_query(**kwargs: Any):
        return w.vector_search_indexes.query_index(
            index_name=index_name,
            columns=_RESULT_COLUMNS,
            num_results=top_k,
            **kwargs,
        )

    try:
        response = _do_query(query_text=query_text)
    except InvalidParameterValue as exc:
        if "query vector" in str(exc).lower():
            logger.info(
                "Index %s has no query-time model endpoint; generating embedding",
                index_name,
            )
            query_vector = _generate_query_embedding(query_text)
            if not query_vector:
                return []
            response = _do_query(query_vector=query_vector)
        else:
            raise

    payload = _as_dict(response)
    manifest = payload.get("manifest") if payload else _field(response, "manifest", {})
    result = payload.get("result") if payload else _field(response, "result", {})
    column_names = _manifest_column_names(manifest) or [*_RESULT_COLUMNS, "score"]
    data_array = _field(result, "data_array", []) or []

    rows: list[dict[str, Any]] = []
    for row in data_array:
        mapped = {
            column_names[index]: row[index]
            for index in range(min(len(row), len(column_names)))
        }
        if "score" not in mapped and len(row) > len(_RESULT_COLUMNS):
            mapped["score"] = row[-1]
        rows.append(
            {
                "chunk_id": mapped.get("chunk_id"),
                "chunk_text": mapped.get("chunk_text", ""),
                "document_path": mapped.get("document_path", ""),
                "chunk_index": mapped.get("chunk_index", 0),
                "relevance_score": float(mapped.get("score", 0.0) or 0.0),
            }
        )
    return rows


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

        Databricks Apps should use ``WorkspaceClient`` because it picks up
        the app service principal credentials injected by the runtime.
        The standalone vector SDK remains as a fallback for older job/notebook
        contexts.
        """
        try:
            return _workspace_query_index(self.index_name, query_text, top_k)
        except ImportError:
            logger.debug("databricks-sdk not installed; trying vector SDK fallback")
        except Exception:
            logger.exception("WorkspaceClient Vector Search query failed; trying vector SDK fallback")

        try:
            from databricks.vector_search.client import VectorSearchClient  # noqa: F401
        except ImportError:
            logger.debug("databricks-vector-sdk not installed; no-op retrieval")
            return []

        client = _vector_search_client()
        endpoint = client.get_index(index_name=self.index_name)
        if endpoint is None:
            logger.warning("Vector Search index %s not found", self.index_name)
            return []

        # Try query_text first; fall back to query_vector if the index
        # does not have a query-time model endpoint wired.
        try:
            raw = endpoint.similarity_search(
                query_text=query_text,
                columns=_RESULT_COLUMNS,
                num_results=top_k,
            )
        except Exception:
            logger.info(
                "similarity_search with query_text failed; retrying with query_vector"
            )
            query_vector = _generate_query_embedding(query_text)
            if not query_vector:
                return []
            raw = endpoint.similarity_search(
                query_vector=query_vector,
                columns=_RESULT_COLUMNS,
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
