from __future__ import annotations

import logging
import os
from typing import Any

from src.rag.policy_labels import policy_display_name

logger = logging.getLogger(__name__)

_RESULT_COLUMNS = ["chunk_id", "chunk_text", "document_path", "chunk_index"]
_QUERY_TEXT_SUPPORT_CACHE: dict[str, bool] = {}
_VECTOR_SEARCH_CLIENT: Any = None

_WORKSPACE_CLIENT: Any = None


def _get_workspace_client() -> Any:
    global _WORKSPACE_CLIENT
    if _WORKSPACE_CLIENT is None:
        from databricks.sdk import WorkspaceClient

        _WORKSPACE_CLIENT = WorkspaceClient()
    return _WORKSPACE_CLIENT


def _reset_workspace_client() -> None:
    global _WORKSPACE_CLIENT
    _WORKSPACE_CLIENT = None


def _reset_vector_search_client() -> None:
    global _VECTOR_SEARCH_CLIENT
    _VECTOR_SEARCH_CLIENT = None


def _reset_embedding_provider() -> None:
    global _EMBEDDING_PROVIDER
    _EMBEDDING_PROVIDER = None


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
    global _VECTOR_SEARCH_CLIENT
    if _VECTOR_SEARCH_CLIENT is not None:
        return _VECTOR_SEARCH_CLIENT

    from databricks.vector_search.client import VectorSearchClient

    workspace_url = _workspace_url()
    personal_access_token = _env("DATABRICKS_TOKEN")
    if workspace_url and personal_access_token:
        _VECTOR_SEARCH_CLIENT = VectorSearchClient(
            workspace_url=workspace_url,
            personal_access_token=personal_access_token,
            disable_notice=True,
        )
        return _VECTOR_SEARCH_CLIENT

    client_id = _env("DATABRICKS_CLIENT_ID")
    client_secret = _env("DATABRICKS_CLIENT_SECRET")
    if workspace_url and client_id and client_secret:
        _VECTOR_SEARCH_CLIENT = VectorSearchClient(
            workspace_url=workspace_url,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
            disable_notice=True,
        )
        return _VECTOR_SEARCH_CLIENT

    logger.warning(
        "VectorSearchClient is unconfigured: set DATABRICKS_HOST "
        "with DATABRICKS_TOKEN or (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)"
    )
    _VECTOR_SEARCH_CLIENT = VectorSearchClient(disable_notice=True)
    return _VECTOR_SEARCH_CLIENT


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


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric != numeric:
        return None
    return numeric


def _extract_relevance_score(entry: dict[str, Any], fallback: Any = None) -> float | None:
    """Read relevance from either internal or SDK score fields."""
    score_value = entry.get("relevance_score")
    if score_value is None:
        score_value = entry.get("score")
    if score_value is None:
        score_value = fallback
    return _coerce_optional_float(score_value)


def _extract_relevance_score_kind(entry: dict[str, Any], fallback: Any = None) -> str | None:
    """Track whether a relevance score is raw search ranking or normalized."""
    raw_kind = entry.get("relevance_score_kind")
    if raw_kind is not None:
        kind = str(raw_kind).strip().lower()
        if kind in {"raw", "normalized"}:
            return kind

    explicit_relevance = _coerce_optional_float(entry.get("relevance_score"))
    if explicit_relevance is not None:
        return "normalized"

    raw_score = _coerce_optional_float(entry.get("score"))
    if raw_score is not None:
        return "raw"

    fallback_score = _coerce_optional_float(fallback)
    if fallback_score is not None:
        return "raw"
    return None


def _build_result_row(
    mapped: dict[str, Any],
    document_path: Any,
    chunk_index: Any,
    fallback_score: Any = None,
) -> dict[str, Any]:
    relevance_score = _extract_relevance_score(mapped, fallback=fallback_score)
    relevance_score_kind = _extract_relevance_score_kind(mapped, fallback=fallback_score)
    return {
        "chunk_id": mapped.get("chunk_id"),
        "chunk_text": mapped.get("chunk_text", ""),
        "document_path": document_path,
        "chunk_index": chunk_index,
        "relevance_score": relevance_score,
        "relevance_score_kind": relevance_score_kind,
        "policy_name": policy_display_name(
            str(document_path) if document_path is not None else None
        ),
    }


def _requires_query_vector(exc: Exception) -> bool:
    text = str(exc).lower()
    return "query vector" in text or "query_vector" in text


_EMBEDDING_ENDPOINT = "databricks-gte-large-en"
_EMBEDDING_DIM = 1024
_EMBEDDING_PROVIDER: Any = None


def _generate_query_embedding(query_text: str) -> list[float]:
    """Generate a 1024-dim embedding vector for the given query text.

    Uses the Databricks GTE Foundation Model endpoint so that callers
    can pass ``query_vector`` when the Vector Search index does not have
    a query-time model endpoint wired.
    """
    from src.rag.embeddings import EmbeddingProvider

    global _EMBEDDING_PROVIDER
    if _EMBEDDING_PROVIDER is None:
        _EMBEDDING_PROVIDER = EmbeddingProvider(
            endpoint_name=_EMBEDDING_ENDPOINT, embedding_dim=_EMBEDDING_DIM
        )

    embeddings = _EMBEDDING_PROVIDER.embed_batch([query_text])
    if not embeddings:
        logger.error("GTE embedding returned empty result for query text")
        return []
    return embeddings[0]


def _query_with_vector_fallback(
    *,
    index_name: str,
    query_text: str,
    query_text_fn,
    query_vector_fn,
) -> Any:
    """Run query_text/query_vector with per-index capability caching."""
    supports_query_text = _QUERY_TEXT_SUPPORT_CACHE.get(index_name)

    if supports_query_text is False:
        query_vector = _generate_query_embedding(query_text)
        if not query_vector:
            return None
        return query_vector_fn(query_vector)

    try:
        response = query_text_fn(query_text)
        _QUERY_TEXT_SUPPORT_CACHE[index_name] = True
        return response
    except Exception as exc:
        if not _requires_query_vector(exc):
            raise
        _QUERY_TEXT_SUPPORT_CACHE[index_name] = False
        logger.info(
            "Index %s has no query-time model endpoint; generating embedding",
            index_name,
        )
        query_vector = _generate_query_embedding(query_text)
        if not query_vector:
            return None
        return query_vector_fn(query_vector)


def _workspace_query_index(index_name: str, query_text: str, top_k: int) -> list[dict[str, Any]]:
    w = _get_workspace_client()

    def _do_query(**kwargs: Any):
        return w.vector_search_indexes.query_index(
            index_name=index_name,
            columns=_RESULT_COLUMNS,
            num_results=top_k,
            **kwargs,
        )

    response = _query_with_vector_fallback(
        index_name=index_name,
        query_text=query_text,
        query_text_fn=lambda text: _do_query(query_text=text),
        query_vector_fn=lambda vector: _do_query(query_vector=vector),
    )
    if response is None:
        return []

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
        fallback_score: Any = row[-1] if len(row) > len(_RESULT_COLUMNS) else None
        document_path = mapped.get("document_path", "")
        chunk_index = mapped.get("chunk_index", 0)
        rows.append(
            _build_result_row(
                mapped=mapped,
                document_path=document_path,
                chunk_index=chunk_index,
                fallback_score=fallback_score,
            )
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
            return results
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
        raw = _query_with_vector_fallback(
            index_name=self.index_name,
            query_text=query_text,
            query_text_fn=lambda text: endpoint.similarity_search(
                query_text=text,
                columns=_RESULT_COLUMNS,
                num_results=top_k,
            ),
            query_vector_fn=lambda vector: endpoint.similarity_search(
                query_vector=vector,
                columns=_RESULT_COLUMNS,
                num_results=top_k,
            ),
        )
        if raw is None:
            return []

        result = raw.get("result", {})
        data = result.get("data_array", [])
        rows: list[dict[str, Any]] = []
        for row in data:
            relevance_score = _coerce_optional_float(row[4]) if len(row) > 4 else None
            rows.append(
                _build_result_row(
                    mapped={
                        "chunk_id": row[0],
                        "chunk_text": row[1],
                        "document_path": row[2],
                        "chunk_index": row[3],
                        "relevance_score": relevance_score,
                    },
                    document_path=row[2] if len(row) > 2 else None,
                    chunk_index=row[3] if len(row) > 3 else 0,
                    fallback_score=None,
                )
            )
        return rows

    @staticmethod
    def _normalize_results(
        raw: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Ensure every result dict has the expected keys."""
        normalized = []
        for item in raw:
            relevance_score = _extract_relevance_score(item)
            relevance_score_kind = _extract_relevance_score_kind(item)
            normalized.append(
                {
                    "chunk_text": str(item.get("chunk_text", "")),
                    "document_path": str(item.get("document_path", "")),
                    "chunk_index": int(item.get("chunk_index", 0)),
                    "relevance_score": relevance_score,
                    "relevance_score_kind": relevance_score_kind,
                    "policy_name": policy_display_name(item.get("document_path")),
                }
            )
        return normalized
