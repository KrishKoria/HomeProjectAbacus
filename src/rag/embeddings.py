from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class EmbeddingProvider:
    """Thin wrapper around the Databricks GTE Foundation Model API.

    Handles batching, retries with exponential backoff, and rate-limit
    responses for the ``databricks-gte-large-en`` embedding endpoint.
    The GTE-large model outputs 1024-dimensional embeddings.
    """

    def __init__(
        self,
        endpoint_name: str = "databricks-gte-large-en",
        embedding_dim: int = 1024,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> None:
        self.endpoint_name = endpoint_name
        self.embedding_dim = embedding_dim
        self.max_retries = max_retries
        self.base_delay = base_delay

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts, returning float vectors.

        On Databricks this calls the Foundation Model API. Outside
        Databricks, a zero-vector fallback is returned so that tests
        and local iteration do not fail.
        """
        if not texts:
            return []
        try:
            return self._embed_via_foundation_model(texts)
        except Exception:
            logger.exception(
                "GTE embedding failed for batch of %d texts; falling back to zero vectors",
                len(texts),
            )
            return [[0.0] * self.embedding_dim for _ in texts]

    def _embed_via_foundation_model(self, texts: list[str]) -> list[list[float]]:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                return self._call_endpoint(texts)
            except _SdkUnavailableError:
                # Local/non-Databricks runtime: skip retry backoff and
                # let embed_batch() return the zero-vector fallback.
                raise
            except _RateLimitError:
                if attempt == self.max_retries:
                    raise
                delay = self.base_delay * (2 ** attempt)
                logger.warning(
                    "GTE rate-limited; retrying in %.1fs (attempt %d/%d)",
                    delay,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(delay)
            except Exception as exc:
                last_exc = exc
                if attempt == self.max_retries:
                    raise
                delay = self.base_delay * (2 ** attempt)
                logger.warning(
                    "GTE embedding error; retrying in %.1fs (attempt %d/%d): %s",
                    delay,
                    attempt + 1,
                    self.max_retries,
                    exc,
                )
                time.sleep(delay)
        raise last_exc  # type: ignore[misc]

    def _call_endpoint(self, texts: list[str]) -> list[list[float]]:
        """Call the Databricks Foundation Model API for embeddings.

        Uses the Databricks-provided serving endpoint. On non-Databricks
        runtimes this raises ImportError, triggering the fallback path.
        """
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise _SdkUnavailableError(
                "databricks-sdk not available; using fallback embeddings"
            )

        w = WorkspaceClient()
        response = w.serving_endpoints.query(
            name=self.endpoint_name,
            input=texts,
        )
        zero_vector = [0.0] * self.embedding_dim
        embeddings: list[list[float]] = [list(zero_vector) for _ in texts]
        assigned = [False for _ in texts]
        fallback_index = 0

        for item in (response.data or []):
            if isinstance(item, dict):
                embedding = item.get("embedding")
                index = item.get("index")
            else:
                embedding = getattr(item, "embedding", None)
                index = getattr(item, "index", None)

            target_index: int | None = None
            if isinstance(index, int) and 0 <= index < len(texts):
                if assigned[index]:
                    continue
                target_index = index
            else:
                while fallback_index < len(texts) and assigned[fallback_index]:
                    fallback_index += 1
                if fallback_index >= len(texts):
                    break
                target_index = fallback_index
                fallback_index += 1

            embeddings[target_index] = list(embedding) if embedding else list(zero_vector)
            assigned[target_index] = True
        return embeddings


class _RateLimitError(Exception):
    """Signals a 429 or other transient error from the embedding endpoint."""


class _SdkUnavailableError(Exception):
    """Signals that Databricks SDK is unavailable in this runtime."""
