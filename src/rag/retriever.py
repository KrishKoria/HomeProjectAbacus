from __future__ import annotations

import logging
import time
from typing import Any

from src.rag.policy_labels import _scrub_phi
from src.rag.synthesizer import synthesize
from src.rag.vector_search import PolicyRetriever

logger = logging.getLogger(__name__)


def retrieve_and_explain(
    shap_reasons: list[dict[str, Any]],
    retriever: PolicyRetriever | None = None,
    top_k: int = 5,
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
) -> dict[str, Any]:
    """Orchestrate RAG retrieval and LLM synthesis for a claim.

    1. Build a PHI-safe query from top SHAP reasons
    2. Retrieve matching policy chunks via Vector Search
    3. Synthesize natural-language explanation via LLM (with fallback)
    4. Return combined result dict
    """
    if retriever is None:
        retriever = PolicyRetriever()

    if not shap_reasons:
        return {
            "prediction": None,
            "explanations": [],
            "policy_chunks": [],
            "narrative": "No SHAP explanations available to drive policy retrieval.",
            "policy_citations": [],
            "source": "none",
            "timing": {
                "retrieval_ms": 0.0,
                "synthesis_ms": 0.0,
            },
        }

    # Build PHI-safe query from top-5 reasons
    query_parts = [r["reason"] for r in shap_reasons[:5]]
    query_text = _scrub_phi(" ".join(query_parts))

    logger.debug("RAG query (PHI-scrubbed): %s", query_text[:200])

    # Retrieve policy chunks
    retrieval_start = time.perf_counter()
    policy_chunks = retriever.search(query_text, top_k=top_k)
    retrieval_ms = (time.perf_counter() - retrieval_start) * 1000.0

    # Synthesize explanation
    synthesis_start = time.perf_counter()
    synthesis = synthesize(
        shap_reasons=shap_reasons,
        policy_chunks=policy_chunks,
        model_endpoint=model_endpoint,
    )
    synthesis_ms = (time.perf_counter() - synthesis_start) * 1000.0

    return {
        "explanations": shap_reasons,
        "policy_chunks": policy_chunks,
        "narrative": synthesis["narrative"],
        "policy_citations": synthesis["policy_citations"],
        "source": synthesis["source"],
        "timing": {
            "retrieval_ms": retrieval_ms,
            "synthesis_ms": synthesis_ms,
        },
    }
