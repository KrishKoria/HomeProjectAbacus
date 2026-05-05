from __future__ import annotations

import logging
from typing import Any

from src.rag.synthesizer import synthesize
from src.rag.vector_search import PolicyRetriever

logger = logging.getLogger(__name__)

# PHI patterns to strip from synthesized queries — matches SSN fragments,
# date-like patterns, dollar amounts, and common ID prefixes.
_PHI_STRIP_PATTERNS = [
    r"\b\d{3}-\d{2}-\d{4}\b",       # SSN-like
    r"\b\d{4}-\d{2}-\d{2}\b",       # ISO date
    r"\b\d{2}/\d{2}/\d{4}\b",       # US date
    r"\$\d[\d,.]*\b",               # dollar amounts
    r"\b(?:PAT|PT|MRN)[-_ ]?\d+\b", # ID prefixes
]


def _scrub_phi(text: str) -> str:
    """Remove PHI-like patterns from a query string."""
    import re

    cleaned = text
    for pattern in _PHI_STRIP_PATTERNS:
        cleaned = re.sub(pattern, "[REDACTED]", cleaned, flags=re.IGNORECASE)
    return cleaned


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
        }

    # Build PHI-safe query from top-5 reasons
    query_parts = [r["reason"] for r in shap_reasons[:5]]
    query_text = _scrub_phi(" ".join(query_parts))

    logger.debug("RAG query (PHI-scrubbed): %s", query_text[:200])

    # Retrieve policy chunks
    policy_chunks = retriever.search(query_text, top_k=top_k)

    # Synthesize explanation
    synthesis = synthesize(
        shap_reasons=shap_reasons,
        policy_chunks=policy_chunks,
        model_endpoint=model_endpoint,
    )

    return {
        "explanations": shap_reasons,
        "policy_chunks": policy_chunks,
        "narrative": synthesis["narrative"],
        "policy_citations": synthesis["policy_citations"],
        "source": synthesis["source"],
    }
