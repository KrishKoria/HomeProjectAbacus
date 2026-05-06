from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a medical-policy reasoning assistant. "
    "Respond ONLY using the policy chunks provided below. "
    "Cite the specific policy document and section when you refer to a rule. "
    "Do NOT mention any patient identifiers, dates, dollar amounts, or protected health information. "
    "If no policy chunks are available, explain based on general coding and billing principles and "
    "clearly state that no specific policy was matched. "
    "Keep your response to 2-4 sentences."
)


def synthesize(
    shap_reasons: list[dict[str, Any]],
    policy_chunks: list[dict[str, Any]],
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
    temperature: float = 0.1,
) -> dict[str, Any]:
    """Synthesize a natural-language explanation from SHAP reasons and policy chunks.

    Calls Llama 70B via Databricks Foundation Model API. Falls back to
    template assembly if the LLM is unreachable.

    Returns a dict with keys: narrative (str), policy_citations (list[str]),
    and source (str indicating "llm" or "template").
    """
    if not shap_reasons and not policy_chunks:
        return {
            "narrative": "Insufficient information available to generate an explanation.",
            "policy_citations": [],
            "source": "none",
        }

    try:
        return _synthesize_via_llm(shap_reasons, policy_chunks, model_endpoint, temperature)
    except Exception:
        logger.exception("LLM synthesis failed; falling back to template assembly")
        return _synthesize_via_template(shap_reasons, policy_chunks)


def _synthesize_via_llm(
    shap_reasons: list[dict[str, Any]],
    policy_chunks: list[dict[str, Any]],
    model_endpoint: str,
    temperature: float,
) -> dict[str, Any]:
    """Call Llama 70B on Databricks for natural-language synthesis."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage
    except ImportError:
        raise RuntimeError("databricks-sdk not available for LLM call")

    reasons_text = "\n".join(
        f"- {r['reason']} (direction: {r['direction']})"
        for r in shap_reasons[:5]
    )
    chunks_text = "\n\n".join(
        f"[{c.get('document_path', 'unknown')} §{c.get('chunk_index', 0)}] "
        f"{c.get('chunk_text', '')}"
        for c in policy_chunks
    )

    user_message = (
        f"Claim risk factors:\n{reasons_text}\n\n"
        f"Relevant policy excerpts:\n{chunks_text}\n\n"
        "Explain why this claim is at risk of denial, citing the specific policy sections above."
    )

    w = WorkspaceClient()
    response = w.serving_endpoints.query(
        name=model_endpoint,
        messages=[
            ChatMessage(role="system", content=_SYSTEM_PROMPT),
            ChatMessage(role="user", content=user_message),
        ],
        temperature=temperature,
        max_tokens=300,
    )

    narrative = ""
    choices = getattr(response, "choices", None)
    if choices and len(choices) > 0:
        narrative = choices[0].get("message", {}).get("content", "")

    citations = [
        f"{c.get('document_path', '')} §{c.get('chunk_index', 0)}"
        for c in policy_chunks
    ]

    return {
        "narrative": narrative.strip(),
        "policy_citations": citations,
        "source": "llm",
    }


def _synthesize_via_template(
    shap_reasons: list[dict[str, Any]],
    policy_chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    """Template-based fallback when LLM is unavailable."""
    import re

    def _strip_phi(text: str) -> str:
        for pattern in [
            r"\b\d{3}-\d{2}-\d{4}\b",
            r"\b\d{4}-\d{2}-\d{2}\b",
            r"\b\d{2}/\d{2}/\d{4}\b",
            r"\$\d[\d,.]*\b",
            r"\b(?:PAT|PT|MRN)[-_ ]?\d+\b",
        ]:
            text = re.sub(pattern, "[REDACTED]", text, flags=re.IGNORECASE)
        return text

    reasons_summary = "; ".join(
        f"{_strip_phi(r['reason'])} ({r['direction']})"
        for r in shap_reasons[:5]
    )

    if policy_chunks:
        narrative = (
            "Based on the claim characteristics and applicable policy rules, "
            "the following factors contribute to the denial risk assessment: "
            f"{reasons_summary}. "
            f"{len(policy_chunks)} policy document(s) were matched."
        )
    else:
        narrative = (
            "No specific policy documents were matched for this claim. "
            "The denial risk assessment is based on the following factors: "
            f"{reasons_summary}. "
            "Review the claim against the payer's general medical-necessity guidelines."
        )

    citations = [
        f"{c.get('document_path', '')} §{c.get('chunk_index', 0)}"
        for c in policy_chunks
    ]

    return {
        "narrative": narrative,
        "policy_citations": citations,
        "source": "template",
    }
