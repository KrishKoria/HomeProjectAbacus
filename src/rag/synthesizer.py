from __future__ import annotations

import logging
from typing import Any

from src.rag._workspace_client import get_workspace_client, reset_workspace_client
from src.rag.policy_labels import _scrub_phi, policy_reference_label

logger = logging.getLogger(__name__)


_SYSTEM_PROMPT = (
    "You are a medical coding assistant explaining claim denial risks to a billing analyst. "
    "Use plain, everyday language. Short sentences. No jargon. "
    "Do NOT include policy document names, rule IDs, excerpt numbers, or parenthetical citations "
    "in your response. References are shown to the analyst separately. "
    "Focus on what the analyst should check or fix, not just what is wrong. "
    "Do NOT mention any patient identifiers, dates, dollar amounts, or protected health information. "
    "If no policy chunks are available, give general coding guidance and note that no specific "
    "policy was matched. "
    "Keep your response to 2-4 short sentences."
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
    if shap_reasons and not policy_chunks:
        return _synthesize_via_template(shap_reasons, policy_chunks)

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
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    except ImportError:
        raise RuntimeError("databricks-sdk not available for LLM call")

    reasons_text = "\n".join(
        f"- {r['reason']} (direction: {r['direction']})"
        for r in shap_reasons[:5]
    )
    chunks_text = "\n\n".join(
        f"[{policy_reference_label(c.get('document_path'), c.get('chunk_index'))}] "
        f"{c.get('chunk_text', '')}"
        for c in policy_chunks
    )

    user_message = (
        f"Claim risk factors:\n{reasons_text}\n\n"
        f"Relevant policy excerpts:\n{chunks_text}\n\n"
        "In 2-4 short, simple sentences: what makes this claim risky and what should "
        "the analyst check or fix? Use plain language. Do not mention policy names or rule IDs."
    )

    w = get_workspace_client()
    response = w.serving_endpoints.query(
        name=model_endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=_SYSTEM_PROMPT),
            ChatMessage(role=ChatMessageRole.USER, content=user_message),
        ],
        temperature=temperature,
        max_tokens=300,
    )

    narrative = ""
    choices = getattr(response, "choices", None)
    if choices and len(choices) > 0:
        message = choices[0].message
        narrative = message.content if message else ""

    citations = [
        policy_reference_label(c.get("document_path"), c.get("chunk_index"))
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
    reasons_summary = "; ".join(
        f"{_scrub_phi(r['reason'])} ({r['direction']})"
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
        policy_reference_label(c.get("document_path"), c.get("chunk_index"))
        for c in policy_chunks
    ]

    return {
        "narrative": narrative,
        "policy_citations": citations,
        "source": "template",
    }
