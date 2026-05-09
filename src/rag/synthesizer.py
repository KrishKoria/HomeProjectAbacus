from __future__ import annotations

import logging
from typing import Any

from src.rag.policy_labels import _scrub_phi, policy_reference_label

logger = logging.getLogger(__name__)

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
        "Explain why this claim is at risk of denial, citing the specific policy sections above."
    )

    w = _get_workspace_client()
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
