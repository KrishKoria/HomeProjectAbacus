from __future__ import annotations

import re

_UNKNOWN_POLICY_LABEL = "Unknown Policy"

_PHI_STRIP_PATTERNS = [
    r"\b\d{3}-\d{2}-\d{4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{2}/\d{2}/\d{4}\b",
    r"\$\d[\d,.]*\b",
    r"\b(?:PAT|PT|MRN)[-_ ]?\d+\b",
]


def _scrub_phi(text: str) -> str:
    """Remove PHI-like patterns from a string."""
    cleaned = text
    for pattern in _PHI_STRIP_PATTERNS:
        cleaned = re.sub(pattern, "[REDACTED]", cleaned, flags=re.IGNORECASE)
    return cleaned


def policy_display_name(document_path: str | None) -> str:
    """Return a human-readable policy label from a stored document path."""
    if not document_path:
        return _UNKNOWN_POLICY_LABEL

    normalized = str(document_path).strip().replace("\\", "/")
    if not normalized:
        return _UNKNOWN_POLICY_LABEL

    name = normalized.rsplit("/", maxsplit=1)[-1] or normalized
    if name.lower().endswith(".pdf"):
        name = name[:-4]

    cleaned = re.sub(r"[_-]+", " ", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return _UNKNOWN_POLICY_LABEL

    return cleaned.title()


def policy_excerpt_label(chunk_index: object) -> str:
    """Return a stable excerpt label for a policy chunk index."""
    try:
        index = int(chunk_index)
    except (TypeError, ValueError):
        index = 0
    if index < 0:
        index = 0
    return f"Excerpt {index + 1}"


def policy_reference_label(document_path: str | None, chunk_index: object) -> str:
    """Return a combined policy and excerpt label."""
    return f"{policy_display_name(document_path)}, {policy_excerpt_label(chunk_index)}"


__all__ = [
    "_scrub_phi",
    "policy_display_name",
    "policy_excerpt_label",
    "policy_reference_label",
]
