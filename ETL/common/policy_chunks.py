from __future__ import annotations

from io import BytesIO
from typing import Final


TOKEN_SPLIT_DELIMITERS: Final[tuple[str, ...]] = ("\r", "\n", "\t")


def extract_pdf_text(pdf_bytes: bytes | None) -> str | None:
    """Extract text from PDF bytes with pdfplumber."""
    if not pdf_bytes:
        return None

    import pdfplumber

    page_text: list[str] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            extracted = page.extract_text() or ""
            normalized = extracted.strip()
            if normalized:
                page_text.append(normalized)

    if not page_text:
        return None
    return "\n".join(page_text)


def normalize_policy_text(text: str | None) -> str | None:
    """Collapse whitespace while preserving paragraph-like breaks."""
    if text is None:
        return None
    normalized = text
    for delimiter in TOKEN_SPLIT_DELIMITERS:
        normalized = normalized.replace(delimiter, " ")
    normalized = " ".join(normalized.split())
    return normalized or None


def chunk_policy_text(
    text: str | None,
    chunk_size_tokens: int,
    overlap_tokens: int,
) -> list[dict[str, object]]:
    """Split normalized text into token-count chunks with fixed overlap."""
    normalized = normalize_policy_text(text)
    if normalized is None:
        return []

    tokens = normalized.split(" ")
    if not tokens:
        return []

    step = max(1, chunk_size_tokens - overlap_tokens)
    chunks: list[dict[str, object]] = []
    chunk_index = 0

    for start_index in range(0, len(tokens), step):
        token_slice = tokens[start_index:start_index + chunk_size_tokens]
        if not token_slice:
            continue
        chunks.append(
            {
                "chunk_index": chunk_index,
                "chunk_text": " ".join(token_slice),
                "token_count": len(token_slice),
            }
        )
        chunk_index += 1
        if start_index + chunk_size_tokens >= len(tokens):
            break

    return chunks


__all__ = [
    "chunk_policy_text",
    "extract_pdf_text",
    "normalize_policy_text",
]
