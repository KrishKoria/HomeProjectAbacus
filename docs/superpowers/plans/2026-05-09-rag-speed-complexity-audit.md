# RAG Module Speed & Complexity Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate duplicated code, client-creation overhead, and redundant computation across `src/rag/` — 5 files, ~5 lines net change.

**Architecture:** Surgical refactoring — no new files, no API surface changes. Three categories: DRY (PHI patterns into policy_labels.py), caching (WorkspaceClient, EmbeddingProvider, VectorSearchClient), dedup (result row builder, double normalization pass).

**Tech Stack:** Python 3.10+, Databricks SDK, databricks-vector-sdk

---

### Task 1: DRY PHI Patterns — Move to policy_labels.py

**Files:**
- Modify: `src/rag/policy_labels.py:1-49`
- Modify: `src/rag/retriever.py:14-30`
- Modify: `src/rag/synthesizer.py:109-125`

- [ ] **Step 1: Add shared PHI patterns and `_scrub_phi` to policy_labels.py**

Add after the `_UNKNOWN_POLICY_LABEL` line (after line 5) and before `policy_display_name`:

```python
import re

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
```

Update `__all__` to include `"_scrub_phi"`:
```python
__all__ = [
    "_scrub_phi",
    "policy_display_name",
    "policy_excerpt_label",
    "policy_reference_label",
]
```

- [ ] **Step 2: Remove duplicated code from retriever.py**

Remove lines 14-30 (`_PHI_STRIP_PATTERNS` and `_scrub_phi` function).

Add import at top (after `from src.rag.vector_search import PolicyRetriever`):
```python
from src.rag.policy_labels import _scrub_phi
```

The `_scrub_phi(...)` call on line 66 stays unchanged — same function, different import.

- [ ] **Step 3: Remove duplicated code from synthesizer.py**

Remove the inner `_strip_phi` function definition (lines 114-125 inside `_synthesize_via_template`).

The import is already partially set — add to existing `from src.rag.policy_labels import policy_reference_label`:
```python
from src.rag.policy_labels import _scrub_phi, policy_reference_label
```

Replace `_strip_phi(r['reason'])` on line 128 with `_scrub_phi(r['reason'])`.

- [ ] **Step 4: Run tests to verify**

```bash
uv run pytest -q tests/test_rag.py::PhiScrubbingTests tests/test_rag.py::PolicyLabelTests tests/test_rag.py::SynthesizerTests -v
```

Expected: All pass. The PHI scrubbing tests confirm `_scrub_phi` works from its new location.

- [ ] **Step 5: Commit**

```bash
git add src/rag/policy_labels.py src/rag/retriever.py src/rag/synthesizer.py
git commit -m "refactor(rag): extract shared PHI patterns into policy_labels.py"
```

---

### Task 2: Cache WorkspaceClient — Lazy Singleton in 3 Modules

**Files:**
- Modify: `src/rag/embeddings.py:83-129`
- Modify: `src/rag/vector_search.py:189-192`
- Modify: `src/rag/synthesizer.py:51-106`

- [ ] **Step 1: Add lazy singleton to embeddings.py**

Add after the `logger` line (after line 7), before the `EmbeddingProvider` class:

```python
import math

_WORKSPACE_CLIENT: Any = None


def _get_workspace_client() -> Any:
    global _WORKSPACE_CLIENT
    if _WORKSPACE_CLIENT is None:
        from databricks.sdk import WorkspaceClient

        _WORKSPACE_CLIENT = WorkspaceClient()
    return _WORKSPACE_CLIENT
```

In `_call_endpoint` (line 95-96), replace:
```python
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise _SdkUnavailableError(
                "databricks-sdk not available; using fallback embeddings"
            )

        w = WorkspaceClient()
```

With:
```python
        try:
            from databricks.sdk import WorkspaceClient  # noqa: F811
        except ImportError:
            raise _SdkUnavailableError(
                "databricks-sdk not available; using fallback embeddings"
            )

        w = _get_workspace_client()
```

- [ ] **Step 2: Add lazy singleton to vector_search.py**

Add after the `_QUERY_TEXT_SUPPORT_CACHE` line (after line 12):

```python
_WORKSPACE_CLIENT: Any = None


def _get_workspace_client() -> Any:
    global _WORKSPACE_CLIENT
    if _WORKSPACE_CLIENT is None:
        from databricks.sdk import WorkspaceClient

        _WORKSPACE_CLIENT = WorkspaceClient()
    return _WORKSPACE_CLIENT
```

In `_workspace_query_index` (line 192), replace:
```python
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
```

With:
```python
    w = _get_workspace_client()
```

- [ ] **Step 3: Add lazy singleton to synthesizer.py**

Add after the `logger` line (after line 8), before `_SYSTEM_PROMPT`:

```python
_WORKSPACE_CLIENT: Any = None


def _get_workspace_client() -> Any:
    global _WORKSPACE_CLIENT
    if _WORKSPACE_CLIENT is None:
        from databricks.sdk import WorkspaceClient

        _WORKSPACE_CLIENT = WorkspaceClient()
    return _WORKSPACE_CLIENT
```

In `_synthesize_via_llm` (line 80), replace:
```python
    w = WorkspaceClient()
```

With:
```python
    w = _get_workspace_client()
```

- [ ] **Step 4: Run tests to verify**

```bash
uv run pytest -q tests/test_rag.py::EmbeddingProviderTests tests/test_rag.py::PolicyRetrieverTests tests/test_rag.py::SynthesizerTests -v
```

Expected: All pass. The tests mock WorkspaceClient via `sys.modules`, so the lazy singleton doesn't interfere.

- [ ] **Step 5: Commit**

```bash
git add src/rag/embeddings.py src/rag/vector_search.py src/rag/synthesizer.py
git commit -m "perf(rag): reuse WorkspaceClient via lazy singleton in embeddings, vector_search, synthesizer"
```

---

### Task 3: Cache EmbeddingProvider & VectorSearchClient

**Files:**
- Modify: `src/rag/vector_search.py:30-56,136-152`

- [ ] **Step 1: Cache VectorSearchClient in `_vector_search_client()`**

Add after the `_QUERY_TEXT_SUPPORT_CACHE` line (after line 12):

```python
_VECTOR_SEARCH_CLIENT: Any = None
```

Modify `_vector_search_client()` — wrap the three return paths with caching. At the top of the function (after `def _vector_search_client():`), add:

```python
    global _VECTOR_SEARCH_CLIENT
    if _VECTOR_SEARCH_CLIENT is not None:
        return _VECTOR_SEARCH_CLIENT
```

And before each `return VectorSearchClient(...)` (3 call sites — lines 36, 45, 56), set the global:

```python
        _VECTOR_SEARCH_CLIENT = VectorSearchClient(...)
        return _VECTOR_SEARCH_CLIENT
```

- [ ] **Step 2: Cache EmbeddingProvider in `_generate_query_embedding()`**

Add after line 134 (after `_EMBEDDING_DIM = 1024`):

```python
_EMBEDDING_PROVIDER: Any = None
```

Modify `_generate_query_embedding()` — replace the body:

```python
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
```

- [ ] **Step 3: Run tests to verify**

```bash
uv run pytest -q tests/test_rag.py::PolicyRetrieverTests -v
```

Expected: All pass (8 tests). Tests patch `_vector_search_client`, `_generate_query_embedding`, and `_workspace_query_index` — caches reset via existing patch patterns.

- [ ] **Step 4: Commit**

```bash
git add src/rag/vector_search.py
git commit -m "perf(rag): cache VectorSearchClient and EmbeddingProvider instances"
```

---

### Task 4: Deduplicate Result Row Building + Remove Double Normalization

**Files:**
- Modify: `src/rag/vector_search.py:218-238,330-346,348-367,274`

- [ ] **Step 1: Extract `_build_result_row()` helper**

Add after `_extract_relevance_score_kind` (after line 125):

```python
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
```

- [ ] **Step 2: Replace workspace path row-building**

In `_workspace_query_index`, replace lines 218-238 (the `rows.append(...)` with dict literal):

```python
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
```

- [ ] **Step 3: Replace SDK fallback path row-building**

In `PolicyRetriever._query_index`, replace lines 330-346 (the `rows.append(...)` with dict literal):

```python
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
```

- [ ] **Step 4: Remove dual-pass normalization from `search()`**

In `PolicyRetriever.search()`, change line 274:

```python
            return self._normalize_results(results)
```
→
```python
            return results
```

Note: `_normalize_results` static method stays — it's used directly in tests and could be used by external callers.

- [ ] **Step 5: Run tests to verify**

```bash
uv run pytest -q tests/test_rag.py::PolicyRetrieverTests -v
```

Expected: All pass (8 tests). The `_normalize_results` tests still exercise the static method directly.

- [ ] **Step 6: Commit**

```bash
git add src/rag/vector_search.py
git commit -m "refactor(rag): extract _build_result_row helper, remove duplicate normalization pass"
```

---

### Task 5: Minor Cleanup — NaN Check

**Files:**
- Modify: `src/rag/vector_search.py:90`

- [ ] **Step 1: Add `import math` at top of file**

Add `import math` to the imports at lines 1-5:
```python
from __future__ import annotations

import logging
import math
import os
from typing import Any
```

- [ ] **Step 2: Replace NaN check in `_coerce_optional_float`**

Line 90 — change:
```python
    if numeric != numeric:
```
To:
```python
    if math.isnan(numeric):
```

- [ ] **Step 3: Run tests to verify**

```bash
uv run pytest -q tests/test_rag.py::PolicyRetrieverTests -v
```

Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add src/rag/vector_search.py
git commit -m "refactor(rag): use math.isnan for NaN check readability"
```

---

### Task 6: Final Verification

- [ ] **Step 1: Run full RAG test suite**

```bash
uv run pytest -q tests/test_rag.py tests/test_xai_rag_integration.py -v
```

Expected: All 49 tests pass.

- [ ] **Step 2: Check no import cycles or lint errors**

```bash
uv run python -c "from src.rag import EmbeddingProvider, PolicyRetriever, retrieve_and_explain, synthesize; print('All imports OK')"
```

Expected: `All imports OK`
