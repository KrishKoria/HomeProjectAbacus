# RAG Module Speed & Complexity Audit — Design Spec

## Summary

Surgical refactoring of `src/rag/` to eliminate duplicated code, client-creation overhead,
and redundant computation. 5 files changed, ~5 lines net change. All 65 existing tests
pass before and after.

## Changes

### 1. DRY PHI patterns → `policy_labels.py`

**Files:**
- `src/rag/policy_labels.py` — Add `_PHI_STRIP_PATTERNS` constant and `_scrub_phi()` function
- `src/rag/retriever.py` — Remove duplicated `_PHI_STRIP_PATTERNS` and `_scrub_phi`, import from `policy_labels`
- `src/rag/synthesizer.py` — Remove duplicated inner `_strip_phi()`, import `_scrub_phi` from `policy_labels`

### 2. WorkspaceClient lazy singleton (3 modules)

**Files:**
- `src/rag/embeddings.py` — Add `_get_workspace_client()` lazy singleton
- `src/rag/vector_search.py` — Same
- `src/rag/synthesizer.py` — Same

### 3. Cache EmbeddingProvider & VectorSearchClient

**Files:**
- `src/rag/vector_search.py` — Add module-level caching for both `_generate_query_embedding` and `_vector_search_client`

### 4. Deduplicate result row building

**Files:**
- `src/rag/vector_search.py` — Extract `_build_result_row()` helper, replace duplicated dict-building in both call sites

### 5. Remove dual-pass normalization

**Files:**
- `src/rag/vector_search.py` — Remove `_normalize_results()` call in `search()`, results are already formatted

### 6. Minor cleanups

**Files:**
- `src/rag/vector_search.py` — `math.isnan()` instead of `numeric != numeric`
