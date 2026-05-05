## 1. Silver cleanup — remove embedding placeholder columns

- [x] 1.1 Remove `embedding_vector` and `embedding_status` from `silver_policy_chunks.py` select list (lines 214-215, 223-224)
- [x] 1.2 Update `test_silver_contract.py` to remove assertions for `embedding_vector` and `embedding_status` (lines 183-184)
- [x] 1.3 Run Silver pipeline and contract tests to verify clean removal

## 2. XAI engine — `src/xai/`

- [x] 2.1 Create `src/xai/__init__.py` with `from __future__ import annotations` and sorted `__all__`
- [x] 2.2 Create `src/xai/feature_reasons.py` — module constant dictionary mapping each of the 20 `FEATURE_COLUMNS` to business-language description strings, with PHI-safe templates
- [x] 2.3 Create `src/xai/explainer.py` — `explain()` function that: loads model, unwraps calibration, computes SHAP values per claim, maps to business reasons via `feature_reasons.py`, returns top-N (feature, importance, reason, direction) tuples
- [x] 2.4 Write unit tests in `tests/test_xai.py`: feature mapping completeness (20 entries), SHAP computation on sample claim, direction correctness, top-N sorting, PHI absence in reason text, fewer-features-than-N edge case

## 3. RAG retrieval — `src/rag/`

- [x] 3.1 Create `src/rag/__init__.py` with `from __future__ import annotations` and sorted `__all__`
- [x] 3.2 Create `src/rag/embeddings.py` — `EmbeddingProvider` class wrapping Databricks GTE Foundation Model API: `embed_batch(texts: list[str]) -> list[list[float]]` with retry logic (3 retries, exponential backoff)
- [x] 3.3 Create `src/rag/vector_search.py` — `PolicyRetriever` class wrapping `databricks-vector-sdk`: `search(query_text, top_k=5) -> list[dict]` returning chunk text, document path, relevance score
- [x] 3.4 Create `src/rag/synthesizer.py` — `synthesize()` function: accepts SHAP reasons + policy chunks, calls Llama 70B with constrained system prompt (cite policies, no PHI, low temperature), falls back to template assembly on LLM failure
- [x] 3.5 Create `src/rag/retriever.py` — `retrieve_and_explain()` orchestrator: synthesize query from SHAP reasons → vector search → LLM synthesis → combined result dict
- [x] 3.6 Write unit tests in `tests/test_rag.py`: query PHI scrubbing, empty result handling, LLM unavailable fallback, partial output on retrieval failure

## 4. Gold embedding pipeline — `ETL/pipelines/gold/gold_policy_embeddings.py`

- [x] 4.1 Create `ETL/pipelines/gold/gold_policy_embeddings.py` — SDP `@dp.materialized_view` pipeline that: reads `healthcare.silver.policy_chunks`, filters to unembedded chunks, calls GTE batch embedding, writes to `healthcare.gold.policy_chunks` with `embedding_vector`, `embedding_status`, `embedding_model`, `embedded_at`
- [x] 4.2 Add Gold embedding pipeline config: table properties, clustering by `chunk_id`, NON-PHI sensitivity classification
- [x] 4.3 Add Gold contract test in `tests/test_gold_contract.py`: schema verification (columns, types), embedding vector dimension (768), embedding status values, incremental skip logic
- [x] 4.4 Create `src/scripts/create_vector_index.py` — CLI script to create/update the Databricks Vector Search delta-sync index on `healthcare.gold.policy_chunks`

## 5. Streamlit UI — `app_streamlit.py`

- [x] 5.1 Create `app_streamlit.py` at project root — Databricks-hosted Streamlit app with: claim_id text input, predict button, results display section
- [x] 5.2 Implement model loading via `src.ml.predict.load_from_registry()` cached in `st.session_state`
- [x] 5.3 Implement prediction display: denial probability as percentage, risk level as colored badge (green/yellow/red), latency indicator
- [x] 5.4 Implement explanation section: top-5 SHAP reasons with horizontal importance bars and directional arrows
- [x] 5.5 Implement policy section: LLM narrative at top, expandable policy snippet cards with relevance scores and document sources
- [x] 5.6 Implement error handling: claim not found, model load failure, Vector Search unavailable — each with user-friendly message and diagnostic ID
- [x] 5.7 Implement PHI safety: verify no patient identifiers rendered in explanation or policy sections

## 6. Integration testing and documentation

- [x] 6.1 Write integration test `tests/test_xai_rag_integration.py`: end-to-end flow from claim_id → prediction → SHAP explanation → RAG retrieval → combined output
- [x] 6.2 Run full test suite (`uv run pytest -q`) — verify 0 failures, existing tests unchanged
- [x] 6.3 Update `ARCHITECTURE.md` §14 (RAG Architecture) to reflect implemented state (Gold pipeline, GTE embeddings, Vector Search index)
- [x] 6.4 Update `CLAUDE.md` quick-reference with new commands: embedding pipeline run, Vector Search index creation, Streamlit launch
- [x] 6.5 Add `databricks-vector-sdk` to `pyproject.toml` ml dependencies group
