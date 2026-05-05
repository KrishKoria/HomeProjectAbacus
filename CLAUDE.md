# homeprojectabacus — Claude Code Quick Reference

## Week 5+6: XAI + RAG

### Embedding Pipeline
```bash
# Run Gold embedding pipeline on Databricks
databricks bundle run gold_policy_embeddings

# Check embedding status
databricks sql -q "SELECT embedding_status, COUNT(*) FROM healthcare.gold.policy_chunks GROUP BY 1"
```

### Vector Search Index
```bash
# Create/update the Vector Search delta-sync index
python src/scripts/create_vector_index.py

# Dry-run (validate config only)
python src/scripts/create_vector_index.py --dry-run

# Custom endpoint name
python src/scripts/create_vector_index.py --endpoint-name my-custom-index
```

### Streamlit UI
```bash
# Launch on Databricks workspace
streamlit run app_streamlit.py

# Local development (no Databricks — fallback behavior)
streamlit run app_streamlit.py --server.headless true
```

### Testing
```bash
# XAI unit tests
uv run pytest tests/test_xai.py -q

# RAG unit tests
uv run pytest tests/test_rag.py -q

# Integration tests
uv run pytest tests/test_xai_rag_integration.py -q

# Gold embedding contract tests
uv run pytest tests/test_gold_policy_embeddings_contract.py -q

# Full suite
uv run pytest tests/ -q
```

### Module Layout
```
src/xai/          SHAP explanations + business-reason mapping
src/rag/          GTE embeddings + Vector Search + Llama 70B synthesis
app_streamlit.py  Databricks-hosted Streamlit UI
ETL/pipelines/gold/gold_policy_embeddings.py  SDP embedding pipeline
src/scripts/create_vector_index.py  Vector Search index CLI
```
