# Weeks 1-3 Completion Status

This status note captures the current boundary after the hardening pass. Weeks 1-3 are the active scope. Gold, ML, production RAG, FastAPI, and agent workflows are future roadmap items unless explicitly listed here as prerequisites or design references.

## Scope Boundary

| Area | Status | Evidence state |
|---|---|---|
| Week 1 Bronze ingestion | Code-ready | Spark Declarative Pipeline source exists for CSV datasets and policy PDFs; local Bronze fallback works. Databricks runtime evidence still needs to be collected. |
| Bronze governance contract | Code-ready | Shared table properties use `interval 2190 days` for both Delta log and deleted-file retention. |
| Week 1 RBAC and profiling | Code-ready | Bootstrap, verification, RBAC, and profiling notebooks/runbooks exist. Workspace run proof is pending. |
| Week 2 analytics | Code-ready | Databricks analytics SQL/Python assets and local analytics builders exist; local smoke coverage is available. |
| Week 2 dashboard | Local workflow only | Streamlit dashboard lives under `local_dev/streamlit_app`; this remains a temporary local fallback, not the production UI architecture. |
| Observability | Code-ready | Analytics/ops table builders and helper contracts exist for pipeline updates, expectation metrics, user actions, and latest failures. |
| Week 3 Silver and quarantine | Code-ready | Silver trusted tables, quarantine tables, policy chunking, and validation notebook/runbook exist. Databricks runtime evidence is pending. |
| Tests | Hardened locally | Root tests and explicit `local_dev/tests` are the verification targets for this pass. |

## Local Commands

Run these commands from the project root:

```bash
uv sync
uv run python local_dev/scripts/local_ingest_bronze.py
uv run python local_dev/scripts/build_local_analytics.py
uv run streamlit run local_dev/streamlit_app/Home.py
```

## Verification Commands

Root contract tests:

```bash
rtk uv run pytest -q
```

Explicit local workflow tests:

```bash
rtk uv run pytest local_dev/tests -q
```

Local smoke path:

```bash
rtk uv run python -X utf8 -c "from local_dev.local_pipeline.bronze_ingest import run_local_bronze_ingest; from local_dev.local_pipeline.analytics_builders import run_local_analytics_build; import pathlib, tempfile; root = pathlib.Path(tempfile.mkdtemp()) / 'data'; print(run_local_bronze_ingest(data_root=root)); print(run_local_analytics_build(data_root=root))"
```

## Future Roadmap

The following remain out of scope for the current hardening pass:

- Week 4 Gold feature store and business-ready claim feature tables.
- ML model training, MLflow registration, SHAP explanations, and serving endpoints.
- Production RAG retrieval, vector search indexing, and LLM explanation generation.
- FastAPI claim validation API and production backend orchestration.
- Agent-generated remediation workflows.

The policy PDF Bronze ingestion and Silver policy chunking code are prerequisites for later RAG work, but they do not constitute a production RAG implementation.
