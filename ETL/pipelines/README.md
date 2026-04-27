# ETL Pipeline Deployment

The `ETL/common` package is a compatibility layer over the canonical `src.common`
modules. Databricks deployments must include the repository root on `PYTHONPATH`
so imports such as `from src.common.silver_pipeline_config import ...` resolve
when pipeline files import `common.*`.

For Databricks Repos, keep both `ETL/` and `src/` in the synced repository. For
asset-bundle or file-only deployments, package `src/` beside `ETL/` or configure
the pipeline cluster/init script to add the project root to `PYTHONPATH`.
