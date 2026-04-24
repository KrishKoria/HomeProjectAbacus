# Local Bronze and Streamlit Dashboard Runbook

This runbook covers the temporary local workflow while Databricks is not yet available.

## Commands

Run these commands from the project root:

```bash
uv sync
uv run python scripts/local_ingest_bronze.py
uv run python scripts/build_local_analytics.py
uv run streamlit run streamlit_app/Home.py
```

## What Each Step Does

- `uv sync`
  Installs the locked project environment and development dependencies.
- `uv run python scripts/local_ingest_bronze.py`
  Reads the CSVs in `datasets/` and lands local Bronze parquet artifacts into `data/bronze/`.
- `uv run python scripts/build_local_analytics.py`
  Builds local analytics and ops parquet artifacts into `data/analytics/` and `data/ops/`.
- `uv run streamlit run streamlit_app/Home.py`
  Starts the multipage local dashboard.

## Generated Directories

- `data/bronze/`
- `data/analytics/`
- `data/ops/`

These directories are local runtime artifacts and are ignored by git.

## Troubleshooting

### Missing source file

Symptom:
- Bronze ingest fails with a missing dataset error.

Fix:
- Verify the expected CSV exists under `datasets/`.
- Re-run `uv run python scripts/local_ingest_bronze.py`.

### Bronze artifacts missing

Symptom:
- Analytics build fails because a Bronze artifact does not exist.

Fix:
- Run the Bronze ingest step first.
- Confirm files exist under `data/bronze/<dataset>/`.

### Empty dashboard page

Symptom:
- A Streamlit page shows an empty or missing-artifact message.

Fix:
- Re-run the ingest and analytics commands in order.
- Check `data/ops/ingest_runs.parquet` and `data/ops/analytics_runs.parquet`.

### Stale outputs

Symptom:
- Dashboard timestamps do not match the latest local run.

Fix:
- Re-run both pipeline commands.
- Refresh the Streamlit page after the analytics build finishes.

### Benchmark join gaps

Symptom:
- The anomalies page shows fewer claims than expected.

Fix:
- Check `data/ops/quality_metrics.parquet` for `benchmark_join_missing`.
- Check `data/ops/latest_warnings.parquet` for the latest exclusion summary.

### Local quality visibility

Useful ops artifacts:

- `data/ops/dataset_row_counts.parquet`
- `data/ops/quality_metrics.parquet`
- `data/ops/latest_warnings.parquet`
- `data/ops/latest_failures.parquet`
- `data/ops/artifact_inventory.parquet`
