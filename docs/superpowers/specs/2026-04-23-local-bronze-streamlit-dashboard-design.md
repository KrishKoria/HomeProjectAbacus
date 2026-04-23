# Local Bronze Ingestion and Streamlit Dashboard Design

Date: 2026-04-23
Status: Approved for spec review
Owner: Codex + user

## Summary

Build a temporary local analytics path while the Databricks environment is unavailable.
The solution will ingest the existing CSV datasets into a local Bronze layer on the filesystem,
materialize analyst-safe local analytics artifacts that mirror the existing week-2 analytics
shape, and expose them through a multipage Streamlit dashboard managed with `uv`.

The design intentionally keeps the storage/runtime local and temporary, but keeps the logical
table shapes aligned with the current repo's analytics intent so the later Databricks cutover
is primarily an execution-platform swap.

## Goals

- Ingest the four CSV datasets from `datasets/` into a local Bronze landing zone.
- Preserve Bronze-style audit metadata on landed records.
- Build local derived analytics artifacts that mirror the current week-2 dashboard summaries.
- Provide a multipage Streamlit dashboard for EDA, anomaly analysis, data quality, and pipeline ops.
- Keep all app-facing datasets analyst-safe and never expose `patient_id`.
- Use `uv` as the package and execution manager.
- Add enough automated validation that the temporary local workflow is reliable.

## Non-Goals

- No Databricks runtime integration in this iteration.
- No live or continuous streaming source.
- No production deployment setup.
- No authentication or multi-user access controls.
- No policy PDF ingestion in this iteration.

## Key Decisions

### Storage Approach

Use local parquet files in a medallion-style folder layout:

- `data/bronze/<dataset>/`
- `data/analytics/<artifact>/`
- `data/ops/`

This is simpler and faster to implement than adding a temporary local database while still
providing durable, query-friendly artifacts for Streamlit.

### Dashboard Data Strategy

The dashboard will not compute joins and summaries on every rerun. Instead:

1. Ingestion writes Bronze parquet.
2. A local analytics build step materializes shaped parquet artifacts.
3. Streamlit reads only those shaped outputs with caching.

This keeps the UI thin and preserves future compatibility with the existing Databricks-oriented
analytics model.

### Streamlit App Structure

Use explicit multipage navigation with `st.Page(...)` and `st.navigation(...)` rather than
implicit `pages/` discovery alone. This matches current official Streamlit guidance for
structured multipage apps and gives clearer control over grouped navigation sections.

### PHI Boundary

`patient_id` may exist in Bronze parquet for lineage and local validation, but no app-facing
analytics artifact or Streamlit page may display it. All dashboard datasets must remain
analyst-safe.

## Data Sources

The local Bronze workflow covers these four CSVs already present in the repo:

- `datasets/claims_1000.csv`
- `datasets/providers_1000.csv`
- `datasets/diagnosis.csv`
- `datasets/cost.csv`

Known characteristics from current source inspection:

- Claims has missing `procedure_code` and `billed_amount` values in some rows.
- Providers has missing `location` values in some rows.
- Diagnosis and cost are small reference tables.

## Proposed Repository Additions

### Scripts

- `scripts/local_ingest_bronze.py`
  - Reads the CSV sources.
  - Applies basic type coercion.
  - Adds local Bronze audit columns.
  - Writes parquet outputs into `data/bronze/`.
  - Writes ingest summaries into `data/ops/`.

- `scripts/build_local_analytics.py`
  - Reads local Bronze parquet.
  - Builds analyst-safe joins and summary tables.
  - Writes analytics artifacts into `data/analytics/`.
  - Writes run status and validation outputs into `data/ops/`.

### Shared Python Modules

Add small reusable modules under a local pipeline package, for example:

- `src/local_pipeline/paths.py`
- `src/local_pipeline/bronze_ingest.py`
- `src/local_pipeline/analytics_builders.py`
- `src/local_pipeline/quality_checks.py`
- `src/local_pipeline/ops_artifacts.py`

These modules keep ingestion, analytics, and app-loading logic separated and testable.

### Streamlit App

Create a dedicated app package, for example:

- `streamlit_app/Home.py`
- `streamlit_app/pages/overview.py`
- `streamlit_app/pages/trends.py`
- `streamlit_app/pages/provider_region.py`
- `streamlit_app/pages/cost_anomalies.py`
- `streamlit_app/pages/data_quality.py`
- `streamlit_app/pages/pipeline_ops.py`
- `streamlit_app/lib/data_access.py`
- `streamlit_app/lib/charts.py`
- `streamlit_app/lib/formatting.py`

The entrypoint should define navigation explicitly with grouped sections and run the selected page.

## Bronze Layer Design

### Bronze Outputs

Write one parquet dataset per source:

- `data/bronze/claims/`
- `data/bronze/providers/`
- `data/bronze/diagnosis/`
- `data/bronze/cost/`

### Bronze Audit Columns

Each landed Bronze dataset should include local audit metadata analogous to the current Bronze
contract:

- `_ingested_at`
- `_source_file`
- `_pipeline_run_id`

Optional local-only operational fields may be added if useful, such as:

- `_row_number`
- `_parse_status`
- `_parse_warning`

The local audit shape should remain simple and explicit rather than trying to mimic every
Databricks Auto Loader behavior.

### Ingestion Rules

- Fail the run if an expected source file is missing.
- Preserve raw rows in Bronze even when some business fields are missing.
- Coerce types where straightforward, but track parse/coercion problems into ops outputs.
- Record source row counts and landed row counts for each dataset.

## Local Analytics Design

### Canonical Analytics Artifacts

Materialize local parquet outputs aligned to the current week-2 analytics intent:

- `claims_provider_joined`
- `claims_diagnosis_joined`
- `claims_by_specialty_summary`
- `claims_by_region_summary`
- `high_cost_claims_summary`
- `week2_dashboard_summary`

These should be written under `data/analytics/<artifact>/`.

### Logic Alignment

The local analytics builders should mirror the repo's current analytics behavior:

- Join claims to providers by `provider_id`.
- Join claims to diagnosis by `diagnosis_code`.
- Join claims to cost benchmarks by `procedure_code` plus provider `location == region`.
- Compute high-cost claims using the existing threshold ratio of `1.5`.
- Build date-grain trend summaries from claim date.

### Analyst-Safe Output Rule

App-facing analytics artifacts must not expose `patient_id`.

Allowed fields include:

- `claim_id`
- `provider_id`
- `doctor_name`
- `specialty`
- `region`
- `procedure_code`
- `billed_amount`
- `expected_cost`
- `amount_to_benchmark_ratio`
- summary metrics and counts

## Ops and Data-Quality Artifacts

Materialize simple local ops outputs under `data/ops/` so the app can display pipeline health
without recomputing everything at runtime.

Recommended artifacts:

- `ingest_runs`
- `analytics_runs`
- `dataset_row_counts`
- `quality_metrics`
- `latest_warnings`
- `latest_failures`

These are local-file equivalents of the current Databricks-oriented operational views. They do
not need to mirror warehouse SQL exactly, but should preserve the same intent:

- what ran
- when it ran
- whether it succeeded
- which datasets had issues
- how many records were affected

## Streamlit Dashboard Design

### Navigation Structure

Use explicit grouped navigation:

- `Analytics`
  - `Overview`
  - `Trends`
  - `Provider & Region`
  - `Cost Anomalies`
- `Operations`
  - `Data Quality`
  - `Pipeline Ops`

### Page Responsibilities

#### Overview

Show the executive snapshot:

- total claims
- total billed amount
- active provider count
- high-cost claim count
- latest refresh time
- compact status summary for ingestion/build health

#### Trends

Show time-based EDA:

- claims by day
- total billed amount by day
- average billed amount by day
- high-cost claim count trend

Include date-range filtering.

#### Provider & Region

Show concentration and distribution analysis:

- claim count by specialty
- billed amount by specialty
- claim count by region
- billed amount by region
- top providers/doctors by billed amount or claim volume

#### Cost Anomalies

Show analyst-safe drilldown for benchmark exceedance:

- claim identifier
- doctor
- specialty
- region
- procedure
- billed amount
- expected cost
- billed-to-benchmark ratio

Allow filtering by ratio threshold, region, specialty, and date where available.

#### Data Quality

Show quality and reconciliation metrics:

- source rows versus Bronze rows
- Bronze rows versus summary totals
- missing value counts by field
- null provider location counts
- unmatched benchmark join counts
- parse/coercion warning counts

#### Pipeline Ops

Show local run health:

- latest ingestion run status
- latest analytics build status
- artifact freshness timestamps
- per-dataset run summaries
- latest warnings/failures

### Streamlit UX Behavior

- Use `st.set_page_config(...)` at app entry.
- Use `st.cache_data` for parquet loads and lightweight derived display transforms.
- Use sidebar controls for shared filters.
- Use `st.metric`, `st.dataframe`, and chart components suited for EDA.
- Prefer graceful empty/error states over raw tracebacks.
- Show a visible "last refreshed" indicator on every page.

## Error Handling

### Ingestion

- Missing required CSV file: stop the ingest step with a targeted error.
- Row-level coercion or parse issue: preserve the row in Bronze when possible and record a warning.
- Unreadable or malformed source file: mark dataset run as failed and do not silently continue.

### Analytics Build

- Missing upstream Bronze artifact: fail the dependent analytics artifact with a clear reason.
- Missing cost benchmark match: exclude the row from ratio-based anomaly metrics but count it in
  quality outputs.
- Partial output risk: write artifacts in a way that avoids presenting half-complete datasets as
  successful fresh outputs.

### Streamlit App

- Missing expected parquet: show an actionable message telling the user to run ingest/build first.
- Missing one page's artifact: degrade that page only; do not take down the entire app.
- All pages should expose freshness and status context so temporary local outputs can be judged
  for trustworthiness.

## Testing Strategy

### Unit Tests

- Bronze ingest helpers
  - expected file discovery
  - audit column generation
  - schema coercion behavior
  - row-count summaries

- Analytics builders
  - specialty summary output
  - region summary output
  - high-cost ratio threshold behavior
  - daily trend rollups
  - unmatched benchmark handling

### Contract Tests

- Required analytics artifact names
- Required columns in app-facing datasets
- No `patient_id` in app-facing outputs
- Expected ops artifact schema

### Smoke Tests

- `uv run python scripts/local_ingest_bronze.py`
- `uv run python scripts/build_local_analytics.py`
- Streamlit data-loading smoke test against generated parquet

### Run-Time Validation Checks

- source CSV row counts versus Bronze landed row counts
- Bronze claims counts versus downstream trend totals
- latest refresh timestamp presence on all required artifacts

## UV Project Management

Use `uv` as the authoritative dependency and execution tool.

Expected setup pattern:

- create or update `pyproject.toml`
- add runtime dependencies with `uv add`
- add dev/test dependencies with `uv add --dev`
- run scripts with `uv run ...`
- sync environments with `uv sync`

This follows current official uv guidance that `uv run` keeps the environment in sync and that
`uv sync` should be available for explicit environment synchronization.

## Documentation and Run Commands

Document the local workflow in the repo with commands equivalent to:

```bash
uv sync
uv run python scripts/local_ingest_bronze.py
uv run python scripts/build_local_analytics.py
uv run streamlit run streamlit_app/Home.py
```

Include troubleshooting notes for:

- missing files
- missing parquet outputs
- stale artifacts
- empty dashboard pages
- benchmark join gaps

## Delivery Sequence

1. Add `uv` project metadata and dependencies.
2. Implement local Bronze ingestion helpers and CLI.
3. Implement local analytics builders and ops artifacts.
4. Add automated tests for ingest/build contracts.
5. Implement the Streamlit multipage dashboard.
6. Add run documentation and verification steps.

## Risks and Mitigations

- Risk: temporary local logic drifts from future Databricks logic.
  - Mitigation: keep artifact names and summary logic aligned with existing analytics builders.

- Risk: Streamlit recomputation becomes slow or noisy.
  - Mitigation: precompute analytics parquet and cache loads with `st.cache_data`.

- Risk: PHI leaks into analyst pages during fast iteration.
  - Mitigation: enforce a no-`patient_id` contract test on all app-facing outputs.

- Risk: users misread stale local outputs as current.
  - Mitigation: expose freshness timestamps and latest run status prominently in the app.

## Acceptance Criteria

- Running the local ingest command creates parquet Bronze outputs for all four datasets.
- Running the local analytics build command creates the expected analyst-safe analytics artifacts.
- The Streamlit app launches from `uv run streamlit run ...` and renders all approved pages.
- The app shows EDA, anomaly analysis, data quality, and pipeline ops views.
- No Streamlit page exposes `patient_id`.
- Automated tests cover artifact contracts and key analytics logic.

## Notes From Current Research

- Current official Streamlit guidance supports explicit multipage navigation using `st.Page` and
  `st.navigation`.
- Current official Streamlit guidance recommends `st.cache_data` for cached data loading and
  display transformations.
- Current official uv guidance supports project-managed dependencies with `uv add`, `uv run`,
  `uv lock`, and `uv sync`.
