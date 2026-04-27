# Databricks Weeks 1-3 Evidence Checklist

Use this checklist when Databricks workspace validation is ready. This pass prepares the evidence plan only; it does not require running the workspace now.

## Bronze Pipeline Run

- [ ] Bootstrap notebook `src/notebooks/bootstrap_bronze_landing.py` completed for catalog `healthcare`, schema `bronze`, volume `raw_landing`.
- [ ] CSV files uploaded to `/Volumes/healthcare/bronze/raw_landing/{claims,providers,diagnosis,cost}/` with expected filenames.
- [ ] Policy PDFs uploaded to `/Volumes/healthcare/bronze/raw_landing/policies/` when policy ingestion is included.
- [ ] Bronze SDP pipeline started with all source files under `src/pipelines/bronze/`.
- [ ] Bronze tables exist for `claims`, `providers`, `diagnosis`, `cost`, and `policies`.
- [ ] Row counts match source baselines: claims 1000, providers 21, diagnosis 6, cost 6.
- [ ] Audit columns `_ingested_at`, `_source_file`, and `_pipeline_run_id` are populated.
- [ ] Delta properties show `delta.enableChangeDataFeed = true`.
- [ ] Delta properties show `delta.logRetentionDuration = interval 2190 days`.
- [ ] Delta properties show `delta.deletedFileRetentionDuration = interval 2190 days`.

## RBAC Verification

- [ ] `src/notebooks/bronze_verify_and_rbac.ipynb` completed without failed checks.
- [ ] Analyst/read principal can `SELECT` from allowed Bronze tables.
- [ ] Analyst/read principal cannot mutate Bronze tables.
- [ ] ETL/write principal has the required write privileges for ingestion paths.
- [ ] Grant statements and tested principal names are recorded in the evidence notes.

## Profiling

- [ ] `src/notebooks/bronze_profiling.ipynb` completed.
- [ ] Missing-value baselines are recorded for claims and providers.
- [ ] Known provider `location` nulls are confirmed as expected Bronze warnings.
- [ ] Overbilling or benchmark anomaly outputs are captured without exposing PHI in logs.

## Week 2 Analytics

- [ ] Week 2 analytics tables/views were built under the expected analytics schema.
- [ ] Claim-provider and claim-diagnosis joins produce non-empty outputs.
- [ ] Specialty, region, high-cost, and dashboard summary outputs exist.
- [ ] Dashboard-facing outputs contain no `patient_id` column.
- [ ] Any benchmark join gaps are summarized through ops metrics rather than raw PHI logs.

## Observability Tables

- [ ] `healthcare.analytics.ops_pipeline_updates` exists and has recent pipeline rows.
- [ ] `healthcare.analytics.ops_expectation_metrics` exists and captures expectation results.
- [ ] `healthcare.analytics.ops_user_actions` exists or is explicitly deferred until UI auth exists.
- [ ] `healthcare.analytics.ops_latest_failures` exists and uses diagnostic IDs such as `CLAIMOPS-OBS-401`.
- [ ] Observability rows do not include patient identifiers, dates, billed amounts, or raw source payloads.

## Silver and Quarantine Outputs

- [ ] Silver tables exist for `claims`, `providers`, `diagnosis`, `cost`, and `policy_chunks`.
- [ ] Quarantine tables exist for `claims`, `providers`, `diagnosis`, `cost`, and `policy_chunks`.
- [ ] Trusted `silver.claims` excludes rows with missing critical `diagnosis_code`.
- [ ] Missing `procedure_code` and `billed_amount` remain in trusted `silver.claims` with quality flags.
- [ ] Missing provider `location` is imputed to `Unknown` in trusted `silver.providers`.
- [ ] Quarantine outputs include rule names and diagnostic IDs without raw PHI in status messages.

## Week 3 Quality Tables

- [ ] `src/notebooks/silver_validation.ipynb` completed.
- [ ] `healthcare.analytics.ops_silver_table_status` includes all five Week 3 datasets.
- [ ] `healthcare.analytics.ops_quarantine_summary` includes expected quarantine counts.
- [ ] Silver row counts and quarantine row counts are captured in the evidence notes.
- [ ] Policy chunking dependency status for `pdfplumber` is recorded.

## Evidence Capture

- [ ] Save notebook run URLs or exported notebook outputs.
- [ ] Save table row-count query results.
- [ ] Save screenshots or query output for RBAC allow/deny checks.
- [ ] Save TBLPROPERTIES output proving `interval 2190 days`.
- [ ] Record Databricks workspace, catalog, schema, pipeline ID, and validation date.
