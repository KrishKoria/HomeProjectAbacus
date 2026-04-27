# Week 3 Silver Validation Runbook

## Purpose

Use this runbook after the Week 3 Silver pipeline refresh completes. The goal is to confirm that:

- trusted Silver tables were created under `healthcare.silver`
- invalid rows were redirected into `healthcare.quarantine`
- week 3 observability tables were materialized under `healthcare.analytics`
- no PHI values are required in logs or summaries to diagnose data quality issues

The companion notebook is `src/notebooks/silver_validation.ipynb`.

## Expected outputs

The notebook builds and displays:

- `healthcare.analytics.ops_silver_table_status`
- `healthcare.analytics.ops_quarantine_summary`

These depend on the following Week 3 tables already existing:

- `healthcare.silver.claims`
- `healthcare.silver.providers`
- `healthcare.silver.diagnosis`
- `healthcare.silver.cost`
- `healthcare.silver.policy_chunks`
- `healthcare.quarantine.claims`
- `healthcare.quarantine.providers`
- `healthcare.quarantine.diagnosis`
- `healthcare.quarantine.cost`
- `healthcare.quarantine.policy_chunks`

## Notebook inputs

The notebook exposes four widgets:

- `catalog` default `healthcare`
- `silver_schema` default `silver`
- `quarantine_schema` default `quarantine`
- `analytics_schema` default `analytics`

Keep the defaults unless your workspace uses a different governed namespace.

## Validation checks

Run all cells and confirm:

1. `ops_silver_table_status` shows a row for all five datasets.
2. `silver_row_count` is non-zero for the datasets you ingested.
3. `quarantine_row_count` is expected for the known synthetic issues:
   - claims: rows with missing `diagnosis_code`
   - providers: rows with missing `provider_id` or `doctor_name` only
   - policy chunks: unreadable or textless PDFs only
4. `ops_quarantine_summary` contains `diagnostic_id` and `rule_name` values, but no patient identifiers, dates, or billed amounts in `status_message`.

## Known baseline for bundled CSV seed data

With the checked-in synthetic datasets, the raw Bronze profiling baseline is:

- `claims_1000.csv`
  - 307 missing `diagnosis_code`
  - 241 missing `procedure_code`
  - 343 missing `billed_amount`
- `providers_1000.csv`
  - 4 missing `location`

Expected Week 3 behavior:

- missing `procedure_code` and `billed_amount` remain in trusted `silver.claims` with `_data_quality_flags`
- missing `diagnosis_code` is quarantined from trusted `silver.claims`
- missing provider `location` is imputed to `"Unknown"` in trusted `silver.providers`

## Troubleshooting

### `ops_silver_table_status` is empty

The silver/quarantine tables were not created yet, or the notebook is pointed at the wrong catalog/schema values.

### `policy_chunks` quarantine spikes unexpectedly

Check that the Databricks runtime has `pdfplumber` available. This project adds it to `requirements.txt`, but the workspace runtime still needs that dependency installed for the policy extraction UDF.

### PHI appears in a summary table

This is a bug. Week 3 status messages must only contain dataset names, rule names, diagnostic IDs, table names, and counts. Remove any raw claim/provider/patient field values from the emitted strings before promoting the pipeline.
