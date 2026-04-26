# Adjudication & Revenue Dashboard Design

## Context

The claims dataset and Bronze/Silver pipeline now carry synthetic adjudication and payment fields:

- `claim_status`
- `denial_reason_code`
- `allowed_amount`
- `paid_amount`
- `is_denied`
- `follow_up_required`

The current Lakeview dashboard focuses on claim volume, billed amount, data quality, provider mix, clinical mix, high-cost risk, and pipeline operations. It does not yet show the new claim outcome or reimbursement information. The approved direction is analytics-first: add reusable aggregate tables in `src/analytics/week2_analytics.py`, then update the Lakeview dashboard JSON to consume those tables.

## Goals

- Add an Adjudication & Revenue dashboard page that combines denial operations and financial yield.
- Keep the dashboard aggregate-only because adjudication labels are PHI-linked in `datasets/DATA_CLASSIFICATION.md`.
- Make the new metrics reusable and testable through analytics tables instead of embedding long dashboard SQL.
- Preserve the existing dashboard pages and add the new page without unrelated visual or navigation churn.

## Non-Goals

- No patient-level drilldowns.
- No model training or prediction changes.
- No changes to Bronze/Silver pipeline behavior unless tests reveal that an analytics input field is not available.
- No overhaul of the existing Lakeview dashboard aesthetic.

## Analytics Design

Add three analytics outputs to `src/analytics/week2_analytics.py` and include them in `DASHBOARD_SOURCE_TABLES` and `build_and_persist_week2_assets`.

### `claims_adjudication_summary`

One-row KPI table over Bronze claims:

- `total_claims`
- `approved_claims`
- `denied_claims`
- `denial_rate_pct`
- `follow_up_required_claims`
- `total_billed_amount`
- `total_allowed_amount`
- `total_paid_amount`
- `allowed_rate_pct`
- `paid_rate_pct`

This table backs KPI tiles and makes the dashboard top line independent of a large inline SQL query.

### `claims_denial_reason_summary`

Grouped by `denial_reason_code`, keeping approved rows in the `NONE` bucket so totals reconcile cleanly:

- `denial_reason_code`
- `claim_count`
- `denied_claims`
- `total_billed_amount`
- `total_allowed_amount`
- `total_paid_amount`
- `denied_billed_amount`
- `follow_up_required_claims`

This table backs the denial reason mix and the denial financial impact visuals.

### `claims_revenue_daily_summary`

Date-grain trend table:

- `claim_date`
- `total_claims`
- `approved_claims`
- `denied_claims`
- `denial_rate_pct`
- `total_billed_amount`
- `total_allowed_amount`
- `total_paid_amount`
- `follow_up_required_claims`

This table backs daily denial and payment trends.

## Dashboard Design

Add a new Lakeview page named **Adjudication & Revenue** to `src/dashboards/week2_claims_exploration.lvdash.json`.

The page contains:

- KPI tiles for denied claims, denial rate, follow-up required, billed amount, allowed amount, paid amount, and paid rate.
- A denial reason chart showing count by `denial_reason_code`.
- A denial financial impact chart showing denied billed amount by reason.
- A grouped bar visual comparing billed, allowed, and paid totals.
- A daily trend visual for denied claims and paid amount by date.

Existing pages remain intact. If the dashboard needs new datasets, those datasets should be short `SELECT` queries against the new analytics tables.

## Privacy And Governance

The dashboard must not expose `claim_id`, `patient_id`, or row-level adjudication details. All new visuals use aggregate counts, rates, and amounts. The analytics tables are minimum-necessary reporting outputs and should use the same `MINIMUM-NECESSARY` readiness log pattern as existing Week 2 analytics assets.

## Testing

Update `tests/test_observability_contract.py` so `DASHBOARD_SOURCE_TABLES` includes the new analytics tables. Add focused contract coverage for the exported builder functions if the existing pattern requires it.

Run the existing test suite after implementation:

```bash
rtk pytest -q
```

If dashboard JSON manipulation is automated, validate that the JSON parses successfully after edits.
