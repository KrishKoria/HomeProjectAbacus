-- Week 2 Databricks AI/BI starter dashboard queries.
-- Keep dashboard SQL thin: query already-shaped analytics tables only.

SELECT claim_date, total_claims, active_provider_count, total_billed_amount, avg_billed_amount, high_cost_claim_count
FROM healthcare.analytics.week2_dashboard_summary
ORDER BY claim_date;

SELECT specialty, claim_count, provider_count, avg_billed_amount, total_billed_amount
FROM healthcare.analytics.claims_by_specialty_summary
ORDER BY claim_count DESC, specialty ASC;

SELECT region, claim_count, provider_count, avg_billed_amount, total_billed_amount
FROM healthcare.analytics.claims_by_region_summary
ORDER BY claim_count DESC, region ASC;

SELECT claim_id, provider_id, doctor_name, specialty, region, procedure_code, billed_amount, expected_cost, amount_to_benchmark_ratio
FROM healthcare.analytics.high_cost_claims_summary
ORDER BY amount_to_benchmark_ratio DESC, claim_id ASC;

SELECT timestamp, update_id, pipeline_id, update_state, actor
FROM healthcare.analytics.ops_pipeline_updates
ORDER BY timestamp DESC;

SELECT timestamp, update_id, dataset, expectation, passed_records, failed_records
FROM healthcare.analytics.ops_expectation_metrics
ORDER BY timestamp DESC;

SELECT timestamp, actor, event_type, action, pipeline_id
FROM healthcare.analytics.ops_user_actions
ORDER BY timestamp DESC;

SELECT timestamp, dataset, update_id, platform_error_code, diagnostic_id, error_message
FROM healthcare.analytics.ops_latest_failures
ORDER BY timestamp DESC;
