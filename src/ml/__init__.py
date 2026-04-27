from __future__ import annotations

FEATURE_COLUMNS = (
    "is_procedure_missing",
    "is_amount_missing",
    "amount_to_benchmark_ratio",
    "billed_vs_avg_cost",
    "high_cost_flag",
    "severity_procedure_mismatch",
    "specialty_diagnosis_mismatch",
    "provider_location_missing",
    "diagnosis_severity_encoded",
    "diagnosis_count",
    "provider_claim_count",
    "provider_claim_count_30d",
    "provider_risk_score",
)

TARGET_COLUMN = "denial_label"

__all__ = ["FEATURE_COLUMNS", "TARGET_COLUMN"]