from __future__ import annotations

from typing import Final

FEATURE_REASONS: Final[dict[str, str]] = {
    "is_procedure_missing": (
        "The procedure code for this claim was missing in the submitted data. "
        "Claims with unreported procedures are flagged more often during utilization review "
        "because the medical necessity rationale cannot be verified."
    ),
    "is_amount_missing": (
        "The billed amount was absent from the claim. Missing financial fields correlate "
        "with incomplete submissions that are returned or denied during adjudication."
    ),
    "amount_to_benchmark_ratio": (
        "The billed amount relative to the procedure-specific benchmark was elevated. "
        "Claims substantially above the peer-group reference price face additional scrutiny "
        "and a higher probability of denial."
    ),
    "billed_vs_avg_cost": (
        "The billed amount was compared with the average cost for this procedure in the "
        "provider's peer group. Large positive deviations increase denial risk."
    ),
    "high_cost_flag": (
        "This claim exceeded the high-cost threshold for the procedure category. "
        "High-cost claims undergo mandatory secondary review."
    ),
    "severity_procedure_mismatch": (
        "The diagnosis severity level did not align with the billed procedure. "
        "When a low-acuity diagnosis is paired with a high-complexity procedure, "
        "medical-necessity denials are more frequent."
    ),
    "specialty_diagnosis_mismatch": (
        "The provider specialty did not match the diagnosis category. "
        "Specialty-diagnosis alignment is a common medical-necessity gate."
    ),
    "provider_location_missing": (
        "The provider location was not recorded. Missing geography data prevents "
        "regional benchmarking and is treated as a risk factor."
    ),
    "diagnosis_severity_encoded": (
        "The encoded diagnosis severity contributed to the risk assessment. "
        "Higher severity diagnoses typically support more complex procedures."
    ),
    "diagnosis_count": (
        "The number of diagnoses on the claim affected the risk score. "
        "A higher count of distinct diagnoses may indicate more complex cases."
    ),
    "provider_claim_count": (
        "The provider's total claim volume was factored in. Providers with very low "
        "claim counts tend to have less stable denial-rate estimates."
    ),
    "provider_claim_count_30d": (
        "The number of claims filed by this provider in the trailing 30-day window "
        "was used to assess recent utilization patterns."
    ),
    "provider_risk_score": (
        "This provider's historical risk score exceeded the baseline. The score "
        "aggregates denial frequency, charge outliers, and practice-pattern metrics."
    ),
    "provider_claim_count_60d": (
        "The provider's claim count over the trailing 60 days was included as a "
        "medium-term utilization signal."
    ),
    "provider_claim_count_90d": (
        "The provider's claim count over the trailing 90 days provides a longer "
        "utilization trend for risk assessment."
    ),
    "cost_overbenchmark_and_highseverity": (
        "This claim was flagged at the intersection of high cost and high severity. "
        "The combination triggers the most intensive manual-review protocols."
    ),
    "mismatch_and_overbenchmark": (
        "The claim combines a specialty-diagnosis mismatch with above-benchmark charges. "
        "Dual misalignment is a strong denial predictor."
    ),
    "provider_30d_denial_rate": (
        "This provider's recent 30-day denial rate was elevated. Providers with high "
        "short-term denial rates face closer claims review."
    ),
    "missing_fields_count": (
        "Multiple required fields were absent from the claim submission. "
        "Higher missing-field counts correlate with incomplete documentation."
    ),
    "low_volume_provider_risk": (
        "This provider had a low claim volume, making their historical denial "
        "patterns less statistically reliable for risk assessment."
    ),
    "dx_px_compatible": (
        "The diagnosis-procedure pair was checked against the claim coding "
        "compatibility reference. Incompatible combinations increase medical-necessity "
        "review risk because the submitted diagnosis may not support the billed procedure."
    ),
    "dx_px_pair_risk_prior": (
        "The diagnosis-procedure pair carried an elevated synthetic prior risk score. "
        "Higher prior-risk pairings are treated as stronger coding-review signals before "
        "historical provider behavior is considered."
    ),
}
