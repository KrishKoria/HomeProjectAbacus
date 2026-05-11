from __future__ import annotations

from typing import Any, Final

FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    # Original 13 features
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
    # New temporal windows
    "provider_claim_count_60d",
    "provider_claim_count_90d",
    # New interaction / derived features
    "cost_overbenchmark_and_highseverity",
    "mismatch_and_overbenchmark",
    "provider_30d_denial_rate",
    "missing_fields_count",
    "low_volume_provider_risk",
    # Code-pair features (dx_px)
    "dx_px_compatible",
    "dx_px_pair_risk_prior",
)

TARGET_COLUMN: Final[str] = "denial_label"

RISK_THRESHOLD_LOW: Final[float] = 0.3
HIGH_RISK_PROBABILITY_THRESHOLD: Final[float] = 0.7
RISK_THRESHOLD_HIGH: float = HIGH_RISK_PROBABILITY_THRESHOLD

__all__ = [
    "FEATURE_COLUMNS",
    "HIGH_RISK_PROBABILITY_THRESHOLD",
    "RISK_THRESHOLD_HIGH",
    "RISK_THRESHOLD_LOW",
    "TARGET_COLUMN",
    "unwrap_model_for_shap",
]

from src.ml.evaluate import unwrap_model_for_shap  # noqa: E402
