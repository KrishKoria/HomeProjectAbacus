from __future__ import annotations

from typing import Any

import numpy as np

from src.ml.evaluate import _unwrap_for_shap
from src.xai.feature_reasons import FEATURE_REASONS


def explain(
    model: Any,
    X: np.ndarray,
    feature_names: list[str],
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Compute per-claim SHAP explanations mapped to business-language reasons.

    Returns a list of (feature, importance, reason, direction) dicts sorted
    by descending absolute SHAP value, limited to *top_n*.
    """
    import shap

    explainer = shap.TreeExplainer(_unwrap_for_shap(model))
    shap_values = explainer.shap_values(X)
    if shap_values.ndim == 1:
        shap_values = np.expand_dims(shap_values, axis=0)

    results: list[dict[str, Any]] = []
    for feature, value in zip(feature_names, shap_values[0]):
        direction = "increases_risk" if value > 0 else "decreases_risk"
        reason = FEATURE_REASONS.get(
            feature,
            f"Analysis of {feature} contributed to the risk assessment.",
        )
        results.append(
            {
                "feature": feature,
                "importance": float(abs(value)),
                "shap_value": float(value),
                "reason": reason,
                "direction": direction,
            }
        )

    results.sort(key=lambda r: r["importance"], reverse=True)
    return results[:top_n]
