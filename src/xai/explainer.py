from __future__ import annotations

import weakref
from typing import Any, Final

import numpy as np

from src.ml.evaluate import _unwrap_for_shap
from src.xai.feature_reasons import FEATURE_REASONS

_EXPLAINER_CACHE: Final[weakref.WeakKeyDictionary] = weakref.WeakKeyDictionary()


def _cached_tree_explainer(model: Any):
    import shap

    explainer = _EXPLAINER_CACHE.get(model)
    if explainer is not None:
        return explainer
    raw: Any = _unwrap_for_shap(model)
    explainer = shap.TreeExplainer(raw)
    _EXPLAINER_CACHE[model] = explainer
    return explainer


def _normalize_single_claim_input(X: np.ndarray) -> np.ndarray:
    X_array = np.asarray(X)
    if X_array.ndim == 1:
        return X_array.reshape(1, -1)
    if X_array.ndim == 2:
        if X_array.shape[0] != 1:
            raise ValueError(
                f"explain() supports one claim only; got batch with {X_array.shape[0]} rows."
            )
        return X_array
    raise ValueError(
        f"Unsupported input shape {X_array.shape}. "
        "Expected 1D feature vector or 2D single-row array."
    )


def _extract_single_sample_shap_values(shap_values: Any) -> np.ndarray:
    if isinstance(shap_values, list):
        if len(shap_values) != 2:
            raise ValueError(
                "Legacy list SHAP output is only supported for binary outputs (2 classes). "
                f"Got {len(shap_values)} outputs."
            )
        class_values = np.asarray(shap_values[1])
        if class_values.ndim == 1:
            return class_values
        if class_values.ndim == 2:
            if class_values.shape[0] != 1:
                raise ValueError(
                    f"Expected one sample in SHAP output; got shape {class_values.shape}."
                )
            return class_values[0]
        raise ValueError(
            f"Unsupported legacy SHAP class output shape {class_values.shape}; "
            "expected 1D or 2D."
        )

    values = np.asarray(shap_values)
    if values.ndim == 2:
        if values.shape[0] != 1:
            raise ValueError(f"Expected one sample in SHAP output; got shape {values.shape}.")
        return values[0]

    if values.ndim == 3:
        if values.shape[0] != 1:
            raise ValueError(f"Expected one sample in SHAP output; got shape {values.shape}.")
        if values.shape[2] != 2:
            raise ValueError(
                "Multi-output SHAP arrays are only supported for binary outputs (2 classes). "
                f"Got shape {values.shape}."
            )
        return values[0, :, 1]

    raise ValueError(
        f"Unsupported SHAP output shape {values.shape}; expected 2D or binary 3D output."
    )


def explain(
    model: Any,
    X: np.ndarray,
    feature_names: list[str],
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Compute one-claim SHAP explanations mapped to business-language reasons.

    Returns a list of (feature, importance, reason, direction) dicts sorted
    by descending absolute SHAP value, limited to *top_n*. `X` must represent
    exactly one claim and each direction is one of: increases_risk,
    decreases_risk, or neutral.
    """
    X_input = _normalize_single_claim_input(X)
    explainer = _cached_tree_explainer(model)
    shap_values = explainer.shap_values(X_input)
    sample_values = _extract_single_sample_shap_values(shap_values)

    if len(feature_names) != sample_values.shape[0]:
        raise ValueError(
            "Feature names length does not match SHAP value length: "
            f"{len(feature_names)} != {sample_values.shape[0]}."
        )

    top_indices = np.argsort(np.abs(sample_values))[::-1][:top_n]
    results: list[dict[str, Any]] = [
        {
            "feature": feature_names[idx],
            "importance": float(abs(sample_values[idx])),
            "shap_value": float(sample_values[idx]),
            "reason": FEATURE_REASONS.get(
                feature_names[idx],
                f"Analysis of {feature_names[idx]} contributed to the risk assessment.",
            ),
            "direction": (
                "increases_risk"
                if sample_values[idx] > 0
                else "decreases_risk"
                if sample_values[idx] < 0
                else "neutral"
            ),
        }
        for idx in top_indices
    ]
    return results
