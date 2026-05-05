from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Literal

import numpy as np
from src.common.diagnostics import get_ml_diagnostic_id

from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


# ARCHITECTURE.md §13 caps the HIGH risk tier at probability >= 0.7. Keeping the
# constant here (and in src/ml/predict.py) lets the evaluation gate stay aligned
# with the runtime classification.
HIGH_RISK_PROBABILITY_THRESHOLD: Final[float] = 0.7

DEFAULT_MIN_RECALL_AT_HIGH: Final[float] = 0.80
DEFAULT_MIN_PRECISION: Final[float] = 0.70
DEFAULT_MIN_ROC_AUC: Final[float] = 0.85


@dataclass(frozen=True)
class EvaluationMetrics:
    """Summary metrics produced for one model on a held-out test split."""

    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float
    recall_at_high: float

    def meets_thresholds(
        self,
        min_recall_at_high: float = DEFAULT_MIN_RECALL_AT_HIGH,
        min_precision: float = DEFAULT_MIN_PRECISION,
        min_roc_auc: float = DEFAULT_MIN_ROC_AUC,
    ) -> bool:
        """Return True iff the model clears the ARCHITECTURE.md §13 release gate.

        The gate is intentionally Recall@HIGH, not global recall: we care that
        truly-denied claims surface in the HIGH risk tier where remediation is
        actually triggered, not just that the model flagged them at any
        threshold.
        """
        return (
            self.recall_at_high >= min_recall_at_high
            and self.precision >= min_precision
            and self.roc_auc >= min_roc_auc
        )


def recall_at_high(
    y_true: Any,
    y_prob: Any,
    threshold: float = HIGH_RISK_PROBABILITY_THRESHOLD,
) -> float:
    """Fraction of truly-denied claims surfaced into the HIGH risk tier.

    Computed as ``TP@HIGH / total_positives`` where TP@HIGH counts true
    positives whose predicted probability clears ``threshold``. Returns 0.0
    when there are no positives in ``y_true`` so the metric stays well-defined
    on tiny synthetic splits.
    """
    y_true_arr = np.asarray(y_true)
    y_prob_arr = np.asarray(y_prob)
    positives = y_true_arr == 1
    total_positives = int(positives.sum())
    if total_positives == 0:
        return 0.0
    high_and_positive = int(((y_prob_arr >= threshold) & positives).sum())
    return float(high_and_positive) / float(total_positives)


def find_optimal_threshold(
    y_true: Any,
    y_prob: Any,
    metric: Literal["youden_j", "f1", "recall_at_high"] = "youden_j",
) -> float:
    """Find the decision threshold that maximises the chosen metric on validation data.

    Youden's J statistic (sensitivity + specificity - 1) gives equal weight to
    both classes and is robust for imbalanced data. The returned threshold is
    used for binary ``predict()`` calls and does NOT change the
    ``HIGH_RISK_PROBABILITY_THRESHOLD`` used for risk tiering.
    """
    y_true_arr = np.asarray(y_true)
    y_prob_arr = np.asarray(y_prob)
    thresholds = np.linspace(0.1, 0.9, 161)
    best_threshold = 0.5
    best_score = -np.inf
    for t in thresholds:
        pred = (y_prob_arr >= t).astype(int)
        if metric == "youden_j":
            sensitivity = float(recall_score(y_true_arr, pred, zero_division=0))
            specificity = float(recall_score(1 - y_true_arr, 1 - pred, zero_division=0))
            score = sensitivity + specificity - 1.0
        elif metric == "f1":
            score = float(f1_score(y_true_arr, pred, zero_division=0))
        elif metric == "recall_at_high":
            score = recall_at_high(y_true_arr, y_prob_arr, threshold=t)
        else:
            raise ValueError(
            f"[{get_ml_diagnostic_id('unknown_evaluation_metric')}] "
            f"Unknown metric: {metric}"
        )
        if score > best_score:
            best_score = score
            best_threshold = t
    return float(best_threshold)


def evaluate_model(
    model: Any,
    X_test: Any,
    y_test: Any,
) -> EvaluationMetrics:
    """Score ``model`` on the held-out split and return the §13 gate metrics."""
    y_pred = model.predict(X_test)
    y_prob = (
        model.predict_proba(X_test)[:, 1]
        if hasattr(model, "predict_proba")
        else y_pred
    )

    return EvaluationMetrics(
        accuracy=float(accuracy_score(y_test, y_pred)),
        precision=float(precision_score(y_test, y_pred, zero_division=0)),
        recall=float(recall_score(y_test, y_pred, zero_division=0)),
        f1=float(f1_score(y_test, y_pred, zero_division=0)),
        roc_auc=float(roc_auc_score(y_test, y_prob)),
        recall_at_high=recall_at_high(y_test, y_prob),
    )


def _unwrap_for_shap(model: Any) -> Any:
    """Return the underlying tree model for SHAP TreeExplainer.

    Handles:
    - ``mlflow.pyfunc.PyFuncModel`` (unwrap _model_impl)
    - ``sklearn.calibration.CalibratedClassifierCV`` (unwrap first fold's estimator)
    - ``sklearn.ensemble.VotingClassifier`` / ``StackingClassifier`` (unwrap first estimator)
    """
    # 1. Unwrap MLflow PyFuncModel — the app loads models via pyfunc.load_model
    impl = getattr(model, "_model_impl", None)
    if impl is not None and type(model).__name__ == "PyFuncModel":
        model = impl

    # 2. Unwrap CalibratedClassifierCV (Platt scaling)
    sub = getattr(model, "calibrated_classifiers_", None)
    if sub:
        inner = getattr(sub[0], "estimator", None)
        if inner is not None:
            model = inner

    # 3. Unwrap VotingClassifier / StackingClassifier to a single estimator
    estimators_attr = getattr(model, "estimators_", None)
    if estimators_attr is not None:
        model = estimators_attr[0][1] if isinstance(estimators_attr[0], tuple) else estimators_attr[0]

    return model


def compute_shap_values(
    model: Any,
    X_sample: Any,
    max_samples: int = 200,
) -> tuple[Any, list[str] | None]:
    """Return SHAP values and feature names for a sample of test rows."""
    import shap

    if isinstance(X_sample, np.ndarray):
        feature_names = None
    else:
        feature_names = getattr(X_sample, "columns", None)

    X_input = X_sample[:max_samples] if len(X_sample) > max_samples else X_sample

    explainer = shap.TreeExplainer(_unwrap_for_shap(model))
    shap_values = explainer.shap_values(X_input)

    if feature_names is not None:
        return shap_values, list(feature_names)
    return shap_values, None


def compute_confusion_matrix(
    y_true: Any,
    y_pred: Any,
) -> tuple[int, int, int, int]:
    """Return (tn, fp, fn, tp) for the binary classification predictions."""
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    return int(tn), int(fp), int(fn), int(tp)


def compute_psi(
    expected: Any,
    actual: Any,
    bins: int = 10,
) -> float:
    """Population Stability Index between training (expected) and production (actual).

    PSI = sum((actual_i - expected_i) * ln(actual_i / expected_i))
    PSI < 0.1: no significant change
    0.1 <= PSI < 0.2: moderate change
    PSI >= 0.2: significant shift, retrain recommended
    """
    expected_arr = np.asarray(expected, dtype=float)
    actual_arr = np.asarray(actual, dtype=float)
    percentiles = np.percentile(expected_arr, np.linspace(0, 100, bins + 1))
    expected_binned = np.digitize(expected_arr, percentiles[1:-1])
    actual_binned = np.digitize(actual_arr, percentiles[1:-1])

    psi = 0.0
    for i in range(bins):
        expected_pct = float(np.mean(expected_binned == i))
        actual_pct = float(np.mean(actual_binned == i))
        expected_pct = max(expected_pct, 1e-4)
        actual_pct = max(actual_pct, 1e-4)
        psi += (actual_pct - expected_pct) * np.log(actual_pct / expected_pct)
    return float(psi)


def get_top_features(
    model: Any,
    feature_names: list[str],
    n: int = 5,
) -> list[tuple[str, float]]:
    """Extract top-N feature importances from a fitted tree-based model.

    Handles CalibratedClassifierCV unwrapping, and works with XGBoost,
    LightGBM, CatBoost, and sklearn ensemble feature_importances_.
    Returns list of (feature_name, importance) sorted descending.
    """
    inner = _unwrap_for_shap(model)
    if hasattr(inner, "feature_importances_"):
        importances = inner.feature_importances_
    elif hasattr(inner, "get_score"):
        raw = inner.get_score(importance_type="gain")
        importances = np.array([raw.get(f, 0.0) for f in feature_names])
    else:
        return []

    paired = list(zip(feature_names, importances))
    paired.sort(key=lambda x: x[1], reverse=True)
    return paired[:n]


def generate_evaluation_report(
    metrics: EvaluationMetrics,
    confusion: tuple[int, int, int, int],
    model_name: str,
    feature_names: list[str] | None = None,
) -> dict[str, Any]:
    """Build the structured evaluation report consumed by the training script."""
    tn, fp, fn, tp = confusion
    report: dict[str, Any] = {
        "model_name": model_name,
        "accuracy": metrics.accuracy,
        "precision": metrics.precision,
        "recall": metrics.recall,
        "recall_at_high": metrics.recall_at_high,
        "f1": metrics.f1,
        "roc_auc": metrics.roc_auc,
        "confusion_matrix": {
            "true_negatives": tn,
            "false_positives": fp,
            "false_negatives": fn,
            "true_positives": tp,
        },
        "meets_thresholds": metrics.meets_thresholds(),
    }
    if feature_names is not None:
        report["feature_count"] = len(feature_names)
        report["features"] = feature_names
    return report


__all__ = [
    "DEFAULT_MIN_PRECISION",
    "DEFAULT_MIN_RECALL_AT_HIGH",
    "DEFAULT_MIN_ROC_AUC",
    "EvaluationMetrics",
    "HIGH_RISK_PROBABILITY_THRESHOLD",
    "compute_confusion_matrix",
    "compute_psi",
    "compute_shap_values",
    "evaluate_model",
    "find_optimal_threshold",
    "generate_evaluation_report",
    "get_top_features",
    "recall_at_high",
]
