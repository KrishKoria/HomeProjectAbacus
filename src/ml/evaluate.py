from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class EvaluationMetrics:
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float

    def meets_thresholds(
        self,
        min_recall: float = 0.80,
        min_precision: float = 0.70,
        min_roc_auc: float = 0.85,
    ) -> bool:
        return (
            self.recall >= min_recall
            and self.precision >= min_precision
            and self.roc_auc >= min_roc_auc
        )


def evaluate_model(
    model: Any,
    X_test: Any,
    y_test: Any,
) -> EvaluationMetrics:
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else y_pred

    return EvaluationMetrics(
        accuracy=float(accuracy_score(y_test, y_pred)),
        precision=float(precision_score(y_test, y_pred, zero_division=0)),
        recall=float(recall_score(y_test, y_pred, zero_division=0)),
        f1=float(f1_score(y_test, y_pred, zero_division=0)),
        roc_auc=float(roc_auc_score(y_test, y_prob)),
    )


def compute_shap_values(
    model: Any,
    X_sample: Any,
    max_samples: int = 200,
) -> Any:
    import shap

    if isinstance(X_sample, np.ndarray):
        feature_names = None
    else:
        feature_names = getattr(X_sample, "columns", None)

    X_input = X_sample[:max_samples] if len(X_sample) > max_samples else X_sample

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_input)

    if feature_names is not None:
        return shap_values, list(feature_names)
    return shap_values, None


def compute_confusion_matrix(
    y_true: Any,
    y_pred: Any,
) -> tuple[int, int, int, int]:
    from sklearn.metrics import confusion_matrix

    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()
    return int(tn), int(fp), int(fn), int(tp)


def generate_evaluation_report(
    metrics: EvaluationMetrics,
    confusion: tuple[int, int, int, int],
    model_name: str,
    feature_names: list[str] | None = None,
) -> dict[str, Any]:
    tn, fp, fn, tp = confusion
    report: dict[str, Any] = {
        "model_name": model_name,
        "accuracy": metrics.accuracy,
        "precision": metrics.precision,
        "recall": metrics.recall,
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
    "EvaluationMetrics",
    "compute_confusion_matrix",
    "compute_shap_values",
    "evaluate_model",
    "generate_evaluation_report",
]