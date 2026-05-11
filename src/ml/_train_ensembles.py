from __future__ import annotations

import logging
from typing import Any

from sklearn.base import ClassifierMixin
from sklearn.calibration import CalibratedClassifierCV

logger = logging.getLogger(__name__)


def calibrate_classifier(
    estimator: ClassifierMixin,
    X_train: Any,
    y_train: Any,
    method: str = "sigmoid",
    cv: int = 3,
) -> CalibratedClassifierCV:
    calibrator = CalibratedClassifierCV(estimator, method=method, cv=cv)
    calibrator.fit(X_train, y_train)
    return calibrator


def train_voting_ensemble(
    estimators: list[tuple[str, ClassifierMixin]],
    X_train: Any,
    y_train: Any,
    voting: str = "soft",
) -> Any:
    from sklearn.ensemble import VotingClassifier

    ensemble = VotingClassifier(estimators=estimators, voting=voting)
    ensemble.fit(X_train, y_train)
    return ensemble


def train_stacking_ensemble(
    estimators: list[tuple[str, ClassifierMixin]],
    X_train: Any,
    y_train: Any,
    final_estimator: Any = None,
    cv: int = 5,
) -> Any:
    from sklearn.ensemble import StackingClassifier
    from sklearn.linear_model import LogisticRegression

    if final_estimator is None:
        final_estimator = LogisticRegression(
            max_iter=1000, class_weight="balanced", random_state=42)
    ensemble = StackingClassifier(
        estimators=estimators,
        final_estimator=final_estimator,
        cv=cv,
    )
    ensemble.fit(X_train, y_train)
    return ensemble


def select_best_calibration(
    base_estimator: ClassifierMixin,
    X_train: Any,
    y_train: Any,
    X_val: Any,
    y_val: Any,
    cv: int = 3,
) -> CalibratedClassifierCV:
    from sklearn.metrics import log_loss

    sigmoid_calibrated = calibrate_classifier(
        base_estimator, X_train, y_train, method="sigmoid", cv=cv)
    sigmoid_loss = float(log_loss(y_val, sigmoid_calibrated.predict_proba(X_val)[:, 1]))

    isotonic_calibrated = calibrate_classifier(
        base_estimator, X_train, y_train, method="isotonic", cv=cv)
    isotonic_loss = float(log_loss(y_val, isotonic_calibrated.predict_proba(X_val)[:, 1]))

    if isotonic_loss < sigmoid_loss:
        logger.info(
            "Selected isotonic calibration (log_loss=%.4f vs sigmoid=%.4f)",
            isotonic_loss,
            sigmoid_loss,
        )
        return isotonic_calibrated
    logger.info(
        "Selected sigmoid calibration (log_loss=%.4f vs isotonic=%.4f)",
        sigmoid_loss,
        isotonic_loss,
    )
    return sigmoid_calibrated


__all__ = [
    "calibrate_classifier",
    "select_best_calibration",
    "train_stacking_ensemble",
    "train_voting_ensemble",
]
