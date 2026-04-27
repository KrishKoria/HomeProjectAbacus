from __future__ import annotations

import logging
import os
from typing import Any, Final

import mlflow
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from xgboost import XGBClassifier

from src.ml import FEATURE_COLUMNS, TARGET_COLUMN

logger = logging.getLogger(__name__)


def _resolve_experiment_name(model_name: str) -> str:
    """Return an MLflow experiment name valid for the active tracking backend.

    Databricks rejects relative experiment names — they must be absolute
    workspace paths like ``/Users/<user>/<experiment>``. Detect the runtime
    via ``DATABRICKS_RUNTIME_VERSION`` and resolve the current user via the
    standard Databricks env vars; fall back to a plain relative name for
    local runs (which MLflow's local file-based tracking accepts).
    """
    base = f"claim_denial_{model_name}"
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return base
    user = (
        os.environ.get("DATABRICKS_USER")
        or os.environ.get("USER")
        or "shared"
    )
    return f"/Users/{user}/{base}"

XGBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "use_label_encoder": False,
    "early_stopping_rounds": 50,
    # Synthetic claim labels are ~70/30 (approved/denied); without rebalancing
    # XGBoost biases toward the majority class and silently misses ARCHITECTURE
    # §13's Recall@HIGH gate. The Optuna search refines this further per fold.
    "scale_pos_weight": 2.5,
    "random_state": 42,
}

LOGREG_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_iter": 1000,
    "class_weight": "balanced",
    "random_state": 42,
}


def train_logistic_regression(
    X_train: Any,
    y_train: Any,
    params: dict[str, Any] | None = None,
) -> LogisticRegression:
    """Fit the baseline logistic-regression model using class-balanced weights."""
    training_params = {**LOGREG_DEFAULT_PARAMS, **(params or {})}
    model = LogisticRegression(**training_params)
    model.fit(X_train, y_train)
    return model


def train_xgboost(
    X_train: Any,
    y_train: Any,
    X_val: Any = None,
    y_val: Any = None,
    params: dict[str, Any] | None = None,
) -> XGBClassifier:
    """Fit the primary XGBoost classifier with optional early-stopping eval set."""
    training_params = {**XGBOOST_DEFAULT_PARAMS, **(params or {})}
    training_params.pop("early_stopping_rounds", 50)
    model = XGBClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
        fit_kwargs["verbose"] = False
    model.fit(X_train, y_train, **fit_kwargs)
    return model


def _optuna_objective(
    trial: Any,
    X_train: Any,
    y_train: Any,
) -> float:
    """Objective for the Optuna study: 5-fold CV ROC-AUC on the training fold."""
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 300),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 10.0),
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "use_label_encoder": False,
        "random_state": 42,
    }
    model = XGBClassifier(**params)
    scores = cross_val_score(model, X_train, y_train, cv=5, scoring="roc_auc")
    return scores.mean()


def tune_xgboost_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
) -> tuple[XGBClassifier, dict[str, Any]]:
    """Run Optuna hyperparameter tuning and return the best-fit XGBoost model."""
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: _optuna_objective(trial, X_train, y_train),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    best_params = study.best_trial.params
    best_params.update({
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "use_label_encoder": False,
        "random_state": 42,
    })
    logger.info("Optuna best AUC: %.4f, params: %s", study.best_value, best_params)
    model = XGBClassifier(**best_params)
    model.fit(X_train, y_train)
    return model, best_params


def train_with_mlflow(
    model: Any,
    model_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    artifact_path: str = "model",
) -> str:
    """Log a fit model + params + metrics to an MLflow experiment and return the run id."""
    mlflow.set_experiment(_resolve_experiment_name(model_name))
    with mlflow.start_run(run_name=model_name):
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        signature_input = None
        try:
            from mlflow.models import infer_signature

            if hasattr(model, "feature_names_in_"):
                import pandas as pd
                from sklearn.base import is_classifier

                sample_input = pd.DataFrame(
                    {col: [0.0] for col in model.feature_names_in_},
                ).astype(float)
                signature_output = (
                    pd.DataFrame({col: [0.5] for col in ["denial_probability"]})
                    if is_classifier(model)
                    else None
                )
                signature_input = infer_signature(sample_input, signature_output)
        except Exception:
            logger.warning("MLflow signature inference failed", exc_info=True)
            signature_input = None
        mlflow.sklearn.log_model(
            model,
            artifact_path,
            signature=signature_input,
        )
        return mlflow.active_run().info.run_id


__all__ = [
    "LOGREG_DEFAULT_PARAMS",
    "XGBOOST_DEFAULT_PARAMS",
    "train_logistic_regression",
    "train_with_mlflow",
    "train_xgboost",
    "tune_xgboost_optuna",
]
