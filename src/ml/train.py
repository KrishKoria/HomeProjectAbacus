from __future__ import annotations

import logging
from typing import Any

import mlflow
import numpy as np
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from src.ml import FEATURE_COLUMNS, TARGET_COLUMN
from src.ml.features import prepare_training_data, stratified_split

logger = logging.getLogger(__name__)

XGBoost_DEFAULT_PARAMS: dict[str, Any] = {
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "use_label_encoder": False,
    "early_stopping_rounds": 50,
    "random_state": 42,
}

LOGREG_DEFAULT_PARAMS: dict[str, Any] = {
    "max_iter": 1000,
    "class_weight": "balanced",
    "random_state": 42,
}


def train_logistic_regression(
    X_train: Any,
    y_train: Any,
    params: dict[str, Any] | None = None,
) -> LogisticRegression:
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
    training_params = {**XGBoost_DEFAULT_PARAMS, **(params or {})}
    early_stopping = training_params.pop("early_stopping_rounds", 50)
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
    from sklearn.model_selection import cross_val_score

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
    mlflow.set_experiment(f"claim_denial_{model_name}")
    with mlflow.start_run(run_name=model_name):
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        signature_input = None
        try:
            from mlflow.models import infer_signature
            if hasattr(model, "feature_names_in_"):
                import pandas as pd
                sample_input = pd.DataFrame(
                    {col: [0.0] for col in model.feature_names_in_},
                )
                sample_input = sample_input.astype(float)
                from sklearn.base import is_classifier
                if is_classifier(model):
                    signature_output = pd.DataFrame(
                        {col: [0.5] for col in ["denial_probability"]},
                    )
                else:
                    signature_output = None
                signature_input = infer_signature(sample_input, signature_output)
        except Exception:
            signature_input = None
        mlflow.sklearn.log_model(
            model,
            artifact_path,
            signature=signature_input,
        )
        return mlflow.active_run().info.run_id


__all__ = [
    "LOGREG_DEFAULT_PARAMS",
    "XGBoost_DEFAULT_PARAMS",
    "train_logistic_regression",
    "train_xgboost",
    "train_with_mlflow",
    "tune_xgboost_optuna",
]