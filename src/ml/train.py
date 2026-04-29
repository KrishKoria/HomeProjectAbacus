from __future__ import annotations

import logging
import os
from typing import Any, Final

import mlflow
import numpy as np
from sklearn.base import ClassifierMixin
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier

from src.ml.evaluate import recall_at_high

logger = logging.getLogger(__name__)


# Soft-constraint precision floor inside the Optuna objective. Trials that fall
# below this floor are penalised so the search is steered into the feasible
# region rather than discarded outright (which would give the sampler no signal).
OPTUNA_PRECISION_FLOOR: Final[float] = 0.70


def _resolve_experiment_name(model_name: str) -> str:
    """Return an MLflow experiment name valid for the active tracking backend.

    Databricks rejects relative experiment names — they must be absolute
    workspace paths like ``/Users/<user>/<experiment>``. ``$USER`` on
    Databricks compute is the cluster service account (e.g. ``spark-…``)
    not the workspace user, so we resolve the actual user via Spark's
    ``current_user()`` SQL function. ``MLFLOW_EXPERIMENT_NAME`` env var
    overrides everything for explicit control. Local runs keep the plain
    relative name (MLflow's file-based tracker accepts it).
    """
    override = os.environ.get("MLFLOW_EXPERIMENT_NAME")
    if override:
        return override
    base = f"claim_denial_{model_name}"
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return base
    user = "shared"
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        row = spark.sql("SELECT current_user() AS user").first()
        if row and row["user"]:
            user = row["user"]
    except Exception:
        logger.warning("Failed to resolve Databricks workspace user", exc_info=True)
    return f"/Users/{user}/{base}"

XGBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
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


def calibrate_classifier(
    estimator: ClassifierMixin,
    X_train: Any,
    y_train: Any,
    method: str = "sigmoid",
    cv: int = 3,
) -> CalibratedClassifierCV:
    """Wrap ``estimator`` in CalibratedClassifierCV so predict_proba is well-calibrated.

    XGBoost's raw ``predict_proba`` is uncalibrated, so a fixed cutoff (e.g.
    the ARCHITECTURE.md §13 ``HIGH_RISK_PROBABILITY_THRESHOLD = 0.7``) lands
    in an arbitrary part of the score distribution. Platt-scaling the
    probabilities via ``CalibratedClassifierCV(method='sigmoid')`` makes
    ``proba >= 0.7`` correspond to "≈ 70% confidence" so the gate metric
    (Recall@HIGH) measures what the spec actually means. ``method='sigmoid'``
    is preferred over ``'isotonic'`` on the small synthetic dataset because
    isotonic needs more data per fold to fit a stable step function.

    The returned calibrator exposes ``predict``/``predict_proba`` like any
    sklearn classifier and can be unwrapped via
    ``CalibratedClassifierCV.calibrated_classifiers_[0].estimator`` for
    SHAP/feature-importance work that needs the underlying tree model.
    """
    calibrator = CalibratedClassifierCV(estimator, method=method, cv=cv)
    calibrator.fit(X_train, y_train)
    return calibrator


def _build_xgb_from_trial(trial: Any) -> XGBClassifier:
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 300),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        # Wider scale_pos_weight range so the search can aggressively rebalance
        # for the gate metric (Recall@HIGH). A higher upper bound is safe
        # because Optuna penalises trials that crater Precision.
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "random_state": 42,
    }
    return XGBClassifier(**params)


def _optuna_objective(
    trial: Any,
    X_train: Any,
    y_train: Any,
) -> float:
    """Objective: maximise mean Recall@HIGH under a soft Precision floor.

    The previous objective optimised ROC-AUC, which is a ranking metric and
    does not reward putting positives above the §13 ``HIGH_RISK_PROBABILITY_THRESHOLD``.
    Both prior runs hit AUC > 0.94 yet failed Recall@HIGH because the score
    distribution did not concentrate above 0.7. This objective wraps each
    fold's classifier in Platt calibration (the same wrapping used in
    production) so trial-time scores match the deployed model's behaviour.
    """
    base_estimator = _build_xgb_from_trial(trial)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_recalls: list[float] = []
    fold_precisions: list[float] = []
    for tr_idx, va_idx in skf.split(X_train, y_train):
        X_tr = X_train.iloc[tr_idx] if hasattr(X_train, "iloc") else X_train[tr_idx]
        X_va = X_train.iloc[va_idx] if hasattr(X_train, "iloc") else X_train[va_idx]
        y_tr = y_train.iloc[tr_idx] if hasattr(y_train, "iloc") else y_train[tr_idx]
        y_va = y_train.iloc[va_idx] if hasattr(y_train, "iloc") else y_train[va_idx]
        # cv=2 inside calibration keeps fold cost manageable (5 outer × 2 inner
        # = 10 fits per trial × 50 trials ≈ minutes on CPU).
        calibrated = CalibratedClassifierCV(base_estimator, method="sigmoid", cv=2)
        calibrated.fit(X_tr, y_tr)
        proba = calibrated.predict_proba(X_va)[:, 1]
        fold_recalls.append(recall_at_high(y_va, proba))
        pred = (proba >= 0.5).astype(int)
        fold_precisions.append(float(precision_score(y_va, pred, zero_division=0)))
    mean_recall = float(np.mean(fold_recalls))
    mean_precision = float(np.mean(fold_precisions))
    if mean_precision < OPTUNA_PRECISION_FLOOR:
        # Soft penalty: keep the gradient signal but punish infeasible trials.
        return mean_recall - 2.0 * (OPTUNA_PRECISION_FLOOR - mean_precision)
    return mean_recall


def tune_xgboost_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    """Run Optuna hyperparameter tuning, then refit + calibrate the best model."""
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: _optuna_objective(trial, X_train, y_train),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    best_params = dict(study.best_trial.params)
    best_params.update({
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "random_state": 42,
    })
    logger.info(
        "Optuna best Recall@HIGH (penalised): %.4f, params: %s",
        study.best_value,
        best_params,
    )
    base = XGBClassifier(**best_params)
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def _is_unity_catalog_model_name(name: str) -> bool:
    """Unity Catalog requires three-level names: ``<catalog>.<schema>.<model>``."""
    return name.count(".") == 2


def _configure_registry_for_runtime(registered_model_name: str | None) -> None:
    """Point MLflow at the right registry for the active runtime.

    On Databricks with a 3-level (Unity Catalog) model name we set the
    registry URI to ``databricks-uc`` so the model lands in UC alongside
    ``healthcare.bronze.*`` / ``healthcare.silver.*`` / ``healthcare.gold.*``.
    Local runs use whatever registry MLflow defaults to (file-based or
    whatever ``MLFLOW_TRACKING_URI`` points at).
    """
    if not registered_model_name:
        return
    on_databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    if on_databricks and _is_unity_catalog_model_name(registered_model_name):
        mlflow.set_registry_uri("databricks-uc")


def train_with_mlflow(
    model: Any,
    model_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    artifact_path: str = "model",
    registered_model_name: str | None = None,
    champion_alias: str | None = "champion",
    training_metadata: dict[str, Any] | None = None,
) -> str:
    """Log a fit model + params + metrics to an MLflow experiment and return the run id.

    When ``registered_model_name`` is provided, the model is also registered
    as a new version in the MLflow Model Registry (Unity Catalog on
    Databricks) and ``champion_alias`` is moved to point at it. Prediction
    callers can then load with::

        mlflow.sklearn.load_model(f"models:/{registered_model_name}@{champion_alias}")

    so they never depend on a run_id or local pickle path. Pass
    ``champion_alias=None`` to register without moving the alias (useful
    for shadow/staging models).
    """
    _configure_registry_for_runtime(registered_model_name)
    mlflow.set_experiment(_resolve_experiment_name(model_name))
    with mlflow.start_run(run_name=model_name):
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        metadata = dict(training_metadata or {})
        feature_columns = metadata.pop("feature_columns", None)
        if metadata:
            mlflow.log_params({key: str(value) for key, value in metadata.items()})
        if "release_gate_passed" in metadata:
            mlflow.set_tag("release_gate_passed", str(metadata["release_gate_passed"]).lower())
        if feature_columns is not None:
            mlflow.log_dict({"columns": list(feature_columns)}, "feature_columns.json")
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
        log_kwargs: dict[str, Any] = {
            "name": artifact_path,
            "signature": signature_input,
        }
        if registered_model_name:
            log_kwargs["registered_model_name"] = registered_model_name
        logged = mlflow.sklearn.log_model(model, **log_kwargs)
        run_id = mlflow.active_run().info.run_id

    if registered_model_name and champion_alias:
        try:
            client = mlflow.tracking.MlflowClient()
            version = getattr(logged, "registered_model_version", None)
            if version is None:
                # Fallback: look up the latest version registered against this run.
                versions = client.search_model_versions(
                    f"name='{registered_model_name}' and run_id='{run_id}'"
                )
                if versions:
                    version = max(int(v.version) for v in versions)
            if version is not None:
                client.set_registered_model_alias(
                    name=registered_model_name,
                    alias=champion_alias,
                    version=str(version),
                )
                logger.info(
                    "Registered %s version %s and moved alias '%s'",
                    registered_model_name,
                    version,
                    champion_alias,
                )
        except Exception:
            logger.warning("Setting champion alias failed", exc_info=True)

    return run_id


__all__ = [
    "LOGREG_DEFAULT_PARAMS",
    "OPTUNA_PRECISION_FLOOR",
    "XGBOOST_DEFAULT_PARAMS",
    "calibrate_classifier",
    "train_logistic_regression",
    "train_with_mlflow",
    "train_xgboost",
    "tune_xgboost_optuna",
]
