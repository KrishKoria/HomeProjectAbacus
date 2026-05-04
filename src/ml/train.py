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

from src.common.diagnostics import get_ml_diagnostic_id
from src.common.log_categories import LOG_CATEGORY_ML_TRAINING
from src.common.log_messages import (
    MESSAGE_TEMPLATE_ML_TRAINING_FAILURE,
    MESSAGE_TEMPLATE_ML_REGISTRY_ERROR,
)
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
        logger.warning(
            "[%s] Failed to resolve Databricks workspace user",
            get_ml_diagnostic_id("databricks_user_resolution_failed"),
            exc_info=True,
        )
    return f"/Users/{user}/{base}"


XGBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "early_stopping_rounds": 50,
    # Synthetic claim labels are ~70/30 (approved/denied); without rebalancing
    # XGBoost biases toward the majority class and silently misses
    # Recall@HIGH gate. The Optuna search refines this further per fold.
    "scale_pos_weight": 2.5,
    "random_state": 42,
}

LIGHTGBM_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "objective": "binary",
    "metric": "binary_logloss",
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "scale_pos_weight": 2.5,
    "class_weight": "balanced",
    "random_state": 42,
    "verbose": -1,
}

CATBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "objective": "Logloss",
    "eval_metric": "Logloss",
    "learning_rate": 0.1,
    "depth": 6,
    "iterations": 100,
    "scale_pos_weight": 2.5,
    "random_seed": 42,
    "verbose": False,
    "allow_writing_files": False,
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
    random_seed: int = 42,
) -> LogisticRegression:
    """Fit the baseline logistic-regression model using class-balanced weights."""
    training_params = {**LOGREG_DEFAULT_PARAMS, **
                       (params or {}), "random_state": random_seed}
    model = LogisticRegression(**training_params)
    model.fit(X_train, y_train)
    return model


def train_xgboost(
    X_train: Any,
    y_train: Any,
    X_val: Any = None,
    y_val: Any = None,
    params: dict[str, Any] | None = None,
    random_seed: int = 42,
    sample_weight: Any = None,
) -> XGBClassifier:
    """Fit the primary XGBoost classifier with optional early-stopping eval set."""
    training_params = {**XGBOOST_DEFAULT_PARAMS, **
                       (params or {}), "random_state": random_seed}
    training_params.pop("early_stopping_rounds", 50)
    model = XGBClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
        fit_kwargs["verbose"] = False
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)
    return model


def train_lightgbm(
    X_train: Any,
    y_train: Any,
    X_val: Any = None,
    y_val: Any = None,
    params: dict[str, Any] | None = None,
    random_seed: int = 42,
    sample_weight: Any = None,
) -> Any:
    """Fit a LightGBM classifier with optional early-stopping eval set."""
    import lightgbm as lgb

    training_params = {**LIGHTGBM_DEFAULT_PARAMS, **
                       (params or {}), "random_state": random_seed}
    model = lgb.LGBMClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)
    return model


def train_catboost(
    X_train: Any,
    y_train: Any,
    X_val: Any = None,
    y_val: Any = None,
    params: dict[str, Any] | None = None,
    random_seed: int = 42,
    sample_weight: Any = None,
) -> Any:
    """Fit a CatBoost classifier with optional early-stopping eval set."""
    from catboost import CatBoostClassifier

    training_params = {**CATBOOST_DEFAULT_PARAMS, **
                       (params or {}), "random_seed": random_seed}
    model = CatBoostClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
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


def train_voting_ensemble(
    estimators: list[tuple[str, ClassifierMixin]],
    X_train: Any,
    y_train: Any,
    voting: str = "soft",
) -> Any:
    """Soft-voting ensemble of calibrated base estimators.

    Each base estimator should already be calibrated via
    ``CalibratedClassifierCV`` so that soft voting combines
    meaningful probability distributions.
    """
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
    """Stacking ensemble with a logistic regression meta-learner.

    ``cv=5`` means each base estimator is refit on 4/5 folds and
    predicts on the held-out 1/5. The meta-learner trains on these
    out-of-fold predictions, preventing overfitting to base model
    idiosyncrasies.
    """
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
    """Try both Platt (sigmoid) and isotonic calibration, returning the one with
    lower log-loss on the validation set.

    Isotonic often produces better-calibrated probabilities when the dataset
    is large enough, while sigmoid is more data-efficient for small datasets.
    A held-out validation fold prevents overfitting the calibration method
    choice.
    """
    from sklearn.metrics import log_loss

    sigmoid_calibrated = calibrate_classifier(
        base_estimator, X_train, y_train, method="sigmoid", cv=cv)
    sigmoid_loss = float(
        log_loss(y_val, sigmoid_calibrated.predict_proba(X_val)[:, 1]))

    isotonic_calibrated = calibrate_classifier(
        base_estimator, X_train, y_train, method="isotonic", cv=cv)
    isotonic_loss = float(
        log_loss(y_val, isotonic_calibrated.predict_proba(X_val)[:, 1]))

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


def _build_xgb_from_trial(trial: Any, random_seed: int = 42) -> XGBClassifier:
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
        "random_state": random_seed,
    }
    return XGBClassifier(**params)


def _build_lgb_from_trial(trial: Any, random_seed: int = 42) -> Any:
    import lightgbm as lgb

    params = {
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 300),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
        "lambda_l1": trial.suggest_float("lambda_l1", 0.0, 10.0),
        "lambda_l2": trial.suggest_float("lambda_l2", 0.0, 10.0),
        "min_split_gain": trial.suggest_float("min_split_gain", 0.0, 1.0),
        "objective": "binary",
        "metric": "binary_logloss",
        "boosting_type": "gbdt",
        "random_state": random_seed,
        "verbose": -1,
    }
    return lgb.LGBMClassifier(**params)


def _build_catboost_from_trial(trial: Any, random_seed: int = 42) -> Any:
    from catboost import CatBoostClassifier

    params = {
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "iterations": trial.suggest_int("iterations", 50, 300),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.6, 1.0),
        "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 50),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
        "objective": "Logloss",
        "eval_metric": "Logloss",
        "random_seed": random_seed,
        "verbose": False,
        "allow_writing_files": False,
    }
    return CatBoostClassifier(**params)


def _optuna_objective(
    trial: Any,
    X_train: Any,
    y_train: Any,
    random_seed: int = 42,
) -> float:
    """Objective: maximise mean Recall@HIGH under a soft Precision floor.

    The previous objective optimised ROC-AUC, which is a ranking metric and
    does not reward putting positives above the §13 ``HIGH_RISK_PROBABILITY_THRESHOLD``.
    Both prior runs hit AUC > 0.94 yet failed Recall@HIGH because the score
    distribution did not concentrate above 0.7. This objective wraps each
    fold's classifier in Platt calibration (the same wrapping used in
    production) so trial-time scores match the deployed model's behaviour.
    """
    base_estimator = _build_xgb_from_trial(trial, random_seed=random_seed)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
    fold_recalls: list[float] = []
    fold_precisions: list[float] = []
    for tr_idx, va_idx in skf.split(X_train, y_train):
        X_tr = X_train.iloc[tr_idx] if hasattr(
            X_train, "iloc") else X_train[tr_idx]
        X_va = X_train.iloc[va_idx] if hasattr(
            X_train, "iloc") else X_train[va_idx]
        y_tr = y_train.iloc[tr_idx] if hasattr(
            y_train, "iloc") else y_train[tr_idx]
        y_va = y_train.iloc[va_idx] if hasattr(
            y_train, "iloc") else y_train[va_idx]
        # cv=2 inside calibration keeps fold cost manageable (5 outer × 2 inner
        # = 10 fits per trial × 50 trials ≈ minutes on CPU).
        calibrated = CalibratedClassifierCV(
            base_estimator, method="sigmoid", cv=2)
        calibrated.fit(X_tr, y_tr)
        proba = calibrated.predict_proba(X_va)[:, 1]
        fold_recalls.append(recall_at_high(y_va, proba))
        pred = (proba >= 0.5).astype(int)
        fold_precisions.append(
            float(precision_score(y_va, pred, zero_division=0)))
    mean_recall = float(np.mean(fold_recalls))
    mean_precision = float(np.mean(fold_precisions))
    if mean_precision < OPTUNA_PRECISION_FLOOR:
        # Soft penalty: keep the gradient signal but punish infeasible trials.
        return mean_recall - 2.0 * (OPTUNA_PRECISION_FLOOR - mean_precision)
    return mean_recall


def compare_and_promote(
    new_recall_at_high: float,
    current_champion_recall_at_high: float | None,
    min_improvement: float = 0.01,
) -> bool:
    """Return True if the new model should replace champion.

    Requires a minimum absolute improvement in Recall@HIGH of 1 percentage
    point to prevent flapping from noise. When no champion exists (first
    training run), the new model is promoted by default.
    """
    if current_champion_recall_at_high is None:
        return True
    return (new_recall_at_high - current_champion_recall_at_high) >= min_improvement


def compute_sample_weights(
    y_train: Any,
    positive_weight: float = 3.0,
) -> Any:
    """Assign higher weight to positive examples for imbalanced training.

    Simple, stable approach that achieves similar effect to custom loss
    functions without numerical risk. Pass result as ``sample_weight``
    to ``.fit()`` for XGBoost, LightGBM, or CatBoost.
    """
    y_arr = np.asarray(y_train)
    weights = np.ones_like(y_arr, dtype=float)
    weights[y_arr == 1] = positive_weight
    return weights


def _make_optuna_objective(
    builder_fn: Any,
    X_train: Any,
    y_train: Any,
    random_seed: int = 42,
    groups: Any = None,
) -> Any:
    """Factory: return an Optuna objective that maximises mean Recall@HIGH.

    ``builder_fn`` is a callable ``(trial, random_seed) -> base_estimator``
    (e.g. ``_build_xgb_from_trial``, ``_build_lgb_from_trial``,
    ``_build_catboost_from_trial``). The base estimator is wrapped in
    Platt calibration inside each CV fold so trial-time scores match
    the deployed model's behaviour.

    When ``groups`` is provided (provider IDs), GroupKFold is used
    instead of StratifiedKFold to prevent provider-level leakage.
    """

    def _objective(trial: Any) -> float:
        from sklearn.model_selection import GroupKFold

        base_estimator = builder_fn(trial, random_seed=random_seed)
        if groups is not None:
            groups_arr = np.asarray(groups)
            unique_groups = np.unique(groups_arr)
            n_unique_groups = int(unique_groups.size)
            if n_unique_groups >= 2:
                n_group_splits = min(5, n_unique_groups)
                splitter = GroupKFold(n_splits=n_group_splits)
                split_iter = splitter.split(X_train, y_train, groups=groups)
            else:
                splitter = StratifiedKFold(
                    n_splits=5, shuffle=True, random_state=random_seed)
                split_iter = splitter.split(X_train, y_train)
        else:
            splitter = StratifiedKFold(
                n_splits=5, shuffle=True, random_state=random_seed)
            split_iter = splitter.split(X_train, y_train)
        fold_recalls: list[float] = []
        fold_precisions: list[float] = []
        for tr_idx, va_idx in split_iter:
            X_tr = X_train.iloc[tr_idx] if hasattr(
                X_train, "iloc") else X_train[tr_idx]
            X_va = X_train.iloc[va_idx] if hasattr(
                X_train, "iloc") else X_train[va_idx]
            y_tr = y_train.iloc[tr_idx] if hasattr(
                y_train, "iloc") else y_train[tr_idx]
            y_va = y_train.iloc[va_idx] if hasattr(
                y_train, "iloc") else y_train[va_idx]
            calibrated = CalibratedClassifierCV(
                base_estimator, method="sigmoid", cv=2)
            calibrated.fit(X_tr, y_tr)
            proba = calibrated.predict_proba(X_va)[:, 1]
            fold_recalls.append(recall_at_high(y_va, proba))
            pred = (proba >= 0.5).astype(int)
            fold_precisions.append(
                float(precision_score(y_va, pred, zero_division=0)))
        mean_recall = float(np.mean(fold_recalls))
        mean_precision = float(np.mean(fold_precisions))
        if mean_precision < OPTUNA_PRECISION_FLOOR:
            return mean_recall - 2.0 * (OPTUNA_PRECISION_FLOOR - mean_precision)
        return mean_recall

    return _objective


def tune_xgboost_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
    random_seed: int = 42,
    groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    """Run Optuna hyperparameter tuning for XGBoost, then refit + calibrate the best model."""
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_xgb_from_trial,
                               X_train, y_train, random_seed, groups=groups),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    if len(study.trials) == 0 or study.best_trial is None:
        diag_id = get_ml_diagnostic_id("optuna_xgboost_no_trials")
        raise RuntimeError(
            f"[{diag_id}] Optuna XGBoost tuning failed: no successful trials. "
            "Check trial logs for per-fold errors."
        )
    best_params = dict(study.best_trial.params)
    best_params.update({
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "random_state": random_seed,
    })
    logger.info(
        "Optuna XGBoost best Recall@HIGH (penalised): %.4f, params: %s",
        study.best_value,
        best_params,
    )
    base = XGBClassifier(**best_params)
    calibrated = calibrate_classifier(
        base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def tune_lightgbm_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
    random_seed: int = 42,
    groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    """Run Optuna hyperparameter tuning for LightGBM, then refit + calibrate the best model."""
    import lightgbm as lgb
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_lgb_from_trial,
                               X_train, y_train, random_seed, groups=groups),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    if len(study.trials) == 0 or study.best_trial is None:
        diag_id = get_ml_diagnostic_id("optuna_lightgbm_no_trials")
        raise RuntimeError(
            f"[{diag_id}] Optuna LightGBM tuning failed: no successful trials. "
            "Check trial logs for per-fold errors."
        )
    best_params = dict(study.best_trial.params)
    best_params.update({
        "objective": "binary",
        "metric": "binary_logloss",
        "boosting_type": "gbdt",
        "random_state": random_seed,
        "verbose": -1,
    })
    logger.info(
        "Optuna LightGBM best Recall@HIGH (penalised): %.4f, params: %s",
        study.best_value,
        best_params,
    )
    base = lgb.LGBMClassifier(**best_params)
    calibrated = calibrate_classifier(
        base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def tune_catboost_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
    random_seed: int = 42,
    groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    """Run Optuna hyperparameter tuning for CatBoost, then refit + calibrate the best model."""
    from catboost import CatBoostClassifier
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_catboost_from_trial,
                               X_train, y_train, random_seed, groups=groups),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    if len(study.trials) == 0 or study.best_trial is None:
        diag_id = get_ml_diagnostic_id("optuna_catboost_no_trials")
        raise RuntimeError(
            f"[{diag_id}] Optuna CatBoost tuning failed: no successful trials. "
            "Check trial logs for per-fold errors."
        )
    best_params = dict(study.best_trial.params)
    best_params.update({
        "objective": "Logloss",
        "eval_metric": "Logloss",
        "random_seed": random_seed,
        "verbose": False,
        "allow_writing_files": False,
    })
    logger.info(
        "Optuna CatBoost best Recall@HIGH (penalised): %.4f, params: %s",
        study.best_value,
        best_params,
    )
    base = CatBoostClassifier(**best_params)
    calibrated = calibrate_classifier(
        base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def select_features_by_importance(
    model: Any,
    X_val: Any,
    y_val: Any,
    feature_names: list[str],
    min_importance: float = 0.001,
) -> list[str]:
    """Return feature names whose permutation importance exceeds the threshold.

    Uses sklearn's ``permutation_importance`` with ``n_repeats=5`` for
    stable estimates. Features with importance below ``min_importance``
    are candidates for removal on larger datasets.
    """
    from sklearn.inspection import permutation_importance

    result = permutation_importance(
        model,
        X_val,
        y_val,
        n_repeats=5,
        random_state=42,
        scoring="roc_auc",
    )
    return [
        name
        for name, imp in zip(feature_names, result.importances_mean)
        if imp >= min_importance
    ]


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
            mlflow.log_params({key: str(value)
                              for key, value in metadata.items()})
        if "release_gate_passed" in metadata:
            mlflow.set_tag("release_gate_passed", str(
                metadata["release_gate_passed"]).lower())
        if feature_columns is not None:
            mlflow.log_dict({"columns": list(feature_columns)},
                            "feature_columns.json")
        signature_input = None
        try:
            from mlflow.models import infer_signature

            import pandas as pd
            from sklearn.base import is_classifier

            col_names: list[str] | None = None
            if hasattr(model, "feature_names_in_"):
                col_names = list(model.feature_names_in_)
            elif feature_columns:
                col_names = list(feature_columns)
            if col_names:
                sample_input = pd.DataFrame(
                    {col: [0.0] for col in col_names},
                ).astype(float)
                signature_output = (
                    pd.DataFrame({"denial_probability": [0.5]})
                    if is_classifier(model)
                    else None
                )
                signature_input = infer_signature(
                    sample_input, signature_output)
        except Exception:
            logger.warning(
                "[%s] MLflow signature inference failed",
                get_ml_diagnostic_id("mlflow_signature_inference_failed"),
                exc_info=True,
            )
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
        client = mlflow.tracking.MlflowClient()
        version = getattr(logged, "registered_model_version", None)
        if version is None:
            versions = client.search_model_versions(
                f"name='{registered_model_name}' and run_id='{run_id}'"
            )
            if versions:
                version = max(int(v.version) for v in versions)
        if version is None:
            diag_id = get_ml_diagnostic_id("mlflow_version_resolution_failed")
            raise RuntimeError(
                f"[{diag_id}] Registered model {registered_model_name} under run "
                f"{run_id} but could not resolve version. "
                f"Champion alias '{champion_alias}' cannot be set."
            )
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

    return run_id


__all__ = [
    "CATBOOST_DEFAULT_PARAMS",
    "LIGHTGBM_DEFAULT_PARAMS",
    "LOGREG_DEFAULT_PARAMS",
    "OPTUNA_PRECISION_FLOOR",
    "XGBOOST_DEFAULT_PARAMS",
    "calibrate_classifier",
    "compare_and_promote",
    "compute_sample_weights",
    "select_best_calibration",
    "select_features_by_importance",
    "train_catboost",
    "train_lightgbm",
    "train_logistic_regression",
    "train_stacking_ensemble",
    "train_voting_ensemble",
    "train_with_mlflow",
    "train_xgboost",
    "tune_catboost_optuna",
    "tune_lightgbm_optuna",
    "tune_xgboost_optuna",
]
