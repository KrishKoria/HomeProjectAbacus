from __future__ import annotations

import logging
import os
from typing import Any, Final

import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier

from src.common.diagnostics import get_ml_diagnostic_id
from src.ml.evaluate import recall_at_high

logger = logging.getLogger(__name__)


# Soft-constraint precision floor inside the Optuna objective. Trials that fall
# below this floor are penalised so the search is steered into the feasible
# region rather than discarded outright (which would give the sampler no signal).
OPTUNA_PRECISION_FLOOR: Final[float] = 0.70


def _resolve_experiment_name(model_name: str) -> str:
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
    training_params = {**LOGREG_DEFAULT_PARAMS, **(params or {}), "random_state": random_seed}
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
    training_params = {**XGBOOST_DEFAULT_PARAMS, **(params or {}), "random_state": random_seed}
    if X_val is None or y_val is None:
        training_params.pop("early_stopping_rounds", None)
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
    import lightgbm as lgb

    training_params = {**LIGHTGBM_DEFAULT_PARAMS, **(params or {}), "random_state": random_seed}
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
    from catboost import CatBoostClassifier

    training_params = {**CATBOOST_DEFAULT_PARAMS, **(params or {}), "random_seed": random_seed}
    model = CatBoostClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)
    return model


def _build_xgb_from_trial(trial: Any, random_seed: int = 42) -> XGBClassifier:
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 300),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
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
    base_estimator = _build_xgb_from_trial(trial, random_seed=random_seed)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
    fold_recalls: list[float] = []
    fold_precisions: list[float] = []
    for tr_idx, va_idx in skf.split(X_train, y_train):
        X_tr = X_train.iloc[tr_idx] if hasattr(X_train, "iloc") else X_train[tr_idx]
        X_va = X_train.iloc[va_idx] if hasattr(X_train, "iloc") else X_train[va_idx]
        y_tr = y_train.iloc[tr_idx] if hasattr(y_train, "iloc") else y_train[tr_idx]
        y_va = y_train.iloc[va_idx] if hasattr(y_train, "iloc") else y_train[va_idx]
        calibrated = CalibratedClassifierCV(base_estimator, method="sigmoid", cv=2)
        calibrated.fit(X_tr, y_tr)
        proba = calibrated.predict_proba(X_va)[:, 1]
        fold_recalls.append(recall_at_high(y_va, proba))
        pred = (proba >= 0.5).astype(int)
        fold_precisions.append(float(precision_score(y_va, pred, zero_division=0)))
    mean_recall = float(np.mean(fold_recalls))
    mean_precision = float(np.mean(fold_precisions))
    if mean_precision < OPTUNA_PRECISION_FLOOR:
        return mean_recall - 2.0 * (OPTUNA_PRECISION_FLOOR - mean_precision)
    return mean_recall


def compute_sample_weights(
    y_train: Any,
    positive_weight: float = 3.0,
) -> Any:
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
                splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
                split_iter = splitter.split(X_train, y_train)
        else:
            splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
            split_iter = splitter.split(X_train, y_train)
        fold_recalls: list[float] = []
        fold_precisions: list[float] = []
        for tr_idx, va_idx in split_iter:
            X_tr = X_train.iloc[tr_idx] if hasattr(X_train, "iloc") else X_train[tr_idx]
            X_va = X_train.iloc[va_idx] if hasattr(X_train, "iloc") else X_train[va_idx]
            y_tr = y_train.iloc[tr_idx] if hasattr(y_train, "iloc") else y_train[tr_idx]
            y_va = y_train.iloc[va_idx] if hasattr(y_train, "iloc") else y_train[va_idx]
            calibrated = CalibratedClassifierCV(base_estimator, method="sigmoid", cv=2)
            calibrated.fit(X_tr, y_tr)
            proba = calibrated.predict_proba(X_va)[:, 1]
            fold_recalls.append(recall_at_high(y_va, proba))
            pred = (proba >= 0.5).astype(int)
            fold_precisions.append(float(precision_score(y_va, pred, zero_division=0)))
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
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_xgb_from_trial, X_train, y_train, random_seed, groups=groups),
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
    from src.ml._train_ensembles import calibrate_classifier
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def tune_lightgbm_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
    random_seed: int = 42,
    groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    import lightgbm as lgb
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_lgb_from_trial, X_train, y_train, random_seed, groups=groups),
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
    from src.ml._train_ensembles import calibrate_classifier
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


def tune_catboost_optuna(
    X_train: Any,
    y_train: Any,
    n_trials: int = 50,
    random_seed: int = 42,
    groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    from catboost import CatBoostClassifier
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_catboost_from_trial, X_train, y_train, random_seed, groups=groups),
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
    from src.ml._train_ensembles import calibrate_classifier
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params


__all__ = [
    "CATBOOST_DEFAULT_PARAMS",
    "LIGHTGBM_DEFAULT_PARAMS",
    "LOGREG_DEFAULT_PARAMS",
    "OPTUNA_PRECISION_FLOOR",
    "XGBOOST_DEFAULT_PARAMS",
    "compute_sample_weights",
    "train_catboost",
    "train_lightgbm",
    "train_logistic_regression",
    "train_xgboost",
    "tune_catboost_optuna",
    "tune_lightgbm_optuna",
    "tune_xgboost_optuna",
]
