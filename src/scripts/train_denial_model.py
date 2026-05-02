from __future__ import annotations

import argparse
import logging
import pickle
import sys
from pathlib import Path

import pandas as pd

from src.ml.evaluate import (
    compute_confusion_matrix,
    compute_shap_values,
    evaluate_model,
)
from src.ml.features import extract_provider_groups, prepare_training_data, stratified_split
from src.ml import FEATURE_COLUMNS, TARGET_COLUMN
from src.ml.retrain_gate import _current_gold_version, compute_fingerprint
from src.ml.train import (
    CATBOOST_DEFAULT_PARAMS,
    LIGHTGBM_DEFAULT_PARAMS,
    LOGREG_DEFAULT_PARAMS,
    XGBOOST_DEFAULT_PARAMS,
    calibrate_classifier,
    compute_sample_weights,
    select_best_calibration,
    train_catboost,
    train_lightgbm,
    train_logistic_regression,
    train_stacking_ensemble,
    train_voting_ensemble,
    train_with_mlflow,
    train_xgboost,
    tune_catboost_optuna,
    tune_lightgbm_optuna,
    tune_xgboost_optuna,
)

logger = logging.getLogger(__name__)


def _entrypoint_argv() -> list[str]:
    return sys.argv[1:] if len(sys.argv) > 1 else ["--tune"]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train claim denial prediction model")
    parser.add_argument(
        "--gold-table",
        default="healthcare.gold.claim_features",
        help="Fully-qualified Gold feature table name",
    )
    parser.add_argument(
        "--gold-csv",
        default=None,
        help=(
            "Optional path to a Gold-features CSV used when Spark is unavailable. "
            "Must contain the engineered feature columns; the Bronze claims CSV "
            "is NOT a valid substitute."
        ),
    )
    parser.add_argument(
        "--catalog",
        default="healthcare",
        help="Unity Catalog catalog name",
    )
    parser.add_argument(
        "--model-output",
        default="models/claim_denial_model.pkl",
        help="Output path for the trained model pickle file",
    )
    parser.add_argument(
        "--tune",
        action="store_true",
        help="Run Optuna hyperparameter tuning",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Skip Optuna tuning, use default XGBoost params",
    )
    parser.add_argument(
        "--optuna-trials",
        type=int,
        default=50,
        help="Number of Optuna trials when --tune is active (default: 50)",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=None,
        help="MLflow tracking URI (defaults to Databricks)",
    )
    parser.add_argument(
        "--registered-model-name",
        default="healthcare.ml.claim_denial_model",
        help=(
            "MLflow Registry name for the gate-passing model. Use a 3-level "
            "Unity Catalog name (catalog.schema.model) on Databricks; pass "
            "an empty string to skip registration."
        ),
    )
    parser.add_argument(
        "--champion-alias",
        default="champion",
        help="Registry alias to move onto the new version (empty = no alias).",
    )
    return parser.parse_args(argv)


def _load_features(args: argparse.Namespace) -> pd.DataFrame:
    """Load Gold features from Spark or, when unavailable, from an explicit CSV.

    The previous implementation silently fell back to ``datasets/claims_1000.csv``
    (the Bronze synthetic claims file), which lacks every engineered feature
    and would crash inside ``prepare_training_data`` with a confusing KeyError.
    The fallback now requires an explicit Gold-features CSV path.
    """
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        return spark.table(args.gold_table).toPandas()
    except Exception:
        logger.warning("Spark unavailable; checking --gold-csv fallback", exc_info=True)

    if args.gold_csv:
        csv_path = Path(args.gold_csv)
        if not csv_path.exists():
            logger.error("--gold-csv path does not exist: %s", csv_path)
            sys.exit(1)
        return pd.read_csv(csv_path)

    logger.error(
        "No data source available. Either run inside Databricks (Spark) or "
        "pass --gold-csv pointing at a CSV exported from healthcare.gold.claim_features."
    )
    sys.exit(1)


def train_pipeline(
    df: pd.DataFrame,
    tune: bool = False,
    optuna_trials: int = 50,
    random_seed: int = 42,
    mlflow_tracking_uri: str | None = None,
    registered_model_name: str | None = None,
    champion_alias: str | None = "champion",
    register_only_on_pass: bool = True,
    gold_table_name: str = "healthcare.gold.claim_features",
) -> tuple:
    """Run the full LR + XGBoost training + MLflow logging pipeline.

    Both candidates are wrapped in ``CalibratedClassifierCV`` (Platt scaling)
    before evaluation so the §13 ``HIGH_RISK_PROBABILITY_THRESHOLD = 0.7``
    cutoff lands on properly calibrated probabilities. ``tune_xgboost_optuna``
    already returns a calibrated model, so we only calibrate the no-tune
    XGBoost path and the LR baseline here.
    """
    provider_groups = extract_provider_groups(df)
    X, y = prepare_training_data(df)
    X_train, X_test, y_train, y_test = stratified_split(X, y, random_state=random_seed)

    # Align provider groups with X_train indices for GroupKFold
    train_groups = provider_groups.loc[X_train.index] if provider_groups is not None else None

    # Further split X_train for calibration selection (80/20)
    from sklearn.model_selection import train_test_split

    X_tr, X_cal, y_tr, y_cal = train_test_split(
        X_train, y_train, test_size=0.2, stratify=y_train, random_state=random_seed,
    )
    sample_weight_tr = compute_sample_weights(y_tr)

    # --- Logistic Regression (baseline) ---
    logreg_raw = train_logistic_regression(X_train, y_train, random_seed=random_seed)
    logreg = select_best_calibration(logreg_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
    logreg_metrics = evaluate_model(logreg, X_test, y_test)
    logreg_params = dict(LOGREG_DEFAULT_PARAMS)
    logreg_params["random_state"] = random_seed

    # --- XGBoost ---
    if tune:
        xgb_model, xgb_params = tune_xgboost_optuna(
            X_train, y_train, n_trials=optuna_trials, random_seed=random_seed, groups=train_groups,
        )
    else:
        xgb_raw = train_xgboost(X_tr, y_tr, random_seed=random_seed, sample_weight=sample_weight_tr)
        xgb_model = select_best_calibration(xgb_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
        xgb_params = {
            k: v for k, v in XGBOOST_DEFAULT_PARAMS.items()
            if k != "early_stopping_rounds"
        }
        xgb_params["random_state"] = random_seed
    xgb_metrics = evaluate_model(xgb_model, X_test, y_test)

    # --- LightGBM ---
    if tune:
        lgb_model, lgb_params = tune_lightgbm_optuna(
            X_train, y_train, n_trials=optuna_trials, random_seed=random_seed, groups=train_groups,
        )
    else:
        lgb_raw = train_lightgbm(X_tr, y_tr, random_seed=random_seed, sample_weight=sample_weight_tr)
        lgb_model = select_best_calibration(lgb_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
        lgb_params = dict(LIGHTGBM_DEFAULT_PARAMS)
        lgb_params["random_state"] = random_seed
    lgb_metrics = evaluate_model(lgb_model, X_test, y_test)

    # --- CatBoost ---
    if tune:
        catboost_model, catboost_params = tune_catboost_optuna(
            X_train, y_train, n_trials=optuna_trials, random_seed=random_seed, groups=train_groups,
        )
    else:
        catboost_raw = train_catboost(X_tr, y_tr, random_seed=random_seed, sample_weight=sample_weight_tr)
        catboost_model = select_best_calibration(catboost_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
        catboost_params = dict(CATBOOST_DEFAULT_PARAMS)
        catboost_params["random_seed"] = random_seed
    catboost_metrics = evaluate_model(catboost_model, X_test, y_test)

    # --- Ensembles (tree-based models only, already calibrated) ---
    tree_estimators = [
        ("xgboost", xgb_model),
        ("lightgbm", lgb_model),
        ("catboost", catboost_model),
    ]
    voting_model = train_voting_ensemble(tree_estimators, X_train, y_train)
    voting_metrics = evaluate_model(voting_model, X_test, y_test)
    voting_params = {"voting": "soft", "estimators": [n for n, _ in tree_estimators]}

    stacking_model = train_stacking_ensemble(tree_estimators, X_train, y_train)
    stacking_metrics = evaluate_model(stacking_model, X_test, y_test)
    stacking_params = {"cv": 5, "meta_learner": "LogisticRegression", "estimators": [n for n, _ in tree_estimators]}

    candidates = [
        ("xgboost", xgb_model, xgb_params, xgb_metrics),
        ("lightgbm", lgb_model, lgb_params, lgb_metrics),
        ("catboost", catboost_model, catboost_params, catboost_metrics),
        ("voting_ensemble", voting_model, voting_params, voting_metrics),
        ("stacking_ensemble", stacking_model, stacking_params, stacking_metrics),
        ("logistic_regression", logreg, logreg_params, logreg_metrics),
    ]
    candidates.sort(
        key=lambda c: (c[3].meets_thresholds(), c[3].recall_at_high, c[3].roc_auc),
        reverse=True,
    )
    best_name, best_model, best_params, best_metrics = candidates[0]

    if mlflow_tracking_uri:
        import mlflow

        mlflow.set_tracking_uri(mlflow_tracking_uri)

    should_register = bool(registered_model_name) and (
        best_metrics.meets_thresholds() or not register_only_on_pass
    )
    register_target = registered_model_name if should_register else None

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        gold_version = _current_gold_version(spark, gold_table_name)
        fingerprint = compute_fingerprint(spark, gold_table_name, list(FEATURE_COLUMNS))
    except Exception as exc:
        logger.error(
            "Cannot compute Gold-table metadata required for MLflow logging: %s",
            str(exc),
        )
        raise RuntimeError(
            f"Gold metadata computation failed for {gold_table_name}: {exc}"
        ) from exc

    if not fingerprint:
        raise RuntimeError(
            f"Empty/None fingerprint computed for {gold_table_name}. "
            f"Training cannot proceed without a valid data fingerprint."
        )

    training_metadata = {
        "training_row_count": len(df),
        "gold_table_name": gold_table_name,
        "gold_table_version": gold_version,
        "training_data_fingerprint": fingerprint,
        "feature_columns": list(FEATURE_COLUMNS),
        "target_column": TARGET_COLUMN,
        "release_gate_passed": best_metrics.meets_thresholds(),
    }

    try:
        train_with_mlflow(
            best_model,
            best_name,
            best_params,
            {
                "accuracy": best_metrics.accuracy,
                "precision": best_metrics.precision,
                "recall": best_metrics.recall,
                "recall_at_high": best_metrics.recall_at_high,
                "f1": best_metrics.f1,
                "roc_auc": best_metrics.roc_auc,
            },
            registered_model_name=register_target,
            champion_alias=champion_alias or None,
            training_metadata=training_metadata,
        )
    except Exception:
        logger.warning("MLflow logging failed, continuing without tracking", exc_info=True)

    return best_model, best_name, best_metrics, logreg_metrics, xgb_metrics, lgb_metrics, catboost_metrics, voting_metrics, stacking_metrics


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (0=pass, 1=fail)."""
    args = _parse_args(argv)
    tune = args.tune and not args.no_tune

    df = _load_features(args)

    model, name, best_metrics, logreg_metrics, xgb_metrics, lgb_metrics, catboost_metrics, voting_metrics, stacking_metrics = train_pipeline(
        df,
        tune=tune,
        optuna_trials=args.optuna_trials,
        random_seed=args.random_seed,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
        registered_model_name=args.registered_model_name or None,
        champion_alias=args.champion_alias or None,
        gold_table_name=args.gold_table,
    )

    print(f"Best model: {name}")
    for label, metrics in [
        ("LogReg", logreg_metrics),
        ("XGBoost", xgb_metrics),
        ("LightGBM", lgb_metrics),
        ("CatBoost", catboost_metrics),
        ("Voting", voting_metrics),
        ("Stacking", stacking_metrics),
    ]:
        print(
            f"  {label:>10s}  ROC-AUC: {metrics.roc_auc:.4f}  "
            f"Recall@HIGH: {metrics.recall_at_high:.4f}  "
            f"Precision: {metrics.precision:.4f}  "
            f"gate={'PASS' if metrics.meets_thresholds() else 'FAIL'}"
        )
    print(
        f"Best: {name} (ROC-AUC: {best_metrics.roc_auc:.4f}, "
        f"Recall@HIGH: {best_metrics.recall_at_high:.4f}, "
        f"Precision: {best_metrics.precision:.4f})"
    )

    if not best_metrics.meets_thresholds():
        failures = []
        if best_metrics.recall_at_high < 0.80:
            failures.append(f"recall_at_high={best_metrics.recall_at_high:.4f} < 0.80")
        if best_metrics.precision < 0.70:
            failures.append(f"precision={best_metrics.precision:.4f} < 0.70")
        if best_metrics.roc_auc < 0.85:
            failures.append(f"roc_auc={best_metrics.roc_auc:.4f} < 0.85")
        print(f"FAIL: Threshold misses: {', '.join(failures)}")
        print("Model NOT saved (release gate blocked promotion).")
        return 1

    model_dir = Path(args.model_output).parent
    model_dir.mkdir(parents=True, exist_ok=True)
    with Path(args.model_output).open("wb") as handle:
        pickle.dump(model, handle)

    print("PASS: Model meets evaluation thresholds")
    print(f"Model saved to {args.model_output}")
    if args.registered_model_name:
        print(
            f"Registered to MLflow Registry as '{args.registered_model_name}'"
            + (f" (alias '{args.champion_alias}')" if args.champion_alias else "")
        )
    return 0


if __name__ == "__main__":
    import traceback

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    _rc = 1
    try:
        _rc = main(_entrypoint_argv())
    except Exception:
        traceback.print_exc()
    finally:
        try:
            from pyspark.sql import SparkSession
        except ModuleNotFoundError:
            SparkSession = None

        if SparkSession is not None:
            try:
                SparkSession.builder.getOrCreate().stop()
            except Exception:
                pass
    if _rc != 0:
        raise RuntimeError(f"Training failed with exit code {_rc}")
