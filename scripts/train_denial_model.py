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
from src.ml.features import prepare_training_data, stratified_split
from src.ml.train import (
    LOGREG_DEFAULT_PARAMS,
    XGBOOST_DEFAULT_PARAMS,
    train_logistic_regression,
    train_with_mlflow,
    train_xgboost,
    tune_xgboost_optuna,
)

logger = logging.getLogger(__name__)


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
        help="Run Optuna hyperparameter tuning (50 trials)",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Skip Optuna tuning, use default XGBoost params",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=None,
        help="MLflow tracking URI (defaults to Databricks)",
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
    mlflow_tracking_uri: str | None = None,
) -> tuple:
    """Run the full LR + XGBoost training + MLflow logging pipeline."""
    X, y = prepare_training_data(df)
    X_train, X_test, y_train, y_test = stratified_split(X, y)

    logreg = train_logistic_regression(X_train, y_train)
    logreg_metrics = evaluate_model(logreg, X_test, y_test)

    if tune:
        xgb_model, xgb_params = tune_xgboost_optuna(X_train, y_train, n_trials=50)
    else:
        xgb_model = train_xgboost(X_train, y_train, X_val=X_test, y_val=y_test)
        xgb_params = {
            k: v for k, v in XGBOOST_DEFAULT_PARAMS.items()
            if k != "early_stopping_rounds"
        }

    xgb_metrics = evaluate_model(xgb_model, X_test, y_test)

    best_model = xgb_model if xgb_metrics.roc_auc >= logreg_metrics.roc_auc else logreg
    best_name = "xgboost" if xgb_metrics.roc_auc >= logreg_metrics.roc_auc else "logistic_regression"
    best_params = xgb_params if best_name == "xgboost" else LOGREG_DEFAULT_PARAMS
    best_metrics = xgb_metrics if best_name == "xgboost" else logreg_metrics

    if mlflow_tracking_uri:
        import mlflow

        mlflow.set_tracking_uri(mlflow_tracking_uri)

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
        )
    except Exception:
        logger.warning("MLflow logging failed, continuing without tracking", exc_info=True)

    return best_model, best_name, best_metrics, logreg_metrics, xgb_metrics


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (0=pass, 1=fail)."""
    args = _parse_args(argv)
    tune = args.tune and not args.no_tune

    df = _load_features(args)

    model, name, best_metrics, logreg_metrics, xgb_metrics = train_pipeline(
        df,
        tune=tune,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )

    print(f"Model: {name}")
    print(f"LogReg ROC-AUC: {logreg_metrics.roc_auc:.4f}")
    print(f"XGBoost ROC-AUC: {xgb_metrics.roc_auc:.4f}")
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
        # Release gate per ARCHITECTURE.md §13: a failing model must not be
        # persisted under the production artifact path, otherwise downstream
        # serving could pick it up. Print the failures and exit non-zero so
        # CI/CD blocks promotion.
        print(f"FAIL: Threshold misses: {', '.join(failures)}")
        print("Model NOT saved (release gate blocked promotion).")
        return 1

    model_dir = Path(args.model_output).parent
    model_dir.mkdir(parents=True, exist_ok=True)
    with Path(args.model_output).open("wb") as handle:
        pickle.dump(model, handle)

    print("PASS: Model meets evaluation thresholds")
    print(f"Model saved to {args.model_output}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    sys.exit(main())
