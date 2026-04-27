from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from src.ml import FEATURE_COLUMNS, TARGET_COLUMN
from src.ml.evaluate import (
    EvaluationMetrics,
    compute_confusion_matrix,
    compute_shap_values,
    evaluate_model,
    generate_evaluation_report,
)
from src.ml.features import fill_nulls, prepare_training_data, stratified_split
from src.ml.predict import RiskLevel
from src.ml.train import (
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


def train_pipeline(
    df: pd.DataFrame,
    tune: bool = False,
    mlflow_tracking_uri: str | None = None,
) -> tuple:
    from src.ml.train import LOGREG_DEFAULT_PARAMS, XGBoost_DEFAULT_PARAMS

    X, y = prepare_training_data(df)
    X_train, X_test, y_train, y_test = stratified_split(X, y)

    logreg = train_logistic_regression(X_train, y_train)
    logreg_metrics = evaluate_model(logreg, X_test, y_test)

    if tune:
        xgb_model, xgb_params = tune_xgboost_optuna(X_train, y_train, n_trials=50)
    else:
        xgb_model = train_xgboost(X_train, y_train, X_val=X_test, y_val=y_test)
        xgb_params = {k: v for k, v in XGBoost_DEFAULT_PARAMS.items() if k != "early_stopping_rounds"}

    xgb_metrics = evaluate_model(xgb_model, X_test, y_test)

    best_model = xgb_model if xgb_metrics.roc_auc >= logreg_metrics.roc_auc else logreg
    best_name = "xgboost" if xgb_metrics.roc_auc >= logreg_metrics.roc_auc else "logistic_regression"
    best_params = xgb_params if best_name == "xgboost" else LOGREG_DEFAULT_PARAMS
    best_metrics = xgb_metrics if best_name == "xgboost" else logreg_metrics

    shap_values, feature_names = compute_shap_values(xgb_model, X_test)

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
                "f1": best_metrics.f1,
                "roc_auc": best_metrics.roc_auc,
            },
        )
    except Exception:
        logger.warning("MLflow logging failed, continuing without tracking")

    return best_model, best_name, best_metrics, logreg_metrics, xgb_metrics


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    tune = args.tune and not args.no_tune

    import pickle

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.table(args.gold_table).toPandas()
    except Exception:
        logger.warning("Spark unavailable, attempting to load from local CSV")
        csv_path = Path("datasets/claims_1000.csv")
        if not csv_path.exists():
            logger.error("No data source available")
            sys.exit(1)
        df = pd.read_csv(csv_path)

    model, name, best_metrics, logreg_metrics, xgb_metrics = train_pipeline(
        df,
        tune=tune,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )

    model_dir = Path(args.model_output).parent
    model_dir.mkdir(parents=True, exist_ok=True)
    with Path(args.model_output).open("wb") as handle:
        pickle.dump(model, handle)

    print(f"Model: {name}")
    print(f"LogReg ROC-AUC: {logreg_metrics.roc_auc:.4f}")
    print(f"XGBoost ROC-AUC: {xgb_metrics.roc_auc:.4f}")
    print(f"Best: {name} (ROC-AUC: {best_metrics.roc_auc:.4f})")

    if best_metrics.meets_thresholds():
        print("PASS: Model meets evaluation thresholds")
    else:
        failures = []
        if best_metrics.recall < 0.80:
            failures.append(f"recall={best_metrics.recall:.4f} < 0.80")
        if best_metrics.precision < 0.70:
            failures.append(f"precision={best_metrics.precision:.4f} < 0.70")
        if best_metrics.roc_auc < 0.85:
            failures.append(f"roc_auc={best_metrics.roc_auc:.4f} < 0.85")
        print(f"FAIL: Threshold misses: {', '.join(failures)}")

    print(f"Model saved to {args.model_output}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    main()