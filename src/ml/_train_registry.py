from __future__ import annotations

import logging
import os
from typing import Any

import mlflow
import pandas as pd
from sklearn.base import is_classifier

from src.common.diagnostics import get_ml_diagnostic_id

logger = logging.getLogger(__name__)


def compare_and_promote(
    new_recall_at_high: float,
    current_champion_recall_at_high: float | None,
    min_improvement: float = 0.01,
) -> bool:
    if current_champion_recall_at_high is None:
        return True
    return (new_recall_at_high - current_champion_recall_at_high) >= min_improvement


def select_features_by_importance(
    model: Any,
    X_val: Any,
    y_val: Any,
    feature_names: list[str],
    min_importance: float = 0.001,
) -> list[str]:
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
    return name.count(".") == 2


def _configure_registry_for_runtime(registered_model_name: str | None) -> None:
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
    from src.ml._train_algorithms import _resolve_experiment_name

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

            col_names: list[str] | None = None
            if hasattr(model, "feature_names_in_"):
                col_names = list(model.feature_names_in_)
            elif feature_columns:
                col_names = list(feature_columns)
            if col_names:
                sample_input = pd.DataFrame({col: [0.0] for col in col_names}).astype(float)
                signature_output = (
                    pd.DataFrame({"denial_probability": [0.5]})
                    if is_classifier(model)
                    else None
                )
                signature_input = infer_signature(sample_input, signature_output)
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
    "compare_and_promote",
    "select_features_by_importance",
    "train_with_mlflow",
]
