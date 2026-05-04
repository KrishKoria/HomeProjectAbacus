from __future__ import annotations

import enum
import logging
import os
import pickle
import time
from pathlib import Path
from typing import Any, Final

import pandas as pd

from src.common.diagnostics import get_ml_diagnostic_id
from src.ml import FEATURE_COLUMNS
from src.ml.features import fill_nulls

logger = logging.getLogger(__name__)


# Risk-tier cutoffs used both at inference time and for the §13 Recall@HIGH gate.
RISK_THRESHOLD_LOW: Final[float] = 0.3
RISK_THRESHOLD_HIGH: Final[float] = 0.7

# ARCHITECTURE.md §13 budgets single-claim inference at < 150 ms p95.
LATENCY_BUDGET_MS: Final[float] = 150.0


class RiskLevel(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

    @classmethod
    def from_probability(cls, prob: float) -> RiskLevel:
        if prob < RISK_THRESHOLD_LOW:
            return cls.LOW
        if prob < RISK_THRESHOLD_HIGH:
            return cls.MEDIUM
        return cls.HIGH


def load_trained_model(path: str | Path) -> Any:
    """Load a pickled model artifact from disk."""
    model_path = Path(path)
    with model_path.open("rb") as handle:
        return pickle.load(handle)


def _looks_like_databricks() -> bool:
    """Detect Databricks runtime via env var or filesystem markers.

    ``DATABRICKS_RUNTIME_VERSION`` is the canonical signal but isn't always
    populated in every notebook kernel context, so fall back to the
    ``/databricks`` and ``/Workspace`` directories that exist on every
    Databricks compute node.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    return os.path.exists("/databricks") or os.path.exists("/Workspace")


def _registry_model_uri(name: str, alias: str) -> str:
    return f"models:/{name}@{alias}"


def _configure_registry_uri(mlflow: Any, name: str) -> None:
    if name.count(".") == 2 and _looks_like_databricks():
        mlflow.set_registry_uri("databricks-uc")


def get_registry_model_dependencies(
    name: str = "healthcare.ml.claim_denial_model",
    alias: str = "champion",
) -> str:
    """Return the dependency spec path bundled with a registered pyfunc model."""
    import mlflow

    _configure_registry_uri(mlflow, name)
    return mlflow.pyfunc.get_model_dependencies(_registry_model_uri(name, alias))


def load_from_registry(
    name: str = "healthcare.ml.claim_denial_model",
    alias: str = "champion",
) -> Any:
    """Load the gate-passing champion model from the MLflow Model Registry.

    Databricks-canonical two-call load. The alias (default ``champion``)
    always points at the latest version that cleared the ARCHITECTURE.md
    §13 gate, so callers do not need to know about run_ids or pickle
    paths::

        mlflow.set_registry_uri("databricks-uc")
        mlflow.pyfunc.load_model(f"models:/{name}@{alias}")

    **Cluster prerequisites** (one-time, not a code concern):

    - Databricks Runtime **17.3 LTS ML** for current deployments. Older
      LTS ML clusters can still be supported by Databricks, but this repo
      targets the current long-lived ML runtime for UC model artifact access.
    - Quick fix without changing runtime: install latest MLflow in the
      notebook session::

          %pip install -U mlflow databricks-sdk
          dbutils.library.restartPython()

    - The cluster service principal needs ``USE CATALOG`` + ``USE SCHEMA``
      on ``healthcare.ml`` and ``EXECUTE`` on the registered model.

    For non-Databricks runs (local, CI), the registry resolves to
    whatever ``MLFLOW_TRACKING_URI`` points at — the local file-based
    registry by default.
    """
    import mlflow

    _configure_registry_uri(mlflow, name)
    model_uri = _registry_model_uri(name, alias)
    try:
        return mlflow.pyfunc.load_model(model_uri)
    except ModuleNotFoundError as exc:
        try:
            deps_path = mlflow.pyfunc.get_model_dependencies(model_uri)
        except Exception as deps_exc:
            diag_id = get_ml_diagnostic_id("registry_dependency_spec_failed")
            raise ModuleNotFoundError(
                f"[{diag_id}] {exc}. Model load failed and the dependency spec "
                f"could not be resolved for {model_uri}: {deps_exc}"
            ) from exc
        diag_id = get_ml_diagnostic_id("registry_model_load_failed")
        raise ModuleNotFoundError(
            f"[{diag_id}] {exc}. Install the registered model dependencies from "
            f"{deps_path} and restart Python before retrying.\n"
            f"%pip install -q -r {deps_path}\n"
            "dbutils.library.restartPython()"
        ) from exc


def _resolve_column_order(model: Any, feature_columns: tuple[str, ...]) -> list[str]:
    """Return the column order the model expects, falling back to ``feature_columns``.

    XGBoost and other sklearn-style estimators store ``feature_names_in_``
    after fitting. Using that order prevents ``feature_names mismatch``
    errors when the model was trained with a different column order than
    the current ``FEATURE_COLUMNS`` constant.
    """
    trained_names = getattr(model, "feature_names_in_", None)
    if trained_names is not None:
        return list(trained_names)
    return list(feature_columns)


def _coerce_features(
    df: pd.DataFrame,
    feature_columns: tuple[str, ...],
    model: Any = None,
) -> pd.DataFrame:
    filled = fill_nulls(df)
    for col in feature_columns:
        if col in filled.columns and filled[col].dtype == bool:
            filled[col] = filled[col].astype(int)
    columns = _resolve_column_order(model, feature_columns) if model is not None else list(feature_columns)
    return filled[columns].astype("float64")


def predict_single(
    model: Any,
    feature_dict: dict[str, Any],
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
) -> dict[str, Any]:
    """Score a single claim and emit a latency log line for the §13 budget."""
    start = time.perf_counter()
    df = pd.DataFrame([feature_dict])
    X = _coerce_features(df, feature_columns, model=model)
    prob = (
        model.predict_proba(X)[0, 1]
        if hasattr(model, "predict_proba")
        else float(model.predict(X)[0])
    )
    risk = RiskLevel.from_probability(prob)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    logger.debug(
        "predict_single latency_ms=%.2f risk_level=%s budget_ms=%.0f",
        elapsed_ms,
        risk.value,
        LATENCY_BUDGET_MS,
    )
    return {
        "denial_probability": float(prob),
        "risk_level": risk.value,
    }


def predict_batch(
    model: Any,
    feature_df: pd.DataFrame,
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
) -> pd.DataFrame:
    """Score a batch of claims and return probabilities + risk tiers."""
    X = _coerce_features(feature_df, feature_columns, model=model)
    probs = (
        model.predict_proba(X)[:, 1]
        if hasattr(model, "predict_proba")
        else model.predict(X)
    )
    risk_levels = [RiskLevel.from_probability(p).value for p in probs]
    result = (
        feature_df[["claim_id"]].copy()
        if "claim_id" in feature_df.columns
        else pd.DataFrame()
    )
    result["denial_probability"] = probs
    result["risk_level"] = risk_levels
    return result


__all__ = [
    "LATENCY_BUDGET_MS",
    "RISK_THRESHOLD_HIGH",
    "RISK_THRESHOLD_LOW",
    "RiskLevel",
    "get_registry_model_dependencies",
    "load_from_registry",
    "load_trained_model",
    "predict_batch",
    "predict_single",
]
