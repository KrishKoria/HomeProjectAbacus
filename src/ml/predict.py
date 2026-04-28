from __future__ import annotations

import enum
import logging
import os
import pickle
import time
from pathlib import Path
from typing import Any, Final

import pandas as pd

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


def load_from_registry(
    name: str = "healthcare.ml.claim_denial_model",
    alias: str = "champion",
) -> Any:
    """Load the gate-passing model from the MLflow Model Registry.

    Prefer this over ``load_trained_model`` in production code paths: the
    registry alias (default ``champion``) always points at the latest
    version that cleared the ARCHITECTURE.md §13 gate, so callers do not
    need to know about run_ids or pickle paths.

    On Databricks with a 3-level (Unity Catalog) name we set BOTH:

    - ``mlflow.set_registry_uri('databricks-uc')`` so the version lookup
      hits the same registry the training script wrote to.
    - ``mlflow.set_tracking_uri('databricks')`` so artifact download goes
      through the Databricks REST API instead of trying direct S3 access
      against the underlying ``dbstorage-*`` bucket (which fails with 400
      Bad Request because the cluster lacks raw S3 credentials).
    """
    import mlflow

    if name.count(".") == 2 and _looks_like_databricks():
        mlflow.set_registry_uri("databricks-uc")
        if not mlflow.get_tracking_uri().startswith("databricks"):
            mlflow.set_tracking_uri("databricks")
    return mlflow.sklearn.load_model(f"models:/{name}@{alias}")


def _coerce_features(
    df: pd.DataFrame,
    feature_columns: tuple[str, ...],
) -> pd.DataFrame:
    filled = fill_nulls(df)
    for col in feature_columns:
        if col in filled.columns and filled[col].dtype == bool:
            filled[col] = filled[col].astype(int)
    return filled[list(feature_columns)]


def predict_single(
    model: Any,
    feature_dict: dict[str, Any],
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
) -> dict[str, Any]:
    """Score a single claim and emit a latency log line for the §13 budget."""
    start = time.perf_counter()
    df = pd.DataFrame([feature_dict])
    X = _coerce_features(df, feature_columns)
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
    X = _coerce_features(feature_df, feature_columns)
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
    "load_from_registry",
    "load_trained_model",
    "predict_batch",
    "predict_single",
]
