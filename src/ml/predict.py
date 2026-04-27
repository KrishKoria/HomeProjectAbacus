from __future__ import annotations

import enum
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.ml import FEATURE_COLUMNS, TARGET_COLUMN
from src.ml.features import fill_nulls


class RiskLevel(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

    @classmethod
    def from_probability(cls, prob: float) -> RiskLevel:
        if prob < 0.3:
            return cls.LOW
        if prob < 0.7:
            return cls.MEDIUM
        return cls.HIGH


def load_trained_model(path: str | Path) -> Any:
    model_path = Path(path)
    with model_path.open("rb") as handle:
        return pickle.load(handle)


def predict_single(
    model: Any,
    feature_dict: dict[str, Any],
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
) -> dict[str, Any]:
    df = pd.DataFrame([feature_dict])
    df = fill_nulls(df)
    for col in feature_columns:
        if col in df.columns:
            if df[col].dtype == bool:
                df[col] = df[col].astype(int)
    X = df.get(list(feature_columns), pd.DataFrame())
    prob = model.predict_proba(X)[0, 1] if hasattr(model, "predict_proba") else float(model.predict(X)[0])
    risk = RiskLevel.from_probability(prob)
    return {
        "denial_probability": float(prob),
        "risk_level": risk.value,
    }


def predict_batch(
    model: Any,
    feature_df: pd.DataFrame,
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
) -> pd.DataFrame:
    filled = fill_nulls(feature_df)
    for col in feature_columns:
        if col in filled.columns:
            if filled[col].dtype == bool:
                filled[col] = filled[col].astype(int)
    X = filled[list(feature_columns)]
    probs = model.predict_proba(X)[:, 1] if hasattr(model, "predict_proba") else model.predict(X)
    risk_levels = [RiskLevel.from_probability(p).value for p in probs]
    result = feature_df[["claim_id"]].copy() if "claim_id" in feature_df.columns else pd.DataFrame()
    result["denial_probability"] = probs
    result["risk_level"] = risk_levels
    return result


__all__ = [
    "RiskLevel",
    "load_trained_model",
    "predict_batch",
    "predict_single",
]