from __future__ import annotations

from typing import Final

import pandas as pd
from sklearn.model_selection import train_test_split

from src.ml import FEATURE_COLUMNS, TARGET_COLUMN


BOOLEAN_FEATURES: Final[tuple[str, ...]] = (
    "is_procedure_missing",
    "is_amount_missing",
    "high_cost_flag",
    "severity_procedure_mismatch",
    "specialty_diagnosis_mismatch",
    "provider_location_missing",
)

NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "amount_to_benchmark_ratio",
    "billed_vs_avg_cost",
    "diagnosis_severity_encoded",
    "diagnosis_count",
    "provider_claim_count",
    "provider_claim_count_30d",
    "provider_risk_score",
)

DEFAULT_FILL_VALUES: Final[dict[str, float | int]] = {
    "is_procedure_missing": 0,
    "is_amount_missing": 0,
    "amount_to_benchmark_ratio": 0.0,
    "billed_vs_avg_cost": 0.0,
    "high_cost_flag": 0,
    "severity_procedure_mismatch": 0,
    "specialty_diagnosis_mismatch": 0,
    "provider_location_missing": 0,
    "diagnosis_severity_encoded": 0,
    "diagnosis_count": 1,
    "provider_claim_count": 0,
    "provider_claim_count_30d": 0,
    "provider_risk_score": 0.0,
}

DEFAULT_TEST_SIZE: Final[float] = 0.3
DEFAULT_RANDOM_SEED: Final[int] = 42


def load_gold_features(
    spark,
    catalog: str = "healthcare",
    gold_schema: str = "gold",
) -> pd.DataFrame:
    table_fqn = f"{catalog}.{gold_schema}.claim_features"
    return spark.table(table_fqn).toPandas()


def fill_nulls(df: pd.DataFrame, fill_values: dict[str, float | int] | None = None) -> pd.DataFrame:
    values = fill_values if fill_values is not None else DEFAULT_FILL_VALUES
    filled = df.copy()
    for col_name, fill_val in values.items():
        if col_name in filled.columns:
            filled[col_name] = filled[col_name].fillna(fill_val).infer_objects(copy=False)
    return filled


def prepare_training_data(
    df: pd.DataFrame,
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
    target_column: str = TARGET_COLUMN,
) -> tuple[pd.DataFrame, pd.Series]:
    filled = fill_nulls(df)
    X = filled[list(feature_columns)].copy()
    for col in BOOLEAN_FEATURES:
        if col in X.columns:
            X[col] = X[col].astype(int)
    y = filled[target_column].astype(int)
    return X, y


def stratified_split(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = DEFAULT_TEST_SIZE,
    random_state: int = DEFAULT_RANDOM_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    return train_test_split(
        X,
        y,
        test_size=test_size,
        stratify=y,
        random_state=random_state,
    )


__all__ = [
    "BOOLEAN_FEATURES",
    "DEFAULT_FILL_VALUES",
    "DEFAULT_RANDOM_SEED",
    "DEFAULT_TEST_SIZE",
    "NUMERIC_FEATURES",
    "fill_nulls",
    "load_gold_features",
    "prepare_training_data",
    "stratified_split",
]