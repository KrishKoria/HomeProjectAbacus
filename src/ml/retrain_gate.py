from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any

import mlflow
from mlflow.tracking import MlflowClient


@dataclass(frozen=True, slots=True)
class RetrainDecision:
    should_retrain: bool
    reason: str
    current_row_count: int
    current_gold_version: int
    current_fingerprint: str
    champion_run_id: str | None

    @classmethod
    def should_retrain_false(
        cls,
        reason: str,
        current_row_count: int,
        current_gold_version: int,
        current_fingerprint: str,
        champion_run_id: str | None,
    ) -> RetrainDecision:
        return cls(
            should_retrain=False,
            reason=reason,
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    @classmethod
    def should_retrain_true(
        cls,
        reason: str,
        current_row_count: int,
        current_gold_version: int,
        current_fingerprint: str,
        champion_run_id: str | None,
    ) -> RetrainDecision:
        return cls(
            should_retrain=True,
            reason=reason,
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    def summary_line(self) -> str:
        state = "RETRAIN" if self.should_retrain else "SKIP"
        return (
            f"{state}: reason={self.reason} current_row_count={self.current_row_count} "
            f"current_gold_version={self.current_gold_version}"
        )


def _feature_columns_from_run(run_id: str) -> list[str]:
    try:
        payload = mlflow.artifacts.load_dict(
            f"runs:/{run_id}/feature_columns.json")
    except Exception:
        return []
    columns = payload.get("columns", []) if isinstance(payload, dict) else []
    return [str(column) for column in columns]


def _current_gold_version(spark, gold_table: str) -> int:
    """
    Return the Delta table version when gold_table is a Delta table.

    Lakeflow materialized views/views do not support DESCRIBE HISTORY.
    In that case, return -1 and rely on the content fingerprint for
    retrain decisions.
    """
    try:
        row = spark.sql(f"DESCRIBE HISTORY {gold_table} LIMIT 1").collect()[0]
        return int(row["version"])
    except Exception as exc:
        message = str(exc)
        if (
            "EXPECT_TABLE_NOT_VIEW" in message
            or "expects a table" in message
            or "is a view" in message
        ):
            return -1
        raise


def compute_fingerprint(spark, gold_table: str, feature_columns: list[str]) -> str:
    # The caller must supply the non-PHI model feature list; this function never
    # introspects arbitrary table columns on its own.
    columns = sorted(feature_columns)
    frame = spark.table(gold_table).select(*columns)
    row_count = frame.count()
    try:
        from pyspark.sql import functions as F

        sample_rows = (
            frame.withColumn(
                "_sample_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        *[F.coalesce(F.col(column).cast("string"),
                                     F.lit("<NULL>")) for column in columns],
                    ),
                    256,
                ),
            )
            .orderBy(F.col("_sample_key").asc())
            .limit(256)
            .drop("_sample_key")
            .collect()
        )
    except ModuleNotFoundError:
        sample_rows = frame.collect()[:256]
    payload = {
        "columns": columns,
        "row_count": row_count,
        "rows": [row.asDict(recursive=True) for row in sample_rows],
    }
    return sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def decide_retrain(
    spark,
    gold_table: str,
    feature_columns: list[str],
    registered_model_name: str,
    champion_alias: str,
    mlflow_client=None,
) -> RetrainDecision:
    client = mlflow_client or MlflowClient()
    current_row_count = int(spark.table(gold_table).count())
    if current_row_count <= 0:
        raise ValueError(f"{gold_table} has zero rows")

    current_gold_version = _current_gold_version(spark, gold_table)
    current_fingerprint = compute_fingerprint(
        spark, gold_table, feature_columns)

    try:
        champion = client.get_model_version_by_alias(
            registered_model_name, champion_alias)
    except Exception:
        return RetrainDecision.should_retrain_true(
            reason="no champion model found",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=None,
        )

    champion_run_id = getattr(champion, "run_id", None)
    champion_run = client.get_run(champion_run_id)
    champion_params = champion_run.data.params
    champion_fingerprint = champion_params.get("training_data_fingerprint", "")
    if champion_fingerprint != current_fingerprint:
        return RetrainDecision.should_retrain_true(
            reason="data fingerprint changed",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    champion_feature_columns = _feature_columns_from_run(champion_run_id)
    if feature_columns and not champion_feature_columns:
        return RetrainDecision.should_retrain_true(
            reason="champion feature_columns metadata missing",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )
    if champion_feature_columns and champion_feature_columns != list(feature_columns):
        return RetrainDecision.should_retrain_true(
            reason="feature columns changed",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    return RetrainDecision.should_retrain_false(
        reason="no data changes",
        current_row_count=current_row_count,
        current_gold_version=current_gold_version,
        current_fingerprint=current_fingerprint,
        champion_run_id=champion_run_id,
    )


__all__ = ["RetrainDecision", "compute_fingerprint", "decide_retrain"]
