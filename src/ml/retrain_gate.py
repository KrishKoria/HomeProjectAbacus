from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import math
from typing import Any, Final, Literal

import mlflow
from mlflow.exceptions import MlflowException, RestException
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST
from mlflow.tracking import MlflowClient

import logging

logger = logging.getLogger(__name__)

_RETRAIN_ROW_COUNT_MIN_DELTA: Final[int] = 100
_RETRAIN_ROW_COUNT_PCT_THRESHOLD: Final[float] = 0.05


@dataclass(frozen=True, slots=True)
class RetrainDecision:
    decision_status: Literal["retrain", "skip", "error"]
    should_retrain: bool | None
    reason: str
    error_detail: str | None
    current_row_count: int
    current_gold_version: int
    current_fingerprint: str
    champion_run_id: str | None
    previous_training_row_count: int | None
    row_count_delta: int | None
    row_count_delta_pct: float | None

    @classmethod
    def retrain(
        cls,
        reason: str,
        current_row_count: int,
        current_gold_version: int,
        current_fingerprint: str,
        champion_run_id: str | None,
        previous_training_row_count: int | None = None,
    ) -> RetrainDecision:
        delta = (
            abs(current_row_count - previous_training_row_count)
            if previous_training_row_count is not None
            else None
        )
        pct = (
            (delta / previous_training_row_count) if delta is not None and previous_training_row_count else None
        )
        return cls(
            decision_status="retrain",
            should_retrain=True,
            reason=reason,
            error_detail=None,
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=previous_training_row_count,
            row_count_delta=delta,
            row_count_delta_pct=pct,
        )

    @classmethod
    def skip(
        cls,
        reason: str,
        current_row_count: int,
        current_gold_version: int,
        current_fingerprint: str,
        champion_run_id: str | None,
        previous_training_row_count: int | None = None,
    ) -> RetrainDecision:
        delta = (
            abs(current_row_count - previous_training_row_count)
            if previous_training_row_count is not None
            else None
        )
        pct = (
            (delta / previous_training_row_count) if delta is not None and previous_training_row_count else None
        )
        return cls(
            decision_status="skip",
            should_retrain=False,
            reason=reason,
            error_detail=None,
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=previous_training_row_count,
            row_count_delta=delta,
            row_count_delta_pct=pct,
        )

    @classmethod
    def error(
        cls,
        reason: str,
        error_detail: str,
        current_row_count: int,
        current_gold_version: int,
        current_fingerprint: str,
        champion_run_id: str | None,
    ) -> RetrainDecision:
        return cls(
            decision_status="error",
            should_retrain=None,
            reason=reason,
            error_detail=error_detail,
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=None,
            row_count_delta=None,
            row_count_delta_pct=None,
        )

    def summary_line(self) -> str:
        state = self.decision_status.upper()
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


def _parse_three_part_name(name: str) -> tuple[str, str, str] | None:
    parts = [part.strip().strip("`") for part in name.split(".")]
    if len(parts) != 3 or any(not part for part in parts):
        return None
    return (parts[0], parts[1], parts[2])


def _current_gold_version(spark, gold_table: str) -> int:
    """
    Return the Delta table version when gold_table is a Delta table.

    Lakeflow materialized views/views do not support DESCRIBE HISTORY.
    In that case, return -1 and rely on the content fingerprint for
    retrain decisions.
    """
    parsed = _parse_three_part_name(gold_table)
    if parsed is not None:
        catalog, schema, relation = parsed
        table_type_rows = spark.sql(
            f"""
            SELECT table_type
            FROM `{catalog}`.information_schema.tables
            WHERE table_schema = '{schema.replace("'", "''")}'
              AND table_name = '{relation.replace("'", "''")}'
            LIMIT 1
            """
        ).collect()
        if table_type_rows:
            first_row = table_type_rows[0]
            if isinstance(first_row, dict) and "table_type" in first_row:
                table_type = str(first_row["table_type"]).upper()
                if "VIEW" in table_type:
                    return -1
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


def _current_gold_object_metadata(spark, gold_table: str) -> tuple[str, str | None]:
    parsed = _parse_three_part_name(gold_table)
    if parsed is None:
        return ("unknown", None)
    catalog, schema, relation = parsed
    try:
        rows = spark.sql(
            f"""
            SELECT table_type, last_altered
            FROM `{catalog}`.information_schema.tables
            WHERE table_schema = '{schema.replace("'", "''")}'
              AND table_name = '{relation.replace("'", "''")}'
            LIMIT 1
            """
        ).collect()
        if rows:
            first = rows[0].asDict(recursive=True)
            obj_type = str(first.get("table_type", "unknown"))
            last_altered = str(first["last_altered"]) if first.get("last_altered") is not None else None
            return (obj_type, last_altered)
    except Exception:
        logger.warning("Could not resolve Gold object metadata", exc_info=True)
    return ("unknown", None)


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


def _row_count_threshold_exceeded(current_row_count: int, previous_training_row_count: int) -> bool:
    threshold = max(_RETRAIN_ROW_COUNT_MIN_DELTA, math.ceil(_RETRAIN_ROW_COUNT_PCT_THRESHOLD * previous_training_row_count))
    return abs(current_row_count - previous_training_row_count) >= threshold


def _resolve_champion_alias(client: MlflowClient, registered_model_name: str, champion_alias: str):
    try:
        return client.get_model_version_by_alias(registered_model_name, champion_alias)
    except (MlflowException, RestException) as exc:
        error_code = getattr(exc, "error_code", None)
        if error_code == RESOURCE_DOES_NOT_EXIST or (isinstance(error_code, str) and error_code.upper() == "RESOURCE_DOES_NOT_EXIST"):
            return None
        raise


def _resolve_champion_run(client: MlflowClient, run_id: str):
    try:
        return client.get_run(run_id)
    except (MlflowException, RestException) as exc:
        error_code = getattr(exc, "error_code", None)
        if error_code == RESOURCE_DOES_NOT_EXIST or (isinstance(error_code, str) and error_code.upper() == "RESOURCE_DOES_NOT_EXIST"):
            return None
        raise


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
        champion = _resolve_champion_alias(client, registered_model_name, champion_alias)
    except Exception as exc:
        return RetrainDecision.error(
            reason="mlflow champion alias lookup failed",
            error_detail=str(exc),
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=None,
        )

    if champion is None:
        return RetrainDecision.retrain(
            reason="no champion model found",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=None,
        )

    champion_run_id = getattr(champion, "run_id", None)
    if champion_run_id is None:
        return RetrainDecision.error(
            reason="champion model version has no run_id",
            error_detail="champion model version missing run_id",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=None,
        )

    try:
        champion_run = _resolve_champion_run(client, champion_run_id)
    except Exception as exc:
        return RetrainDecision.error(
            reason="mlflow champion run lookup failed",
            error_detail=str(exc),
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    if champion_run is None:
        return RetrainDecision.retrain(
            reason="champion run not found (orphaned reference)",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
        )

    champion_params = champion_run.data.params
    previous_training_row_count_raw = champion_params.get("training_row_count")
    if previous_training_row_count_raw is not None:
        try:
            previous_training_row_count = int(previous_training_row_count_raw)
        except (ValueError, TypeError):
            previous_training_row_count = None
    else:
        previous_training_row_count = None

    champion_fingerprint = champion_params.get("training_data_fingerprint", "")

    if champion_fingerprint != current_fingerprint:
        if previous_training_row_count is not None and not _row_count_threshold_exceeded(
            current_row_count, previous_training_row_count
        ):
            return RetrainDecision.skip(
                reason="fingerprint changed but row_count delta below retrain threshold",
                current_row_count=current_row_count,
                current_gold_version=current_gold_version,
                current_fingerprint=current_fingerprint,
                champion_run_id=champion_run_id,
                previous_training_row_count=previous_training_row_count,
            )
        return RetrainDecision.retrain(
            reason="data fingerprint changed",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=previous_training_row_count,
        )

    champion_feature_columns = _feature_columns_from_run(champion_run_id)
    if feature_columns and not champion_feature_columns:
        return RetrainDecision.retrain(
            reason="champion feature_columns metadata missing",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=previous_training_row_count,
        )
    if champion_feature_columns and champion_feature_columns != list(feature_columns):
        return RetrainDecision.retrain(
            reason="feature columns changed",
            current_row_count=current_row_count,
            current_gold_version=current_gold_version,
            current_fingerprint=current_fingerprint,
            champion_run_id=champion_run_id,
            previous_training_row_count=previous_training_row_count,
        )

    return RetrainDecision.skip(
        reason="no data changes",
        current_row_count=current_row_count,
        current_gold_version=current_gold_version,
        current_fingerprint=current_fingerprint,
        champion_run_id=champion_run_id,
        previous_training_row_count=previous_training_row_count,
    )


__all__ = [
    "RetrainDecision",
    "_current_gold_object_metadata",
    "_current_gold_version",
    "compute_fingerprint",
    "decide_retrain",
]
