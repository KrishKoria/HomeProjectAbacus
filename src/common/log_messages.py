from __future__ import annotations

from typing import Final


MESSAGE_BRONZE_APPEND_ONLY: Final[str] = (
    "Do NOT apply transforms or deletes — Bronze is the source-of-truth for HIPAA audit."
)

MESSAGE_EVENT_LOG_SQL_BRIDGE: Final[str] = (
    "Reading event_log() through a minimal SQL bridge; downstream parsing and persistence remain in PySpark."
)

MESSAGE_TEMPLATE_EXPECTATION_METRIC: Final[str] = (
    "Expectation metric recorded: expectation={expectation} dataset={dataset} "
    "passed_records={passed_records} failed_records={failed_records}"
)

MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY: Final[str] = (
    "Analytics dataset ready: table={table_name} category={category} sensitivity={sensitivity}"
)

MESSAGE_TEMPLATE_PIPELINE_FAILURE: Final[str] = (
    "Pipeline failure observed: diagnostic_id={diagnostic_id} dataset={dataset} update_id={update_id}"
)

MESSAGE_TEMPLATE_SILVER_TABLE_READY: Final[str] = (
    "Silver dataset ready: table={table_name} category={category} sensitivity={sensitivity}"
)

MESSAGE_TEMPLATE_QUARANTINE_SUMMARY: Final[str] = (
    "Quarantine summary recorded: dataset={dataset} rule_name={rule_name} "
    "diagnostic_id={diagnostic_id} quarantined_records={quarantined_records}"
)

MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY: Final[str] = (
    "Policy chunk extraction recorded: document_path={document_path} chunk_count={chunk_count} "
    "diagnostic_id={diagnostic_id}"
)

MESSAGE_TEMPLATE_GOLD_TABLE_READY: Final[str] = (
    "Gold dataset ready: table={table_name} category={category} sensitivity={sensitivity}"
)

MESSAGE_TEMPLATE_ML_TRAINING_FAILURE: Final[str] = (
    "ML training failure: diagnostic_id={diagnostic_id} model={model_name} reason={reason}"
)

MESSAGE_TEMPLATE_ML_REGISTRY_ERROR: Final[str] = (
    "ML registry error: diagnostic_id={diagnostic_id} registered_model={registered_model_name} detail={detail}"
)

MESSAGE_TEMPLATE_ML_PREDICTION_ERROR: Final[str] = (
    "ML prediction error: diagnostic_id={diagnostic_id} operation={operation} detail={detail}"
)

MESSAGE_TEMPLATE_ML_RETRAIN_DECISION: Final[str] = (
    "ML retrain decision: diagnostic_id={diagnostic_id} decision={decision} "
    "gold_table={gold_table} row_count={row_count} reason={reason}"
)

MESSAGE_TEMPLATE_ML_DATA_LOAD_FAILURE: Final[str] = (
    "ML data load failure: diagnostic_id={diagnostic_id} source={source} detail={detail}"
)


def render_silver_table_ready(table_name: str, category: str, sensitivity: str) -> str:
    """Render a stable PHI-safe status line for trusted Silver assets."""
    return MESSAGE_TEMPLATE_SILVER_TABLE_READY.format(
        table_name=table_name,
        category=category,
        sensitivity=sensitivity,
    )


def render_gold_table_ready(table_name: str, category: str, sensitivity: str) -> str:
    """Render a stable PHI-safe status line for Gold feature assets."""
    return MESSAGE_TEMPLATE_GOLD_TABLE_READY.format(
        table_name=table_name,
        category=category,
        sensitivity=sensitivity,
    )


def render_quarantine_summary(
    dataset: str,
    rule_name: str,
    diagnostic_id: str,
    quarantined_records: int | str,
) -> str:
    """Render a stable PHI-safe quarantine summary line."""
    return MESSAGE_TEMPLATE_QUARANTINE_SUMMARY.format(
        dataset=dataset,
        rule_name=rule_name,
        diagnostic_id=diagnostic_id,
        quarantined_records=quarantined_records,
    )


def render_ml_training_failure(
    diagnostic_id: str,
    model_name: str,
    reason: str,
) -> str:
    """Render a stable PHI-safe ML training failure line."""
    return MESSAGE_TEMPLATE_ML_TRAINING_FAILURE.format(
        diagnostic_id=diagnostic_id,
        model_name=model_name,
        reason=reason,
    )


def render_ml_registry_error(
    diagnostic_id: str,
    registered_model_name: str,
    detail: str,
) -> str:
    """Render a stable PHI-safe ML registry error line."""
    return MESSAGE_TEMPLATE_ML_REGISTRY_ERROR.format(
        diagnostic_id=diagnostic_id,
        registered_model_name=registered_model_name,
        detail=detail,
    )


def render_ml_prediction_error(
    diagnostic_id: str,
    operation: str,
    detail: str,
) -> str:
    """Render a stable PHI-safe ML prediction error line."""
    return MESSAGE_TEMPLATE_ML_PREDICTION_ERROR.format(
        diagnostic_id=diagnostic_id,
        operation=operation,
        detail=detail,
    )


def render_ml_retrain_decision(
    diagnostic_id: str,
    decision: str,
    gold_table: str,
    row_count: int,
    reason: str,
) -> str:
    """Render a stable PHI-safe ML retrain decision line."""
    return MESSAGE_TEMPLATE_ML_RETRAIN_DECISION.format(
        diagnostic_id=diagnostic_id,
        decision=decision,
        gold_table=gold_table,
        row_count=row_count,
        reason=reason,
    )


def render_ml_data_load_failure(
    diagnostic_id: str,
    source: str,
    detail: str,
) -> str:
    """Render a stable PHI-safe ML data load failure line."""
    return MESSAGE_TEMPLATE_ML_DATA_LOAD_FAILURE.format(
        diagnostic_id=diagnostic_id,
        source=source,
        detail=detail,
    )


def render_policy_chunk_summary(
    document_path: str,
    chunk_count: int,
    diagnostic_id: str,
) -> str:
    """Render a stable PHI-safe policy chunk extraction summary."""
    return MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY.format(
        document_path=document_path,
        chunk_count=chunk_count,
        diagnostic_id=diagnostic_id,
    )


__all__ = [
    "MESSAGE_BRONZE_APPEND_ONLY",
    "MESSAGE_EVENT_LOG_SQL_BRIDGE",
    "MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY",
    "MESSAGE_TEMPLATE_EXPECTATION_METRIC",
    "MESSAGE_TEMPLATE_GOLD_TABLE_READY",
    "MESSAGE_TEMPLATE_ML_DATA_LOAD_FAILURE",
    "MESSAGE_TEMPLATE_ML_PREDICTION_ERROR",
    "MESSAGE_TEMPLATE_ML_REGISTRY_ERROR",
    "MESSAGE_TEMPLATE_ML_RETRAIN_DECISION",
    "MESSAGE_TEMPLATE_ML_TRAINING_FAILURE",
    "MESSAGE_TEMPLATE_PIPELINE_FAILURE",
    "MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY",
    "MESSAGE_TEMPLATE_QUARANTINE_SUMMARY",
    "MESSAGE_TEMPLATE_SILVER_TABLE_READY",
    "render_gold_table_ready",
    "render_ml_data_load_failure",
    "render_ml_prediction_error",
    "render_ml_registry_error",
    "render_ml_retrain_decision",
    "render_ml_training_failure",
    "render_policy_chunk_summary",
    "render_quarantine_summary",
    "render_silver_table_ready",
]
