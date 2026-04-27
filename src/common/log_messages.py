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
    "MESSAGE_TEMPLATE_PIPELINE_FAILURE",
    "MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY",
    "MESSAGE_TEMPLATE_GOLD_TABLE_READY",
    "MESSAGE_TEMPLATE_QUARANTINE_SUMMARY",
    "MESSAGE_TEMPLATE_SILVER_TABLE_READY",
    "render_gold_table_ready",
    "render_policy_chunk_summary",
    "render_quarantine_summary",
    "render_silver_table_ready",
]
