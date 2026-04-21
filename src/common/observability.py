from __future__ import annotations

from typing import Final


LOG_CATEGORY_PIPELINE_OPS: Final[str] = "pipeline_ops"
LOG_CATEGORY_DATA_QUALITY: Final[str] = "data_quality"
LOG_CATEGORY_GOVERNANCE_AUDIT: Final[str] = "governance_audit"
LOG_CATEGORY_ANALYTICS_BUILD: Final[str] = "analytics_build"

LOG_CATEGORIES: Final[tuple[str, ...]] = (
    LOG_CATEGORY_PIPELINE_OPS,
    LOG_CATEGORY_DATA_QUALITY,
    LOG_CATEGORY_GOVERNANCE_AUDIT,
    LOG_CATEGORY_ANALYTICS_BUILD,
)

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

__all__ = [
    "LOG_CATEGORY_ANALYTICS_BUILD",
    "LOG_CATEGORY_DATA_QUALITY",
    "LOG_CATEGORY_GOVERNANCE_AUDIT",
    "LOG_CATEGORY_PIPELINE_OPS",
    "LOG_CATEGORIES",
    "MESSAGE_BRONZE_APPEND_ONLY",
    "MESSAGE_EVENT_LOG_SQL_BRIDGE",
    "MESSAGE_TEMPLATE_ANALYTICS_TABLE_READY",
    "MESSAGE_TEMPLATE_EXPECTATION_METRIC",
    "MESSAGE_TEMPLATE_PIPELINE_FAILURE",
]
