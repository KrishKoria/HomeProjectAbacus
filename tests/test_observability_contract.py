import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.common.bronze_pipeline_config import (  # noqa: E402
    AUDIT_COLUMNS,
    CLAIMOPS_DOMAINS,
    COMMON_DELTA_TABLE_PROPERTIES,
    csv_autoloader_options,
    format_claimops_diagnostic_id,
    table_properties_for_sensitivity,
)
from src.common.observability import (  # noqa: E402
    LOG_CATEGORY_ANALYTICS_BUILD,
    LOG_CATEGORY_DATA_QUALITY,
    LOG_CATEGORY_GOVERNANCE_AUDIT,
    LOG_CATEGORY_PIPELINE_OPS,
    MESSAGE_BRONZE_APPEND_ONLY,
    MESSAGE_EVENT_LOG_SQL_BRIDGE,
    MESSAGE_TEMPLATE_EXPECTATION_METRIC,
)
from src.analytics.observability_assets import (  # noqa: E402
    ANALYTICS_OBSERVABILITY_TABLES,
    event_log_bridge_sql,
    latest_failure_diagnostic_id,
)
from src.analytics.week2_analytics import (  # noqa: E402
    DASHBOARD_SOURCE_TABLES,
    HIGH_COST_THRESHOLD_RATIO,
    analytics_table_name,
)


class BronzePipelineConfigTests(unittest.TestCase):
    def test_audit_columns_are_stable_and_ordered(self) -> None:
        self.assertEqual(
            AUDIT_COLUMNS,
            ("_ingested_at", "_source_file", "_pipeline_run_id"),
        )

    def test_common_delta_properties_match_hipaa_defaults(self) -> None:
        self.assertEqual(
            COMMON_DELTA_TABLE_PROPERTIES,
            {
                "delta.enableChangeDataFeed": "true",
                "delta.logRetentionDuration": "interval 2190 days",
                "delta.deletedFileRetentionDuration": "interval 2190 days",
            },
        )

    def test_csv_autoloader_options_cover_bronze_defaults(self) -> None:
        self.assertEqual(
            csv_autoloader_options(),
            {
                "cloudFiles.format": "csv",
                "header": "true",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.schemaEvolutionMode": "addNewColumns",
                "cloudFiles.rescuedDataColumn": "_rescued_data",
            },
        )

    def test_diagnostic_id_format_is_stable(self) -> None:
        self.assertEqual(format_claimops_diagnostic_id("BRZ", 7), "CLAIMOPS-BRZ-007")
        self.assertEqual(format_claimops_diagnostic_id("ANL", 42), "CLAIMOPS-ANL-042")
        self.assertIn("HIPAA", CLAIMOPS_DOMAINS)

    def test_table_properties_include_sensitivity_fields(self) -> None:
        self.assertEqual(
            table_properties_for_sensitivity("PHI", ("patient_id", "date")),
            {
                "delta.enableChangeDataFeed": "true",
                "delta.logRetentionDuration": "interval 2190 days",
                "delta.deletedFileRetentionDuration": "interval 2190 days",
                "hipaa.phi_columns": "patient_id,date",
                "hipaa.data_sensitivity": "PHI",
            },
        )
        self.assertEqual(
            table_properties_for_sensitivity("NON-PHI", ()),
            {
                "delta.enableChangeDataFeed": "true",
                "delta.logRetentionDuration": "interval 2190 days",
                "delta.deletedFileRetentionDuration": "interval 2190 days",
                "hipaa.phi_columns": "",
                "hipaa.data_sensitivity": "NON-PHI",
            },
        )


class ObservabilityMessageTests(unittest.TestCase):
    def test_log_categories_are_explicit(self) -> None:
        self.assertEqual(LOG_CATEGORY_PIPELINE_OPS, "pipeline_ops")
        self.assertEqual(LOG_CATEGORY_DATA_QUALITY, "data_quality")
        self.assertEqual(LOG_CATEGORY_GOVERNANCE_AUDIT, "governance_audit")
        self.assertEqual(LOG_CATEGORY_ANALYTICS_BUILD, "analytics_build")

    def test_message_constants_are_phi_safe_templates(self) -> None:
        self.assertIn("Do NOT apply transforms or deletes", MESSAGE_BRONZE_APPEND_ONLY)
        self.assertIn("event_log()", MESSAGE_EVENT_LOG_SQL_BRIDGE)

        rendered = MESSAGE_TEMPLATE_EXPECTATION_METRIC.format(
            expectation="no_parse_errors",
            dataset="claims",
            passed_records=998,
            failed_records=2,
        )
        self.assertEqual(
            rendered,
            "Expectation metric recorded: expectation=no_parse_errors dataset=claims passed_records=998 failed_records=2",
        )


class AnalyticsContractTests(unittest.TestCase):
    def test_dashboard_table_inventory_is_stable(self) -> None:
        self.assertEqual(
            DASHBOARD_SOURCE_TABLES,
            (
                "claims_provider_joined",
                "claims_diagnosis_joined",
                "claims_by_specialty_summary",
                "claims_by_region_summary",
                "high_cost_claims_summary",
                "week2_dashboard_summary",
            ),
        )
        self.assertEqual(analytics_table_name("healthcare", "analytics", "week2_dashboard_summary"), "healthcare.analytics.week2_dashboard_summary")
        self.assertEqual(HIGH_COST_THRESHOLD_RATIO, 1.5)

    def test_observability_helpers_keep_sql_bridge_minimal(self) -> None:
        self.assertEqual(
            ANALYTICS_OBSERVABILITY_TABLES,
            (
                "ops_pipeline_updates",
                "ops_expectation_metrics",
                "ops_user_actions",
                "ops_latest_failures",
            ),
        )
        self.assertEqual(
            event_log_bridge_sql("1234-567890-test"),
            "SELECT * FROM event_log('1234-567890-test')",
        )
        self.assertEqual(
            latest_failure_diagnostic_id("claims"),
            "CLAIMOPS-OBS-401",
        )


if __name__ == "__main__":
    unittest.main()
