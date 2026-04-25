import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.common.bronze_pipeline_config import (  # noqa: E402
    AUDIT_COLUMNS,
    BRONZE_SCHEMA_DEFAULT,
    BRONZE_VOLUME_DEFAULT,
    CATALOG_DEFAULT,
    CLAIMOPS_DOMAINS,
    COMMON_DELTA_TABLE_PROPERTIES,
    bronze_table_name,
    bronze_volume_root,
    bronze_volume_path,
    csv_autoloader_options,
    format_claimops_diagnostic_id,
    table_name,
    table_properties_for_sensitivity,
)
from src.common.bronze_sources import BRONZE_SOURCES  # noqa: E402
from src.common.observability import (  # noqa: E402
    LOG_CATEGORY_ANALYTICS_BUILD,
    LOG_CATEGORY_DATA_QUALITY,
    LOG_CATEGORY_GOVERNANCE_AUDIT,
    LOG_CATEGORY_PIPELINE_OPS,
    MESSAGE_BRONZE_APPEND_ONLY,
    MESSAGE_EVENT_LOG_SQL_BRIDGE,
    MESSAGE_TEMPLATE_EXPECTATION_METRIC,
)
from src.common.phi_registry import build_phi_columns_registry, get_phi_columns  # noqa: E402
from src.analytics.observability_assets import (  # noqa: E402
    ANALYTICS_OBSERVABILITY_TABLES,
    event_log_bridge_sql,
    latest_failure_diagnostic_id,
)
from src.analytics.claims_analytics import (  # noqa: E402
    DASHBOARD_SOURCE_TABLES,
    HIGH_COST_THRESHOLD_RATIO,
    analytics_table_name,
    build_and_persist_claims_assets,
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

    def test_default_namespace_helpers_preserve_current_contracts(self) -> None:
        self.assertEqual(CATALOG_DEFAULT, "healthcare")
        self.assertEqual(BRONZE_SCHEMA_DEFAULT, "bronze")
        self.assertEqual(BRONZE_VOLUME_DEFAULT, "raw_landing")
        self.assertEqual(
            table_name("healthcare", "bronze", "claims"),
            "healthcare.bronze.claims",
        )
        self.assertEqual(bronze_table_name("claims"), "healthcare.bronze.claims")
        self.assertEqual(bronze_volume_root(), "/Volumes/healthcare/bronze/raw_landing")
        self.assertEqual(bronze_volume_path("claims"), "/Volumes/healthcare/bronze/raw_landing/claims/")

    def test_namespace_helpers_support_dataset_portability(self) -> None:
        self.assertEqual(
            bronze_table_name("claims", catalog="staging_healthcare", schema="raw_bronze"),
            "staging_healthcare.raw_bronze.claims",
        )
        self.assertEqual(
            bronze_volume_root(
                catalog="staging_healthcare",
                schema="raw_bronze",
                volume="s3_landing",
            ),
            "/Volumes/staging_healthcare/raw_bronze/s3_landing",
        )
        self.assertEqual(
            bronze_volume_path(
                "providers",
                catalog="staging_healthcare",
                schema="raw_bronze",
                volume="s3_landing",
            ),
            "/Volumes/staging_healthcare/raw_bronze/s3_landing/providers/",
        )

    def test_bronze_sources_carry_required_columns_in_manifest(self) -> None:
        self.assertEqual(
            BRONZE_SOURCES["claims"].required_columns,
            (
                "claim_id",
                "patient_id",
                "provider_id",
                "diagnosis_code",
                "procedure_code",
                "billed_amount",
                "date",
                "claim_status",
                "denial_reason_code",
                "allowed_amount",
                "paid_amount",
                "is_denied",
                "follow_up_required",
            ),
        )
        self.assertEqual(BRONZE_SOURCES["claims"].source_profile, "current_fixture")
        self.assertEqual(BRONZE_SOURCES["claims"].canonical_dataset, "claims")
        for dataset, source in BRONZE_SOURCES.items():
            with self.subTest(dataset=dataset):
                self.assertTrue(source.required_columns)
                self.assertEqual(len(source.required_columns), len(set(source.required_columns)))

    def test_phi_registry_can_be_derived_for_custom_namespace(self) -> None:
        custom_registry = build_phi_columns_registry(catalog="staging_healthcare", schema="raw_bronze")
        self.assertEqual(
            custom_registry["staging_healthcare.raw_bronze.claims"],
            frozenset(
                {
                    "patient_id",
                    "diagnosis_code",
                    "billed_amount",
                    "date",
                    "claim_status",
                    "denial_reason_code",
                    "allowed_amount",
                    "paid_amount",
                    "is_denied",
                    "follow_up_required",
                }
            ),
        )
        self.assertEqual(
            get_phi_columns("healthcare.bronze.claims"),
            frozenset(
                {
                    "patient_id",
                    "diagnosis_code",
                    "billed_amount",
                    "date",
                    "claim_status",
                    "denial_reason_code",
                    "allowed_amount",
                    "paid_amount",
                    "is_denied",
                    "follow_up_required",
                }
            ),
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
                "claims_dashboard_summary",
                "claims_adjudication_summary",
                "claims_denial_reason_summary",
                "claims_revenue_daily_summary",
            ),
        )
        self.assertEqual(analytics_table_name("healthcare", "analytics", "claims_dashboard_summary"), "healthcare.analytics.claims_dashboard_summary")
        self.assertEqual(HIGH_COST_THRESHOLD_RATIO, 1.5)

    def test_dashboard_json_includes_adjudication_page_and_datasets(self) -> None:
        dashboard_path = PROJECT_ROOT / "src" / "dashboards" / "claims_exploration.lvdash.json"
        with dashboard_path.open("r", encoding="utf-8") as handle:
            dashboard = json.load(handle)

        dataset_names = {dataset["name"] for dataset in dashboard["datasets"]}
        self.assertTrue(
            {
                "adjudication_kpis",
                "denial_reason_mix",
                "denial_reason_impact",
                "revenue_funnel",
                "daily_adjudication_revenue_trend",
            }.issubset(dataset_names)
        )
        self.assertIn("Adjudication & Revenue", [page["displayName"] for page in dashboard["pages"]])

    def test_build_and_persist_claims_assets_wires_new_analytics_tables(self) -> None:
        fake_spark = object()
        with (
            patch("src.analytics.claims_analytics.ensure_analytics_schema"),
            patch("src.analytics.claims_analytics.write_managed_table"),
            patch("src.analytics.claims_analytics.build_claims_provider_joined", return_value="provider"),
            patch("src.analytics.claims_analytics.build_claims_diagnosis_joined", return_value="diagnosis"),
            patch("src.analytics.claims_analytics.build_claims_by_specialty_summary", return_value="specialty"),
            patch("src.analytics.claims_analytics.build_claims_by_region_summary", return_value="region"),
            patch("src.analytics.claims_analytics.build_high_cost_claims_summary", return_value="risk"),
            patch("src.analytics.claims_analytics.build_claims_dashboard_summary", return_value="overview"),
            patch("src.analytics.claims_analytics.build_claims_adjudication_summary", return_value="adjudication", create=True),
            patch("src.analytics.claims_analytics.build_claims_denial_reason_summary", return_value="denial_reason", create=True),
            patch("src.analytics.claims_analytics.build_claims_revenue_daily_summary", return_value="revenue_daily", create=True),
        ):
            persisted = build_and_persist_claims_assets(fake_spark)

        self.assertTrue(
            {
                "claims_adjudication_summary",
                "claims_denial_reason_summary",
                "claims_revenue_daily_summary",
            }.issubset(persisted.keys())
        )

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
