from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.bronze_pipeline_config import (  # noqa: E402
    escape_backtick_identifier,
    validate_identifier,
)
from src.common.silver_pipeline_config import (  # noqa: E402
    NON_PHI_TABLE_PROPERTIES,
    PHI_TABLE_PROPERTIES,
    SENSITIVE_TABLE_PROPERTIES,
)
from src.analytics.claims_analytics import (  # noqa: E402
    DASHBOARD_SOURCE_TABLES,
    PHI_COLUMNS_BY_TABLE,
    TABLE_SENSITIVITY_CLASSIFICATIONS,
    build_and_persist_claims_assets,
)


class IdentifierValidationTests(unittest.TestCase):
    def test_valid_identifiers_accepted(self) -> None:
        for name in ("healthcare", "bronze", "analytics", "claims_provider_joined", "A", "a1b2c3"):
            with self.subTest(name=name):
                self.assertEqual(validate_identifier(name, "test"), name)

    def test_invalid_identifiers_rejected(self) -> None:
        invalid = [
            ("", "empty"),
            ("123abc", "starts with digit"),
            ("table name", "contains space"),
            ("drop;table", "contains semicolon"),
            ("catalog.table", "contains dot"),
            ("table-name", "contains hyphen"),
        ]
        for name, reason in invalid:
            with self.subTest(name=name, reason=reason):
                with self.assertRaises(ValueError):
                    validate_identifier(name)

    def test_validate_identifier_label_in_error_message(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            validate_identifier("", "catalog")
        self.assertIn("catalog", str(ctx.exception))

    def test_escape_backtick_wraps_identifier(self) -> None:
        self.assertEqual(escape_backtick_identifier("healthcare"), "`healthcare`")

    def test_escape_backtick_doubles_embedded(self) -> None:
        self.assertEqual(escape_backtick_identifier("table`name"), "`table``name`")

    def test_escape_backtick_normal_no_embedded(self) -> None:
        self.assertEqual(escape_backtick_identifier("simple"), "`simple`")


class SensitivityClassificationTests(unittest.TestCase):
    def test_classification_covers_all_output_tables(self) -> None:
        output_keys = {
            "claims_provider_joined",
            "claims_diagnosis_joined",
            "claims_by_specialty_summary",
            "claims_by_region_summary",
            "claims_by_diagnosis_summary",
            "claims_provider_specialty_mismatch",
            "high_cost_claims_summary",
            "claims_dashboard_summary",
            "claims_adjudication_summary",
            "claims_denial_reason_summary",
            "claims_revenue_daily_summary",
            "bronze_pipeline_audit",
            "ops_data_freshness",
            "silver_claims_cost_enriched",
            "silver_claim_lineage",
        }
        self.assertEqual(set(TABLE_SENSITIVITY_CLASSIFICATIONS.keys()), output_keys)

    def test_phi_tables_contain_patient_id(self) -> None:
        for table_key in ("claims_provider_joined", "claims_diagnosis_joined"):
            with self.subTest(table=table_key):
                self.assertIn("patient_id", PHI_COLUMNS_BY_TABLE[table_key])

    def test_phi_tables_have_phi_columns(self) -> None:
        for table_key, sensitivity in TABLE_SENSITIVITY_CLASSIFICATIONS.items():
            if sensitivity == "PHI":
                with self.subTest(table=table_key):
                    self.assertIn(table_key, PHI_COLUMNS_BY_TABLE)

    def test_non_phi_tables_have_no_phi_columns(self) -> None:
        for table_key, sensitivity in TABLE_SENSITIVITY_CLASSIFICATIONS.items():
            if sensitivity != "PHI":
                with self.subTest(table=table_key):
                    self.assertNotIn(table_key, PHI_COLUMNS_BY_TABLE)

    def test_phi_tables_classified_correctly(self) -> None:
        phi_tables = {"claims_provider_joined", "claims_diagnosis_joined"}
        for table_key in phi_tables:
            with self.subTest(table=table_key):
                self.assertEqual(TABLE_SENSITIVITY_CLASSIFICATIONS[table_key], "PHI")

    def test_sensitive_tables_classified_correctly(self) -> None:
        sensitive_tables = {
            "high_cost_claims_summary",
            "silver_claim_lineage",
            "silver_claims_cost_enriched",
            "claims_provider_specialty_mismatch",
        }
        for table_key in sensitive_tables:
            with self.subTest(table=table_key):
                self.assertEqual(TABLE_SENSITIVITY_CLASSIFICATIONS[table_key], "SENSITIVE")

    def test_aggregate_tables_are_non_phi(self) -> None:
        aggregate = {
            "claims_by_specialty_summary",
            "claims_by_region_summary",
            "claims_by_diagnosis_summary",
            "claims_dashboard_summary",
            "claims_adjudication_summary",
            "claims_denial_reason_summary",
            "claims_revenue_daily_summary",
            "bronze_pipeline_audit",
            "ops_data_freshness",
        }
        for table_key in aggregate:
            with self.subTest(table=table_key):
                self.assertEqual(TABLE_SENSITIVITY_CLASSIFICATIONS[table_key], "NON-PHI")

    def test_table_property_constants_exist(self) -> None:
        self.assertIsInstance(PHI_TABLE_PROPERTIES, dict)
        self.assertIsInstance(SENSITIVE_TABLE_PROPERTIES, dict)
        self.assertIsInstance(NON_PHI_TABLE_PROPERTIES, dict)
        self.assertIn("hipaa.data_sensitivity", PHI_TABLE_PROPERTIES)
        self.assertEqual(PHI_TABLE_PROPERTIES["hipaa.data_sensitivity"], "PHI")

    def test_phi_columns_include_identifying_fields(self) -> None:
        for table_key, columns in PHI_COLUMNS_BY_TABLE.items():
            with self.subTest(table=table_key):
                self.assertIn("patient_id", columns)
                self.assertIn("claim_id", columns)


class BuildAndPersistInterfaceTests(unittest.TestCase):
    def test_build_and_persist_has_expected_signature(self) -> None:
        import inspect

        sig = inspect.signature(build_and_persist_claims_assets)
        params = list(sig.parameters.keys())
        self.assertIn("spark", params)
        self.assertIn("catalog", params)
        self.assertIn("bronze_schema", params)
        self.assertIn("analytics_schema", params)


if __name__ == "__main__":
    unittest.main()
