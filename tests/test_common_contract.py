from __future__ import annotations

import ast
import unittest
from decimal import Decimal
from datetime import date
from pathlib import Path

from src.common.diagnostics import CLAIMOPS_DOMAINS, format_claimops_diagnostic_id
from src.common.log_messages import (
    render_gold_table_ready,
    render_ml_prediction_error,
    render_ml_retrain_decision,
    render_ml_training_failure,
    render_quarantine_summary,
    render_silver_table_ready,
)
from src.common.phi_registry import get_phi_columns, get_sensitive_columns, is_phi_column
from src.common.silver_cleaning import (
    normalize_code_value,
    normalize_nullable_string,
    normalize_title_value,
    parse_bool_value,
    parse_date_value,
    parse_decimal_value,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMMON_ROOT = PROJECT_ROOT / "src" / "common"


class CommonModuleContractTests(unittest.TestCase):
    def test_common_modules_start_with_future_annotations_import(self) -> None:
        for path in sorted(COMMON_ROOT.glob("*.py")):
            with self.subTest(module=path.name):
                first_line = path.read_text(encoding="utf-8").splitlines()[0]
                self.assertEqual(first_line, "from __future__ import annotations")

    def test_common_module_exports_are_alphabetically_sorted(self) -> None:
        for path in sorted(COMMON_ROOT.glob("*.py")):
            with self.subTest(module=path.name):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in tree.body:
                    if isinstance(node, ast.Assign) and any(
                        isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
                    ):
                        values = [
                            element.value
                            for element in getattr(node.value, "elts", [])
                            if isinstance(element, ast.Constant) and isinstance(element.value, str)
                        ]
                        self.assertEqual(values, sorted(values))


class TestDiagnostics(unittest.TestCase):
    def test_format_valid_domain_and_number(self) -> None:
        result = format_claimops_diagnostic_id("ML", 42)
        self.assertEqual(result, "CLAIMOPS-ML-042")

    def test_format_lowercase_domain_normalized(self) -> None:
        result = format_claimops_diagnostic_id("slv", 5)
        self.assertEqual(result, "CLAIMOPS-SLV-005")

    def test_format_max_number_boundary(self) -> None:
        result = format_claimops_diagnostic_id("BRZ", 999)
        self.assertEqual(result, "CLAIMOPS-BRZ-999")

    def test_format_min_number_boundary(self) -> None:
        result = format_claimops_diagnostic_id("BRZ", 0)
        self.assertEqual(result, "CLAIMOPS-BRZ-000")

    def test_format_invalid_domain_raises(self) -> None:
        with self.assertRaises(ValueError):
            format_claimops_diagnostic_id("INVALID", 1)

    def test_format_negative_number_raises(self) -> None:
        with self.assertRaises(ValueError):
            format_claimops_diagnostic_id("ML", -1)

    def test_format_number_over_999_raises(self) -> None:
        with self.assertRaises(ValueError):
            format_claimops_diagnostic_id("ML", 1000)

    def test_claimops_domains_contains_known_domains(self) -> None:
        for domain in ("BRZ", "SLV", "ML", "ANL", "HIPAA", "OBS", "QRT", "FWK"):
            self.assertIn(domain, CLAIMOPS_DOMAINS)


class TestPhiRegistry(unittest.TestCase):
    def test_is_phi_column_for_known_phi(self) -> None:
        table = "healthcare.bronze.claims"
        self.assertTrue(is_phi_column(table, "member_name"))

    def test_is_phi_column_for_non_phi_column(self) -> None:
        table = "healthcare.bronze.claims"
        self.assertFalse(is_phi_column(table, "procedure_code"))

    def test_is_phi_column_for_unknown_table(self) -> None:
        self.assertFalse(is_phi_column("healthcare.bronze.nonexistent", "member_name"))

    def test_get_phi_columns_returns_frozenset(self) -> None:
        columns = get_phi_columns("healthcare.bronze.claims")
        self.assertIsInstance(columns, frozenset)

    def test_get_phi_columns_for_unknown_table_returns_empty(self) -> None:
        columns = get_phi_columns("healthcare.bronze.nonexistent")
        self.assertEqual(columns, frozenset())

    def test_get_sensitive_columns_for_claims_contains_procedure_code(self) -> None:
        columns = get_sensitive_columns("healthcare.bronze.claims")
        self.assertIn("procedure_code", columns)


class TestLogMessages(unittest.TestCase):
    def test_render_silver_table_ready(self) -> None:
        result = render_silver_table_ready(
            table_name="healthcare.silver.claims",
            category="trusted",
            sensitivity="phi",
        )
        self.assertIn("healthcare.silver.claims", result)
        self.assertIn("trusted", result)
        self.assertIn("phi", result)

    def test_render_gold_table_ready(self) -> None:
        result = render_gold_table_ready(
            table_name="healthcare.gold.claim_features",
            category="features",
            sensitivity="non_phi",
        )
        self.assertIn("healthcare.gold.claim_features", result)
        self.assertIn("features", result)

    def test_render_quarantine_summary(self) -> None:
        result = render_quarantine_summary(
            dataset="claims",
            rule_name="missing_claim_id",
            diagnostic_id="CLAIMOPS-SLV-101",
            quarantined_records=5,
        )
        self.assertIn("claims", result)
        self.assertIn("missing_claim_id", result)
        self.assertIn("CLAIMOPS-SLV-101", result)
        self.assertIn("5", result)

    def test_render_ml_training_failure(self) -> None:
        result = render_ml_training_failure(
            diagnostic_id="CLAIMOPS-ML-001",
            model_name="xgboost",
            reason="Out of memory",
        )
        self.assertIn("CLAIMOPS-ML-001", result)
        self.assertIn("xgboost", result)
        self.assertIn("Out of memory", result)

    def test_render_ml_prediction_error(self) -> None:
        result = render_ml_prediction_error(
            diagnostic_id="CLAIMOPS-ML-002",
            operation="predict_single",
            detail="Model not loaded",
        )
        self.assertIn("predict_single", result)
        self.assertIn("Model not loaded", result)

    def test_render_ml_retrain_decision(self) -> None:
        result = render_ml_retrain_decision(
            diagnostic_id="CLAIMOPS-ML-003",
            decision="retrain",
            gold_table="healthcare.gold.claim_features",
            row_count=1500,
            reason="data fingerprint changed",
        )
        self.assertIn("retrain", result)
        self.assertIn("healthcare.gold.claim_features", result)
        self.assertIn("1500", result)
        self.assertIn("data fingerprint changed", result)


class TestSilverCleaning(unittest.TestCase):
    def test_normalize_nullable_string_with_value(self) -> None:
        self.assertEqual(normalize_nullable_string("  hello  "), "hello")

    def test_normalize_nullable_string_with_none(self) -> None:
        self.assertIsNone(normalize_nullable_string(None))

    def test_normalize_nullable_string_with_blank(self) -> None:
        self.assertIsNone(normalize_nullable_string("   "))

    def test_normalize_code_value(self) -> None:
        self.assertEqual(normalize_code_value("  abc "), "ABC")

    def test_normalize_code_value_blank(self) -> None:
        self.assertIsNone(normalize_code_value(""))

    def test_normalize_title_value(self) -> None:
        self.assertEqual(normalize_title_value("john smith"), "John Smith")

    def test_normalize_title_value_handles_acronyms(self) -> None:
        self.assertEqual(normalize_title_value("john smith md"), "John Smith MD")

    def test_normalize_title_value_blank(self) -> None:
        self.assertIsNone(normalize_title_value(""))

    def test_parse_decimal_value_valid(self) -> None:
        result = parse_decimal_value("123.456")
        self.assertIsInstance(result, Decimal)
        self.assertEqual(float(result), 123.46)

    def test_parse_decimal_value_none(self) -> None:
        self.assertIsNone(parse_decimal_value(None))

    def test_parse_decimal_value_invalid(self) -> None:
        self.assertIsNone(parse_decimal_value("not_a_number"))

    def test_parse_date_value_valid(self) -> None:
        result = parse_date_value("2024-03-15")
        self.assertEqual(result, date(2024, 3, 15))

    def test_parse_date_value_none(self) -> None:
        self.assertIsNone(parse_date_value(None))

    def test_parse_date_value_invalid(self) -> None:
        self.assertIsNone(parse_date_value("15/03/2024"))

    def test_parse_bool_value_true_variants(self) -> None:
        for val in ("1", "TRUE", "true", "YES", "Y"):
            self.assertTrue(parse_bool_value(val))

    def test_parse_bool_value_false_variants(self) -> None:
        for val in ("0", "FALSE", "false", "NO", "N"):
            self.assertFalse(parse_bool_value(val))

    def test_parse_bool_value_unknown(self) -> None:
        self.assertIsNone(parse_bool_value("maybe"))

    def test_parse_bool_value_none(self) -> None:
        self.assertIsNone(parse_bool_value(None))


if __name__ == "__main__":
    unittest.main()
