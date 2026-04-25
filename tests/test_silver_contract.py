import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
ETL_ROOT = PROJECT_ROOT / "ETL"
if str(ETL_ROOT) not in sys.path:
    sys.path.insert(0, str(ETL_ROOT))


from common.diagnostics import (  # noqa: E402
    CLAIMOPS_DOMAINS,
    SILVER_DIAGNOSTIC_IDS,
    format_claimops_diagnostic_id,
    get_silver_diagnostic_id,
)
from common.log_categories import (  # noqa: E402
    LOG_CATEGORY_POLICY_CHUNKING,
    LOG_CATEGORY_QUARANTINE_AUDIT,
    LOG_CATEGORY_SILVER_PIPELINE,
)
from common.log_messages import (  # noqa: E402
    MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY,
    MESSAGE_TEMPLATE_QUARANTINE_SUMMARY,
    MESSAGE_TEMPLATE_SILVER_TABLE_READY,
    render_policy_chunk_summary,
    render_quarantine_summary,
    render_silver_table_ready,
)
from common.policy_chunks import chunk_policy_text, normalize_policy_text  # noqa: E402
from common.silver_cleaning import (  # noqa: E402
    build_quality_flags,
    normalize_code_value,
    normalize_nullable_string,
    normalize_title_value,
    parse_bool_value,
    parse_date_value,
    parse_decimal_value,
)
from common.silver_pipeline_config import (  # noqa: E402
    POLICY_CHUNK_OVERLAP_TOKENS,
    POLICY_CHUNK_SIZE_TOKENS,
    QUARANTINE_AUDIT_COLUMNS,
    SILVER_AUDIT_COLUMNS,
    quarantine_table_name,
    read_bronze_cdf,
    silver_table_name,
)


class SilverDiagnosticsTests(unittest.TestCase):
    def test_silver_domains_and_ids_are_stable(self) -> None:
        self.assertIn("SLV", CLAIMOPS_DOMAINS)
        self.assertIn("policy_chunks", SILVER_DIAGNOSTIC_IDS)
        self.assertEqual(
            format_claimops_diagnostic_id("SLV", 101),
            "CLAIMOPS-SLV-101",
        )
        self.assertEqual(
            get_silver_diagnostic_id("claims", "missing_diagnosis_code"),
            "CLAIMOPS-SLV-104",
        )
        self.assertEqual(
            get_silver_diagnostic_id("unknown_dataset", "unknown_rule"),
            "CLAIMOPS-SLV-999",
        )


class SilverCleaningTests(unittest.TestCase):
    def test_normalization_helpers_trim_and_canonicalize(self) -> None:
        self.assertIsNone(normalize_nullable_string("   "))
        self.assertEqual(normalize_code_value(" proc4 "), "PROC4")
        self.assertEqual(normalize_title_value("  dr patel "), "Dr Patel")

    def test_decimal_and_date_parsing_helpers_are_strict(self) -> None:
        self.assertEqual(parse_decimal_value("28733.0"), Decimal("28733.00"))
        self.assertIsNone(parse_decimal_value("abc"))
        self.assertEqual(parse_date_value("2024-02-22"), date(2024, 2, 22))
        self.assertIsNone(parse_date_value("2024/02/22"))

    def test_bool_parsing_helper_accepts_label_values(self) -> None:
        self.assertIs(parse_bool_value("1"), True)
        self.assertIs(parse_bool_value("true"), True)
        self.assertIs(parse_bool_value("0"), False)
        self.assertIs(parse_bool_value("false"), False)
        self.assertIsNone(parse_bool_value("maybe"))

    def test_quality_flags_are_sorted_and_truthy_only(self) -> None:
        self.assertEqual(
            build_quality_flags(
                {
                    "missing_billed_amount": True,
                    "missing_procedure_code": False,
                    "provider_location_unknown": True,
                }
            ),
            ["missing_billed_amount", "provider_location_unknown"],
        )

    def test_spark_quality_flags_empty_map_is_array_string(self) -> None:
        try:
            from pyspark.sql import SparkSession, functions as F
        except ModuleNotFoundError:
            self.skipTest("pyspark is not installed in the local test environment")

        from common.silver_cleaning import spark_quality_flags

        spark = SparkSession.builder.master("local[1]").appName("silver-quality-flags-test").getOrCreate()
        try:
            frame = spark.range(1).select(spark_quality_flags({}).alias("_data_quality_flags"))
            self.assertEqual(frame.schema["_data_quality_flags"].dataType.simpleString(), "array<string>")
            self.assertEqual(frame.select(F.size("_data_quality_flags")).first()[0], 0)
        finally:
            spark.stop()


class PolicyChunkingTests(unittest.TestCase):
    def test_policy_text_is_normalized_before_chunking(self) -> None:
        self.assertEqual(
            normalize_policy_text("Line one\n\nLine two\tLine three"),
            "Line one Line two Line three",
        )

    def test_policy_chunker_uses_overlap_and_stable_indexes(self) -> None:
        text = " ".join(f"token{i}" for i in range(700))
        chunks = chunk_policy_text(text, POLICY_CHUNK_SIZE_TOKENS, POLICY_CHUNK_OVERLAP_TOKENS)
        self.assertEqual(chunks[0]["chunk_index"], 0)
        self.assertEqual(chunks[0]["token_count"], POLICY_CHUNK_SIZE_TOKENS)
        self.assertGreaterEqual(len(chunks), 2)
        self.assertEqual(chunks[1]["chunk_index"], 1)
        self.assertEqual(
            chunks[0]["chunk_text"].split(" ")[-POLICY_CHUNK_OVERLAP_TOKENS:],
            chunks[1]["chunk_text"].split(" ")[:POLICY_CHUNK_OVERLAP_TOKENS],
        )


class SilverContractTests(unittest.TestCase):
    def test_silver_and_quarantine_contract_constants_are_stable(self) -> None:
        self.assertEqual(SILVER_AUDIT_COLUMNS, ("_silver_processed_at", "_data_quality_flags"))
        self.assertEqual(
            QUARANTINE_AUDIT_COLUMNS,
            ("_quarantined_at", "diagnostic_id", "rule_name", "quarantine_reason"),
        )
        self.assertEqual(silver_table_name("healthcare", "claims"), "healthcare.silver.claims")
        self.assertEqual(
            quarantine_table_name("healthcare", "policy_chunks"),
            "healthcare.quarantine.policy_chunks",
        )

    def test_bronze_reader_uses_batch_snapshot_for_silver_windows(self) -> None:
        class FakeReader:
            def __init__(self) -> None:
                self.table_name: str | None = None

            def table(self, table_name: str):
                self.table_name = table_name
                return "frame"

        class FakeSpark:
            def __init__(self) -> None:
                self.read = FakeReader()

            @property
            def readStream(self):  # pragma: no cover - should never be touched
                raise AssertionError("Silver CDF reader must not use streaming mode with analytic windows")

        spark = FakeSpark()

        self.assertEqual(read_bronze_cdf(spark, "healthcare.bronze.claims"), "frame")
        self.assertEqual(spark.read.table_name, "healthcare.bronze.claims")

    def test_new_log_categories_and_messages_are_phi_safe_templates(self) -> None:
        self.assertEqual(LOG_CATEGORY_SILVER_PIPELINE, "silver_pipeline")
        self.assertEqual(LOG_CATEGORY_QUARANTINE_AUDIT, "quarantine_audit")
        self.assertEqual(LOG_CATEGORY_POLICY_CHUNKING, "policy_chunking")
        self.assertIn("table=", MESSAGE_TEMPLATE_SILVER_TABLE_READY)
        self.assertIn("diagnostic_id=", MESSAGE_TEMPLATE_QUARANTINE_SUMMARY)
        self.assertIn("document_path=", MESSAGE_TEMPLATE_POLICY_CHUNK_SUMMARY)
        self.assertEqual(
            render_silver_table_ready(
                "healthcare.silver.claims",
                LOG_CATEGORY_SILVER_PIPELINE,
                "PHI",
            ),
            "Silver dataset ready: table=healthcare.silver.claims category=silver_pipeline sensitivity=PHI",
        )
        self.assertEqual(
            render_quarantine_summary("claims", "missing_diagnosis_code", "CLAIMOPS-SLV-104", 307),
            "Quarantine summary recorded: dataset=claims rule_name=missing_diagnosis_code diagnostic_id=CLAIMOPS-SLV-104 quarantined_records=307",
        )
        self.assertEqual(
            render_policy_chunk_summary("/Volumes/.../doc.pdf", 0, "CLAIMOPS-SLV-501"),
            "Policy chunk extraction recorded: document_path=/Volumes/.../doc.pdf chunk_count=0 diagnostic_id=CLAIMOPS-SLV-501",
        )


if __name__ == "__main__":
    unittest.main()
