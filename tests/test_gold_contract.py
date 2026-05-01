from __future__ import annotations

import ast
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_PIPELINE_PATH = PROJECT_ROOT / "ETL" / "pipelines" / "gold" / "gold_claim_features.py"
GOLD_CONFIG_PATH = PROJECT_ROOT / "src" / "common" / "gold_pipeline_config.py"


class GoldConfigContractTests(unittest.TestCase):
    def test_gold_schema_default_is_stable(self):
        from src.common.gold_pipeline_config import GOLD_SCHEMA_DEFAULT

        self.assertEqual(GOLD_SCHEMA_DEFAULT, "gold")

    def test_gold_audit_columns_are_stable(self):
        from src.common.gold_pipeline_config import GOLD_AUDIT_COLUMNS

        self.assertEqual(GOLD_AUDIT_COLUMNS, ())

    def test_gold_table_name_formats_correctly(self):
        from src.common.gold_pipeline_config import gold_table_name

        self.assertEqual(
            gold_table_name("healthcare", "claim_features", "gold"),
            "healthcare.gold.claim_features",
        )

    def test_gold_table_properties_include_layer_metadata(self):
        from src.common.gold_pipeline_config import gold_table_properties

        props = gold_table_properties("SENSITIVE", ("billed_amount", "diagnosis_code"))
        self.assertEqual(props["claimops.layer"], "gold")
        self.assertIn("hipaa.phi_columns", props)
        self.assertIn("billed_amount", props["hipaa.phi_columns"])
        self.assertEqual(props["hipaa.data_sensitivity"], "SENSITIVE")

    def test_phi_columns_gold_are_stable(self):
        from src.common.gold_pipeline_config import PHI_COLUMNS_GOLD

        # Must include every column the Gold final select emits that is also
        # classified PHI in src/common/bronze_sources.py (patient_id,
        # diagnosis_code, billed_amount, date, is_denied). The narrower 3-set
        # used previously under-reported PHI on the Gold table property.
        self.assertEqual(
            set(PHI_COLUMNS_GOLD),
            {"billed_amount", "date", "diagnosis_code", "is_denied", "patient_id"},
        )

    def test_read_silver_snapshot_uses_batch_mode(self):
        from src.common.gold_pipeline_config import read_silver_snapshot

        import inspect
        source = inspect.getsource(read_silver_snapshot)
        self.assertIn("spark.read.table", source)
        self.assertNotIn("readStream", source)


class GoldPipelineContractTests(unittest.TestCase):
    @classmethod
    def _parse_pipeline(cls):
        return ast.parse(GOLD_PIPELINE_PATH.read_text(encoding="utf-8"))

    def test_gold_pipeline_defines_materialized_view_decorator(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("@dp.materialized_view(\n", source)
        self.assertIn("gold_claim_features", source)
        self.assertIn('refresh_policy="incremental"', source)

    def test_gold_pipeline_defines_private_materialized_views(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("private=True", source)
        self.assertIn("claims_feature_base", source)
        self.assertIn("provider_daily_stats", source)
        self.assertIn("provider_lifetime_stats", source)

    def test_gold_pipeline_reads_from_silver_not_bronze(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("read_silver_snapshot", source)
        self.assertNotIn("read_bronze_snapshot", source)
        self.assertIn("SILVER_SCHEMA_DEFAULT", source)

    def test_gold_materialized_view_uses_incremental_refresh(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn('refresh_policy="incremental"', source)
        self.assertNotIn("cluster_by", source)

    def test_gold_pipeline_phi_safe_log_messages(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        for phi_col in ("patient_id", "billed_amount", "diagnosis_code"):
            pattern = f"format({phi_col})"
            self.assertNotIn(pattern, source)

    def test_gold_pipeline_denial_label_uses_is_denied(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("is_denied", source)
        self.assertIn("denial_label", source)

    def test_gold_pipeline_includes_required_features(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        required_features = [
            "is_procedure_missing",
            "is_amount_missing",
            "amount_to_benchmark_ratio",
            "high_cost_flag",
            "severity_procedure_mismatch",
            "specialty_diagnosis_mismatch",
            "provider_location_missing",
            "diagnosis_severity_encoded",
            "provider_claim_count",
            "provider_claim_count_30d",
            "provider_risk_score",
            "denial_label",
        ]
        for feature in required_features:
            with self.subTest(feature=feature):
                self.assertIn(feature, source)

    def test_gold_pipeline_window_function_for_30d_claims(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("rangeBetween", source)
        self.assertIn("86400", source)

    def test_gold_pipeline_30d_window_casts_date_through_timestamp(self):
        # Regression: DateType.cast("long") yields days-since-epoch, which made
        # `rangeBetween(-30 * 86400, 0)` (seconds) effectively unbounded and
        # silently turned provider_claim_count_30d into provider_claim_count.
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn('cast("timestamp").cast("long")', source)

    def test_gold_pipeline_diagnosis_count_partition_is_provider_only(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn('F.sum("daily_diagnosis_count").alias("diagnosis_count")', source)

    def test_gold_pipeline_drops_target_leak_columns(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        for leak_col in (
            "claim_status",
            "denial_reason_code",
            "allowed_amount",
            "paid_amount",
            "follow_up_required",
        ):
            with self.subTest(column=leak_col):
                self.assertIn(f'"{leak_col}"', source)

    def test_gold_pipeline_broadcast_joins(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("F.broadcast", source)

    def test_gold_pipeline_uses_named_config_constants(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("HIGH_COST_RATIO_THRESHOLD", source)
        self.assertIn("HIGH_SEVERITY_EXPECTED_COST_FLOOR", source)
        self.assertIn("MIN_PROVIDER_RISK_COUNT", source)
        self.assertIn("PROVIDER_LOOKBACK_WINDOW_DAYS", source)
        self.assertNotIn("_THRESHOLD_RATIO", source)

    def test_gold_pipeline_provider_risk_null_below_five_claims(self):
        source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
        self.assertIn("MIN_PROVIDER_RISK_COUNT", source)
        self.assertIn("provider_claim_count", source)

    def test_gold_config_module_exports_all_public_names(self):
        from src.common import gold_pipeline_config

        for name in gold_pipeline_config.__all__:
            self.assertTrue(hasattr(gold_pipeline_config, name), f"Missing export: {name}")

    def test_gold_config_named_constants_are_stable(self):
        from src.common.gold_pipeline_config import (
            HIGH_COST_RATIO_THRESHOLD,
            HIGH_SEVERITY_EXPECTED_COST_FLOOR,
            MIN_PROVIDER_RISK_COUNT,
            PROVIDER_LOOKBACK_WINDOW_DAYS,
        )

        self.assertEqual(HIGH_COST_RATIO_THRESHOLD, 1.5)
        self.assertEqual(HIGH_SEVERITY_EXPECTED_COST_FLOOR, 5000.0)
        self.assertEqual(MIN_PROVIDER_RISK_COUNT, 5)
        self.assertEqual(PROVIDER_LOOKBACK_WINDOW_DAYS, 30)


if __name__ == "__main__":
    unittest.main()
