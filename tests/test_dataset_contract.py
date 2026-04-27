import sys
import unittest
from decimal import Decimal
from pathlib import Path

import pandas as pd
import re
import subprocess


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


SYNTHETIC_LABEL_COLUMNS = (
    "claim_status",
    "denial_reason_code",
    "allowed_amount",
    "paid_amount",
    "is_denied",
    "follow_up_required",
)


class DatasetContractTests(unittest.TestCase):
    def test_claims_dataset_contains_synthetic_denial_labels(self) -> None:
        claims = pd.read_csv(PROJECT_ROOT / "datasets" / "claims_1000.csv")

        self.assertTrue(set(SYNTHETIC_LABEL_COLUMNS).issubset(claims.columns))
        self.assertEqual(len(claims), 1000)
        self.assertEqual(set(claims["claim_status"].dropna().unique()), {"APPROVED", "DENIED"})
        self.assertEqual(set(claims["is_denied"].dropna().unique()), {0, 1})
        self.assertGreater(int(claims["is_denied"].sum()), 0)
        self.assertGreater(int((claims["is_denied"] == 0).sum()), 0)
        self.assertTrue((claims.loc[claims["is_denied"] == 1, "denial_reason_code"] != "NONE").all())
        self.assertTrue((claims.loc[claims["is_denied"] == 0, "denial_reason_code"] == "NONE").all())
        self.assertTrue((claims.loc[claims["is_denied"] == 1, "claim_status"] == "DENIED").all())
        self.assertTrue((claims.loc[claims["is_denied"] == 0, "claim_status"] == "APPROVED").all())
        self.assertTrue((claims["follow_up_required"] == claims["is_denied"]).all())
        self.assertTrue((claims.loc[claims["is_denied"] == 1, "paid_amount"] == 0).all())
        self.assertTrue(
            (
                claims.loc[claims["is_denied"] == 0, "paid_amount"]
                == claims.loc[claims["is_denied"] == 0, "allowed_amount"]
            ).all()
        )

    def test_claims_phi_registry_matches_data_classification_documentation(self) -> None:
        from src.common.bronze_sources import BRONZE_SOURCES

        classification = (PROJECT_ROOT / "datasets" / "DATA_CLASSIFICATION.md").read_text(encoding="utf-8")
        claims_section = classification.split("### providers_1000.csv", maxsplit=1)[0]
        documented_phi_columns = frozenset(
            match.group(1)
            for match in re.finditer(r"^\| `([^`]+)` \| \*\*PHI\*\* \|", claims_section, re.MULTILINE)
        )

        self.assertEqual(BRONZE_SOURCES["claims"].phi_columns, documented_phi_columns)

    def test_synthetic_claim_label_regenerator_is_present(self) -> None:
        script_path = PROJECT_ROOT / "scripts" / "generate_synthetic_claim_labels.py"

        self.assertTrue(script_path.exists())
        result = subprocess.run(
            [sys.executable, str(script_path), "--check"],
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_synthetic_claim_label_generator_treats_zero_expected_cost_as_missing(self) -> None:
        from scripts.generate_synthetic_claim_labels import classify_claim

        reason, allowed_amount = classify_claim(
            {
                "claim_id": "C9999",
                "provider_id": "PR100",
                "procedure_code": "PROC1",
                "billed_amount": "100.00",
            },
            {"PR100": "Delhi"},
            {("PROC1", "Delhi"): Decimal("0")},
        )

        self.assertEqual(reason, "NONE")
        self.assertEqual(allowed_amount, "85.00")


if __name__ == "__main__":
    unittest.main()
