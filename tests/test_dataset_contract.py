import sys
import unittest
from pathlib import Path

import pandas as pd


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


if __name__ == "__main__":
    unittest.main()
