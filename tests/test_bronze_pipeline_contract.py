from __future__ import annotations

from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
TRANSFORMATIONS_ROOT = REPO_ROOT / "src" / "pipelines" / "bronze" / "transformations"


class BronzePipelineContractScaffoldTest(unittest.TestCase):
    def test_shared_helper_exists(self) -> None:
        self.assertTrue(
            (TRANSFORMATIONS_ROOT / "common.py").exists(),
            "T02 must add src/pipelines/bronze/transformations/common.py.",
        )

    def test_claims_table_module_exists(self) -> None:
        self.assertTrue(
            (TRANSFORMATIONS_ROOT / "claims.py").exists(),
            "T02 must add src/pipelines/bronze/transformations/claims.py.",
        )


if __name__ == "__main__":
    unittest.main()
