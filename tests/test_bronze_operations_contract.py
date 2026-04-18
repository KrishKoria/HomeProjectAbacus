from __future__ import annotations

from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = REPO_ROOT / "docs" / "runbooks"
TRANSFORMATIONS_ROOT = REPO_ROOT / "src" / "pipelines" / "bronze" / "transformations"


class BronzeOperationsContractScaffoldTest(unittest.TestCase):
    def test_remaining_dataset_modules_exist(self) -> None:
        for module_name in ("providers.py", "diagnosis.py", "cost.py"):
            with self.subTest(module_name=module_name):
                self.assertTrue(
                    (TRANSFORMATIONS_ROOT / module_name).exists(),
                    f"T03 must add src/pipelines/bronze/transformations/{module_name}.",
                )

    def test_bronze_operations_runbook_exists(self) -> None:
        self.assertTrue(
            (DOCS_ROOT / "bronze-pipeline-operations.md").exists(),
            "T03 must add docs/runbooks/bronze-pipeline-operations.md.",
        )


if __name__ == "__main__":
    unittest.main()
