from __future__ import annotations

import ast
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
