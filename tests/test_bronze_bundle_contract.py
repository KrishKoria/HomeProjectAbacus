from __future__ import annotations

import re
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATABRICKS_YML = REPO_ROOT / "databricks.yml"
PIPELINE_YML = REPO_ROOT / "resources" / "bronze.pipeline.yml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _indented_block(text: str, header: str, indent: int) -> list[str]:
    lines = text.splitlines()
    block: list[str] = []
    collecting = False

    for line in lines:
        if not collecting:
            if line == header:
                collecting = True
            continue

        if not line.strip():
            continue

        current_indent = len(line) - len(line.lstrip(" "))
        if current_indent <= indent:
            break
        block.append(line)

    if not collecting:
        raise AssertionError(f"Missing YAML header: {header}")

    return block


class BronzeBundleContractTest(unittest.TestCase):
    maxDiff = None

    def test_bundle_includes_resource_glob(self) -> None:
        text = _read(DATABRICKS_YML)
        self.assertIn("bundle:\n  name:", text)
        self.assertIn("include:\n  - resources/*.yml", text)

    def test_bundle_declares_required_variables(self) -> None:
        text = _read(DATABRICKS_YML)
        variable_block = _indented_block(text, "variables:", indent=0)
        declared_variables = {
            match.group(1)
            for line in variable_block
            if (match := re.match(r"^ {2}([a-z_]+):$", line))
        }
        self.assertEqual(
            declared_variables,
            {"catalog", "schema", "volume", "repo_root", "pipeline_label"},
        )

    def test_pipeline_resource_is_single_serverless_python_pipeline(self) -> None:
        text = _read(PIPELINE_YML)
        pipelines_block = _indented_block(text, "  pipelines:", indent=2)
        pipeline_keys = [
            match.group(1)
            for line in pipelines_block
            if (match := re.match(r"^ {4}([A-Za-z0-9_-]+):$", line))
        ]

        self.assertEqual(pipeline_keys, ["bronze"])
        self.assertIn("      serverless: true", text)
        self.assertIn("      continuous: false", text)
        self.assertIn("      development: true", text)
        self.assertIn("      channel: current", text)
        self.assertIn("      root_path: ../src/pipelines/bronze", text)
        self.assertNotIn("notebook:", text)
        self.assertNotIn("file:", text)
        self.assertNotIn("jar:", text)
        self.assertNotIn("maven:", text)

    def test_pipeline_library_root_is_only_the_transformation_tree(self) -> None:
        text = _read(PIPELINE_YML)
        include_values = re.findall(r"^ {12}include: (.+)$", text, re.MULTILINE)
        self.assertEqual(include_values, ["../src/pipelines/bronze/transformations/**"])
        self.assertNotIn("/Volumes/", text)
        self.assertNotIn("datasets/", text)

    def test_pipeline_configuration_is_parameterized_from_bundle_variables(self) -> None:
        text = _read(PIPELINE_YML)
        expected_bindings = {
            "name: ${var.pipeline_label}",
            "catalog: ${var.catalog}",
            "schema: ${var.schema}",
            "bronze.catalog: ${var.catalog}",
            "bronze.schema: ${var.schema}",
            "bronze.volume: ${var.volume}",
            "bronze.repo_root: ${var.repo_root}",
            "bronze.pipeline_label: ${var.pipeline_label}",
        }

        for binding in expected_bindings:
            self.assertIn(binding, text)

        self.assertNotIn("/Volumes/healthcare/bronze/raw_landing", text)


if __name__ == "__main__":
    unittest.main()
