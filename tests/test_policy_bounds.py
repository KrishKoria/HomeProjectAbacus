from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
ETL_ROOT = PROJECT_ROOT / "ETL"
if str(ETL_ROOT) not in sys.path:
    sys.path.insert(0, str(ETL_ROOT))

from common.diagnostics import (  # noqa: E402
    SILVER_DIAGNOSTIC_IDS,
    format_claimops_diagnostic_id,
    get_silver_diagnostic_id,
)
from common.silver_pipeline_config import (  # noqa: E402
    MAX_CHUNK_COUNT,
    MAX_EXTRACTED_TEXT_LENGTH,
    MAX_PDF_PAGE_COUNT,
    MAX_PDF_SIZE_BYTES,
    MAX_PDF_TOKEN_COUNT,
)


def _read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


class PDFBoundsConstantsTests(unittest.TestCase):
    def test_max_pdf_size_positive(self) -> None:
        self.assertGreater(MAX_PDF_SIZE_BYTES, 0)

    def test_max_pdf_page_count_positive(self) -> None:
        self.assertGreater(MAX_PDF_PAGE_COUNT, 0)

    def test_max_extracted_text_length_positive(self) -> None:
        self.assertGreater(MAX_EXTRACTED_TEXT_LENGTH, 0)

    def test_max_chunk_count_positive(self) -> None:
        self.assertGreater(MAX_CHUNK_COUNT, 0)

    def test_max_pdf_token_count_positive(self) -> None:
        self.assertGreater(MAX_PDF_TOKEN_COUNT, 0)

    def test_constants_are_reasonable(self) -> None:
        self.assertLessEqual(MAX_PDF_SIZE_BYTES, 500_000_000)
        self.assertLessEqual(MAX_PDF_PAGE_COUNT, 10_000)
        self.assertLessEqual(MAX_EXTRACTED_TEXT_LENGTH, 50_000_000)
        self.assertLessEqual(MAX_CHUNK_COUNT, 20_000)
        self.assertLessEqual(MAX_PDF_TOKEN_COUNT, 2_000_000)


class PDFDiagnosticIdTests(unittest.TestCase):
    def test_oversized_diagnostic_ids_exist(self) -> None:
        policy_diags = SILVER_DIAGNOSTIC_IDS["policy_chunks"]
        self.assertIn("oversized_pdf_file", policy_diags)
        self.assertIn("oversized_pdf_pages", policy_diags)
        self.assertIn("oversized_pdf_text", policy_diags)

    def test_oversized_diagnostic_ids_are_unique(self) -> None:
        policy_diags = SILVER_DIAGNOSTIC_IDS["policy_chunks"]
        oversized_ids = [
            policy_diags[k]
            for k in ("oversized_pdf_file", "oversized_pdf_pages", "oversized_pdf_text")
        ]
        self.assertEqual(len(oversized_ids), len(set(oversized_ids)))

    def test_get_oversized_diagnostic_ids(self) -> None:
        for rule in ("oversized_pdf_file", "oversized_pdf_pages", "oversized_pdf_text"):
            with self.subTest(rule=rule):
                diag_id = get_silver_diagnostic_id("policy_chunks", rule)
                self.assertTrue(diag_id.startswith("CLAIMOPS-SLV-"))
                self.assertNotEqual(diag_id, "CLAIMOPS-SLV-999")

    def test_oversized_ids_are_consecutive_after_existing(self) -> None:
        policy_diags = SILVER_DIAGNOSTIC_IDS["policy_chunks"]
        self.assertEqual(
            policy_diags["oversized_pdf_file"],
            format_claimops_diagnostic_id("SLV", 504),
        )
        self.assertEqual(
            policy_diags["oversized_pdf_pages"],
            format_claimops_diagnostic_id("SLV", 505),
        )
        self.assertEqual(
            policy_diags["oversized_pdf_text"],
            format_claimops_diagnostic_id("SLV", 506),
        )


class ExtractPolicyTextASTTests(unittest.TestCase):
    """Verify _extract_policy_text contains bounds checks via AST inspection."""

    @classmethod
    def setUpClass(cls) -> None:
        source = _read_text("ETL/pipelines/silver/silver_policy_chunks.py")
        cls.tree = ast.parse(source)
        cls.func_node = None
        for node in ast.walk(cls.tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_extract_policy_text":
                cls.func_node = node
                break

    def test_func_exists(self) -> None:
        self.assertIsNotNone(self.func_node, "_extract_policy_text not found")

    def test_checks_max_pdf_size(self) -> None:
        names = {node.id for node in ast.walk(self.func_node) if isinstance(node, ast.Name)}
        self.assertIn("MAX_PDF_SIZE_BYTES", names)

    def test_checks_max_pdf_page_count(self) -> None:
        names = {node.id for node in ast.walk(self.func_node) if isinstance(node, ast.Name)}
        self.assertIn("MAX_PDF_PAGE_COUNT", names)

    def test_checks_max_extracted_text_length(self) -> None:
        names = {node.id for node in ast.walk(self.func_node) if isinstance(node, ast.Name)}
        self.assertIn("MAX_EXTRACTED_TEXT_LENGTH", names)

    def test_returns_oversized_statuses(self) -> None:
        source = ast.unparse(self.func_node)
        self.assertIn("OVERSIZED_PDF_FILE", source)
        self.assertIn("OVERSIZED_PDF_PAGES", source)
        self.assertIn("OVERSIZED_PDF_TEXT", source)


class ChunkPolicyTextASTTests(unittest.TestCase):
    """Verify _chunk_policy_text contains bounds checks via AST inspection."""

    @classmethod
    def setUpClass(cls) -> None:
        source = _read_text("ETL/pipelines/silver/silver_policy_chunks.py")
        cls.tree = ast.parse(source)
        cls.func_node = None
        for node in ast.walk(cls.tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_chunk_policy_text":
                cls.func_node = node
                break

    def test_func_exists(self) -> None:
        self.assertIsNotNone(self.func_node, "_chunk_policy_text not found")

    def test_checks_max_pdf_token_count(self) -> None:
        names = {node.id for node in ast.walk(self.func_node) if isinstance(node, ast.Name)}
        self.assertIn("MAX_PDF_TOKEN_COUNT", names)

    def test_checks_max_chunk_count(self) -> None:
        names = {node.id for node in ast.walk(self.func_node) if isinstance(node, ast.Name)}
        self.assertIn("MAX_CHUNK_COUNT", names)


class QuarantineRoutingASTTests(unittest.TestCase):
    """Verify extraction status metadata defines all oversized routing keys."""

    @classmethod
    def setUpClass(cls) -> None:
        source = _read_text("ETL/pipelines/silver/silver_policy_chunks.py")
        cls.tree = ast.parse(source)
        cls.meta_dict = None
        for node in ast.walk(cls.tree):
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "_EXTRACTION_STATUS_META":
                cls.meta_dict = node.value
                break

    def test_meta_dict_exists(self) -> None:
        self.assertIsNotNone(self.meta_dict, "_EXTRACTION_STATUS_META not found")

    def test_meta_dict_contains_oversized_file(self) -> None:
        source = ast.unparse(self.meta_dict)
        self.assertIn("OVERSIZED_PDF_FILE", source)
        self.assertIn("oversized_pdf_file", source)

    def test_meta_dict_contains_oversized_pages(self) -> None:
        source = ast.unparse(self.meta_dict)
        self.assertIn("OVERSIZED_PDF_PAGES", source)
        self.assertIn("oversized_pdf_pages", source)

    def test_meta_dict_contains_oversized_text(self) -> None:
        source = ast.unparse(self.meta_dict)
        self.assertIn("OVERSIZED_PDF_TEXT", source)
        self.assertIn("oversized_pdf_text", source)


class BronzePoliciesASTTests(unittest.TestCase):
    """Verify bronze_policies has the size expectation."""

    @classmethod
    def setUpClass(cls) -> None:
        source = _read_text("ETL/pipelines/bronze/bronze_policies.py")
        cls.tree = ast.parse(source)

    def test_pdf_size_within_bounds_expectation(self) -> None:
        source = ast.unparse(self.tree)
        self.assertIn("pdf_size_within_bounds", source)
        self.assertIn("MAX_PDF_SIZE_BYTES", source)


if __name__ == "__main__":
    unittest.main()
