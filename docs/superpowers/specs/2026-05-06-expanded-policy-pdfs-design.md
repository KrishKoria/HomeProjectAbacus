# Expanded Synthetic Policy PDFs — Design Spec

**Date:** 2026-05-06
**Status:** Approved
**Scope:** `tools/generate_expanded_policy_pdfs.py` (new file), `datasets/policies/` (6 new PDFs)

---

## Context

The project has 5 synthetic policy rule PDFs (CLAIMOPS-POL-001 through -005) covering: claim completeness, medical necessity, cost benchmarks, provider documentation, and denial reason remediation. These feed the RAG pipeline (Bronze → Silver → Gold → Vector Search index) and provide policy-backed explanations in the Streamlit UI.

Gap: The 5 existing policies map to denial reasons but cover only 5 of the 22 ML features. The RAG system needs richer policy content to retrieve relevant chunks for ANY SHAP explanation — currently many feature explanations (dx_px_compatible, provider_30d_denial_rate, missing_fields_count, cost_overbenchmark_and_highseverity, etc.) have no matching policy document.

Goal: Generate 6 additional policy PDFs that cover the remaining feature space, using data-driven rules from reference datasets, via a reusable script.

---

## Design

### 6 New Policy PDFs

| # | Policy ID | Title | Covers | Rule Source |
|---|-----------|-------|--------|-------------|
| 6 | CLAIMOPS-POL-006 | Diagnosis-Procedure Compatibility Policy | `dx_px_compatible`, `dx_px_pair_risk_prior` | `dx_px_mapping.csv` rows |
| 7 | CLAIMOPS-POL-007 | Temporal Provider Utilization Policy | `provider_claim_count_30d/60d/90d`, `provider_30d_denial_rate`, `low_volume_provider_risk` | Feature reasons + thresholds |
| 8 | CLAIMOPS-POL-008 | Specialty-Diagnosis Alignment Policy | `specialty_diagnosis_mismatch`, `severity_procedure_mismatch` | `diagnosis.csv`, `providers_1000.csv` |
| 9 | CLAIMOPS-POL-009 | Missing Data Field Triage Policy | `missing_fields_count`, `is_procedure_missing`, `is_amount_missing`, `provider_location_missing` | Feature reasons + field catalog |
| 10 | CLAIMOPS-POL-010 | Cost-Severity Interaction Policy | `cost_overbenchmark_and_highseverity`, `high_cost_flag`, `mismatch_and_overbenchmark`, `amount_to_benchmark_ratio` | `cost.csv` benchmarks |
| 11 | CLAIMOPS-POL-011 | Provider Risk Stratification Policy | `provider_risk_score`, `provider_claim_count`, `low_volume_provider_risk` | Feature reasons + risk tiers |

### Content Principles

- **Zero claim IDs** — policies are generic rules only, no C0001/C0002 references
- **Zero patient data** — no patient_id, no dates, no billed amounts as examples
- **Data-driven rules** — rule tables populated from reference CSV values where applicable (dx_px_mapping, cost benchmarks)
- **Consistent format** — same ReportLab layout as existing 5 (DocTitle, metadata table, summary bullets, rules table, remediation, RAG safety note)
- **PHI-free** — all text safe for LLM context, same as existing corpus

### Script Architecture

**File:** `tools/generate_expanded_policy_pdfs.py`

Reuses from existing `tools/generate_synthetic_policy_pdfs.py`:
- `build_styles()` — ReportLab paragraph styles
- `add_header_footer()` — page header/footer with synthetic-PHI disclaimer
- `build_policy()` — assembles story list from policy dict + styles
- `paragraph_list()` — bullet helper

New additions:
- `read_dataset(path)` → `pd.DataFrame` — reads a CSV from `datasets/`
- `generate_dx_px_rules()` → list of `(rule_id, text)` tuples from `dx_px_mapping.csv`
- `generate_cost_rules()` → list of `(rule_id, text)` tuples from `cost.csv`
- `generate_severity_rules()` → list from `diagnosis.csv`
- `generate_provider_rules()` → list from `providers_1000.csv`
- `EXPANDED_POLICIES` list — 6 policy dicts, some with inline rules, some calling data-driven generators
- `generate()` — creates output dir, builds PDFs, returns list of paths

Output directory: `datasets/policies/` (same as existing 5)

### CLI

```bash
uv run python tools/generate_expanded_policy_pdfs.py
```

No arguments needed. Always generates all 6. Idempotent — overwrites existing files.

---

## Implementation Plan (High-Level)

1. Read reference datasets to understand exact values for rule generation
2. Create `tools/generate_expanded_policy_pdfs.py` with:
   - Helper functions to read datasets and build data-driven rules
   - 6 policy definitions (same dict format as existing generator)
   - Reuse `build_styles`, `add_header_footer`, `build_policy` from existing generator
   - Generate PDFs with ReportLab
3. Run the script and verify all 6 PDFs are generated
4. Verify PDFs are valid (open with pdfplumber, check text extraction)
5. Verify all text is PHI-free (no claim IDs, patient IDs, dates, dollar amounts)

---

## Verification

1. **Script runs**: `uv run python tools/generate_expanded_policy_pdfs.py` produces 6 PDFs in `datasets/policies/`
2. **PDF validity**: Open each PDF with pdfplumber, confirm text extraction succeeds with status OK
3. **Content check**: Grep each extracted text for claim IDs (C0001, etc.), patient IDs (P001, etc.), dates — must be zero matches
4. **Format match**: Each PDF has policy_id, title, purpose, summary, rules, remediation, RAG safety note sections
5. **Ingestion test**: Verify `bronze_policies` pipeline can ingest the new PDFs without errors
6. **RAG relevance**: After embedding + indexing, query with a SHAP reason like "diagnosis-procedure compatibility" and verify relevant chunks from POL-006 are returned
