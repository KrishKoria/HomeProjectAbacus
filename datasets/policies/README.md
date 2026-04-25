# Synthetic Policy PDF Corpus

This directory contains locally generated, non-PHI policy PDFs for the ClaimOps Abacus demo.
They are designed to match the synthetic diagnosis, procedure, provider, cost, and denial
reason codes in `datasets/`.

Upload the PDF files to the Databricks policy landing folder when testing policy ingestion:

```text
/Volumes/healthcare/bronze/raw_landing/policies/
```

The Bronze policy pipeline ingests only `*.pdf` files. The Silver policy pipeline extracts
text with `pdfplumber` and chunks the content into `healthcare.silver.policy_chunks`.

Regenerate the PDFs with:

```powershell
uv run python scripts/generate_synthetic_policy_pdfs.py
```

