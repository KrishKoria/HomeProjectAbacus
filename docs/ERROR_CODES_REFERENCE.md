# ClaimOps Error Codes & Constants Reference

> Auto-generated lookup for all error codes, diagnostic IDs, status values, and named constants across the codebase.
> Sources: `src/common/`, `src/analytics/`, `ETL/pipelines/silver/`, `scripts/`

---

## Table of Contents

1. [Diagnostic ID System](#1-diagnostic-id-system)
2. [Silver Layer Diagnostics](#2-silver-layer-diagnostics)
3. [Observability Diagnostics](#3-observability-diagnostics)
4. [Claim Status & Denial Reason Codes](#4-claim-status--denial-reason-codes)
5. [Data Quality Flags](#5-data-quality-flags)
6. [Quarantine Reasons](#6-quarantine-reasons)
7. [Policy Chunk Extraction Status](#7-policy-chunk-extraction-status)
8. [Log Categories](#8-log-categories)
9. [Data Sensitivity Levels](#9-data-sensitivity-levels)
10. [Pipeline Configuration Constants](#10-pipeline-configuration-constants)
11. [Analytics Thresholds & Lookup Values](#11-analytics-thresholds--lookup-values)

---

## 1. Diagnostic ID System

**Source:** `src/common/diagnostics.py`

### Domain Prefixes

| Constant | Value | Meaning |
|---|---|---|
| `DIAGNOSTIC_DOMAIN_BRONZE` | `"BRZ"` | Bronze ingestion layer |
| `DIAGNOSTIC_DOMAIN_SILVER` | `"SLV"` | Silver validation layer |
| `DIAGNOSTIC_DOMAIN_ANALYTICS` | `"ANL"` | Analytics build layer |
| `DIAGNOSTIC_DOMAIN_HIPAA` | `"HIPAA"` | HIPAA compliance checks |
| `DIAGNOSTIC_DOMAIN_OBSERVABILITY` | `"OBS"` | Pipeline observability |
| `DIAGNOSTIC_DOMAIN_QUARANTINE` | `"QRT"` | Quarantine audit |

### ID Format

```
CLAIMOPS-{DOMAIN}-{number:03d}
```

Example: `CLAIMOPS-SLV-101`, `CLAIMOPS-OBS-402`

---

## 2. Silver Layer Diagnostics

**Source:** `src/common/diagnostics.py` (lines 35–78)

### Claims (SLV-1xx)

| Diagnostic ID | Rule Name | Description |
|---|---|---|
| `CLAIMOPS-SLV-101` | `missing_claim_id` | claim_id is null or empty |
| `CLAIMOPS-SLV-102` | `missing_patient_id` | patient_id is null or empty |
| `CLAIMOPS-SLV-103` | `missing_provider_id` | provider_id is null or empty |
| `CLAIMOPS-SLV-104` | `missing_diagnosis_code` | diagnosis_code is null or empty |
| `CLAIMOPS-SLV-105` | `invalid_claim_date` | claim date cannot be parsed to yyyy-MM-dd |
| `CLAIMOPS-SLV-106` | `unknown_provider_reference` | provider_id has no matching row in providers table |
| `CLAIMOPS-SLV-107` | `unknown_diagnosis_reference` | diagnosis_code has no matching row in diagnosis table |
| `CLAIMOPS-SLV-108` | `inconsistent_denial_label` | is_denied disagrees with denial_reason_code != NONE |
| `CLAIMOPS-SLV-109` | `duplicate_claim_id` | claim_id appears more than once in validation input |
| `CLAIMOPS-SLV-999` | *(fallback)* | Unknown / unclassified silver rule failure |

### Providers (SLV-2xx)

| Diagnostic ID | Rule Name | Description |
|---|---|---|
| `CLAIMOPS-SLV-201` | `missing_provider_id` | provider_id is null or empty |
| `CLAIMOPS-SLV-202` | `missing_doctor_name` | doctor_name is null or empty |
| `CLAIMOPS-SLV-203` | `duplicate_provider_id` | provider_id appears more than once |

### Diagnosis (SLV-3xx)

| Diagnostic ID | Rule Name | Description |
|---|---|---|
| `CLAIMOPS-SLV-301` | `missing_diagnosis_code` | diagnosis_code is null or empty |
| `CLAIMOPS-SLV-302` | `missing_category` | category field is null or empty |
| `CLAIMOPS-SLV-303` | `missing_severity` | severity field is null or empty |
| `CLAIMOPS-SLV-304` | `invalid_severity` | severity is not "High" or "Low" |
| `CLAIMOPS-SLV-305` | `duplicate_diagnosis_code` | diagnosis_code appears more than once |

### Cost Benchmarks (SLV-4xx)

| Diagnostic ID | Rule Name | Description |
|---|---|---|
| `CLAIMOPS-SLV-401` | `missing_procedure_code` | procedure_code is null or empty |
| `CLAIMOPS-SLV-402` | `invalid_average_cost` | average_cost is null or non-positive |
| `CLAIMOPS-SLV-403` | `invalid_expected_cost` | expected_cost is null or non-positive |
| `CLAIMOPS-SLV-404` | `missing_region` | region is null or empty |
| `CLAIMOPS-SLV-405` | `duplicate_cost_key` | duplicate procedure_code + region composite key |

### Policy Chunks (SLV-5xx)

| Diagnostic ID | Rule Name | Description |
|---|---|---|
| `CLAIMOPS-SLV-501` | `unreadable_pdf` | pdfplumber raised an error reading the file |
| `CLAIMOPS-SLV-502` | `empty_pdf_text` | PDF opened successfully but extracted no text |
| `CLAIMOPS-SLV-503` | `duplicate_policy_path` | same PDF path appears more than once |

---

## 3. Observability Diagnostics

**Source:** `src/analytics/observability_assets.py` (lines 17–23)

These fire when a dataset-level pipeline failure is detected.

| Diagnostic ID | Dataset | Description |
|---|---|---|
| `CLAIMOPS-OBS-401` | `claims` | Claims pipeline failure |
| `CLAIMOPS-OBS-402` | `providers` | Providers pipeline failure |
| `CLAIMOPS-OBS-403` | `diagnosis` | Diagnosis pipeline failure |
| `CLAIMOPS-OBS-404` | `cost` | Cost benchmarks pipeline failure |
| `CLAIMOPS-OBS-405` | `policies` | Policy chunks pipeline failure |
| `CLAIMOPS-OBS-499` | *(fallback)* | Unknown dataset failure |

---

## 4. Claim Status & Denial Reason Codes

**Sources:** `scripts/generate_synthetic_claim_labels.py`, `ETL/pipelines/silver/bronze_claims.py`

### Claim Status Values

| Value | Meaning |
|---|---|
| `APPROVED` | Claim passed adjudication |
| `DENIED` | Claim was denied |

### Denial Reason Codes

| Code | Description | Trigger Condition |
|---|---|---|
| `NONE` | No denial — claim approved | Default for passing claims |
| `MISSING_PROCEDURE` | No procedure code present | procedure_code is null |
| `MISSING_BILLED_AMOUNT` | No billed amount present | billed_amount is null |
| `OVER_BENCHMARK` | Billed amount exceeds cost threshold | billed > 2.5× expected_cost |
| `MEDICAL_REVIEW` | Selected for manual medical review | Randomly sampled subset |

---

## 5. Data Quality Flags

`_data_quality_flags` is written onto **trusted silver records only** — rows that passed all quarantine checks. These flags survive into the silver table so analysts can monitor soft quality issues without discarding the record.

Intermediate boolean columns (`missing_claim_id`, `missing_patient_id`, etc.) are computed during validation to drive the quarantine routing logic, but they are **dropped** before the silver and quarantine tables are written. They do not appear in `_data_quality_flags`.

**Source:** `ETL/pipelines/silver/silver_*.py`

### Claims Quality Flags (written to `_data_quality_flags`)

| Flag | Description |
|---|---|
| `missing_procedure_code` | procedure_code is null — record still trusted |
| `missing_billed_amount` | billed_amount is null — record still trusted |

> All other claims checks (`missing_claim_id`, `missing_patient_id`, `unknown_provider_reference`, etc.) are **quarantine triggers**, not quality flags. See [Section 6](#6-quarantine-reasons) for those.

### Providers Quality Flags (written to `_data_quality_flags`)

| Flag | Description |
|---|---|
| `provider_location_unknown` | location was null — imputed as "Unknown", record still trusted |

> `missing_provider_id` and `missing_doctor_name` are quarantine triggers, not quality flags.

### Diagnosis Quality Flags

None — `spark_quality_flags({})` is called with an empty dict. All diagnosis checks are quarantine triggers only.

### Cost Benchmark Quality Flags

None — `spark_quality_flags({})` is called with an empty dict. All cost checks are quarantine triggers only.

### Policy Chunk Quality Flags (written to `_data_quality_flags`)

| Flag | Description |
|---|---|
| `unreadable_pdf` | pdfplumber error on the source file |
| `empty_pdf_text` | PDF opened but yielded no text |

---

## 6. Quarantine Reasons

Records that fail mandatory checks are routed to the quarantine schema with a human-readable reason string.

**Source:** `ETL/pipelines/silver/silver_*.py`

### Claims

| Quarantine Reason |
|---|
| `"claim_id is required for a trusted silver record"` |
| `"patient_id is required for a trusted silver record"` |
| `"provider_id is required for a trusted silver record"` |
| `"diagnosis_code is required for a trusted silver record"` |
| `"claim date could not be parsed into yyyy-MM-dd"` |
| `"provider_id does not match a known provider reference row"` |
| `"diagnosis_code does not match a known diagnosis reference row"` |
| `"is_denied must agree with denial_reason_code != NONE"` |
| `"duplicate claim_id observed in the silver validation input"` |

### Providers

| Quarantine Reason |
|---|
| `"provider_id is required for a trusted provider record"` |
| `"doctor_name is required for credential verification"` |
| `"duplicate provider_id observed in the silver stream"` |

### Diagnosis

| Quarantine Reason |
|---|
| `"diagnosis_code is required for lookup validation"` |
| `"category is required for specialty mismatch features"` |
| `"severity is required for downstream risk features"` |
| `"severity must normalize to High or Low"` |
| `"duplicate diagnosis_code observed in the silver stream"` |

### Cost Benchmarks

| Quarantine Reason |
|---|
| `"procedure_code is required for benchmark joins"` |
| `"region is required for regional cost benchmarks"` |
| `"average_cost must parse to a positive decimal"` |
| `"expected_cost must parse to a positive decimal"` |
| `"duplicate procedure_code + region observed in the silver stream"` |

---

## 7. Policy Chunk Extraction Status

**Source:** `ETL/pipelines/silver/silver_policy_chunks.py` (lines 72–75, 179)

| Value | Meaning |
|---|---|
| `"OK"` | PDF read and text extracted successfully |
| `"UNREADABLE_PDF"` | pdfplumber raised an exception |
| `"EMPTY_PDF_TEXT"` | No text content extracted from PDF |
| `"NOT_CONFIGURED"` | Embedding not yet generated (placeholder) |

---

## 8. Log Categories

**Source:** `src/common/log_categories.py` (lines 6–12)

| Constant | Value | Used For |
|---|---|---|
| `LOG_CATEGORY_PIPELINE_OPS` | `"pipeline_ops"` | General pipeline execution events |
| `LOG_CATEGORY_DATA_QUALITY` | `"data_quality"` | Data quality check results |
| `LOG_CATEGORY_GOVERNANCE_AUDIT` | `"governance_audit"` | Governance / compliance audit trail |
| `LOG_CATEGORY_ANALYTICS_BUILD` | `"analytics_build"` | Analytics table build events |
| `LOG_CATEGORY_SILVER_PIPELINE` | `"silver_pipeline"` | Silver layer processing events |
| `LOG_CATEGORY_QUARANTINE_AUDIT` | `"quarantine_audit"` | Records routed to quarantine |
| `LOG_CATEGORY_POLICY_CHUNKING` | `"policy_chunking"` | PDF policy chunk extraction |

---

## 9. Data Sensitivity Levels

**Source:** `src/common/silver_pipeline_config.py` (lines 28–30), `src/analytics/claims_analytics.py`

| Level | Applied To |
|---|---|
| `"NON-PHI"` | Non-patient-identifiable fields |
| `"PHI"` | Protected health information fields |
| `"SENSITIVE"` | Sensitive but not strictly PHI |
| `"MINIMUM-NECESSARY"` | PHI fields restricted to minimum necessary access |

---

## 10. Pipeline Configuration Constants

**Source:** `src/common/bronze_pipeline_config.py`, `src/common/silver_pipeline_config.py`

### Catalog & Schema Defaults

| Constant | Value |
|---|---|
| `CATALOG_DEFAULT` | `"healthcare"` |
| `BRONZE_SCHEMA_DEFAULT` | `"bronze"` |
| `SILVER_SCHEMA_DEFAULT` | `"silver"` |
| `QUARANTINE_SCHEMA_DEFAULT` | `"quarantine"` |
| `ANALYTICS_SCHEMA_DEFAULT` | `"analytics"` |
| `BRONZE_VOLUME_DEFAULT` | `"raw_landing"` |
| `BRONZE_VOLUME_ROOT` | `"/Volumes/healthcare/bronze/raw_landing"` |

### Audit Column Names

| Constant | Columns |
|---|---|
| `AUDIT_COLUMNS` | `_ingested_at`, `_source_file`, `_pipeline_run_id` |
| `SILVER_AUDIT_COLUMNS` | `_silver_processed_at`, `_data_quality_flags` |
| `QUARANTINE_AUDIT_COLUMNS` | `_quarantined_at`, `diagnostic_id`, `rule_name`, `quarantine_reason` |
| `RESCUED_DATA_COLUMN` | `_rescued_data` |

### Run ID

| Constant | Value |
|---|---|
| `PIPELINE_RUN_ID_FORMAT` | `"yyyyMMdd_HHmmss"` |
| `PIPELINE_RUN_ID_CONF` | `"claimops.pipeline_run_id"` |

### Monetary Precision

| Constant | Value | Purpose |
|---|---|---|
| `MONEY_DECIMAL_PRECISION` | `18` | Total decimal digits for monetary columns |
| `MONEY_DECIMAL_SCALE` | `2` | Decimal places for monetary columns |

### Policy Chunking

| Constant | Value |
|---|---|
| `POLICY_CHUNK_SIZE_TOKENS` | `512` |
| `POLICY_CHUNK_OVERLAP_TOKENS` | `64` |

---

## 11. Analytics Thresholds & Lookup Values

**Source:** `src/analytics/claims_analytics.py` (lines 21–44)

### Thresholds

| Constant | Value | Description |
|---|---|---|
| `HIGH_COST_THRESHOLD_RATIO` | `1.5` | Billing flagged as high-cost when > 1.5× expected_cost |
| *(synthetic label threshold)* | `2.5` | Denial triggered in label generation when billed > 2.5× expected_cost |

### Boolean Normalization Lookup

| Category | Values |
|---|---|
| `TRUE_VALUES` | `"1"`, `"TRUE"`, `"YES"`, `"Y"` |
| `FALSE_VALUES` | `"0"`, `"FALSE"`, `"NO"`, `"N"` |

### Reference Datasets

| Constant | Datasets |
|---|---|
| `REFERENCE_TABLES` | `claims`, `providers`, `diagnosis`, `cost`, `policies` |

### Record Trust States

| Value | Meaning |
|---|---|
| `"trusted"` | Record passed all checks → written to Silver |
| `"quarantined"` | Record failed a mandatory check → written to Quarantine |

---

*Last updated: 2026-04-27*
