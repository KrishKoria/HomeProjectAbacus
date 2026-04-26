# Bronze Layer — Spark Declarative Pipeline

Raw ingestion of four healthcare datasets into `healthcare.bronze.*` using
Lakeflow Spark Declarative Pipelines (SDP) with Auto Loader.

---

## Files

| File | Output Table | Source | Format |
|---|---|---|---|
| `bronze_claims.py` | `healthcare.bronze.claims` | `claims_1000.csv` | CSV |
| `bronze_providers.py` | `healthcare.bronze.providers` | `providers_1000.csv` | CSV |
| `bronze_diagnosis.py` | `healthcare.bronze.diagnosis` | `diagnosis.csv` | CSV |
| `bronze_cost.py` | `healthcare.bronze.cost` | `cost.csv` | CSV |
| `bronze_policies.py` | `healthcare.bronze.policies` | `policies/*.pdf` | binaryFile |

---

## Prerequisites

1. **Bootstrap notebook** — run `src/notebooks/bootstrap_bronze_landing.ipynb` first.
   This creates the Unity Catalog namespace and volume folders:
   ```
   Catalog  : healthcare
   Schema   : bronze
   Volume   : raw_landing
   Folders  : /Volumes/healthcare/bronze/raw_landing/{claims,providers,diagnosis,cost,policies}/
   ```

2. **Upload CSVs** — copy the four files from `datasets/` into their volume folders.

3. **Upload PDFs** — copy insurance policy PDF documents into the `policies/` folder.
   Only `*.pdf` files are ingested — other file types are ignored by `pathGlobFilter`.

---

## Pipeline Setup (Databricks UI)

1. Go to **Pipelines → Create Pipeline → Declarative ETL**.
2. Set **Catalog** = `healthcare`, **Schema** = `bronze`.
3. Add all four `.py` files from this directory as pipeline source files.
4. Set **Serverless** = enabled (no cluster config needed).
5. Click **Start**.

The pipeline is stateless and idempotent — re-running on the same files is safe.
Auto Loader checkpoints prevent reprocessing already-ingested records.

---

## Output Schema (all tables)

Every Bronze table contains all original CSV columns plus these audit columns:

| Column | Type | Purpose |
|---|---|---|
| `_ingested_at` | timestamp | When the row entered Bronze (HIPAA audit) |
| `_source_file` | string | Volume path of the source file (data lineage) |
| `_pipeline_run_id` | string | Pipeline execution timestamp (audit correlation) |
| `_rescued_data` | string? | Raw content of rows Auto Loader could not parse |

---

## Data Quality

Each table has `@dp.expect()` (warn-only) rules. **Bronze never drops records** —
violations are logged to the pipeline event log (visible in the Pipeline UI → Data Quality tab).

| Table | Expectations |
|---|---|
| claims | claim_id not null, patient_id not null, provider_id not null, billed_amount > 0, no parse errors |
| providers | provider_id not null, doctor_name not null, location not null (known nullable), no parse errors |
| diagnosis | diagnosis_code not null, category not null, severity not null, no parse errors |
| cost | procedure_code not null, average_cost > 0, expected_cost > 0, region not null, no parse errors |
| policies | path not null, content not null, length > 0 |

The `location_present` expectation on providers **will fire** for rows with a null location —
this is an expected, known data quality issue documented in product spec §9.2.
Silver imputes `'Unknown'` for missing locations.

---

## HIPAA Controls

All five tables are created with these properties (`interval 2190 days` is the canonical Delta literal for the six-year organization retention policy used for audit reconstruction, consistent with §164.316(b)(2)(i) compliance documentation standards — see bronze_claims.py for full rationale):

```python
"delta.enableChangeDataFeed"         : "true"          # incremental CDC to Silver
"delta.logRetentionDuration"         : "interval 2190 days"  # § 164.316(b)(2)(i) — six-year retention
"delta.deletedFileRetentionDuration" : "interval 2190 days"  # § 164.316(b)(2)(i) — time-travel audit
```

The claims table additionally carries PHI metadata properties (§ 164.312(a)(2)(iv)):

```python
"hipaa.phi_columns"     : "allowed_amount,billed_amount,claim_status,date,denial_reason_code,diagnosis_code,follow_up_required,is_denied,paid_amount,patient_id"
"hipaa.data_sensitivity": "PHI"
```

**Audit columns** satisfy § 164.312(b) — hardware/software mechanisms to record
and examine activity in systems that contain ePHI:

| Column | CFR Citation | What it captures |
|---|---|---|
| `_ingested_at` | § 164.312(b) audit controls | When the row entered Bronze |
| `_source_file` | § 164.312(b) audit controls | Which source file the row came from |
| `_pipeline_run_id` | § 164.312(b) audit controls | Which pipeline execution created the row |
| `_rescued_data` | § 164.312(c)(1) data integrity | Rows that could not be parsed |

> **Unity Catalog Audit Logging (required for production):** The audit columns above capture
> WHAT was ingested and WHEN/FROM WHERE. To capture WHO accessed ePHI (required by
> § 164.312(b)), the Databricks workspace `system.access.audit` table must be enabled.
> This is a workspace-level configuration, not a pipeline setting.

**binaryFile vs CSV:** `bronze_policies.py` uses `cloudFiles.format=binaryFile` instead of CSV.
The schema is fixed (`path`, `modificationTime`, `length`, `content`) — no `inferColumnTypes`,
`schemaEvolutionMode`, or `rescuedDataColumn` options apply. The `path` column is the source
file reference; `content` holds raw PDF bytes. Text extraction happens in Silver.

**PHI columns** — encrypt at rest in production via Databricks column masking (§ 164.312(a)(2)(iv)):

| Table | PHI Columns | Not PHI |
|---|---|---|
| claims | `patient_id`, `diagnosis_code`, `billed_amount`, `date`, `claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`, `is_denied`, `follow_up_required` | `claim_id` (PHI-adjacent), `provider_id`, `procedure_code` |
| diagnosis | none — reference table without patient linkage (§ 164.501) | all columns |
| providers | none — provider identity is operational data (§ 164.501) | all columns |
| cost | none — benchmark reference data | all columns |
| policies | none — insurance policy text, no patient data (Assumption A-04) | all columns |

> **`date` is PHI:** 45 CFR § 164.514(b)(2)(iv) explicitly lists "all elements of dates
> (except year) for dates directly related to an individual" as PHI identifiers. The claim
> submission date is a date directly related to a patient's health event.

> **`diagnosis_code` in the diagnosis table is NOT PHI:** The reference table has no patient
> linkage. A standalone code (D10=Heart) is medical terminology, not individually identifiable
> health information (§ 164.501). It becomes PHI only when combined with patient_id in claims.

**Minimum Necessary (§ 164.502(b)):** Bronze ingests all CSV columns because the Bronze
layer IS the source-of-truth archive required for audit reconstruction under § 164.316(b)(2)(i).
This is the minimum necessary for compliance. PHI restriction and column masking occur at
Silver and Gold layers where only the minimum PHI needed for each purpose is exposed.

**Breach Notification (§ 164.410):** Any breach of unsecured PHI in the claims table must
be reported to the Covered Entity without unreasonable delay and no later than 60 days after
discovery. See `datasets/DATA_CLASSIFICATION.md` for the full breach notification procedure.

---

## After the Pipeline Runs

Run the two verification notebooks in order:

1. `src/notebooks/bronze_verify_and_rbac.ipynb` — verifies TBLPROPERTIES, audit columns,
   row counts, and applies Unity Catalog RBAC grants.
2. `src/notebooks/bronze_profiling.ipynb` — answers the 6 required data quality questions
   and detects overbilling anomalies.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Table empty after run | CSVs not uploaded to volume | Upload files, re-run pipeline |
| `_rescued_data` non-null rows | CSV encoding or delimiter issue | Check source file, re-export CSV |
| `location_present` violations | Known null locations in providers | Expected — not an error |
| Schema mismatch on re-run | Incompatible column type change | Full refresh required (`databricks pipelines reset`) |
| Pipeline stuck INITIALIZING | Normal serverless cold start | Wait 2–3 minutes |
