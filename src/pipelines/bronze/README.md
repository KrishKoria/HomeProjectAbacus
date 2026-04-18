# Bronze Layer — Spark Declarative Pipeline

Raw ingestion of four healthcare datasets into `healthcare.bronze.*` using
Lakeflow Spark Declarative Pipelines (SDP) with Auto Loader.

---

## Files

| File | Output Table | Source CSV |
|---|---|---|
| `bronze_claims.py` | `healthcare.bronze.claims` | `claims_1000.csv` |
| `bronze_providers.py` | `healthcare.bronze.providers` | `providers_1000.csv` |
| `bronze_diagnosis.py` | `healthcare.bronze.diagnosis` | `diagnosis.csv` |
| `bronze_cost.py` | `healthcare.bronze.cost` | `cost.csv` |

---

## Prerequisites

1. **Bootstrap notebook** — run `src/notebooks/bootstrap_bronze_landing.ipynb` first.
   This creates the Unity Catalog namespace and volume folders:
   ```
   Catalog  : healthcare
   Schema   : bronze
   Volume   : raw_landing
   Folders  : /Volumes/healthcare/bronze/raw_landing/{claims,providers,diagnosis,cost}/
   ```

2. **Upload CSVs** — copy the four files from `datasets/` into their volume folders.

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

The `location_present` expectation on providers **will fire** for rows with a null location —
this is an expected, known data quality issue documented in product spec §9.2.
Silver imputes `'Unknown'` for missing locations.

---

## HIPAA Controls

All four tables are created with identical HIPAA-required properties:

```python
"delta.enableChangeDataFeed"         : "true"          # incremental CDC to Silver
"delta.logRetentionDuration"         : "interval 6 years"  # 45 CFR § 164.316(b)(2)(i)
"delta.deletedFileRetentionDuration" : "interval 6 years"  # time-travel for audit
```

**PHI columns** (encrypt at rest in production):

| Table | PHI Columns |
|---|---|
| claims | `patient_id`, `billed_amount`, `diagnosis_code` |
| diagnosis | `diagnosis_code` |
| providers | none (doctor_name is operational, not PHI) |
| cost | none |

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
