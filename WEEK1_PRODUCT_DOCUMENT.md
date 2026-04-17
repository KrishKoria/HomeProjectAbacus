# WEEK 1 – PRODUCT DOCUMENT

## Project: AI-Powered Claim Denial Prevention & Remediation System

---

## 1. Problem Understanding

In real life: Doctor treats patient → Billing team creates claim → Insurance may approve or deny

**The Problem:** Many claims get denied because of:

- Missing data
- Wrong codes
- Incorrect billing

**This causes:**

- Delay in payment
- Extra rework ($14 per claim to rework)
- Revenue loss (healthcare loses $262 billion/year to denied claims)

---

## 2. Primary User

**Billing Analyst** — the person this system is built for.

| Attribute        | Detail                                                                    |
| ---------------- | ------------------------------------------------------------------------- |
| Role             | Medical billing professional                                              |
| Domain expertise | ICD-10 codes, CPT codes, insurance requirements                           |
| Goal             | Submit clean claims on first pass, avoid denials                          |
| Pain point       | Too many claims to audit manually; policy documents are hundreds of pages |

---

## 3. System Architecture

### 3.1 Overall System Architecture

> Context only — shows the full end-state system. Week 1 builds the Bronze layer inside Databricks.

```mermaid
graph TB
    User["Browser — Analyst / Admin"] -->|"HTTPS TLS 1.3"| ST["Streamlit Dashboard\nClaim Form · History · Analytics"]
    ST -->|"REST API calls"| API["FastAPI Backend\nAuth · Rule Engine · Agent Orchestrator"]
    API --> PG[("PostgreSQL\nUsers · Sessions · Audit Log")]

    subgraph DB["Databricks Workspace"]
        direction TB
        BR["Bronze Layer\nRaw Delta Tables"] --> SI["Silver Layer\nCleaned & Validated"]
        SI --> GO["Gold Layer\nML Feature Store"]
        GO --> ML["ML Layer\nXGBoost + SHAP\nMLflow Registry · Risk Scoring Endpoint"]
        GO --> RAG["RAG Layer\nVector Search Index\nDatabricks-hosted FM endpoint"]
        UC["Unity Catalog\nGovernance · Lineage · RBAC"]
    end

    API -->|"Feature vector"| ML
    API -->|"PHI-safe query"| RAG
```

### 3.2 Medallion Data Architecture

> Shows the full data layer. **Week 1 scope = Bronze layer only.**

```mermaid
flowchart LR
    subgraph SRC["Input Sources"]
        C["claims_1000.csv"]
        P["providers_1000.csv"]
        D["diagnosis.csv"]
        K["cost.csv"]
    end

    subgraph BRONZE["⬛ WEEK 1 — Bronze Layer\nRaw · As-Is · Append-Only\nIngestion TS · Source File · No Transform"]
        B1["healthcare.bronze.claims\n(Delta)"]
        B2["healthcare.bronze.providers\n(Delta)"]
        B3["healthcare.bronze.diagnosis\n(Delta)"]
        B4["healthcare.bronze.cost\n(Delta)"]
    end

    subgraph SILVER["Silver Layer\nCleaned · Null Flagged · Schema Fixed"]
        S1["healthcare.silver.claims"]
        S2["healthcare.silver.providers"]
    end

    subgraph GOLD["Gold Layer\nML Features · Joins Done · Risk Features"]
        G1["healthcare.gold.claim_features"]
    end

    C --> B1 --> S1 --> G1
    P --> B2 --> S2 --> G1
    D --> B3 --> G1
    K --> B4 --> G1
```

### 3.3 Week 1 Ingestion Flow

```mermaid
flowchart TD
    CSV["CSV Files\nLanding Zone\n(cloud storage path — decided at implementation)"] -->|"read_files() function\nCSV format · incremental only"| SDP["Lakeflow Spark Declarative Pipeline\nStreaming Table definition\nCheckpoint + schema state managed by SDP"]
    SDP -->|"Append-only write"| BT["Bronze Delta Table\nhealthcare.bronze.claims\nRaw data preserved as-is"]
    BT --> UC["Unity Catalog\nGovernance · RBAC · Lineage · Data Lineage"]
```

---

## 4. Week 1 Scope

| In Scope                                  | Out of Scope                  |
| ----------------------------------------- | ----------------------------- |
| Understand the problem                    | ML model                      |
| Identify and load datasets                | RAG / Vector Search           |
| Create Bronze Delta tables                | Agent orchestration           |
| Enable audit columns and Change Data Feed | FastAPI / Streamlit           |
| Basic data profiling                      | Any AI features               |
| Unity Catalog RBAC setup                  | Cleaning or transforming data |
| HIPAA Bronze table properties             | Silver / Gold layers          |

---

## 5. Technology Stack — Week 1

| Component            | Choice                                         | Why This Choice                                                                                                                                                                                                                                                                                               |
| -------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data Platform        | **Databricks**                                 | Unified ETL + ML + governance on a single platform. Native Delta Lake. HIPAA controls and BAA support available with required compliance configuration.                                                                                                                                                       |
| Storage Format       | **Delta Lake**                                 | ACID transactions (no partial writes), time-travel for HIPAA audit, Change Data Feed for incremental reads, schema evolution without pipeline failure.                                                                                                                                                        |
| ETL Orchestration    | **Lakeflow Spark Declarative Pipelines (SDP)** | Databricks' modern, production-grade ETL framework. Replaces manual notebook-based Auto Loader. Manages checkpointing, schema evolution, and incremental state automatically. Pipeline code lives in plain `.sql` or `.py` files — not notebooks — enabling version control and CI/CD. Serverless by default. |
| Ingestion Function   | **`read_files()` via SDP**                     | SDP's `read_files()` function uses Auto Loader under the hood but abstracts away manual configuration of checkpoint paths, schema locations, and streaming setup. CSV is a supported format.                                                                                                                  |
| Catalog & Governance | **Unity Catalog**                              | Centralized RBAC at row/column level. Automatic data lineage tracking from source to Gold. Required for serverless SDP pipelines. Meets HIPAA audit requirement natively.                                                                                                                                     |

---

## 6. Input Datasets

| Dataset   | File                 | Key Columns                                                                            | Purpose                         |
| --------- | -------------------- | -------------------------------------------------------------------------------------- | ------------------------------- |
| Claims    | `claims_1000.csv`    | claim_id, patient_id, provider_id, diagnosis_code, procedure_code, billed_amount, date | Primary dataset — 1,000 records |
| Providers | `providers_1000.csv` | provider_id, doctor_name, specialty, location                                          | Who created the claim           |
| Diagnosis | `diagnosis.csv`      | diagnosis_code, category, severity                                                     | Medical reason for claim        |
| Cost      | `cost.csv`           | procedure_code, average_cost, expected_cost, region                                    | Detect overbilling vs benchmark |

### Known Data Quality Issues

| Issue                     | Column                | Impact                                               |
| ------------------------- | --------------------- | ---------------------------------------------------- |
| Missing procedure_code    | claims.procedure_code | Automatic denial — incomplete claim                  |
| Missing billed_amount     | claims.billed_amount  | Unprocessable claim                                  |
| Missing provider location | providers.location    | Administrative rejection                             |
| No approved/denied label  | claims table          | Must derive proxy label later using rule-based logic |

---

## 7. HIPAA & Security Baseline

### 7.1 Data Handling

> **All development data is synthetic/anonymized.** No real PHI is used in this environment. Production deployment requires a signed Databricks BAA before any real PHI is ingested.

### 7.2 Data Classification

| Column                                       | Classification      | Handling                                  |
| -------------------------------------------- | ------------------- | ----------------------------------------- |
| patient_id                                   | PHI                 | Encrypted at rest (AES-256 in production) |
| billed_amount                                | PHI                 | Encrypted at rest                         |
| diagnosis_code                               | PHI                 | Encrypted at rest                         |
| claim_id                                     | PHI-adjacent        | Standard storage, access controlled       |
| provider_id                                  | Operational         | Standard storage                          |
| Audit columns (\_ingested_at, \_source_file) | Compliance metadata | Append-only, never deleted                |

### 7.3 HIPAA Bronze Table Properties

Every Bronze Delta table must have the following properties set at creation — **not retroactively**:

| Property                           | Value            | Reason                                                                                                                          |
| ---------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| delta.enableChangeDataFeed         | true             | Enables incremental reads so downstream layers process only new records — without this, every run re-processes the entire table |
| delta.logRetentionDuration         | interval 6 years | HIPAA requires 6-year retention under 45 CFR § 164.316(b)(2)(i) — Bronze is the source-of-truth for audit reconstruction        |
| delta.deletedFileRetentionDuration | interval 6 years | Retains physical files even after logical deletion, supporting full time-travel for compliance investigations                   |

### 7.4 Unity Catalog Access Control

Apply least-privilege access from day one on all four Bronze tables. Exact principal names are decided at workspace setup — the table below shows the required roles, not specific names:

| Principal                                                      | Permission  | Reason                                      |
| -------------------------------------------------------------- | ----------- | ------------------------------------------- |
| ETL ingestion service account (name decided at implementation) | INSERT only | Pipeline writes data; cannot read or delete |
| Billing analyst role or group (name decided at implementation) | SELECT only | Analysts can query; cannot modify raw data  |
| public                                                         | No access   | Default-deny for all other principals       |

### 7.5 Secrets Management

Storage credentials for the landing zone must be stored in Databricks Secrets — never hardcoded in notebooks or configuration files. Any credentials found hardcoded in a notebook are a HIPAA security violation and must be rotated immediately.

---

## 8. Non-Functional Requirements (Reference)

The Bronze layer must not constrain these system-level targets:

| ID           | Requirement                           | Target                                 |
| ------------ | ------------------------------------- | -------------------------------------- |
| NFR-PERF-01  | Single-claim validation latency (p95) | < 2 seconds                            |
| NFR-PERF-05  | ML model inference (p95)              | < 150ms                                |
| NFR-PERF-06  | RAG retrieval + explanation (p95)     | < 5 seconds                            |
| NFR-REL-01   | System availability                   | 99.9%                                  |
| NFR-COMP-02  | Audit log retention                   | Minimum 6 years, immutable             |
| NFR-SCALE-01 | Claims throughput                     | 10,000/day initial, scalable to 1M/day |

> SDP's `read_files()` incremental-only design is the Bronze layer's direct contribution to NFR-SCALE-01. A full re-scan ingestion pattern would bottleneck throughput at scale.

---

## 9. Cost Estimation

**Cloud:** AWS (us-east-1 region)
**Databricks Tier:** Premium — required for HIPAA compliance controls, Unity Catalog enterprise governance, and audit logging. Standard tier does not cover these requirements.

> Prices are AWS and Databricks public list prices researched April 2026. Enterprise agreements typically carry 20–40% negotiated discounts. These are bottom-up planning estimates — not vendor quotes. Verify current rates at databricks.com/pricing and aws.amazon.com/pricing before budgeting.

---

### 9.1 Usage Assumptions

All cost estimates below are derived from these assumptions. Changing one assumption scales the cost proportionally.

| Parameter                                  | Staging                                | Production               | Basis                                     |
| ------------------------------------------ | -------------------------------------- | ------------------------ | ----------------------------------------- |
| Claims processed per day                   | 1,000                                  | 10,000                   | NFR-SCALE-01: 10,000/day initial target   |
| Claims processed per month                 | 30,000                                 | 300,000                  | 30 working days                           |
| HIGH-risk claims receiving LLM explanation | 15,000/month                           | 150,000/month            | Assumed 50% of claims flagged HIGH risk   |
| Avg. tokens per LLM call                   | 800 input / 500 output                 | 800 input / 500 output   | System prompt + 5 retrieved policy chunks |
| Concurrent analyst users                   | 2                                      | 10                       | Billing team size                         |
| Policy document corpus                     | ~500 chunks (768-dim)                  | ~500 chunks              | ~50 insurance policy PDFs                 |
| Delta Lake storage growth                  | ~15 MB/month                           | ~150 MB/month            | 300K claims/month × ~500 bytes/record     |
| Audit log rows inserted/month              | 30,000                                 | 300,000                  | 1 audit event per claim + admin activity  |
| ETL job runtime per day                    | 30 min                                 | 60 min                   | Daily batch processing window             |
| ML model serving                           | 1 CPU replica, scale-to-zero overnight | 1 CPU replica, always-on | XGBoost inference — no GPU required       |

---

### 9.2 Unit Pricing Reference

| Component                                                                                | Unit Price                                        | Source                                                            |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------- |
| Databricks Premium — Jobs Compute, AWS                                                   | ~$0.37/DBU + EC2 cost                             | Databricks public pricing, Premium tier                           |
| Databricks Vector Search Standard, AWS                                                   | $0.07/DBU · 4 DBU/unit/hour = **$0.28/unit/hour** | Confirmed: Databricks Vector Search pricing page (April 2026)     |
| Foundation Model API — Meta Llama 3.3 70B                                                | $0.90/M input tokens · $2.70/M output tokens      | Databricks Foundation Model API pricing (approximate, April 2026) |
| Foundation Model API — GTE Large embeddings                                              | $0.10/M tokens                                    | Databricks Foundation Model API pricing                           |
| AWS EC2 r5.xlarge on-demand (us-east-1) — indicative, confirmed after performance sizing | $0.252/hour                                       | AWS EC2 pricing                                                   |
| AWS EC2 r5.xlarge spot (us-east-1) — indicative                                          | ~$0.08/hour                                       | AWS Spot pricing (varies)                                         |
| AWS RDS PostgreSQL db.t3.medium (us-east-1) — indicative for staging                     | ~$0.068/hour                                      | AWS RDS pricing                                                   |
| AWS RDS PostgreSQL db.r6g.large (us-east-1) — indicative for production                  | ~$0.192/hour                                      | AWS RDS pricing                                                   |
| AWS RDS gp3 storage                                                                      | $0.115/GB-month                                   | AWS RDS pricing                                                   |
| AWS S3 Standard (us-east-1, first 50 TB)                                                 | $0.023/GB-month                                   | AWS S3 pricing                                                    |

---

### 9.3 Staging Cost Breakdown

**Basis:** 1,000 claims/day · 30,000 claims/month · 15,000 LLM calls/month

| Component                         | Calculation                                                                                                                        | Est./Month      |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| Databricks ETL (Jobs Compute)     | 2-node r5.xlarge cluster · 30 min/day · 30 days = 15 hrs. DBU: 4 DBU/hr × $0.37 × 15 = $22. EC2 spot: 2 × $0.08 × 15 = $2.40       | ~$25            |
| Databricks Model Serving (CPU)    | 1 small CPU replica · scale-to-zero overnight (12 hrs active/day) · ~2 DBU/hr × $0.07 × 360 hrs                                    | ~$50            |
| Databricks Vector Search          | 1 Standard unit × 4 DBU/hr × $0.07/DBU × 720 hrs/month = $201.60                                                                   | ~$202           |
| Foundation Model API — LLM        | 15,000 calls × 800 input tokens = 12M tokens × $0.90/M = $10.80 input. 15,000 × 500 output = 7.5M tokens × $2.70/M = $20.25 output | ~$31            |
| Foundation Model API — Embeddings | Initial indexing only: 500 chunks × 200 tokens = 100K tokens × $0.10/M = $0.01 (one-time, negligible)                              | ~$0             |
| AWS RDS PostgreSQL (db.t3.medium) | $0.068/hr × 720 hrs = $49. Storage: 1 GB × $0.115 = $0.12                                                                          | ~$50            |
| AWS S3 (Delta Lake)               | ~1 GB total staging data × $0.023 + request costs                                                                                  | ~$5             |
| App Hosting (2× EC2 t3.small)     | FastAPI + Streamlit · 2 × ~$15/month                                                                                               | ~$30            |
| Networking + monitoring           | VPC, CloudWatch, basic WAF                                                                                                         | ~$50            |
| **Staging Total**                 |                                                                                                                                    | **~$443/month** |

---

### 9.4 Production Cost Breakdown

**Basis:** 10,000 claims/day · 300,000 claims/month · 150,000 LLM calls/month

| Component                             | Calculation                                                                                                                  | Est./Month               |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| Databricks ETL (Jobs Compute)         | 2-node r5.xlarge cluster · 60 min/day · 30 days = 30 hrs. DBU: 4 DBU/hr × $0.37 × 30 = $44. EC2 spot: 2 × $0.08 × 30 = $4.80 | ~$50                     |
| Databricks Model Serving (CPU)        | 1 CPU replica always-on · auto-scale under load. ~300K predictions/month at <150ms each                                      | ~$150–$250               |
| Databricks Vector Search              | 1 Standard unit × 4 DBU/hr × $0.07/DBU × 720 hrs/month = $201.60                                                             | ~$202                    |
| Foundation Model API — LLM            | 150,000 calls × 800 input = 120M tokens × $0.90/M = $108 input. 150,000 × 500 output = 75M × $2.70/M = $202.50 output        | ~$311                    |
| AWS RDS PostgreSQL (db.r6g.large)     | $0.192/hr × 720 hrs = $138. Storage: 5 GB × $0.115 = $0.58. Multi-AZ read replica for HA                                     | ~$175                    |
| AWS S3 (Delta Lake)                   | ~5 GB after 3 months × $0.023 + ~1M PUT requests/month × $0.005/1K = $5                                                      | ~$20                     |
| App Hosting (EC2 t3.medium × 2 + ALB) | FastAPI + Streamlit · load-balanced                                                                                          | ~$100                    |
| Networking + compliance               | AWS PrivateLink (~$50) · CloudWatch (~$40) · WAF ($25) · Secrets Manager ($5)                                                | ~$120                    |
| **Production Total**                  |                                                                                                                              | **~$1,128–$1,228/month** |

> **Largest fixed cost: Vector Search at ~$202/month regardless of claim volume.** For deployments below 500 claims/day, consider pre-computing policy explanations during ETL rather than real-time RAG to eliminate this cost.

---

### 9.5 Cost Optimization Strategies

| Strategy                                | Saving                              | Approach                                                                    |
| --------------------------------------- | ----------------------------------- | --------------------------------------------------------------------------- |
| Spot instances for ETL clusters         | ~70% on EC2 compute                 | ETL jobs tolerate interruption; Auto Loader checkpoint ensures safe restart |
| Job clusters over all-purpose clusters  | 30–40% DBU saving                   | Clusters auto-terminate after job — no idle billing                         |
| Scale-to-zero on staging model endpoint | Eliminates ~12 hrs/day serving cost | Enable in staging; keep always-on in production only                        |
| LLM explanation caching                 | Avoids repeated FM calls            | Cache output by denial-reason hash — same rule flag = same explanation      |
| Use Llama 3.3 70B over Llama 3.1 405B   | ~5× cheaper per token               | 70B is sufficient for structured billing explanations                       |
| Triggered Vector Search sync only       | Removes continuous indexing cost    | Rebuild index only when policy corpus is updated                            |

---

## 10. Process — What You Will Do

### Step 1: Initialise the SDP Pipeline Project

The ETL pipeline is built using **Lakeflow Spark Declarative Pipelines (SDP)**, not ad-hoc notebooks. Initialise the project using the Databricks CLI (`databricks pipelines init`), which creates a **Databricks Asset Bundle (DAB)** — a version-controlled, deployable project. Pipeline logic lives in plain `.sql` or `.py` files, not notebooks. One pipeline file is created per dataset (claims, providers, diagnosis, cost). Profiling remains as separate exploratory notebooks.

### Step 2: Create Unity Catalog Namespace

Create the catalog and schema in Databricks Unity Catalog before any tables are written:

- Catalog name: `healthcare`
- Schema name: `healthcare.bronze`

### Step 3: Define Bronze Ingestion Pipelines

Each dataset gets its own SDP streaming table definition using the `read_files()` function, which uses Auto Loader under the hood. Auto Loader officially supports CSV (`format => 'csv'` is an explicitly listed allowed value alongside json, parquet, avro, orc, text, and binaryFile — confirmed from Databricks Auto Loader options docs).

SDP manages checkpoint state and schema location automatically — these do not need to be configured manually. The required `read_files()` parameters are:

| Parameter           | Value         | Reason                                                                                                                              |
| ------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| format              | csv           | Declares the source file type — required, no default                                                                                |
| header              | true          | **CSV-specific — defaults to false.** All four datasets have header rows; without this, the header row is ingested as a data record |
| inferColumnTypes    | true          | Infers exact column types (integer, date, float) — use this SDP parameter, not the generic `inferSchema` option                     |
| schemaEvolutionMode | addNewColumns | Tolerates new columns in source files without failing the pipeline                                                                  |

Audit columns to add to every Bronze streaming table:

| Column             | Value                                                     | Purpose                                                                                                                                  |
| ------------------ | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `_ingested_at`     | `current_timestamp()`                                     | HIPAA: records exactly when data entered the system                                                                                      |
| `_source_file`     | `_metadata.file_path`                                     | Data lineage: which file the record came from. Use `_metadata.file_path` — `input_file_name()` is a legacy function not supported in SDP |
| `_pipeline_run_id` | Injected at runtime (mechanism decided at implementation) | Links each row to the pipeline execution that created it                                                                                 |

Create one pipeline file per dataset: claims, providers, diagnosis, cost.

### Step 4: Verify Bronze Table Properties

In SDP, TBLPROPERTIES are declared **inside** the `CREATE OR REFRESH STREAMING TABLE` definition in the pipeline file itself — they are not applied as a separate `ALTER TABLE` step after creation. This keeps configuration declarative and version-controlled within the DAB, and aligns with Section 7.3's requirement that properties be set at creation, not retroactively.

After the pipeline runs for the first time, verify the HIPAA and CDF properties from Section 7.3 are correctly applied by running `DESCRIBE EXTENDED` on each of the four Bronze tables.

### Step 5: Apply Access Control

Apply the Unity Catalog RBAC grants defined in Section 7.4 to all four Bronze tables.

### Step 6: Data Profiling

Run profiling on all four Bronze tables using PySpark (not pandas — these are Spark DataFrames). Profiling must cover:

| Check                  | What to Measure                                                 |
| ---------------------- | --------------------------------------------------------------- |
| Row count              | Total rows; must match source CSV                               |
| Null values            | Count and percentage per column                                 |
| Duplicate primary keys | Duplicate claim_id values in claims table                       |
| Summary statistics     | Min, max, average, p95 for billed_amount                        |
| Referential integrity  | All provider_id values in claims must exist in providers table  |
| Anomalous amounts      | Any billed_amount values significantly above regional benchmark |

### Questions to Answer from Profiling

- Is `claim_id` unique across all 1,000 records?
- What percentage of claims have a missing `procedure_code`?
- What percentage of claims have a missing `billed_amount`?
- Are all `provider_id` values in the claims table present in the providers table?
- What is the maximum `billed_amount`? Does it appear anomalous?
- How many providers have a missing `location`?

---

## 11. Deliverables

### Output 1: Problem Document

- What is the problem
- Who is the primary user (Billing Analyst)
- What the system will do

### Output 2: Dataset Summary Table

| Dataset   | Rows    | Key Issue Found                                    |
| --------- | ------- | -------------------------------------------------- |
| Claims    | 1,000   | Missing procedure_code, missing billed_amount      |
| Providers | TBD     | Missing location for some providers                |
| Diagnosis | 6 codes | D10–D60 (Heart, Bone, Fever, Skin, Diabetes, Cold) |
| Cost      | TBD     | Regional benchmarks available                      |

### Output 3: Bronze Delta Tables

All four tables created in Unity Catalog with correct TBLPROPERTIES applied:

- `healthcare.bronze.claims`
- `healthcare.bronze.providers`
- `healthcare.bronze.diagnosis`
- `healthcare.bronze.cost`

Each table must have: `_ingested_at`, `_source_file`, `_pipeline_run_id` audit columns; Change Data Feed enabled; 6-year log retention set.

### Output 4: Profiling Report

Document the following findings per table:

- Percentage of claims with missing `procedure_code`
- Percentage of claims with missing `billed_amount`
- Duplicate claim IDs found: yes / no
- Providers with missing location: count
- Billed amount anomalies: max value, p95 value

### Output 5: Architecture Draft

The three architecture diagrams in Section 3 serve as the architecture deliverable. Confirm Bronze tables are visible in Unity Catalog data lineage.

---

## 12. Testing & Exit Criteria

| Check                    | How to Verify                                           | Pass Criteria                                                                          |
| ------------------------ | ------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Row count match          | Compare table row count vs source CSV line count        | 100% match, no data loss                                                               |
| Idempotent re-run        | Re-run the SDP pipeline a second time on the same files | No duplicate rows — SDP checkpoint state prevents re-processing already-ingested files |
| TBLPROPERTIES set        | Run DESCRIBE EXTENDED on each Bronze table              | CDF enabled and 6-year retention visible on all 4 tables                               |
| Audit columns present    | Query each table and inspect first row                  | \_ingested_at and \_source_file are populated on all rows                              |
| RBAC applied             | Attempt a SELECT and INSERT with analyst credentials    | SELECT succeeds; INSERT is denied                                                      |
| No hardcoded credentials | Manual review of all notebooks                          | Zero plaintext credentials in any cell                                                 |
| Profiling complete       | Review profiling notebook outputs                       | All 6 profiling questions answered for all 4 tables                                    |

---

## 13. Common Mistakes

| Mistake                                                        | Why It Is Wrong                                                                                                                                                                                                              |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Writing ETL in ad-hoc notebooks instead of SDP pipeline files  | SDP manages checkpointing, schema evolution, and incremental state automatically. Notebooks require all of this to be wired manually, cannot be deployed via DAB, and are not treated as first-class version-controlled code |
| Using batch CSV read instead of SDP / read_files()             | Batch read re-processes the entire dataset on every run — not incremental, not production-grade                                                                                                                              |
| Not setting header = true for CSV                              | Auto Loader (used by read_files() under the hood) defaults header to false — the header row is ingested as a data record, corrupting row counts and column names                                                             |
| Using pandas syntax in Databricks notebooks                    | Databricks uses PySpark DataFrames — pandas methods will throw AttributeError at runtime                                                                                                                                     |
| Using a flat table name instead of the Unity Catalog namespace | Correct format is healthcare.bronze.claims (catalog.schema.table) — flat names bypass governance                                                                                                                             |
| Not setting TBLPROPERTIES after table creation                 | Change Data Feed and 6-year retention must be configured explicitly — they are not defaults                                                                                                                                  |
| Cleaning or transforming data in Bronze                        | Bronze is raw and append-only — any transformation belongs in Silver                                                                                                                                                         |
| Hardcoding storage credentials in notebooks                    | Credentials must come from Databricks Secrets — hardcoding is a HIPAA security violation                                                                                                                                     |
| Skipping profiling                                             | Without profiling, data quality issues are invisible until they break downstream pipelines                                                                                                                                   |

---

## 14. Risk Register

| Risk                                      | Likelihood | Impact | Mitigation                                                                                                                                                                                |
| ----------------------------------------- | ---------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Databricks trial expires mid-week         | Medium     | High   | Use Databricks Community Edition as fallback; start trial on Monday                                                                                                                       |
| CSV schema changes (new column added)     | Low        | Medium | cloudFiles.schemaEvolutionMode set to addNewColumns handles this automatically                                                                                                            |
| Storage credentials expire                | Low        | High   | Store in Databricks Secrets with rotation policy — applies regardless of whether storage is accessed via DBFS mount, Unity Catalog external location, or direct cloud storage             |
| SDP pipeline state corrupted              | Low        | Medium | SDP manages checkpoint state internally. Recovery is a full pipeline refresh — re-run the pipeline; source CSVs are the source of truth. No manual checkpoint folder management required. |
| Row count mismatch between CSV and Bronze | Medium     | High   | Compare table count vs source file line count; investigate before proceeding                                                                                                              |
