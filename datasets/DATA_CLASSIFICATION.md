# HIPAA Data Classification — Development Datasets

## Synthetic Data Certification

All four datasets in this folder are **fully synthetic**. They were generated programmatically
and contain no real patient, provider, or financial information. No real individual can be
identified from any record in these files.

De-identification basis: **Safe Harbor method** per 45 CFR § 164.514(b)(2).
All 18 HIPAA identifiers are absent or replaced with non-identifying synthetic codes:
- Patient names → coded pseudonyms (P001–P999)
- Provider names → fictional Indian physician names (not real practitioners)
- Dates → randomly generated dates in 2024
- Amounts → randomly generated dollar figures
- Geographic data → Indian city names (provider business locations, not patient addresses)
- No SSN, MRN, health plan ID, phone, email, IP, biometric, or photo data exists

**Production note:** A signed Databricks Business Associate Agreement (BAA) with the
Covered Entity is required before ANY real PHI is ingested into this system.
See 45 CFR § 164.504(e) and § 164.314(a) for BAA requirements.

---

## Column-Level PHI Classification

### claims_1000.csv — Primary Dataset (1,000 records)

| Column | PHI? | Classifier | CFR Basis | Production Handling |
|---|---|---|---|---|
| `claim_id` | PHI-adjacent | Unique claim identifier | § 164.514(b)(2)(xviii) — unique code | Access-controlled; not encrypted |
| `patient_id` | **PHI** | Unique patient identifier | § 164.514(b)(2)(xviii) — unique identifying code | AES-256 at rest; column masking |
| `provider_id` | Operational | Provider foreign key | Not PHI — provider identity, not patient | Standard access control |
| `diagnosis_code` | **PHI** | Medical condition of patient | § 164.514(b)(2) — health condition linked to patient_id | AES-256 at rest; column masking |
| `procedure_code` | PHI-adjacent | Treatment type per patient | Linked to patient health event | Access-controlled |
| `billed_amount` | **PHI** | Financial health information | § 164.514(b)(2) — individually identifiable financial health data | AES-256 at rest; column masking |
| `date` | **PHI** | Date of health service | § 164.514(b)(2)(iv) — dates directly related to an individual (except year) | AES-256 at rest; column masking |
| `claim_status` | **PHI** | Synthetic adjudication outcome | Claim outcome linked to patient_id | AES-256 at rest; column masking |
| `denial_reason_code` | **PHI** | Synthetic denial reason | Claim denial rationale linked to patient_id | AES-256 at rest; column masking |
| `allowed_amount` | **PHI** | Synthetic allowed amount | Individually identifiable payment data | AES-256 at rest; column masking |
| `paid_amount` | **PHI** | Synthetic paid amount | Individually identifiable payment data | AES-256 at rest; column masking |
| `is_denied` | **PHI** | Synthetic ML label | Claim outcome linked to patient_id | AES-256 at rest; column masking |
| `follow_up_required` | **PHI** | Synthetic workflow label | Claim action state linked to patient_id | AES-256 at rest; column masking |

> **Synthetic label note:** `claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`,
> `is_denied`, and `follow_up_required` are generated demo labels. They are suitable for
> pipeline and ML workflow testing, not for real-world model validation.

> **Note on `date`:** HIPAA § 164.514(b)(2)(iv) explicitly lists "all elements of dates (except year)
> for dates directly related to an individual, including admission date, discharge date" as PHI
> identifiers. The claim submission date is a date directly related to a patient's health event.

### providers_1000.csv — Provider Reference (21 records)

| Column | PHI? | Classifier | CFR Basis | Production Handling |
|---|---|---|---|---|
| `provider_id` | Operational | Provider primary key | Not PHI — provider business identity | Standard access control |
| `doctor_name` | Operational | Provider name | NOT PHI — § 164.501 defines PHI as health info of **patients**; provider identity is business operational data | Standard access control |
| `specialty` | Operational | Medical specialty | Provider credential, not patient health information | Standard access control |
| `location` | Operational | Provider business location | NOT PHI — provider city address, not patient geographic data | Standard access control |

> **HIPAA clarity:** The Privacy Rule (§ 164.501) protects "individually identifiable health
> information" of **patients** receiving care, not the identities or credentials of healthcare
> providers. Provider names and business addresses are operational data, not PHI.

### diagnosis.csv — Reference Lookup (6 records)

| Column | PHI? | Classifier | CFR Basis | Production Handling |
|---|---|---|---|---|
| `diagnosis_code` | **Not PHI** | Medical terminology code | Standalone code (D10, D20) without patient linkage = medical terminology, NOT PHI per § 164.501. Becomes PHI only when combined with patient_id in claims. | Standard access control |
| `category` | Not PHI | Condition category | Medical terminology | Standard access control |
| `severity` | Not PHI | Risk classification | Medical terminology | Standard access control |

> **PHI distinction:** A diagnosis code in a lookup/reference table (e.g. D10 = Heart, High)
> is medical terminology — it cannot identify any individual. It becomes PHI only when it
> appears alongside a patient identifier (as in `claims_1000.csv`). This distinction is
> grounded in the § 164.501 definition: PHI must be "individually identifiable."

### cost.csv — Cost Benchmark Reference (6 records)

| Column | PHI? | Classifier | CFR Basis | Production Handling |
|---|---|---|---|---|
| `procedure_code` | Not PHI | Procedure reference key | Benchmark/reference data, no patient linkage | Standard access control |
| `average_cost` | Not PHI | Historical cost benchmark | Operational reference data | Standard access control |
| `expected_cost` | Not PHI | Regional cost benchmark | Operational reference data | Standard access control |
| `region` | Not PHI | Geographic region | Provider/operational region, not patient address | Standard access control |

---

## Production PHI Handling Requirements

When this system processes **real** (non-synthetic) healthcare data, the following controls
are **mandatory** before any PHI is ingested:

### Legal Prerequisites
1. **Signed BAA** — Execute a Business Associate Agreement with the Covered Entity per
   45 CFR § 164.504(e) and § 164.314(a)(1). Databricks also requires a signed BAA
   (Databricks is a subcontractor BA under § 164.314(a)(2)(ii)).
2. **Assigned Security Officer** — Designate a HIPAA Security Officer per § 164.308(a)(2).

### Technical Controls
3. **Column-level encryption** — Encrypt PHI columns (`patient_id`, `billed_amount`,
   `diagnosis_code`, `date`, adjudication labels, and payment amounts) at rest using AES-256 via Databricks column masking
   (§ 164.312(a)(2)(iv)).
4. **Unity Catalog RBAC** — Apply least-privilege grants before pipeline first run
   (§ 164.312(a)(1)). See `src/notebooks/bronze_verify_and_rbac.ipynb`.
5. **Unity Catalog Audit Logging** — Enable `system.access.audit` at the Databricks
   workspace level to capture WHO accessed ePHI (§ 164.312(b)).
6. **Transmission encryption** — All data in transit between landing zone and Databricks
   must use TLS 1.2+ (§ 164.312(e)(2)(ii)).

### Breach Notification
7. **60-day CE notification** — Any breach of unsecured PHI must be reported to the
   Covered Entity without unreasonable delay and no later than 60 days after discovery
   per § 164.410(b).

### Termination
8. **Return or destroy PHI** — Upon contract termination, all PHI received from or
   created on behalf of the CE must be returned or securely destroyed per
   § 164.504(e)(2)(ii)(J). Delta Lake tables can be dropped; S3/ADLS volumes must
   be wiped with a data destruction runbook.

---

*Last reviewed: 2026-04-19 | Reviewer: Abacus Insights Engineering*
*CFR citations reference 45 CFR Part 164 (HIPAA Security and Privacy Rules)*
