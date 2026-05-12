# Domain Context: ClaimOps

Resolved terminology for the ClaimOps frontend. Use these exact terms in code, copy, and documentation. When a term here conflicts with a variable name or label in the codebase, prefer the canonical term below.

---

## Canonical Terms

### Analysis
The full result of running a claim through the ML model + RAG pipeline. Produces: `riskScore`, `riskLevel`, `topReasons` (SHAP), `policyGuidance` (RAG excerpts), `narrative` (LLM text), `policyCitations`. Triggered manually by navigating to a claim detail URL. Results are cached in the `claim_reviews` table after first run.

### Analysis Summary
The LLM-generated plain-language explanation shown on the claim detail page. UI label: **"Analysis Summary"**. Internal field name: `narrative`. Do not use "Narrative" in user-facing copy.

### Claim Review
A persisted record in the Neon PostgreSQL `claim_reviews` table that captures the outcome of one Analysis for one claim. Contains: `claimId`, `riskScore`, `riskLevel`, `narrative`, `status`, `analyzedAt`, `reviewedAt`, `reviewedById`. Created when an analyst runs an Analysis for the first time. Updated when status changes.

### Key Findings
The human-readable list of claim risk factors on the claim detail page. Derived from `topReasons` (SHAP output), using the `description` field as the primary text. Raw SHAP importance floats are **never** shown to analysts. UI section heading: **"Key Findings"**.

### Per-claim Chat
A conversational interface on the right panel of the claim detail page. Analyst asks questions about a specific claim in natural language. The system answers using the Analysis result as context. Backed by Databricks Foundation Models API (`databricks-meta-llama-3-3-70b-instruct`). Session-scoped only — history does not persist across page reloads. API route: `POST /api/claims/[claimId]/chat`.

### Queue
The risk-sorted list of analyzed claims. ClaimOps IS the queue — it is not a supplement to an external billing system. Primary landing surface for analysts. Populated from the `claim_reviews` table, sorted by `riskScore` descending. Only claims that have been analyzed at least once appear in the queue.

### Risk Level
Three-value semantic classification: **High**, **Medium**, **Low**. Derived from the ML model's risk score. Display format: sentence case ("High", not "HIGH"). The primary sorting and filtering signal in the Queue.

### Risk Score
A 0–100% value representing denial probability. Display format: **"87%"** — not "0.87". Computed by the ML model. Stored as a decimal (0.0–1.0) in the database; formatted as a percentage in all UI surfaces.

### Status
Lifecycle state of a Claim Review. Three values:
- **New** — claim has been analyzed, analyst has not yet reviewed it
- **Reviewed** — analyst has seen the Analysis result
- **Actioned** — analyst has taken remediation steps in the billing system

Status is changed from the claim detail page only. The queue table shows status but does not allow inline status changes.

### Supporting Policy
The policy documents matched by the RAG retriever for a given claim. Shown as an expandable accordion below Key Findings on the claim detail page. UI section heading: **"Supporting Policy"**. Internal field: `policyGuidance`. Do not use "Policy Guidance" in user-facing copy.

---

## Prohibited Terms (use canonical instead)

| Avoid | Use instead |
|---|---|
| Narrative | Analysis Summary |
| Policy Guidance | Supporting Policy |
| Policy Citations | Policy Sources (when showing file names only) |
| importance: 0.2341 | (never show raw SHAP floats) |
| Feature name (e.g. `proc_code_match`) | description field value |
| 0.87 | 87% |
| HIGH / MEDIUM / LOW | High / Medium / Low |

---

## Boundaries

**ClaimOps does not:**
- Submit claims to insurers (read-only pre-submission gate)
- Replace the billing system (Epic, Meditech) — analysts action remediation there
- Store PHI in logs, client state, or the chat history
- Persist chat history across page reloads

**ClaimOps does:**
- Score denial risk before submission
- Explain risk in plain language
- Show supporting policy evidence
- Track analyst workflow state (New/Reviewed/Actioned)
- Allow analysts to ask questions about individual claims
