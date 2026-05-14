# ADR 0002: claim_reviews Table in Neon PostgreSQL

**Status**: Superseded by ADR 0003
**Date**: 2026-05-12

---

## Context

The claims queue requires risk scores, risk levels, and workflow status (New/Reviewed/Actioned) for each claim. The existing Databricks feature table (`healthcare.gold.claim_features`) contains only `claim_id` and the 22 numeric ML input features — no pre-computed risk scores, no dates, no provider names, no workflow state.

Two data needs must be resolved:

1. **Queue data** — display `riskScore`, `riskLevel`, `analyzedAt`, and basic metadata for each claim in the queue
2. **Status persistence** — store and update workflow status (New/Reviewed/Actioned) per claim, per analyst

Three approaches were considered:

1. **On-demand scoring at queue load** — run the ML model for every claim in the table when the queue loads
2. **Separate Databricks table** — create a `healthcare.ml.claim_scores` table in Databricks storing pre-computed scores and sync it to the queue
3. **Neon PostgreSQL via Drizzle** — add a `claim_reviews` table to the existing frontend database

## Decision

**Neon PostgreSQL via Drizzle** (option 3).

New table `claim_reviews` in `frontend/src/lib/db/schema.ts`:

```ts
export const claimReviews = pgTable("claim_reviews", {
  id: text("id").primaryKey(),
  claimId: text("claim_id").notNull().unique(),
  riskScore: real("risk_score").notNull(),
  riskLevel: text("risk_level").notNull(),       // "low" | "medium" | "high"
  narrative: text("narrative").notNull(),
  status: text("status").notNull().default("new"), // "new" | "reviewed" | "actioned"
  analyzedAt: timestamp("analyzed_at").notNull(),
  reviewedAt: timestamp("reviewed_at"),
  reviewedById: text("reviewed_by_id").references(() => user.id),
});
```

Populated by `POST /api/claims/analyze` after a successful analysis (upsert on `claimId`). Queue reads from this table. Status updated via `PATCH /api/claims/[claimId]/status`.

## Rationale

On-demand scoring (option 1) is impractical: each analysis takes several seconds (SQL warehouse wake-up + ML serving endpoint invocation + LLM call). Running it for 50–200 claims on every queue load is not viable.

A separate Databricks table (option 2) adds operational complexity: a new Databricks job or pipeline to populate scores, Unity Catalog permissions, and a read path from the frontend to Databricks SQL for the queue. It also doesn't solve status persistence without additional infrastructure.

The Neon database already exists and is already used for auth (`user`, `session` tables). Drizzle is already wired in. Adding `claim_reviews` here costs one migration and two new API routes. Status persistence is a natural fit for the application database — it is analyst workflow state, not a data engineering concern.

## Consequences

- The queue only contains claims that have been analyzed at least once. New claims from the Databricks feature table are not visible in the queue until an analyst runs an analysis on them. This is acceptable: the entry point remains the "analyze by claim ID" flow; the queue surfaces already-worked claims.
- `narrative` is cached in the DB to avoid re-running the LLM for queue display. If the underlying model changes and narratives need to be refreshed, a re-analysis of each claim is required.
- `riskLevel` is stored as text, not an enum, to avoid a migration if a fourth level is ever added.
- Status is per-claim globally (not per-analyst-per-claim). Two analysts reviewing the same claim see the same status. This is intentional: status represents the claim's lifecycle, not individual analyst tracking.
