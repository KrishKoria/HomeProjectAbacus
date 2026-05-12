# Shape Brief: ClaimOps UI Redesign

**Date**: 2026-05-12
**Status**: Confirmed — ready for implementation
**Discovery session**: 7-question grill-with-docs interview

---

## 1. Feature Summary

Full product UI redesign for ClaimOps — an AI-powered claim denial risk tool used by billing analysts reviewing 50–200 claims daily on desktop browsers in clinical office environments. The redesign transforms two sparse prototype screens into a complete Linear-style corporate product: a risk-prioritized claims queue with lightweight workflow tracking, and a split-panel claim detail page with human-readable analysis and a per-claim chat interface.

---

## 2. Primary User Action

**On the queue**: Identify the highest-risk claims that need attention today and mark them worked.
**On the detail page**: Understand *why* a claim is high-risk in under 30 seconds and ask follow-up questions to find the fix.

---

## 3. Design Direction

**Color strategy**: Restrained — tinted neutrals + slate-blue primary + semantic risk colors. Existing OKLCH token system is complete and correct. No new decorative color.

**Theme scene sentence**: A billing analyst at a hospital billing department, sitting at a 24-inch monitor in a fluorescent-lit open office at 9am, working through today's denial risk queue before claims go out at noon. The environment is bright, clinical, purposeful.

**Theme**: Light mode. The existing light theme is correct.

**Named references**:
- Linear — issue list density, keyboard shortcuts, status badges, split-panel detail
- Retool — data table as primary interface, inline filters, row-level actions
- Stripe Dashboard — information hierarchy in detail pages, monospace data values

**Font**: Geist Sans + Geist Mono. Already implemented.

---

## 4. Scope

- Fidelity: high-fi, production-ready
- Breadth: all primary surfaces (queue, claim detail, shell/nav)
- Interactivity: shipped-quality components
- Time intent: polish until it ships

---

## 5. Layout Strategy

### App Shell
- Sidebar: "Claims" (primary, links to queue) + "Dashboard" (secondary, summary metrics). User avatar + name in footer.
- Header strip: breadcrumb `Dashboard / CLM-00291` on detail pages.

### Claims Queue
- Full-width table, no max-width
- Columns: Risk Level badge, Claim ID (mono), Risk Score (% + inline progress bar), Status, Date
- Controls bar: search (left) + risk filter pills (High/Medium/Low/All) + status filter + sort
- Sort: risk score descending by default

### Claim Detail — Split Panel
- Left (62%): scrollable. Header → Risk Score → Key Findings → Supporting Policy accordion → Analysis Summary → Policy Sources
- Right (38%): sticky. Chat panel — always visible at ≥1024px
- <1024px: chat collapses to floating action button (bottom-right)
- No tabs. Single scroll with progressive disclosure.

---

## 6. Key States

### Queue
| State | Display |
|---|---|
| Loading | 5 skeleton rows |
| Loaded | Risk-sorted table |
| Empty | "No claims match the current filters." + CTA to search by claim ID |
| Error | Inline error strip + retry button |
| Filter active | Active pill highlighted, table narrows in real-time |

### Claim Detail
| State | Display |
|---|---|
| Loading | Left: skeleton blocks. Right: "Analyzing claim…" |
| Loaded | Full split view |
| Error | Full-width error strip with retry. Chat: "Analysis unavailable." |
| Status change | Badge updates inline, sonner toast confirmation |
| Chat waiting | Typing indicator in thread |
| Chat first message | Pre-seeded: "This claim scored [score]% — [Level] denial risk. Ask me anything about it." |

---

## 7. Interaction Model

### Queue
- `/` focuses search (already implemented)
- Click row → navigate to detail
- Filter pills toggle instantly, no submit
- Column headers are sortable

### Claim Detail
- Back button → queue (with scroll position preserved)
- Status: inline `Select` dropdown. Three values: New, Reviewed, Actioned.
- Risk score: animated counter on load (already implemented)
- Key Findings: `description` as primary text + direction tag ("Raises denial risk" / "Lowers denial risk") + contextually formatted value. No raw SHAP floats.
- Policy accordion: expand/collapse, smooth height transition
- Chat: `Enter` submits, `Shift+Enter` newlines. `C` shortcut focuses chat when no input active. Session-scoped only.

---

## 8. Content Requirements

| Element | Copy |
|---|---|
| Risk level | "High" / "Medium" / "Low" |
| Risk score | "87%" not "0.87" |
| Status | "New" / "Reviewed" / "Actioned" |
| Direction tags | "Raises denial risk" / "Lowers denial risk" |
| Policy section | "Supporting Policy" |
| Narrative section | "Analysis Summary" |
| Status toast | "Marked as Reviewed" |
| Empty queue | "No claims match the current filters." |
| Chat placeholder | "Ask about this claim…" |
| Chat pre-seed | "This claim scored [score]% — [Level] denial risk. Ask me anything about it." |

---

## 9. Resolved Architecture Decisions

### Chat endpoint
New route: `POST /api/claims/[claimId]/chat`
- Accepts `{ messages: Array<{role: "user"|"assistant", content: string}> }`
- Injects cached `ClaimAnalysisResponse` as system context
- Calls Databricks Foundation Models API (`databricks-meta-llama-3-3-70b-instruct`) via `databricksFetch`
- Streamed response
- New env var: `CLAIMOPS_CHAT_MODEL` (default: `databricks-meta-llama-3-3-70b-instruct`)

### Queue data + status persistence
New table `claim_reviews` in Neon PostgreSQL (Drizzle). See ADR 0002.
- `POST /api/claims/analyze` upserts to `claim_reviews` after successful analysis
- `GET /api/claims` queries `claim_reviews`, sorted by `riskScore` DESC
- `PATCH /api/claims/[claimId]/status` updates `status`, `reviewedAt`, `reviewedById`

### Dashboard
Keep as secondary home screen: runtime status dots + count by risk level + count by status (all from `claim_reviews`). Not a blocker.

---

## 10. Open Questions for Implementation

None blocking. Items to confirm during build:
- Streaming response handling: use `ReadableStream` + `EventSource` or `fetch` with `text/event-stream`
- Queue pagination: lazy-load or paginate when `claim_reviews` exceeds 100 rows
- Chat model env var: confirm `databricks-meta-llama-3-3-70b-instruct` is available in the target workspace
