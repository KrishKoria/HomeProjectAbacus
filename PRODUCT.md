# Product

## Register

product

## Users

**Primary — Billing / Claims Analyst:** Medical billing professionals reviewing 50-200 claims per day. Their context is a desktop browser in a clinical office or hospital billing department. They work through a risk-prioritized queue, assess denial risk for each claim, and take remediation action in the billing system before submission. They need to identify the denial cause in under 30 seconds — without wading through policy manuals.

**Secondary — Billing Supervisor / Admin:** Manages team performance, monitors denial rate trends, manages user access, and exports audit logs for compliance. Needs queue summary metrics and admin controls, not per-claim detail.

## Product Purpose

AI-powered claim denial risk scoring and remediation platform. A pre-submission quality gate that intercepts every claim before it reaches the insurer. It scores risk, explains why in plain language, recommends precise fixes, and lets analysts ask follow-up questions — shifting denial prevention from reactive to proactive.

Success looks like: denial rate <5% (from 15–20%), rework cost dropping from $14/claim to $2/claim, and denial cause identified in <30 seconds instead of 2–5 hours.

## Core Features

**Claims Queue** — Risk-sorted list of analyzed claims. Analysts work top-to-bottom through high-risk claims. Filterable by risk level (High/Medium/Low) and status (New/Reviewed/Actioned). Searchable by claim ID.

**Claim Detail (split-panel)** — Left column: risk score, Key Findings (human-readable risk factors), Supporting Policy (RAG-matched policy excerpts), Analysis Summary (LLM narrative), Policy Sources. Right column: per-claim chat panel, always visible at ≥1024px.

**Per-claim Chat** — Analyst asks questions about a specific claim in natural language ("What's the fix for the procedure code issue?", "Show me the relevant policy rule"). Backed by Databricks Llama 70B with the claim's analysis result as context.

**Workflow States** — Claims move through New → Reviewed → Actioned. Status is changed from the claim detail page. Supervisors see queue-level status distribution.

## Brand Personality

Precise. Clinical. Trusted.

Voice is factual and direct — no hype, no healthcare marketing warmth. This is a diagnostic instrument, not a patient portal. Every screen communicates competence and reliability. The interface earns trust by being transparent about what it knows and how it knows it.

## Anti-references

- **Generic SaaS cream:** No tired B2B dashboard template. No Stripe/Linear clones with the same sidebar + card grid layout.
- **Healthcare cliché:** No white + teal, no stethoscope icons, no rounded bubbly cards that pretend healthcare is friendly.
- **Dark tool cliché:** No dark navy observability-dashboard look. This is a clinical tool, not a monitoring console.

## Design Principles

1. **Clinical clarity.** Every pixel serves a decision. If it doesn't help the analyst assess risk or find a fix, it doesn't belong. Decoration is noise.
2. **Trust through transparency.** Show the reasoning behind every risk score — which rules fired, which SHAP features contributed, which policy documents support the finding. Black-box scores erode trust.
3. **Progressive disclosure.** Surface the critical signal first (risk score + top reason), then let the analyst drill into detail (all findings, policy excerpts, analysis summary). Never overwhelm at first glance. No tabs hiding primary content.
4. **Precision over polish.** Information hierarchy, alignment, and data accuracy matter more than visual flair. A well-structured table beats an illustrated dashboard.
5. **HIPAA posture by default.** No PHI in logs. No credentials in the client. Session boundaries are hard. Chat history is session-scoped only. The interface never suggests otherwise.

## Accessibility & Inclusion

Target WCAG 2.1 AA. Color contrast ratios meet AA thresholds. Information is never conveyed by color alone. Keyboard-navigable (keyboard shortcuts: `/` for search, `C` for chat). Support reduced motion preferences.
