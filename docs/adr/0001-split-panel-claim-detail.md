# ADR 0001: Split-Panel Layout for Claim Detail Page

**Status**: Accepted
**Date**: 2026-05-12

---

## Context

The claim detail page previously used a tab structure: risk score and SHAP features in the main view, with Policy Guidance and Narrative hidden behind two separate tabs. Analysts had to click between tabs to assemble a complete picture of why a claim was flagged and what to do about it.

The product also adds a per-claim conversational chat interface. The chat panel needed a permanent home on the page — one that remains visible while the analyst reads the analysis, without requiring a separate click or mode switch.

Three layout candidates were considered:

1. **Single scroll** — remove tabs, stack all analysis content vertically, no chat panel
2. **Keep tabs, add summary strip** — persistent risk/narrative summary above tabs, tabs stay for deep-dive
3. **Split panel** — left column (analysis content), right column (chat, always visible)

## Decision

**Split panel** (option 3).

- **Left column (62% width)**: scrollable, contains all analysis content in a single vertical flow — risk score → Key Findings → Supporting Policy accordion → Analysis Summary → Policy Sources. No tabs.
- **Right column (38% width)**: sticky, full viewport height. Chat panel. Does not scroll away when the left column scrolls.
- At viewport <1024px: the right column collapses to a floating action button. The left column takes full width.

## Rationale

The single scroll (option 1) solves the tab problem but doesn't address where the chat goes. Adding the chat as a section at the bottom of a single scroll means it's invisible until the analyst scrolls past all the analysis — the opposite of the goal.

The tab approach (option 2) is an incremental improvement but preserves the fundamental problem: content is hidden by default. The summary strip is a band-aid on a structural flaw.

The split panel is the natural model for "structured data + conversation simultaneously." Linear (issue detail + activity), GitHub (PR diff + review thread), Intercom (message detail + timeline) all use this pattern for the same reason: analysts need to read evidence and ask questions at the same time, without toggling between views.

## Consequences

- Requires viewport ≥1024px for the full layout. Below this, chat collapses to a FAB — acceptable given primary users are on desktop browsers in office environments.
- The 62/38 split gives the analysis content meaningful width while keeping the chat panel usable. Narrower chat panels (<300px) make conversation threads unreadable.
- No tabs to remove; the tab component can be deprioritized in the component inventory.
- The right column being sticky means the chat input is always accessible regardless of scroll position on the left — critical for a tool where an analyst may scroll deep into policy excerpts and want to ask a follow-up question.
