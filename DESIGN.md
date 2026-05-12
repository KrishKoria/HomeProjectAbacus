---
name: ClaimOps
description: AI-powered claim denial risk scoring and remediation
---

# Design System: ClaimOps

## 1. Overview

**Creative North Star: "The Diagnostic Console"**

A clinical instrument, not a healthcare portal. Every screen serves one purpose: helping the analyst assess risk and find the fix. The interface is typography-forward, structurally transparent, and calibrated for sustained focus over an 8-hour shift.

The palette carries 3–4 deliberate color roles — not because the UI is colorful, but because each color carries specific meaning (risk level, feature contribution direction, policy relevance). No color is decorative.

Rejects: the overcrowded enterprise EHR look (Cerner, Epic) with dense tables and no breathing room; the generic SaaS dashboard template with sidebar + card grid + blue accent; the healthcare cliché of white + teal + rounded friendliness.

**Aesthetic references**: Linear (issue list density, keyboard shortcuts, split-panel detail), Retool (data table as primary UI), Stripe Dashboard (metadata hierarchy in detail pages).

**Key Characteristics:**
- Typography-forward. Information hierarchy is carried by type weight and scale, not containers.
- Generous whitespace. Density only where the data demands it (feature tables, claim lists).
- Color is semantic, not decorative.
- Flat surfaces. No decorative shadows.
- Light mode primary — analysts work in daylit clinical offices.
- Restrained motion — only to clarify state transitions and focus.

---

## 2. Colors

Strategy: **Restrained** — tinted neutrals + one slate-blue primary + semantic risk colors. No decorative color. All values use OKLCH. Implemented as CSS custom properties in `frontend/src/app/globals.css`.

### Background & Surface

| Token | Value | Use |
|---|---|---|
| `--background` | `oklch(0.985 0.006 260)` | Page background — cool off-white, slate-blue tint |
| `--card` | `oklch(0.985 0.006 260)` | Panel surface — flat, same as background |
| `--muted` | `oklch(0.95 0.01 260)` | Subtle surface, hover states |
| `--sidebar` | `oklch(0.965 0.006 260)` | Sidebar — slightly darker than page |

### Text

| Token | Value | Use |
|---|---|---|
| `--foreground` | `oklch(0.15 0.02 260)` | Primary text — near-black, slate-blue tint |
| `--muted-foreground` | `oklch(0.55 0.03 260)` | Secondary/supporting text |
| `--accent-foreground` | `oklch(0.25 0.03 260)` | Text on accent surfaces |

### Primary (Slate Blue)

| Token | Value | Use |
|---|---|---|
| `--primary` | `oklch(0.5 0.12 260)` | Primary actions, active nav, semantic emphasis |
| `--primary-foreground` | `oklch(0.985 0.005 260)` | Text on primary backgrounds |
| `--ring` | `oklch(0.5 0.12 260)` | Focus rings |

### Borders & Inputs

| Token | Value | Use |
|---|---|---|
| `--border` | `oklch(0.88 0.015 260)` | Dividers, table borders, section outlines |
| `--input` | `oklch(0.88 0.015 260)` | Input field borders |

### Semantic Risk Colors

Three risk levels, each with foreground + background token pair. Use only for risk-level signaling.

| Level | Foreground | Background |
|---|---|---|
| High | `oklch(0.5 0.2 22)` — `--risk-high` | `oklch(0.92 0.05 22)` — `--risk-high-bg` |
| Medium | `oklch(0.6 0.18 85)` — `--risk-medium` | `oklch(0.93 0.06 85)` — `--risk-medium-bg` |
| Low | `oklch(0.55 0.14 160)` — `--risk-low` | `oklch(0.92 0.04 160)` — `--risk-low-bg` |

### Contribution Direction Colors

| Direction | Foreground | Background |
|---|---|---|
| Increases risk | `oklch(0.5 0.14 22)` — `--direction-up` | `oklch(0.92 0.04 22)` — `--direction-up-bg` |
| Decreases risk | `oklch(0.5 0.1 160)` — `--direction-down` | `oklch(0.92 0.03 160)` — `--direction-down-bg` |

### Status Indicators

| Status | Token | Value |
|---|---|---|
| OK / connected | `--status-ok` | `oklch(0.55 0.14 160)` |
| Error / failed | `--status-err` | `oklch(0.5 0.2 22)` |
| Warning / degraded | `--status-warn` | `oklch(0.6 0.18 85)` |

### Named Rules

**The Semantic Color Rule.** Every color on screen must answer "what does this mean?" If a color is decorative, remove it.

---

## 3. Typography

**Sans font**: Geist Sans (`var(--font-geist-sans)`) — loaded via `next/font/google`. Sharp, technical grotesk. Works at small sizes in dense data tables, scales confidently for headings.

**Mono font**: Geist Mono (`var(--font-geist-mono)`) — claim IDs, numeric values, data fields only.

> **Note**: DESIGN.md previously specified ABC Favorit (Dinamo Typefoundry). Geist Sans was selected as the production font — equivalent technical grotesk character, free, already wired into the project. No font swap is planned.

### Type Scale

All utilities are defined in `frontend/src/app/globals.css` as `.type-*` classes.

| Role | Class | Size | Line-height | Weight | Notes |
|---|---|---|---|---|---|
| Display | `.type-display` | `3rem` | `1.1` | `500` | -0.02em spacing. Risk score number only. |
| Headline | `.type-headline` | `1.5rem` | `1.2` | `600` | -0.01em spacing. Page titles. |
| Title | `.type-title` | `1.125rem` | `1.3` | `600` | -0.005em spacing. Section headings. |
| Body | `.type-body` | `0.875rem` | `1.6` | `400` | max-width: 70ch. Narrative text. |
| Label | `.type-label` | `0.75rem` | — | `500` | +0.06em spacing, uppercase. Table headers, metric labels. |
| Caption | `.type-caption` | `0.6875rem` | `1.4` | `400` | Timestamps, footnotes. |
| Mono | `.type-mono` | `0.8125rem` | `1.5` | — | `font-feature-settings: "tnum"`. Claim IDs, values. |

### Named Rules

**The One-Face Rule.** All text uses Geist Sans. Hierarchy is achieved through weight, size, and case — not font switching. Geist Mono is the single exception: identifiers and numeric data only.

---

## 4. Elevation & Radius

**Radius**: `--radius: 0` — all corners sharp/square. No rounded cards.

**Elevation**: Flat by default. No shadows at rest. Tonal layering conveys depth:
- Sidebar (`oklch(0.965 …)`) slightly darker than page background (`oklch(0.985 …)`)
- Section panels: `border border-border` for separation — no shadow
- Focused/interactive: `ring` at `--ring` color

**The Flat-By-Default Rule.** Shadows are a response to interaction state, never a structural default.

---

## 5. Layout Patterns

### App Shell

- Left: collapsible sidebar (shadcn `Sidebar`)
- Right: `SidebarInset` with `h-12` header (trigger + breadcrumb) + scrollable `main`
- Main max-width: `max-w-6xl mx-auto` for most surfaces. Queue: full available width.

### Claims Queue

- Full-width data table, no max-width constraint
- Controls bar: search input (left) + risk filter pills + status filter + sort
- Columns: risk indicator, Claim ID (mono), Risk Score (% + inline bar), Status, Date, Provider
- Sort: risk score descending by default
- Row click → claim detail

### Claim Detail — Split Panel

Two columns at viewport ≥1024px:

- **Left (62%)**: scrollable. Page header (claim ID + status control) → Risk Score section → Key Findings → Supporting Policy accordion → Analysis Summary → Policy Sources
- **Right (38%)**: sticky, full viewport height. Chat panel — never scrolls away.
- At <1024px: right column chat collapses to a floating action button (bottom-right corner)
- No tabs. Single scroll with progressive disclosure.

### Chat Panel (right column)

- Fixed height = viewport minus header
- Pre-seeded opener: "This claim scored [score]% — [Level] denial risk. Ask me anything about it."
- `Enter` submits, `Shift+Enter` newlines
- Keyboard shortcut: `C` focuses chat when no input is active
- Session-scoped — history does not persist across page reloads

---

## 6. Components

Shadcn/ui components in `frontend/src/components/ui/`. App components in `frontend/src/components/`.

### Shadcn Components

| Component | Use |
|---|---|
| `Badge` | Risk level (High/Medium/Low), status (New/Reviewed/Actioned) |
| `Button` | Primary actions, ghost back buttons, icon buttons |
| `Progress` | Inline risk score bar — queue rows and detail header |
| `Accordion` | Supporting Policy section |
| `Table` | Claims queue, Key Findings list |
| `Sidebar` | App navigation shell |
| `Skeleton` | Loading states |
| `Input` | Search, chat input |
| `Select` | Status dropdown on claim detail |
| `Collapsible` | Runtime status panel on dashboard |
| `Sonner` | Toast confirmations (status change) |
| `Sheet` | Mobile sidebar |

### App Components

| Component | File | Use |
|---|---|---|
| `AppShell` | `components/app-shell.tsx` | Layout: sidebar + inset |
| `AppSidebar` | `components/app-sidebar.tsx` | Nav: Claims (primary), Dashboard (secondary) |
| `RiskScoreCell` | `components/risk-score-cell.tsx` | Risk score in queue table rows |
| `ErrorBoundary` | `components/error-boundary.tsx` | React error boundary |
| `SkipToContent` | `components/skip-to-content.tsx` | Accessibility skip link |

---

## 7. Copy Conventions

| Context | Use | Avoid |
|---|---|---|
| Risk level | "High" / "Medium" / "Low" | "HIGH", "high" |
| Risk score | "87%" | "0.87" |
| Status | "New" / "Reviewed" / "Actioned" | Other values |
| Direction tags | "Raises denial risk" / "Lowers denial risk" | "+" / "−" |
| Policy section | "Supporting Policy" | "Policy Guidance" |
| Narrative section | "Analysis Summary" | "Narrative" |
| Status toast | "Marked as Reviewed" | "Status updated successfully" |
| Empty queue | "No claims match the current filters." | "No data found" |
| Chat placeholder | "Ask about this claim…" | — |

---

## 8. Do's and Don'ts

### Do
- Lead with the risk score — primary signal on every claim screen
- Use `description` field text for risk factors — never raw feature names or SHAP floats
- Keep the chat panel always visible at ≥1024px — it is a primary surface
- Use type weight and scale for hierarchy — containers are secondary
- Keep color semantic and sparse — if it doesn't signal meaning, remove it

### Don't
- Don't recreate Cerner/Epic EHR density
- Don't use sidebar + identical card grid + blue accent (generic SaaS)
- Don't use white + teal or any healthcare-cliché palette
- Don't use side-stripe borders, gradient text, or glassmorphism
- Don't hide primary content behind tabs
- Don't show raw SHAP floats (e.g. `0.2341`) to analysts
- Don't use rounded corners — `--radius: 0` is intentional
- Don't animate layout properties
