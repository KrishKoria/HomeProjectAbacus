---
name: ClaimOps
description: AI-powered claim denial risk scoring and remediation
---

<!-- SEED — re-run $impeccable document once there's code to capture the actual tokens and components. -->

# Design System: ClaimOps

## 1. Overview

**Creative North Star: "The Diagnostic Console"**

A clinical instrument, not a healthcare portal. Every screen serves one purpose: helping the analyst assess risk and find the fix. The interface is typography-forward, structurally transparent, and calibrated for sustained focus over an 8-hour shift.

The palette carries 3-4 deliberate color roles — not because the UI is colorful, but because each color carries specific meaning (risk level, feature contribution direction, policy relevance). No color is decorative.

Rejects: the overcrowded enterprise EHR look (Cerner, Epic) with dense tables and no breathing room; the generic SaaS dashboard template with sidebar + card grid + blue accent; and the healthcare cliché of white + teal + rounded friendliness.

**Key Characteristics:**
- Typography-forward. Information hierarchy is carried by type weight and scale, not containers.
- Generous whitespace. Density only where the data demands it (feature tables, claim lists).
- Color is semantic, not decorative.
- Flat surfaces with subtle elevation on interaction.
- Restrained motion — only to clarify state transitions and focus.

## 2. Colors

Full palette strategy: 3-4 named color roles, each used deliberately and sparingly. No color appears without a semantic reason.

The anchor hue is a cool slate-blue — clinical without being cold, authoritative without being the teal healthcare default.

### Primary
- **Slate Blue** (`[to be resolved during implementation]`): Primary actions, active navigation, semantic emphasis.

### Secondary (optional — to be resolved)
- `[to be resolved during implementation]`

### Tertiary (optional — to be resolved)
- `[to be resolved during implementation]`

### Neutral
- **Cool off-white** (`[to be resolved]`): Page background. Tinted toward the slate-blue hue at minimal chroma.
- **Cool mid-gray** (`[to be resolved]`): Secondary text, borders, dividers.
- **Near-black** (`[to be resolved]`): Primary text. Never pure #000.

### Semantic
- **Risk colors** (`[to be resolved]`): A restrained palette for risk level indicators — distinct from accent colors. One hue per level (low/medium/high), used only in score badges and risk indicators.

### Named Rules
**The Semantic Color Rule.** Every color on screen must answer "what does this mean?" If a color is decorative, remove it.

## 3. Typography

**Body Font:** ABC Favorit — a sharp, technical grotesk by Dinamo Typefoundry. Commercial license required; self-host woff2 files in `frontend/public/fonts/`.

**Character:** Technical and efficient. No serif warmth, no rounded humanism. This typeface works at small sizes for dense data displays and scales confidently for headings.

### Hierarchy
- **Headline** (`[weight, size, line-height to be resolved]`): Page and section titles.
- **Title** (`[to be resolved]`): Card and panel headings.
- **Body** (`[to be resolved]`): Primary reading text. Capped at 70ch max width.
- **Label** (`[to be resolved]`, uppercase at small sizes): Form labels, metric labels, chip text.
- **Mono** (`[to be resolved]`): Claim IDs, feature values, code-adjacent data. A distinct monospace face.

### Named Rules
**The One-Face Rule.** All text uses the grotesk sans. Hierarchy is achieved through weight, size, and case — not font switching.

## 4. Elevation

Flat by default. No shadows at rest. Subtle elevation appears only on interaction — hovered buttons, focused inputs, active dropdowns. Depth is conveyed through tonal layering (light surface on slightly darker surface) rather than drop shadows.

`[Elevation values to be resolved during implementation]`

### Named Rules
**The Flat-By-Default Rule.** Surfaces are flat at rest. Shadows are a response to state, never a default.

## 5. Components

`[No components exist yet. Populated on first $impeccable document scan pass.]`

## 6. Do's and Don'ts

### Do:
- **Do** lead with the risk score. It is the primary signal on every claim screen.
- **Do** use generous whitespace around decision points (claim detail, analysis results).
- **Do** use type weight and scale to establish hierarchy — containers are secondary.
- **Do** keep color usage semantic and sparse. If a color doesn't signal meaning, it's noise.

### Don't:
- **Don't** recreate the Cerner/Epic EHR look — overcrowded tables, no breathing room, too many competing colors.
- **Don't** use the generic SaaS dashboard template: sidebar + identical card grid + blue accent.
- **Don't** use white + teal or any healthcare cliché palette.
- **Don't** put color on anything that doesn't carry meaning. Decorative color is prohibited.
- **Don't** use side-stripe borders, gradient text, or glassmorphism.
- **Don't** use cards as the default layout container. Explore lists, tables, and inline groupings first.
- **Don't** animate layout properties. Motion is for state transitions only.
