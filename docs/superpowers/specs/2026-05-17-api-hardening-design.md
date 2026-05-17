# API Hardening — Design Spec

Date: 2026-05-17  
Branch: bug-fixes

## Overview

Four targeted fixes across auth, the runtime status route, chat input validation, and the Cloud Build deployment config. All independent; no shared state.

---

## Fix 1 — `/api/runtime/status`: auth, side-effect, OAuth check

### Problem

1. Route uses `requireSession()` instead of `requireAuthorizedSession()` — any logged-in user (even unauthorized domain) can call it.
2. GET handler starts the SQL warehouse when it is stopped (`POST .../start` inside a GET) — infra mutation from a diagnostic endpoint.
3. OAuth health check calls `databricksFetch("/oidc/v1/token")` — `databricksFetch` internally calls `getToken()`, so this is a nested redundant OAuth call. The token endpoint being called with a Bearer token is also semantically wrong.

### Solution

**GET `/api/runtime/status`**
- Switch to `requireAuthorizedSession()`.
- Remove warehouse-start call entirely.
- Replace OAuth check with a direct `try/catch` around `getToken()` from `@/lib/databricks/oauth`.

**New POST `/api/runtime/start-warehouse`**
- `requireAuthorizedSession()`.
- Calls `POST /api/2.0/sql/warehouses/${DATABRICKS_SQL_WAREHOUSE_ID}/start` via `databricksFetch`.
- Returns `{ started: true }` on success, 502 on Databricks error.

**Client update (`[claimId]/page.tsx`)**
- When `runtimeStatusQuery` resolves with `sqlWarehouse === false`, fire POST `/api/runtime/start-warehouse` as a fire-and-forget side-effect (no UI gate, no retry loop).

### Files

- `frontend/src/app/api/runtime/status/route.ts` — edit
- `frontend/src/app/api/runtime/start-warehouse/route.ts` — new file
- `frontend/src/app/(app)/claims/[claimId]/page.tsx` — edit `runtimeStatusQuery` effect

---

## Fix 2 — Chat: server limits + client history corruption

### Problem

**Server**: `bodySchema` in `chat/route.ts` has no upper bounds — unbounded `messages` array and unbounded `content` strings allow accidental huge payloads that cause latency spikes and avoidable Databricks serving cost.

**Client**: `sendMessage()` builds `nextMessages` from `visibleMessages` (line 155 in `[claimId]/page.tsx`), which includes the synthetic opening assistant message (UI-only, not persisted). `setMessages(nextMessages)` then permanently stores the synthetic opener in `messages` state, corrupting subsequent requests.

### Solution

**Server** (`chat/route.ts`)
```ts
messages: z.array(messageSchema).min(1).max(12)
content:  z.string().min(1).max(2000)
```

**Client** (`[claimId]/page.tsx`, inside `sendMessage`)

Replace:
```ts
const nextMessages = [...visibleMessages, userMsg];
setMessages(nextMessages);
```
With:
```ts
const nextMessages = [...messages, userMsg];
setMessages(nextMessages);
```

`visibleMessages` remains for display only; only `messages` (persisted) is sent to the API.

### Files

- `frontend/src/app/api/claims/[claimId]/chat/route.ts` — edit schema
- `frontend/src/app/(app)/claims/[claimId]/page.tsx` — edit `sendMessage`

---

## Fix 3 — `isAuthorized`: case normalization + empty-entry filter

### Problem

`isAuthorized()` in `auth-session.ts` compares `email` against admin email list and allowed domain list without lowercasing. `"Krish@Company.com"` fails while `"krish@company.com"` passes. Comma-separated env vars are also not filtered for empty entries (e.g. trailing comma produces `""`).

### Solution

```ts
function isAuthorized(email: string | null | undefined): boolean {
  if (!email) return false;
  const normalizedEmail = email.toLowerCase();
  const adminEmails = env.CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS
    .split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
  if (adminEmails.includes(normalizedEmail)) return true;
  const allowedDomains = env.CLAIMOPS_ALLOWED_EMAIL_DOMAINS
    .split(",").map((d) => d.trim().toLowerCase()).filter(Boolean);
  if (allowedDomains.includes("*")) return true;
  const domain = normalizedEmail.split("@")[1];
  if (domain && allowedDomains.includes(domain)) return true;
  return false;
}
```

### Files

- `frontend/src/lib/auth-session.ts` — edit `isAuthorized`

---

## Fix 4 — `BETTER_AUTH_TRUSTED_ORIGINS` in Cloud Build

### Problem

`BETTER_AUTH_TRUSTED_ORIGINS` is defined as optional in `env.ts` and consumed by the Better Auth config, but is not set in `cloudbuild.yaml`'s `--set-env-vars`. Better Auth uses trusted origins to validate OAuth state and cookies — omitting it in production is the root cause of OAuth state/cookie mismatch failures seen in earlier deployments.

### Solution

Add to `--set-env-vars` in the `deploy-cloud-run` step:

```
BETTER_AUTH_TRUSTED_ORIGINS=${_CLAIMOPS_APP_ORIGIN}
```

`_CLAIMOPS_APP_ORIGIN` already equals `_BETTER_AUTH_URL` (`https://homeprojectabacus-frontend-88897055243.asia-south1.run.app`) so no new substitution variable is needed.

### Files

- `cloudbuild.yaml` — edit `deploy-cloud-run` step's `--set-env-vars`

---

## Scope boundaries

- No new env vars added (schema already has `BETTER_AUTH_TRUSTED_ORIGINS` as optional).
- No UI changes beyond the fire-and-forget start-warehouse call in the claim detail page.
- No changes to other API routes.
- `POST /api/runtime/start-warehouse` has no UI button — it is called programmatically when status reports warehouse stopped.
