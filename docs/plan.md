# Next.js + Better Auth + Databricks API Pivot From Scratch

## Summary

Build a new `frontend/` Next.js app from zero, hosted on Vercel, with Better Auth Google login backed by Postgres + Drizzle. Keep Databricks as the compute/data/API layer, not the browser-hosted app layer.

The key architectural correction is this: do not reimplement prediction, SHAP, RAG, Vector Search, or MLflow logic in TypeScript. Keep that Python logic in Databricks and expose it through a Databricks serving/API surface. The Next.js server-side API routes become the authenticated BFF that calls Databricks APIs using a service principal.

Docs checked: [Better Auth Google](https://www.better-auth.com/docs/authentication/google), [Better Auth Database](https://www.better-auth.com/docs/concepts/database), [Better Auth Next.js](https://www.better-auth.com/docs/integrations/next), [Databricks OAuth M2M](https://docs.databricks.com/gcp/en/dev-tools/auth/oauth-m2m), [Databricks Apps auth](https://docs.databricks.com/gcp/en/dev-tools/databricks-apps/auth), [Databricks Free Edition limits](https://docs.databricks.com/gcp/en/getting-started/free-edition-limitations), [Next.js Route Handlers](https://nextjs.org/docs/app/building-your-application/routing/route-handlers).

## Key Interfaces

Create these frontend routes:

- `GET/POST /api/auth/[...all]`: Better Auth handler.
- `GET /api/me`: server-side session probe for UI bootstrapping.
- `GET /api/runtime/status`: checks Better Auth DB, Databricks OAuth, SQL warehouse access, and analysis endpoint reachability.
- `GET /api/claims/samples`: returns a small list of non-PHI claim IDs from `healthcare.gold.claim_features`.
- `POST /api/claims/analyze`: accepts `{ claimId: string }`, verifies session, loads feature row from Databricks SQL, calls Databricks claim-analysis endpoint, returns the UI-ready analysis payload.

Create these frontend pages:

- `/sign-in`: Google login button using Better Auth client.
- `/access-denied`: shown for authenticated users without allowed role/domain.
- `/dashboard`: sample claims, runtime status, and claim search.
- `/claims/[claimId]`: risk score, risk level, top SHAP reasons, feature breakdown, and policy guidance cards.

Create these backend/Databricks contracts:

- `ClaimFeatureRow`: exact feature contract derived from `src.ml.FEATURE_COLUMNS`.
- `ClaimAnalysisRequest`: `{ claimId: string, features: Record<string, number | string | boolean | null> }`.
- `ClaimAnalysisResponse`: `{ claimId, riskScore, riskLevel, predictionLabel, topReasons, policyGuidance, model, generatedAt }`.
- `PolicyGuidance`: compact evidence cards with policy display name, excerpt, relevance/rank, and trace metadata.
- `DatabricksStatus`: `{ oauth, sqlWarehouse, analysisEndpoint, vectorSearch, modelServing }`.

## Implementation Plan

2. Run baseline discovery commands before scaffolding: `git status --short`, `rg --files -g package.json -g next.config.* -g tsconfig.json`, `uv run pytest -q`.

3. Decommission legacy Databricks App frontend resources after Next.js rollout.

4. Update planning/docs first in execution mode: mark `docs/deferred.md` `fastapi-react-frontend` as revived because Databricks-hosted auth is blocked by current tier constraints.

5. Keep the durable architecture note `docs/architecture/nextjs-better-auth-databricks-api.md` aligned to the BFF pattern and Next.js-only frontend path.

6. Add frontend dependencies: `better-auth`, `drizzle-orm`, `postgres`, `zod`, `@tanstack/react-query`, and `shadcn`

7. Add frontend dev dependencies: `drizzle-kit`, `vitest`, `jsdom`, `@testing-library/react`, `@testing-library/jest-dom`, and TypeScript tooling needed by the generated scaffold.

8. Add `frontend/.env.example` with `BETTER_AUTH_SECRET`, `BETTER_AUTH_URL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `DATABASE_URL`, `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SQL_HTTP_PATH`, `CLAIMOPS_FEATURE_TABLE`, `CLAIMOPS_ANALYSIS_ENDPOINT`, `CLAIMOPS_ALLOWED_EMAIL_DOMAINS`, and `CLAIMOPS_BOOTSTRAP_ADMIN_EMAILS`.

9. Add `frontend/src/lib/server/env.ts` using Zod so missing environment variables fail at server startup or route invocation with a clear non-secret error.

10. Add `frontend/src/lib/db/schema.ts` with Better Auth tables `user`, `session`, `account`, `verification`, plus app fields for `role`, `status`, and timestamps.

11. Add `frontend/drizzle.config.ts`, generate migrations through Better Auth/Drizzle, and ensure migrations are committed rather than relying on runtime schema sync.

12. Add `frontend/src/lib/auth.ts` using `betterAuth({ baseURL, database, socialProviders.google })`.

13. Add `frontend/src/app/api/auth/[...all]/route.ts` using `toNextJsHandler(auth)`.

14. Add `frontend/src/lib/auth-client.ts` for browser login helpers and `authClient.signIn.social({ provider: "google" })`.

15. Add `frontend/src/lib/auth-session.ts` with `getOptionalSession`, `requireSession`, and `requireAuthorizedSession`.

16. Enforce access server-side in pages and route handlers; do not rely on middleware alone for authorization.

17. Implement initial role policy: allow configured bootstrap admin emails, allow configured email domains as normal users if desired, deny authenticated users with no role/status.

18. Add `tools/export_frontend_contracts.py` to generate a TypeScript feature-column contract from `src.ml.FEATURE_COLUMNS`.

19. Generate `frontend/src/lib/contracts/claimops.ts` from Python instead of manually duplicating the feature list.

20. Add a Python test asserting the generated frontend contract matches `src.ml.FEATURE_COLUMNS`.

21. Add `frontend/src/lib/databricks/oauth.ts` to request and cache Databricks M2M OAuth tokens server-side only.

22. Add `frontend/src/lib/databricks/client.ts` as a small `fetch` wrapper that injects Bearer tokens, handles timeouts, strips secrets from errors, and returns typed failures.

23. Add `frontend/src/lib/databricks/sql.ts` for Databricks SQL Statement Execution calls against `CLAIMOPS_FEATURE_TABLE`.

24. Keep SQL fixed-shape: the table name comes from validated env, selected columns come from generated `FEATURE_COLUMNS`, and `claimId` is passed as a parameter or safely escaped through the documented API mechanism.

25. Add `frontend/src/lib/databricks/analysis.ts` to call the Databricks claim-analysis serving endpoint.

26. Add `frontend/src/lib/databricks/types.ts` for shared Databricks response types and route DTOs.

27. Add `frontend/src/app/api/runtime/status/route.ts` to test auth DB, Databricks OAuth, SQL warehouse, and analysis endpoint.

28. Add `frontend/src/app/api/claims/samples/route.ts` to return a limited list of claim IDs for demo navigation.

29. Add `frontend/src/app/api/claims/analyze/route.ts` to require a session, validate `{ claimId }`, fetch features from SQL, call analysis serving, and return UI-ready JSON.

30. Add a new Python package area `src/serving/` with `from __future__ import annotations` in every file.

31. Add `src/serving/claim_analysis.py` as the Databricks-side orchestration layer around existing `predict_single`, `explain`, and `retrieve_and_explain`.

32. Keep `src/serving/claim_analysis.py` JSON-friendly and PHI-safe; logs may reference `claim_id` only and must not interpolate feature payloads or excerpts.

33. Add `src/scripts/register_claim_analysis_model.py` to package/register a pyfunc-style claim-analysis model or serving wrapper that Databricks Model Serving can invoke.

34. Add `src/scripts/verify_claim_analysis_endpoint.py` to call the deployed endpoint with a known synthetic claim feature row and fail clearly if prediction, SHAP, or policy guidance is unavailable.

35. Add Databricks bundle variables for `claim_analysis_model_name`, `claim_analysis_model_alias`, and `claim_analysis_endpoint_name`.

36. Add a Databricks job/resource under the ML service area to register or verify the claim-analysis endpoint after the existing model and vector-search assets exist.

37. Do not move frontend rendering helpers into serving code; extract only business logic needed for prediction/explanation/policy guidance.

38. Build the UI after APIs are typed: create a restrained app shell, sign-in page, dashboard, claim detail page, risk summary card, feature breakdown card, and compact policy evidence cards.

39. Avoid raw Databricks scores as user-facing percentages; preserve the existing display decision that policy evidence should be shown as ranked compact excerpts, not fake match percentages.

40. Add frontend route tests for unauthenticated access, denied users, authorized users, bad `claimId`, missing feature row, Databricks timeout, and successful analysis.

41. Add unit tests for Databricks OAuth token caching and secret-safe error formatting.

42. Add Python tests for `src/serving/claim_analysis.py` with mocked model, mocked SHAP output, and mocked retriever output.

43. Add integration smoke tests that can run locally without real Databricks by mocking `frontend/src/lib/databricks/*`.

44. Add a manual Databricks smoke checklist for real workspace validation: bundle validate, endpoint verify job, Vercel env check, Google callback check, and `/api/runtime/status`.

45. Add Vercel deployment docs with exact Google OAuth callback URLs: local `http://localhost:3000/api/auth/callback/google` and production `https://<vercel-domain>/api/auth/callback/google`.

46. Add Databricks service-principal setup docs: required workspace host, client id/secret, SQL warehouse permissions, table read permission, model serving endpoint permission, vector index/model serving dependencies.

47. Add security notes: Vercel handles auth only, Better Auth DB stores identity/session metadata only, Databricks remains system of record for claims/model/policy data, real PHI requires Vercel BAA/Secure Compute/private networking review.

48. Add CI-style commands in docs and package scripts: `bun run lint`, `bun x tsc --noEmit`, `bun test`, `bun run build`, `uv run pytest -q`, and `databricks bundle validate -t dev --profile dev`.

49. Commit in reviewable slices: docs decision, frontend scaffold, Better Auth/DB, Databricks TS client/API routes, Python serving endpoint, UI parity, deployment docs/tests.

## Test Plan

Run these after implementation:

- `uv run pytest -q`
- `uv run pytest -q tests/test_claim_analysis_serving.py`
- `cd frontend && bun run lint`
- `cd frontend && bun x tsc --noEmit`
- `cd frontend && bun test`
- `cd frontend && bun run build`
- `databricks bundle validate -t dev --profile dev`
- `databricks bundle run -t dev --profile dev <claim-analysis-verify-job>`
- Local browser smoke: sign in with Google, load `/dashboard`, analyze a sample claim, confirm policy cards render without raw DBFS paths or fake match percentages.
- Production smoke on Vercel: Google callback succeeds, `/api/runtime/status` is green, sample claim analysis returns within the chosen timeout.

## Assumptions And Defaults

- Use root-level `frontend/` for the web app and keep Databricks bundle resources focused on pipelines/jobs/model serving.
- Use Bun for all frontend package management and scripts.
- Use Postgres + Drizzle for Better Auth persistence; default managed provider is Neon unless the user supplies another Postgres host.
- Keep Databricks service-principal credentials only in server-side Vercel environment variables.
- Remove legacy frontend deployment artifacts once Next.js parity and verification are complete.
- Treat this as production-like for synthetic/training data; do not claim real PHI readiness without BAA, networking, audit, retention, and incident-response review.
