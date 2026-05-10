# Manual Databricks Smoke Checklist

## Prerequisites
- [ ] Databricks CLI configured with `dev` profile
- [ ] Vercel project deployed or local dev server running
- [ ] Better Auth secret generated (`openssl rand -base64 32`)
- [ ] Google OAuth credentials configured (client ID + secret)
- [ ] Neon database provisioned with migrations applied
- [ ] Databricks service principal created with SQL warehouse + model serving permissions

## 1. Bundle Validation
- [ ] Run `databricks bundle validate -t dev --profile dev`
- [ ] Confirm no YAML syntax or variable resolution errors

## 2. Endpoint Verify Job
- [ ] Run `uv run python src/scripts/verify_claim_analysis_endpoint.py`
- [ ] Confirm output shows prediction, SHAP, and policy guidance available

## 3. Vercel Environment Check
- [ ] All env vars set in Vercel project settings (12+ vars)
- [ ] `BETTER_AUTH_URL` points to Vercel deployment URL (not localhost)

## 4. Google OAuth Callback
- [ ] `http://localhost:3000/api/auth/callback/google` added to Google Cloud Console test URIs
- [ ] `https://<vercel-domain>/api/auth/callback/google` added to production redirect URIs

## 5. Runtime Status Check
- [ ] Visit `/api/runtime/status` with valid session
- [ ] Confirm all services return `true`:
  - OAuth token acquisition
  - SQL warehouse reachable
  - Analysis endpoint reachable

## 6. Claim Analysis Flow
- [ ] Sign in with Google works end-to-end
- [ ] Dashboard loads sample claim IDs
- [ ] Clicking a claim ID loads analysis with risk score, SHAP reasons, policy guidance
- [ ] Manual claim ID entry works

## 7. Access Control
- [ ] Unauthenticated users redirected to sign-in
- [ ] Authorized email domain users can access dashboard
- [ ] Unauthorized email domain users see `/access-denied`
