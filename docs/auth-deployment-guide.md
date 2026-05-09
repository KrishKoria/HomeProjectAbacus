# Deploy Hybrid Auth on Databricks Apps — Step-by-Step

## Context
This app uses hybrid auth resilience:
- Primary: Google OIDC via Streamlit native auth.
- Fallback: Databricks forwarded identity headers when OIDC preflight fails at startup.

At startup, `launcher.py` selects backend based on `CLAIMOPS_AUTH_MODE`:
- `auto` (default): try OIDC first, fallback to headers if OIDC endpoints are unreachable.
- `oidc_only`: fail closed if OIDC bootstrap or endpoint preflight fails.
- `headers_only`: skip OIDC and force Databricks header mode.

## Prerequisites
- Databricks CLI installed and authenticated (`databricks auth login --profile dev`)
- Access to the Databricks workspace with App Admin or Workspace Admin permissions
- A Google Cloud Console project with ability to create OAuth 2.0 credentials

---

## Step 1: Create Google OAuth 2.0 Credentials

### 1a. Configure OAuth Consent Screen
1. Go to [Google Cloud Console](https://console.cloud.google.com) → **APIs & Services** → **OAuth consent screen**
2. If not already configured:
   - User Type: **External** (or Internal if all users are in your Google Workspace org)
   - App name: `Claim Denial Risk Analyzer` (or your preferred name)
   - User support email: your email
   - Developer contact: your email
   - Scopes: `openid`, `email`, `profile` (add under "Scopes" step)
   - Add test users (your email and any colleagues who need to log in during testing)
3. Save and continue through the remaining screens

### 1b. Create OAuth Client ID
1. Go to **APIs & Services** → **Credentials** → **+ Create Credentials** → **OAuth client ID**
2. Application type: **Web application**
3. Name: `ClaimOps Streamlit (Dev)` (or descriptive name)
4. Authorized redirect URIs — these depend on your Databricks workspace URL:

   **Development (your personal workspace path):**
   ```
   https://<workspace-host>/app/claim-ops-app-dev/oauth2callback
   ```
   
   **Production:**
   ```
   https://<workspace-host>/app/claim-ops-app/oauth2callback
   ```

   Replace `<workspace-host>` with your actual workspace hostname (e.g., `dbc-xxxxx.cloud.databricks.com`).

5. Click **Create**. Save the **Client ID** and **Client Secret** immediately — the secret is shown only once.

---

## Step 2: Store Secrets in Databricks

### 2a. Create a Secret Scope
```bash
databricks secrets create-scope --profile dev --scope claimops-auth
```

### 2b. Store the OAuth Credentials
```bash
databricks secrets put-secret --profile dev --scope claimops-auth --key google-client-id
# Paste the Google OAuth Client ID when prompted

databricks secrets put-secret --profile dev --scope claimops-auth --key google-client-secret
# Paste the Google OAuth Client Secret when prompted

databricks secrets put-secret --profile dev --scope claimops-auth --key cookie-secret
# Enter a strong random string — this encrypts the session cookie
# Generate one: python -c "import secrets; print(secrets.token_hex(32))"
```

### 2c. Store the Redirect URI
```bash
databricks secrets put-secret --profile dev --scope claimops-auth --key redirect-uri
# Enter the full redirect URI, e.g.:
# https://dbc-xxxxx.cloud.databricks.com/app/claim-ops-app-dev/oauth2callback
```

---

## Step 3: Configure `app.yaml` for OIDC Secrets

The current `app.yaml` has no OIDC env vars. Add the following **before deploying** using the Databricks Apps UI (App resources → Secrets) **or** by editing `app.yaml` with `valueFrom` references.

### Option A: Databricks Apps UI (Recommended for initial setup)
1. After the app is created/first-deployed, go to the Databricks workspace
2. Navigate to **Compute** → **Apps** → `claim-ops-app-dev`
3. Go to **App resources** tab → **+ Add resource** → **Secret**
4. Add each secret from the `claimops-auth` scope as a resource:
   - `google-client-id` → assign resource key `google_client_id`, permission `CAN_READ`
   - `google-client-secret` → resource key `google_client_secret`, permission `CAN_READ`
   - `cookie-secret` → resource key `cookie_secret`, permission `CAN_READ`
   - `redirect-uri` → resource key `redirect_uri`, permission `CAN_READ`
5. Go to **Environment** tab → add these env vars referencing the resources:
   ```
   STREAMLIT_OIDC_ENABLED_PROVIDERS = google
   STREAMLIT_OIDC_GOOGLE_CLIENT_ID = {{secrets/google_client_id}}
   STREAMLIT_OIDC_GOOGLE_CLIENT_SECRET = {{secrets/google_client_secret}}
   STREAMLIT_OIDC_GOOGLE_REDIRECT_URI = {{secrets/redirect_uri}}
   ```
6. Restart the app from the UI

### Option B: `app.yaml` with `valueFrom` (for git-tracked config)
```yaml
command:
  - python
  - launcher.py
env:
  - name: STREAMLIT_GATHER_USAGE_STATS
    value: "false"
  - name: CLAIMOPS_SQL_WAREHOUSE_ID
    value: "26e842cddb906c23"
  - name: CLAIMOPS_SQL_HTTP_PATH
    value: "/sql/1.0/warehouses/26e842cddb906c23"
  - name: CLAIMOPS_GOLD_TABLE
    value: "healthcare.gold.claim_features"
  - name: CLAIMOPS_MODEL_NAME
    value: "healthcare.ml.claim_denial_model"
  - name: CLAIMOPS_MODEL_ALIAS
    value: "champion"
  - name: CLAIMOPS_VECTOR_INDEX_NAME
    value: "healthcare.gold.policy_chunks_index"
  - name: STREAMLIT_OIDC_ENABLED_PROVIDERS
    value: "google"
  - name: STREAMLIT_OIDC_GOOGLE_CLIENT_ID
    valueFrom: google_client_id
  - name: STREAMLIT_OIDC_GOOGLE_CLIENT_SECRET
    valueFrom: google_client_secret
  - name: STREAMLIT_OIDC_GOOGLE_REDIRECT_URI
    valueFrom: redirect_uri
  - name: CLAIMOPS_AUTH_MODE
    value: "auto"
```

**Important:** `valueFrom` names must match the secret resource keys configured in the Databricks Apps UI (Step 3, Option A). The `value` fields for `CLIENT_ID` etc. are plaintext and visible in the UI — for sensitive values, use `valueFrom` referencing a Databricks secret.

---

## Step 4: Deploy the Bundle

### 4a. Validate
```bash
cd C:\Users\Krish\Desktop\projects\homeprojectabacus
databricks bundle validate -t dev --profile dev
```

### 4b. Deploy the bundle (registers app resource in workspace)
```bash
databricks bundle deploy -t dev --profile dev
```

### 4c. Verify the app appears
Go to Databricks workspace → **Compute** → **Apps** → you should see `claim-ops-app-dev`. Click on it to open.

---

## Step 5: Verify Authentication Works

1. Open the app URL: `https://<workspace-host>/app/claim-ops-app-dev`
2. You should see the **Sign in** screen with a **Sign in with Google** button
3. Click the button — you should be redirected to Google's OAuth consent screen
4. After granting consent, you should be redirected back to the app with the full Claim Denial Risk Analyzer UI visible
5. You should see the identity bar at the top with your name/email and **Sign out** button
6. Sign out → you should return to the login screen

### Optional mode checks
- Set `CLAIMOPS_AUTH_MODE=oidc_only` to force strict Google OIDC startup behavior.
- Set `CLAIMOPS_AUTH_MODE=headers_only` to force Databricks header auth.
- Keep `CLAIMOPS_AUTH_MODE=auto` for resilient production default.

### Troubleshooting

| Problem | Likely Cause | Fix |
|---------|-------------|-----|
| "Authentication Unavailable" | OIDC env vars not set | Check env vars in Databricks Apps UI → Environment tab |
| Google returns "redirect_uri_mismatch" | Redirect URI wrong | The redirect URI must exactly match what's in Google Cloud Console. Double-check the app name (dev adds `-dev` suffix) |
| Launcher exits with "Missing required OIDC" | A required key is empty | Verify all three env vars are set and non-empty: `STREAMLIT_OIDC_GOOGLE_CLIENT_ID`, `_CLIENT_SECRET`, `_REDIRECT_URI` |
| Launcher starts in header fallback mode | OIDC preflight failed | Check app logs for `CLAIMOPS_AUTH_FALLBACK_REASON`, then validate outbound DNS/egress to Google OIDC endpoints |
| OIDC startup fails with DNS/egress errors | Restricted workspace network policy | Allow outbound connectivity to `accounts.google.com`, `oauth2.googleapis.com`, `www.googleapis.com` and restart app |
| App starts but no auth gate | `.streamlit/secrets.toml` not generated | Check launcher logs in Databricks Apps → Logs tab. The launcher prints what it did to stderr |
| "No authentication providers configured" | `STREAMLIT_OIDC_ENABLED_PROVIDERS` not set | Set it to `"google"` in the environment variables |
| Audit table permission denied | Missing `MODIFY` permission | Verify `frontend.app.yml` has the `app-auth-audit-table` resource with `permission: MODIFY` |

### Investigate blocked outbound network events
Use Databricks system tables to confirm app egress denials:

```sql
SELECT
  event_time,
  action_name,
  request.host AS host,
  response.status_code AS status_code,
  source_ip,
  user_agent
FROM system.access.outbound_network
WHERE network_source_type = 'Apps'
  AND event_date >= current_date() - INTERVAL 1 DAY
ORDER BY event_time DESC
LIMIT 200;
```

---

## Step 6: Production Deployment

For production, repeat Steps 1-5 with the production target:

```bash
# Create a separate Google OAuth client ID for prod (different redirect URI)
# Store secrets (can use same scope, different keys: google-client-id-prod, etc.)

databricks bundle validate -t prod --profile prod
databricks bundle deploy -t prod --profile prod
```

The app name in production will be `claim-ops-app` (without `-dev` suffix), so the redirect URI becomes:
```
https://<workspace-host>/app/claim-ops-app/oauth2callback
```

**For production, also:**
1. Publish the Google OAuth consent screen (remove "Testing" mode in Google Cloud Console)
2. Use a strong, unique `cookie_secret` (not shared with dev)
3. Consider restricting access via `DomainPolicy` or `EmailAllowlistPolicy`
4. Verify the audit table exists: `healthcare.analytics.app_auth_events` (create it if needed)

---

## Verification Checklist

- [ ] Google OAuth consent screen configured
- [ ] OAuth client ID/secret created in Google Cloud Console
- [ ] Redirect URI added to Google Cloud Console (both dev and prod)
- [ ] Databricks secret scope created with all 4 secrets
- [ ] App resources configured in Databricks Apps UI
- [ ] OIDC environment variables set in app environment
- [ ] `databricks bundle deploy` succeeds
- [ ] App URL loads and shows Sign in screen
- [ ] Google login works and returns to app
- [ ] Identity bar shows user info
- [ ] Sign out works
- [ ] After 15 minutes of inactivity, session times out (or session timeout audit event fires)
- [ ] Audit events appear in `healthcare.analytics.app_auth_events`

All of this must be done **in the Databricks workspace** — the `app.yaml` changes are deployed via bundle, but the secret scope, secret values, and app resource bindings are workspace-level operations done via CLI or UI.
