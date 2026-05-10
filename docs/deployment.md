# Deployment Guide

## Vercel Deployment

### Environment Variables

Set all vars from `frontend/.env.example` in Vercel project settings.

### Google OAuth Callback URLs

| Environment | URL |
|-------------|-----|
| Local dev | `http://localhost:3000/api/auth/callback/google` |
| Production | `https://<vercel-domain>/api/auth/callback/google` |

Add both to Google Cloud Console → APIs & Services → Credentials → OAuth 2.0 Client ID → Authorized redirect URIs.

### Neon Database Connection

Use the **pooled connection string** (port 5433 via PgBouncer), NOT the direct connection (port 5432):

```
postgresql://user:pass@ep-xxxx.us-east-2.aws.neon.tech:5433/neondb
```

## Databricks Service Principal Setup

1. Create service principal in Databricks admin console
2. Grant permissions:
   - SQL warehouse: `USE`, `READ` on feature table schema
   - Table: `SELECT` on `healthcare.gold.claim_features`
   - Model serving: `Can Query` on the claim-denial-analysis endpoint
3. Generate OAuth client ID and secret for M2M auth

## Security Notes

- Vercel handles authentication only (Better Auth + Google OAuth)
- Better Auth Postgres DB stores identity/session metadata only — no claims data
- Databricks remains system of record for claims, model, and policy data
- Real PHI deployment requires Vercel BAA review, private networking, and audit controls

## CI Commands

```bash
# Frontend
bun run lint        # ESLint
bun x tsc --noEmit  # TypeScript check
bun test            # Vitest
bun run build       # Next.js build

# Python
uv run pytest -q    # Python tests

# Databricks
databricks bundle validate -t dev --profile dev
```
