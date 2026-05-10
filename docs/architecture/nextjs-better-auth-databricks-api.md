# BFF Architecture: Next.js + Better Auth + Databricks API

## Why auth moved to Vercel

Databricks free-edition workspaces do not support OAuth identity federation for web apps. The Databricks Apps feature (which would host a FastAPI + React frontend with integrated auth) is blocked by this tier constraint. Vercel provides the OAuth callback endpoint, session management, and serverless API routes without any Databricks runtime dependency.

## BFF Pattern

Next.js API routes act as a Backend-for-Frontend (BFF):

```
Browser → Next.js API Route (BFF) → Databricks API
```

- The browser never holds Databricks credentials
- All Databricks calls happen server-side in Next.js route handlers
- Route handlers use a Databricks service principal (M2M OAuth client_credentials grant)
- Tokens are cached in-memory and refreshed preemptively at 50% TTL

## Why Python ML/RAG stays in Databricks

The existing ML pipeline (prediction, SHAP explanations, RAG retrieval, Vector Search) is implemented in Python using Spark, MLflow, and Databricks-specific APIs. Reimplementing in TypeScript would:

- Duplicate complex logic (feature engineering, SHAP computation, vector search)
- Lose access to the registered MLflow model (champion alias)
- Require porting the RAG pipeline (embedding model, vector index, policy chunks)

Instead, the Python logic is packaged as a Databricks Model Serving endpoint (`src/serving/claim_analysis.py`). The Next.js BFF calls this endpoint via the Databricks Serving API.

## Why Streamlit remains fallback

The existing Streamlit app (`app_streamlit.py`, `src/analytics/`) is preserved until the Next.js app reaches feature parity. This ensures unblocked access to claim analysis during the pivot.

## Data flow

```mermaid
sequenceDiagram
    Browser->>Vercel: GET /dashboard
    Vercel->>Neon (Postgres): Session check via Better Auth
    Vercel->>Databricks API: M2M OAuth token request
    Databricks API-->>Vercel: Bearer token
    Vercel->>Databricks SQL: SELECT features FROM claim_features
    Databricks SQL-->>Vercel: Feature row
    Vercel->>Databricks Serving: POST analyze_claim (dataframe_split)
    Databricks Serving-->>Vercel: Predictions
    Vercel-->>Browser: UI-ready JSON
```

## Key decisions

| Decision | Rationale |
|----------|-----------|
| Better Auth (not NextAuth/Auth.js) | TypeScript-native API, explicit server/client split |
| Postgres + Drizzle (not Prisma) | Lightweight, no code generation step |
| SQL Statement Execution API (not JDBC/ODBC) | Suitable for serverless, no heavy driver |
| M2M OAuth (not PAT) | Standardized OAuth flow, easy rotation |
| Neon PgBouncer (port 5433) | Prevents connection exhaustion in serverless |

## Security boundaries

- Better Auth DB stores identity/session metadata only — no claims data
- Databricks remains system of record for claims, model, and policy data
- Real PHI requires Vercel BAA review, private networking, and audit controls
