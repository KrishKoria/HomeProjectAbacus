# Chapter 8: Deployment

This chapter covers day-to-day deployment, production release, GCP environment specifics, and troubleshooting.

---

## 8.1 Prerequisites

### Databricks CLI

The Databricks CLI must be installed and authenticated for both dev and prod workspaces.

```bash
# Verify installation
databricks --version

# Authentication check
databricks auth token -p dev
databricks auth token -p prod
```

### Profiles (`~/.databrickscfg`)

Two profiles are required:

```
[dev]
host  = https://<dev-workspace>.databricks.com
token = <personal-access-token>

[prod]
host  = https://<prod-workspace>.databricks.com
token = <service-principal-token>
```

The dev profile uses a personal access token for day-to-day work. The prod profile uses a service principal token for CI/CD automation.

### GCP Workspace

| Requirement | Value |
|---|---|
| Workspace type | Classic (not serverless) for SDP pipelines |
| Node type | `n2-highmem-2` (GCP) |
| Spark version | `17.3.x-cpu-ml-scala2.13` |
| Unity Catalog storage | GCS (not DBFS root) |

These values are defined in `databricks.yml` lines 26-28 as DAB variables `node_type_id` and `spark_version`.

---

## 8.2 Dev Workflow (Day-to-Day)

### Validate

Always validate before deploying. This catches YAML syntax errors, unresolved variable references, and missing resource definitions.

```bash
databricks bundle validate -t dev --profile dev
```

### Deploy

Uploads the bundle resources to the Databricks workspace. The deploy root path is:

```
/Workspace/Users/${workspace.current_user.userName}/.bundle/healthcare-claim-ops/dev
```

```bash
databricks bundle deploy -t dev --profile dev
```

### Run a Single Job

```bash
databricks bundle run <job_key> -t dev --profile dev
```

| Job Key | Purpose | Run Time (approx) |
|---|---|---|
| `setup_infrastructure` | Create schemas + volumes | < 1 min |
| `ml_retrain_job` | Gate-checked model retraining | 5-15 min |
| `rag_vector_index_job` | Create/sync Vector Search index | 1-3 min |
| `analytics_observability_job` | Build analytics + quality assets | 2-5 min |

### Run the Full ETL Pipeline

The Bronze/Silver/Gold pipeline is a single DLT pipeline, not separate jobs. Trigger it via the fast-dev job:

```bash
databricks bundle run etl_fast_dev_job -t dev --profile dev
```

This runs the `healthcare_etl_pipeline` DLT pipeline followed by a lightweight ETL verification (`src/scripts/verify_etl_light.py`).

### Full Dev Cycle (End-to-End)

```bash
# 1. Validate
databricks bundle validate -t dev --profile dev

# 2. Deploy
databricks bundle deploy -t dev --profile dev

# 3. Run ETL (Bronze -> Silver -> Gold)
databricks bundle run etl_fast_dev_job -t dev --profile dev

# 4. Retrain ML model
databricks bundle run ml_retrain_job -t dev --profile dev

# 5. Create/sync vector index
databricks bundle run rag_vector_index_job -t dev --profile dev

# 6. Build analytics dashboard assets
databricks bundle run analytics_observability_job -t dev --profile dev

# 7. Verify backend health from the frontend runtime
cd frontend && bun run build
```

Vector sync uses two Gold-layer tables intentionally: `healthcare.gold.policy_chunks` (Lakeflow materialized view) and `healthcare.gold.policy_chunks_vs` (CDF-enabled Delta table for Delta Sync). The vector job incrementally mirrors MV changes into `_vs` before running index sync.

The Next.js frontend reads Gold data through Databricks SQL/Serving APIs using service-principal OAuth.
Runtime health checks are exposed via `/api/runtime/status`.

---

## 8.3 Prod Deployment

### Validate

```bash
databricks bundle validate -t prod --profile prod
```

### Deploy

```bash
databricks bundle deploy -t prod --profile prod
```

Prod deploy root path:

```
/Workspace/Users/${workspace.current_user.userName}/.bundle/healthcare-claim-ops/prod
```

### Run a Job in Prod

```bash
databricks bundle run <job_key> -t prod --profile prod
```

### Prod Differences from Dev

| Aspect | Dev | Prod |
|---|---|---|
| `mode` | `development` | `production` |
| `workspace.profile` | `dev` | `prod` |
| Resource naming | No "dev" prefix | No "prod" prefix (same convention) |
| `model_version` | Hardcoded `"1"` | Resolved by CI/CD pipeline |
| Token type | Personal access token | Service principal token |
| Cluster | Interactive (developers) | Automated jobs only |

Note: In `development` mode, Databricks appends `-dev` to resource names. In `production` mode, resource names are clean. The `experimental.skip_name_prefix_for_schema` is set to `true` so schema names are never prefixed regardless of mode.

---

## 8.4 Switching Targets

Change `-t dev` to `-t prod` (or vice versa). All variables in `databricks.yml` switch accordingly:

| Variable | dev value | prod value |
|---|---|---|
| `catalog` | `healthcare` | `healthcare` |
| `model_version` | `"1"` | CI/CD-resolved |
| `workspace.profile` | `dev` | `prod` |
| `mode` | `development` | `production` |
| Frontend runtime | `frontend/` Next.js app | `frontend/` Next.js app |

The catalog name is `healthcare` in both environments -- data is separated by workspace, not by catalog.

---

## 8.5 GCP Setup (Summary)

### Compute

| Property | Value |
|---|---|
| Cluster type | Classic (not serverless) for SDP pipelines |
| Node type | `n2-highmem-2` |
| Spark version | `17.3.x-cpu-ml-scala2.13` |
| Worker type | Spot instances (cost optimization) |

### Storage

| Property | Detail |
|---|---|
| Unity Catalog root | GCS bucket (not DBFS root) |
| Bronze landing zone | `/Volumes/healthcare/bronze/raw_landing/` |
| Bundle root path | `/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}` |
| Dataset location | `datasets/` directory (local development) -> Unity Catalog volumes (Databricks) |

### Bundle Deploy Path Pattern

```
/Workspace/Users/${workspace.current_user.userName}/.bundle/healthcare-claim-ops/{dev|prod}/
```

---

## 8.6 ML Retrain Job Details

The ML retrain job (`services/ml/training/resources/training.job.yml`) runs `maybe_retrain_model.py` with these hardcoded parameters:

| Parameter | Value |
|---|---|
| `--catalog` | `${var.catalog}` (resolves to `healthcare`) |
| `--gold-table` | `${var.catalog}.${var.gold_schema}.claim_features` |
| `--registered-model-name` | `${var.catalog}.${var.ml_schema}.claim_denial_model` |
| `--champion-alias` | `champion` |
| `--optuna-trials` | `50` |
| `--random-seed` | `42` |

The `--force` flag is present as a comment in the YAML (line 23). To force retraining, uncomment it:

```yaml
# In services/ml/training/resources/training.job.yml, change:
              # - --force  (line 23)
# To:
              - --force
```

### Job Environment Dependencies

```
xgboost>=2.0,<3.0
lightgbm>=4.2,<5.0
catboost>=1.2,<2.0
scikit-learn>=1.5,<2.0
shap>=0.44,<1.0
optuna>=3.6,<4.0
mlflow>=3.0,<4.0
imbalanced-learn>=0.12,<1.0
```

Plus `--editable ${workspace.file_path}` for the project itself.

---

## 8.7 Troubleshooting

### `bundle validate` Fails

| Symptom | Likely Cause | Fix |
|---|---|---|
| YAML parse error | Syntax issue in `databricks.yml` or included `*.yml` | Run with `--debug` flag: `databricks bundle validate -t dev --profile dev --debug` |
| Unresolved variable `${var.x}` | Variable `x` missing in target block | Check `databricks.yml` variables section (lines 12-33) |
| Resource type unknown | YAML schema mismatch | Check Databricks CLI version compatibility |

### Job Fails

| Symptom | Action |
|---|---|
| Python traceback in job output | Open job run in Databricks workspace UI -> "Output" tab -> scroll to traceback |
| ModuleNotFoundError | Check job environment dependencies -- the `--editable ${workspace.file_path}` may be missing |
| Spark analysis exception | Check cluster logs in the Spark UI from the job run page |
| Permission denied | Verify service principal has `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on the target catalog |

For the Next.js runtime service principal, grant:
- SQL warehouse: `CAN_USE`
- Gold table: `SELECT` plus `USE CATALOG` and `USE SCHEMA`
- Serving endpoint: `CAN_QUERY`
- Vector Search index: query access

### DESCRIBE HISTORY Error on gold_claim_features

`healthcare.gold.claim_features` is a DLT materialized view in the development target, **not** a Delta table. Running `DESCRIBE HISTORY` on it will fail with `EXPECT_TABLE_NOT_VIEW` or similar errors.

**Expected behavior:** `_current_gold_version()` in `retrain_gate.py` (line 149-186) catches this and returns `-1` for view-type objects. The retrain gate falls back to content fingerprint comparison instead of version comparison.

**No action needed** -- this is not a bug.

### Retrain Gate Skips Training

The retrain gate will skip if:

| Reason | Fix |
|---|---|
| `"no data changes"` | Data fingerprint matches champion training data -- no retrain needed |
| `"fingerprint changed but row_count delta below retrain threshold"` | New data is present but delta is below `max(100, ceil(0.05 * previous_row_count))` -- use `--force` to bypass |
| MLflow champion alias not found | First training run -- create the alias by running with `--force` once |
| Champion run lookup failed | Champion metadata corrupted -- use `--force` to retrain and overwrite |

To force retraining:

```bash
# Via CLI
python src/scripts/maybe_retrain_model.py --force

# Via Databricks job (uncomment --force in training.job.yml first)
databricks bundle run ml_retrain_job -t dev --profile dev
```

### DLT Pipeline Failures

The ETL pipeline (`healthcare_etl_pipeline`) is a single DLT pipeline comprising Bronze, Silver, and Gold stages. If it fails:

1. Check the DLT pipeline event log table: `${var.catalog}.${var.analytics_schema}.etl_pipeline_event_log`
2. Review the pipeline UI in Databricks for the specific materialized view that failed
3. Common failure modes:
   - Reference data missing (bronze.cost, bronze.diagnosis, bronze.providers not loaded)
   - Schema evolution issues (new columns in source data not handled)
   - Autoloader file parsing errors (malformed CSV in the landing zone)

### Debugging with `--debug`

Append `--debug` to any Databricks CLI command for verbose output:

```bash
databricks bundle validate -t dev --profile dev --debug
databricks bundle deploy -t dev --profile dev --debug
```

---

## 8.8 Destroying Resources

```bash
databricks bundle destroy -t dev --profile dev
```

This removes all deployed resources (jobs, pipelines) for the target environment. It does **not** delete Delta tables or Unity Catalog schemas -- only the DAB-deployed job and pipeline definitions.

**Use with caution.** Destroying prod resources requires manual re-deploy to restore.

---

## 8.9 Streamlit Decommission Cleanup

After merging the Streamlit decommission, run one normal deploy so Databricks updates bundle-managed resources:

```bash
databricks bundle deploy -t dev --profile dev
```

Expected result: the removed app resource (`claim_ops_app`) should no longer appear in the workspace resources managed by this bundle.

If Streamlit assets were created manually outside the bundle, clean them up after confirming Next.js runtime health:

1. Delete the legacy Databricks App from the workspace UI (if still present).
2. Remove legacy app secrets/environment entries that are no longer used:
   - `STREAMLIT_OIDC_*`
   - `CLAIMOPS_AUTH_*`
3. Remove any app-only secret scopes that were dedicated to the old Streamlit runtime.
4. Re-run `databricks bundle validate -t dev --profile dev` to confirm no remaining references.

---

## 8.10 Quick Reference Cards

### One-Time Setup

```bash
# 1. Validate connectivity
databricks auth token -p dev

# 2. Deploy bundle
databricks bundle deploy -t dev --profile dev

# 3. Create schemas + load sample data
databricks bundle run setup_infrastructure -t dev --profile dev --params load_sample_data=true
```

### Daily Development Loop

```bash
# Edit -> Validate -> Deploy -> Run ETL -> Run ML
databricks bundle validate -t dev --profile dev && \
databricks bundle deploy -t dev --profile dev && \
databricks bundle run etl_fast_dev_job -t dev --profile dev && \
databricks bundle run ml_retrain_job -t dev --profile dev && \
databricks bundle run rag_vector_index_job -t dev --profile dev && \
databricks bundle run analytics_observability_job -t dev --profile dev
```

### Production Release

```bash
# CI/CD pipeline typically handles this:
databricks bundle validate -t prod --profile prod
databricks bundle deploy -t prod --profile prod
databricks bundle run etl_fast_dev_job -t prod --profile prod
databricks bundle run ml_retrain_job -t prod --profile prod
databricks bundle run rag_vector_index_job -t prod --profile prod
databricks bundle run analytics_observability_job -t prod --profile prod
```

### Backup: Run via CLI (Without DAB)

```bash
# Train with explicit parameters (requires Spark access)
python src/scripts/train_denial_model.py --tune --optuna-trials 200

# Force retrain
python src/scripts/maybe_retrain_model.py --force

# Local test (no Spark)
python src/scripts/train_denial_model.py --gold-csv datasets/claims_features.csv --no-tune --registered-model-name ""
```
