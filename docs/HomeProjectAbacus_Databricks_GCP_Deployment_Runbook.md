# Databricks on GCP Deployment Runbook for `HomeProjectAbacus`

**Branch:** `integration`  
**Purpose:** Validate the Databricks implementation plan on a real Databricks-on-GCP workspace while keeping cost controlled.  
**Primary goal:** Deploy and run the existing Databricks Asset Bundle using **classic clusters**, not serverless-only Free Edition behavior.  
**Safety goal:** Prove the architecture step-by-step without accidentally triggering expensive workloads.

---

## 0. Read this first

This runbook is intentionally strict. Follow it in order.

Do **not** start with the full `etl_ml_pipeline` run.

The correct order is:

```text
GCP billing safety
→ Databricks subscription/workspace
→ local CLI auth
→ repo safety branch
→ cost-control dev changes
→ bundle validate
→ bundle deploy
→ setup_infrastructure
→ load_sample_data
→ Bronze pipeline
→ Silver pipeline
→ Gold pipeline
→ full etl_ml_pipeline manually
→ only then trigger/tuning tests
```

---

## 1. What this repo currently deploys

This runbook is based on the `integration` branch.

### 1.1 Main bundle file

```text
databricks.yml
```

Important repo facts:

```yaml
bundle:
  name: healthcare-claim-ops
  engine: direct

include:
  - resources/schemas/schemas.yml
  - resources/volumes/volumes.yml
  - services/*/resources/*.yml
  - services/*/*/resources/*.yml
  - services/*/*/*/resources/*.yml
```

So DAB discovers:

```text
resources/schemas/schemas.yml
resources/volumes/volumes.yml
services/**/resources/*.yml
```

The `dev` target uses:

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      profile: dev
```

Important implication: Databricks development mode pauses deployed job schedules/triggers by default. We will still add an explicit pause preset so there is no ambiguity.

### 1.2 Default compute settings

Current `databricks.yml` has:

```yaml
variables:
  node_type_id:
    default: n1-standard-4
  spark_version:
    default: 17.3.x-cpu-ml-scala2.13
```

Keep the architecture classic-cluster based. We are only lowering worker counts for the first paid smoke test.

### 1.3 Bundle resources currently present

The bundle is expected to deploy:

```text
Jobs:
  setup_infrastructure
  load_sample_data
  etl_ml_pipeline

Pipelines:
  bronze_pipeline
  silver_pipeline
  gold_pipeline

Schemas:
  healthcare.bronze
  healthcare.silver
  healthcare.quarantine
  healthcare.gold
  healthcare.analytics
  healthcare.ml

Volume:
  healthcare.bronze.raw_landing
```

Relevant source files:

```text
databricks.yml
resources/schemas/schemas.yml
resources/volumes/volumes.yml

services/infrastructure/setup/resources/setup_infrastructure.job.yml
services/infrastructure/load_sample_data/resources/load_sample_data.job.yml
services/ml/training/resources/training.job.yml

services/etl/bronze/resources/bronze.pipeline.yml
services/etl/silver/resources/silver.pipeline.yml
services/etl/gold/resources/gold.pipeline.yml

src/scripts/load_sample_data.py
src/scripts/setup_retrain_decisions.py
scripts/train_denial_model.py
```

---

## 2. External behavior verified against official docs

Use these as the cloud-side assumptions behind the steps:

1. **Databricks on GCP subscription:** You create the Databricks subscription through Google Cloud Marketplace, then create at least one Databricks workspace.
   - Official reference: <https://docs.databricks.com/gcp/admin/account-settings-gcp/create-subscription>

2. **Marketplace billing:** Google Cloud Marketplace product charges and underlying Google Cloud resource charges can be separate.
   - Official reference: <https://docs.cloud.google.com/marketplace/docs/billing>

3. **Classic workspace:** Databricks on GCP supports classic workspaces. Workspace configuration choices should be understood before creation because some settings cannot be changed after creation.
   - Official reference: <https://docs.databricks.com/gcp/admin/workspace/create-workspace>

4. **Databricks Bundles:** Bundles define jobs, pipelines, and other resources as code and support validate/deploy/run/destroy commands.
   - Official reference: <https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands>

5. **Development mode:** `mode: development` pauses schedules and triggers on deployed resources by default. Presets can explicitly set `trigger_pause_status: PAUSED`.
   - Official reference: <https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes>

6. **Unity Catalog volumes:** `/Volumes/<catalog>/<schema>/<volume>/...` is the correct file path format. Volumes require Unity Catalog-enabled compute and DBR 13.3 LTS or above.
   - Official reference: <https://docs.databricks.com/gcp/en/volumes/>

7. **File-arrival triggers:** A job file-arrival trigger can monitor the root/subpath of a Unity Catalog volume and recursively checks subdirectories. Only new files trigger runs; overwrites with the same filename do not.
   - Official reference: <https://docs.databricks.com/gcp/en/jobs/file-arrival-triggers>

8. **Google Cloud budgets:** Budgets and budget alerts notify you; they do not automatically create a hard spending cap.
   - Official reference: <https://docs.cloud.google.com/billing/docs/how-to/budgets>

---

## 3. Hard safety rules

Follow these exactly.

```text
DO NOT deploy to prod.
DO NOT unpause file-arrival triggers until manual runs pass.
DO NOT run --tune on the first paid validation.
DO NOT allow max_workers: 4 during the first paid validation.
DO NOT create GPU compute.
DO NOT create model serving endpoints during first validation.
DO NOT leave clusters running after each test.
DO NOT assume Google credits cover Databricks Marketplace spend.
```

Your first validation is a **controlled dev smoke test**, not a production deployment.

---

## 4. GCP setup

### 4.1 Create a separate GCP project

Create a new GCP project only for this test.

Recommended name:

```text
homeprojectabacus-dbx-dev
```

Do not use your main project.

### 4.2 Link billing

In Google Cloud Console:

```text
Billing
→ Manage billing accounts
→ Select billing account
→ Account management
→ Projects linked to this billing account
→ Link project
```

Link only the new project.

### 4.3 Create budget alerts before doing anything else

In Google Cloud Console:

```text
Billing
→ Budgets & alerts
→ Create budget
```

Use:

```text
Budget scope: the Databricks test project or entire billing account
Budget type: Specified amount
Budget amount: ₹5,000
```

Add alert thresholds:

| Alert | Trigger type |
|---:|---|
| ₹100 | Actual spend |
| ₹500 | Actual spend |
| ₹1,000 | Actual spend |
| ₹2,000 | Actual spend |
| ₹5,000 | Actual spend |
| ₹5,000 | Forecasted spend, if available |

Important: this does **not** cap spend. It only alerts.

### 4.4 Enable basic APIs if prompted

Enable these if Google/Databricks setup asks for them:

```text
Compute Engine API
Cloud Resource Manager API
IAM API
Service Usage API
Cloud Billing API
```

---

## 5. Subscribe to Databricks on GCP

### 5.1 Go to Google Cloud Marketplace

In Google Cloud Console:

```text
Marketplace
→ Search: Databricks
→ Select Databricks on Google Cloud / Databricks Data Intelligence Platform
→ Subscribe / Start trial
```

Use the test GCP project and billing account.

### 5.2 Create or open Databricks account console

After Marketplace subscription, follow the Databricks setup flow.

You need:

```text
Databricks account owner/admin access
Google billing permissions
The GCP project selected for the subscription
```

### 5.3 Create a classic workspace

Create one workspace:

```text
Workspace name: homeprojectabacus-dev
Workspace type: Classic workspace
Cloud: GCP
Unity Catalog: Enabled
Region: choose one region and stick to it
Network: Databricks-managed VPC unless you specifically need customer-managed VPC
```

Do **not** create multiple workspaces.

After workspace creation, open the workspace URL.

---

## 6. Databricks workspace setup

### 6.1 Confirm classic compute is available

In Databricks workspace:

```text
Compute
→ Create compute
```

Check that normal/classic compute options exist.

Your bundle defaults to:

```text
node_type_id = n1-standard-4
spark_version = 17.3.x-cpu-ml-scala2.13
```

If `n1-standard-4` is unavailable in your workspace/region, do **not** guess. Open the compute UI and choose the smallest available non-GPU general-purpose GCP node type, then update only `node_type_id` in `databricks.yml`.

### 6.2 Create the Unity Catalog catalog

Your DAB creates schemas and volume under the catalog, but the top-level catalog should exist first.

In Databricks SQL Editor or a notebook:

```sql
CREATE CATALOG IF NOT EXISTS healthcare;
```

Verify:

```sql
SHOW CATALOGS LIKE 'healthcare';
```

If this fails, you lack Unity Catalog privileges. Fix permissions before proceeding.

### 6.3 Confirm catalog permissions

You need permission to create schemas and volumes under `healthcare`.

Run:

```sql
USE CATALOG healthcare;
CREATE SCHEMA IF NOT EXISTS scratch_permission_test;
DROP SCHEMA IF EXISTS scratch_permission_test;
```

If this fails, get catalog owner/metastore admin permissions or ask an admin to grant:

```sql
GRANT USE CATALOG ON CATALOG healthcare TO `<your_user_or_group>`;
GRANT CREATE SCHEMA ON CATALOG healthcare TO `<your_user_or_group>`;
```

For dev-only personal testing, using your admin identity is acceptable.

---

## 7. Local machine setup

### 7.1 Checkout repo branch

```bash
git checkout integration
git pull origin integration
```

Confirm:

```bash
git branch --show-current
ls databricks.yml
```

Expected:

```text
integration
databricks.yml exists
```

### 7.2 Confirm Databricks CLI

```bash
databricks --version
```

Use a modern Databricks CLI. Official bundle docs require modern CLI support; if your CLI is old, update it before continuing.

### 7.3 Authenticate CLI

Your `dev` target uses Databricks CLI profile `dev`.

Run:

```bash
databricks auth login --profile dev
```

Verify:

```bash
databricks current-user me --profile dev
databricks auth profiles
```

The `dev` profile must point to the Databricks workspace URL you just created.

---

## 8. Create a safe smoke-test branch

Do not edit `integration` directly.

```bash
git checkout integration
git checkout -b integration-paid-smoke
```

This branch contains temporary cost-control changes.

---

## 9. Required cost-control repo changes

These changes preserve the current classic-cluster architecture. They only reduce first-run cost and prevent surprise automation.

---

### 9.1 Add explicit dev presets in `databricks.yml`

Open:

```text
databricks.yml
```

Find:

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      profile: dev
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: healthcare
      model_version: "1"
```

Change to:

```yaml
targets:
  dev:
    default: true
    mode: development
    presets:
      trigger_pause_status: PAUSED
      jobs_max_concurrent_runs: 1
    workspace:
      profile: dev
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: healthcare
      model_version: "1"
```

Why:

```text
mode: development already pauses triggers.
trigger_pause_status: PAUSED makes it explicit.
jobs_max_concurrent_runs: 1 prevents accidental parallel job runs.
```

---

### 9.2 Reduce full job autoscale to one worker

Open:

```text
services/ml/training/resources/training.job.yml
```

Find:

```yaml
job_clusters:
  - job_cluster_key: shared_cluster
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      autoscale:
        min_workers: 1
        max_workers: 4
```

Change to:

```yaml
job_clusters:
  - job_cluster_key: shared_cluster
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      autoscale:
        min_workers: 1
        max_workers: 1
```

Do not change `spark_version` yet.

Do not change `node_type_id` unless the workspace cannot use `n1-standard-4`.

---

### 9.3 Change ML training from `--tune` to `--no-tune`

In the same file:

```text
services/ml/training/resources/training.job.yml
```

Find:

```yaml
parameters:
  - --tune
  - --catalog
  - ${var.catalog}
```

Change to:

```yaml
parameters:
  - --no-tune
  - --catalog
  - ${var.catalog}
```

Why:

```text
--tune runs Optuna tuning.
First paid validation should test architecture, not optimize the model.
```

Your `scripts/train_denial_model.py` on `integration` already supports CLI argument passthrough through `_entrypoint_argv()`, so this parameter will be respected.

---

### 9.4 Reduce Bronze pipeline max workers

Open:

```text
services/etl/bronze/resources/bronze.pipeline.yml
```

Find:

```yaml
autoscale:
  min_workers: 1
  max_workers: 4
  mode: ENHANCED
```

Change to:

```yaml
autoscale:
  min_workers: 1
  max_workers: 1
  mode: ENHANCED
```

Do not remove:

```yaml
gcp_attributes:
  availability: PREEMPTIBLE_WITH_FALLBACK_GCP
```

---

### 9.5 Reduce Silver pipeline max workers

Open:

```text
services/etl/silver/resources/silver.pipeline.yml
```

Find:

```yaml
autoscale:
  min_workers: 1
  max_workers: 4
  mode: ENHANCED
```

Change to:

```yaml
autoscale:
  min_workers: 1
  max_workers: 1
  mode: ENHANCED
```

Do not remove:

```yaml
gcp_attributes:
  availability: PREEMPTIBLE_WITH_FALLBACK_GCP
```

---

### 9.6 Reduce Gold pipeline max workers

Open:

```text
services/etl/gold/resources/gold.pipeline.yml
```

Find:

```yaml
autoscale:
  min_workers: 1
  max_workers: 4
  mode: ENHANCED
```

Change to:

```yaml
autoscale:
  min_workers: 1
  max_workers: 1
  mode: ENHANCED
```

Do not remove:

```yaml
gcp_attributes:
  availability: PREEMPTIBLE_WITH_FALLBACK_GCP
```

---

### 9.7 Do not modify setup/load jobs

Do **not** change these unless validation fails:

```text
services/infrastructure/setup/resources/setup_infrastructure.job.yml
services/infrastructure/load_sample_data/resources/load_sample_data.job.yml
```

They already use:

```yaml
num_workers: 1
```

That is acceptable for the first paid validation.

---

### 9.8 Inspect your diff

Run:

```bash
git diff -- databricks.yml \
  services/ml/training/resources/training.job.yml \
  services/etl/bronze/resources/bronze.pipeline.yml \
  services/etl/silver/resources/silver.pipeline.yml \
  services/etl/gold/resources/gold.pipeline.yml
```

Expected changes:

```text
databricks.yml:
  added presets trigger_pause_status and jobs_max_concurrent_runs

training.job.yml:
  max_workers 4 -> 1
  --tune -> --no-tune

bronze.pipeline.yml:
  max_workers 4 -> 1

silver.pipeline.yml:
  max_workers 4 -> 1

gold.pipeline.yml:
  max_workers 4 -> 1
```

If your diff shows anything else, stop and inspect it.

---

## 10. Local validation before deploying

Run from repo root:

```bash
uv run python -m src.framework.validate_manifests
```

Expected:

```text
OK
```

Run tests:

```bash
uv run pytest -q tests/test_integration_plan_contract.py
```

Expected:

```text
16 passed
```

Run Databricks bundle validation:

```bash
databricks bundle validate -t dev --profile dev
```

Expected:

```text
Validation OK!
```

Run summary:

```bash
databricks bundle summary -t dev --profile dev
```

Expected to see at least:

```text
Resources:
  jobs:
    setup_infrastructure
    load_sample_data
    etl_ml_pipeline

  pipelines:
    bronze_pipeline
    silver_pipeline
    gold_pipeline
```

If `bundle validate` fails, do not deploy.

---

## 11. Deploy the bundle

Run:

```bash
databricks bundle deploy -t dev --profile dev
```

Expected:

```text
Deployment succeeds.
No job starts automatically.
No trigger fires automatically.
```

Because:

```text
dev target uses mode: development
explicit preset trigger_pause_status: PAUSED
```

---

## 12. Post-deploy checks in Databricks

In Databricks workspace UI, check:

```text
Workflows / Jobs & Pipelines
Catalog
Compute
```

Expected bundle resources:

```text
Jobs:
  [dev ...] Setup Infrastructure
  [dev ...] Load Sample Data
  [dev ...] Healthcare ETL + ML Pipeline

Pipelines:
  [dev ...] Healthcare Bronze Ingestion
  [dev ...] Healthcare Silver Validation
  [dev ...] Healthcare Gold Features
```

Expected catalog objects after deploy:

```sql
SHOW SCHEMAS IN healthcare;
```

Expected schemas:

```text
bronze
silver
quarantine
gold
analytics
ml
```

Expected volume:

```sql
SHOW VOLUMES IN healthcare.bronze;
```

Expected:

```text
raw_landing
```

---

## 13. Run setup job first

Run from local terminal:

```bash
databricks bundle run setup_infrastructure -t dev --profile dev
```

What it does:

```text
Task 1: apply_grants
  notebook: src/notebooks/grants.ipynb

Task 2: create_retrain_decisions
  script: src/scripts/setup_retrain_decisions.py
```

Expected final result:

```text
setup_infrastructure succeeds
healthcare.ml.retrain_decisions exists
```

Verify:

```sql
SHOW TABLES IN healthcare.ml LIKE 'retrain_decisions';
```

If this job fails, stop. Do not run sample load or pipelines.

---

## 14. Run sample data loader

Run:

```bash
databricks bundle run load_sample_data -t dev --profile dev
```

What it does:

```text
Runs src/scripts/load_sample_data.py
Copies fixture datasets into /Volumes/healthcare/bronze/raw_landing/
Uses --overwrite
```

Expected output:

```text
OK: load_sample_data ...
```

Verify volume paths:

```python
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing")
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing/claims")
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing/providers")
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing/diagnosis")
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing/cost")
```

Expected files:

```text
claims/claims_1000.csv
providers/providers_1000.csv
diagnosis/diagnosis.csv
cost/cost.csv
```

Policy PDFs may also exist if the repo has policy fixtures.

If this fails with missing volume, your DAB volume did not deploy or catalog/schema permissions are wrong.

---

## 15. Run Bronze pipeline manually

Use CLI:

```bash
databricks bundle run bronze_pipeline -t dev --profile dev
```

If CLI does not run the pipeline by key in your installed version, use UI:

```text
Databricks workspace
→ Jobs & Pipelines
→ Pipelines
→ [dev ...] Healthcare Bronze Ingestion
→ Start
```

Wait for completion.

Verify:

```sql
SHOW TABLES IN healthcare.bronze;
```

Expected key tables:

```text
claims
providers
diagnosis
cost
policies
```

Check row counts:

```sql
SELECT COUNT(*) AS claims_rows FROM healthcare.bronze.claims;
SELECT COUNT(*) AS providers_rows FROM healthcare.bronze.providers;
SELECT COUNT(*) AS diagnosis_rows FROM healthcare.bronze.diagnosis;
SELECT COUNT(*) AS cost_rows FROM healthcare.bronze.cost;
```

Expected baseline:

```text
claims: 1000
providers: 21
diagnosis: 6
cost: 6
```

Also check audit columns:

```sql
DESCRIBE TABLE healthcare.bronze.claims;
```

Expected columns include:

```text
_ingested_at
_source_file
_pipeline_run_id
```

If Bronze fails, fix it before moving to Silver.

---

## 16. Run Silver pipeline manually

Run:

```bash
databricks bundle run silver_pipeline -t dev --profile dev
```

or use UI:

```text
Jobs & Pipelines
→ Pipelines
→ [dev ...] Healthcare Silver Validation
→ Start
```

Verify:

```sql
SHOW TABLES IN healthcare.silver;
SHOW TABLES IN healthcare.quarantine;
```

Expected:

```text
trusted Silver tables exist
quarantine tables exist
```

Check at least:

```sql
SELECT COUNT(*) FROM healthcare.silver.claims;
SELECT COUNT(*) FROM healthcare.silver.providers;
```

If Silver fails, do not run Gold.

---

## 17. Run Gold pipeline manually

Run:

```bash
databricks bundle run gold_pipeline -t dev --profile dev
```

or use UI:

```text
Jobs & Pipelines
→ Pipelines
→ [dev ...] Healthcare Gold Features
→ Start
```

Verify:

```sql
SHOW TABLES IN healthcare.gold;
SELECT COUNT(*) FROM healthcare.gold.claim_features;
```

Expected:

```text
claim_features table exists
claim_features row count > 0
```

If Gold fails, do not run full `etl_ml_pipeline`.

---

## 18. Run the full ETL/ML job manually

Only run this after:

```text
setup_infrastructure passed
load_sample_data passed
Bronze pipeline passed
Silver pipeline passed
Gold pipeline passed
```

Run:

```bash
databricks bundle run etl_ml_pipeline -t dev --profile dev
```

This full job includes:

```text
run_bronze_pipeline
verify_bronze
observe_bronze
run_silver_pipeline
verify_silver
observe_silver
build_analytics
run_gold_pipeline
observe_gold
check_new_data
should_retrain
train_model OR skip_retraining
```

Because you changed `--tune` to `--no-tune`, any first-time training path should avoid Optuna tuning.

Verify full job result in:

```text
Databricks workspace
→ Workflows / Jobs
→ [dev ...] Healthcare ETL + ML Pipeline
→ Latest run
```

Expected:

```text
All required tasks pass
or retraining is skipped cleanly if check_new_data decides no retraining is needed
```

---

## 19. After every run: shut down and check cost

After each major run:

```text
setup_infrastructure
load_sample_data
Bronze
Silver
Gold
etl_ml_pipeline
```

go to:

```text
Databricks workspace
→ Compute
```

Make sure no clusters are still running.

Then check GCP:

```text
Google Cloud Console
→ Billing
→ Reports
→ Group by service
```

Watch for:

```text
Databricks
Google Cloud Marketplace
Compute Engine
Persistent Disk
Cloud Storage
Networking
```

If spend is rising too fast, stop. Do not continue to trigger/tuning tests.

---

## 20. Optional: test file-arrival trigger after manual success

Only do this after the manual full job succeeds.

### 20.1 Create a dedicated trigger-test target

Do **not** unpause your main dev target casually.

Add this to `databricks.yml` under `targets`:

```yaml
  dev_trigger_test:
    mode: development
    presets:
      trigger_pause_status: UNPAUSED
      jobs_max_concurrent_runs: 1
    workspace:
      profile: dev
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: healthcare
      model_version: "1"
```

Also ensure the same `max_workers: 1` smoke-test changes remain in place.

Validate:

```bash
databricks bundle validate -t dev_trigger_test --profile dev
```

Deploy:

```bash
databricks bundle deploy -t dev_trigger_test --profile dev
```

### 20.2 Trigger with a genuinely new file

File-arrival triggers only react to **new files**. Overwriting an existing filename does not trigger a run.

So do not overwrite:

```text
claims_1000.csv
```

Instead, copy a new uniquely named test file into a watched subdirectory if your pipeline supports it, for example:

```text
/Volumes/healthcare/bronze/raw_landing/claims/claims_1000_trigger_test_<timestamp>.csv
```

Important: if your Bronze Auto Loader expects exact filenames or manifests, do not use this trigger test yet. In that case, keep file-arrival trigger validation for later and continue using manual `bundle run`.

---

## 21. Optional: restore tuning after architecture works

Only after you confirm:

```text
DAB deploy works
setup job works
sample loader works
Bronze/Silver/Gold work
full job works manually
billing is acceptable
```

restore training:

```yaml
- --tune
```

in:

```text
services/ml/training/resources/training.job.yml
```

Then run:

```bash
databricks bundle validate -t dev --profile dev
databricks bundle deploy -t dev --profile dev
databricks bundle run etl_ml_pipeline -t dev --profile dev
```

Do this only once you intentionally want to pay for a full ML tuning run.

---

## 22. Cleanup options

### 22.1 Soft cleanup

Use this after a test session:

```text
Databricks workspace
→ Compute
→ Terminate all running clusters
```

Also check:

```text
Jobs & Pipelines
→ make sure no run is active
```

### 22.2 Destroy bundle resources

Only use this if you want to remove deployed jobs/pipelines/artifacts.

Run:

```bash
databricks bundle destroy -t dev --profile dev
```

Read the prompts carefully.

This can delete deployed jobs, pipelines, and artifacts. It is not a casual command.

### 22.3 Delete Databricks workspace / cancel Marketplace order

When fully done:

```text
Databricks Account Console
→ Workspaces
→ Delete workspace if no longer needed

Google Cloud Marketplace
→ Databricks
→ Manage orders
→ Cancel subscription/order if appropriate
```

Do not rely on deleting clusters alone if the Marketplace subscription remains active.

---

## 23. Troubleshooting

### Problem: `databricks bundle validate` fails

Do:

```bash
databricks bundle validate -t dev --profile dev --debug
```

Check:

```text
Wrong profile
Wrong workspace URL
YAML indentation
Invalid resource key
Unsupported runtime/node type
Missing included resource file
```

### Problem: catalog creation fails

You do not have Unity Catalog permissions.

Fix:

```text
Use a metastore admin/account admin
or get USE CATALOG + CREATE SCHEMA on healthcare
```

### Problem: schemas deploy but volume fails

Likely missing volume privilege or managed storage issue.

Check:

```sql
SHOW SCHEMAS IN healthcare;
SHOW VOLUMES IN healthcare.bronze;
```

If volume does not exist, inspect DAB deploy logs.

### Problem: load_sample_data says missing volume

Run:

```sql
SHOW VOLUMES IN healthcare.bronze;
```

If missing, DAB volume resource did not deploy or permissions failed.

### Problem: Bronze has zero rows

Check:

```python
dbutils.fs.ls("/Volumes/healthcare/bronze/raw_landing/claims")
```

If files are missing, rerun:

```bash
databricks bundle run load_sample_data -t dev --profile dev
```

### Problem: file-arrival trigger does not fire

Remember:

```text
dev mode pauses triggers by default
explicit preset may pause triggers
overwriting same filename does not trigger
wildcards are not allowed
path must be a UC volume/external location path
```

Use manual runs until the pipeline itself is proven.

### Problem: training takes too long

Confirm `training.job.yml` uses:

```yaml
- --no-tune
```

If it still takes long, stop the run and test training separately after Gold is verified.

### Problem: cost rising unexpectedly

Immediately:

```text
1. Stop active Databricks job runs.
2. Terminate all clusters.
3. Check GCP Billing → Reports.
4. Check Google Marketplace → Databricks order.
5. Keep trigger paused.
```

---

## 24. Final first-run checklist

Before first deploy:

```text
[ ] Separate GCP project created
[ ] Billing alerts configured
[ ] Databricks Marketplace subscription active
[ ] Classic workspace created
[ ] Unity Catalog enabled
[ ] healthcare catalog exists
[ ] databricks CLI profile dev works
[ ] repo branch = integration-paid-smoke
[ ] dev preset trigger_pause_status: PAUSED added
[ ] job max_workers changed 4 -> 1
[ ] Bronze max_workers changed 4 -> 1
[ ] Silver max_workers changed 4 -> 1
[ ] Gold max_workers changed 4 -> 1
[ ] --tune changed to --no-tune
[ ] bundle validate passes
[ ] bundle summary shows expected jobs/pipelines
```

Execution order:

```text
[ ] databricks bundle deploy -t dev --profile dev
[ ] databricks bundle run setup_infrastructure -t dev --profile dev
[ ] databricks bundle run load_sample_data -t dev --profile dev
[ ] databricks bundle run bronze_pipeline -t dev --profile dev
[ ] verify Bronze tables/counts
[ ] databricks bundle run silver_pipeline -t dev --profile dev
[ ] verify Silver/quarantine tables
[ ] databricks bundle run gold_pipeline -t dev --profile dev
[ ] verify healthcare.gold.claim_features
[ ] databricks bundle run etl_ml_pipeline -t dev --profile dev
[ ] terminate/check compute
[ ] check GCP billing
```

Only after this:

```text
[ ] test file-arrival trigger
[ ] restore --tune
[ ] increase max_workers if needed
[ ] consider prod target
```

---

## 25. Recommended commit message for the smoke-test branch

If you decide to commit the temporary safety changes:

```bash
git add databricks.yml \
  services/ml/training/resources/training.job.yml \
  services/etl/bronze/resources/bronze.pipeline.yml \
  services/etl/silver/resources/silver.pipeline.yml \
  services/etl/gold/resources/gold.pipeline.yml

git commit -m "chore: add cost-controlled Databricks dev smoke-test settings"
```

Do not merge this into main blindly. Treat it as a paid-validation branch.

---

## 26. The clean mental model

Use this as the final rule:

```text
First prove deployment.
Then prove setup.
Then prove data load.
Then prove each pipeline individually.
Then prove the full job manually.
Then prove automation.
Then prove tuning/performance.
```

Anything else increases cost and debugging chaos.
