# Databricks Integration Deployment Guide

## Purpose

Use this guide to understand how the Databricks integration in this repo works end to end, and how to validate, deploy, and run it with the Databricks CLI.

This guide is the operator-facing companion to [docs/integration_plan.md](/C:/Users/Krish/Desktop/projects/homeprojectabacus/docs/integration_plan.md). The integration plan explains the design; this runbook explains how to use it.

## GCP migration summary

This repo is now targeted at Databricks on Google Cloud rather than Databricks on AWS.

The practical deployment difference is not the Spark/Python code. The practical difference is the platform envelope around it:

- Databricks account and workspace are created through the Google Cloud Marketplace / GCP account flow.
- The workspace project, quotas, IAM permissions, VPC choice, and GCS buckets must be ready before bundle deployment.
- Unity Catalog storage should use GCS and Unity Catalog volumes, not DBFS root or S3.
- Bundle cluster settings must use GCP node types and `gcp_attributes`, not AWS node types or `aws_attributes`.

Current repo alignment:

- [databricks.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/databricks.yml) now defaults `node_type_id` to `n1-standard-4`.
- The Bronze, Silver, and Gold pipeline resources now use `gcp_attributes.availability: PREEMPTIBLE_WITH_FALLBACK_GCP`.
- The application tables and model registry names still use the same Unity Catalog namespace: `healthcare.bronze`, `healthcare.silver`, `healthcare.gold`, `healthcare.analytics`, and `healthcare.ml`.

## What this integration is

The repo now defines a Databricks Asset Bundle that deploys:

- Unity Catalog schemas
- a managed Bronze landing volume
- three Lakeflow pipelines: Bronze, Silver, Gold
- one setup job
- one sample-data loading job
- one production ETL + ML orchestration job
- notebook and script entry points for verification, observability, analytics, and retraining

The bundle root is [databricks.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/databricks.yml).

The main orchestration job is [services/ml/training/resources/training.job.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/ml/training/resources/training.job.yml).

## Step 0: Enable Databricks on GCP

Start here if you do not already have a usable Databricks on GCP workspace.

### 0.1 Choose the workspace type

Use a **classic workspace** for this project unless you intentionally decide to simplify the bundle for a fully serverless demo.

Reason: this bundle currently defines classic job clusters and Lakeflow pipeline clusters. A serverless-first trial workspace is useful for initial learning, but a classic workspace is the safer target for this repo's explicit cluster/runtime/node configuration.

Databricks documents two workspace types on GCP:

- serverless workspaces, which come preconfigured with serverless compute and default storage
- classic workspaces, which deploy storage and compute resources in your existing Google Cloud account while still allowing serverless compute where supported

### 0.2 Create or select the Google Cloud project

In Google Cloud, prepare:

- a billing-enabled GCP project
- a Google Cloud organization or Cloud Identity / Google Workspace identity
- enough regional Compute Engine quota for the node type and autoscaling range you plan to use
- admin access for whoever will create the Databricks workspace

For a paid or production-style setup, prefer a dedicated workspace project such as:

```text
abacus-databricks-dev
abacus-databricks-prod
```

For a training/demo workspace, one project is acceptable, but keep production customer data out of the workspace root buckets.

### 0.3 Subscribe through Google Cloud Marketplace

If this is a free trial or standard marketplace subscription:

1. Open the Databricks listing in Google Cloud Marketplace.
2. Select the GCP project associated with the billing account you want to use.
3. Click **Subscribe**.
4. Set the billing account.
5. Accept the terms.
6. Click **Sign up with Databricks** and authenticate with the Google account that should own the Databricks account.
7. Complete the Databricks account setup.

If your company has a Databricks contract or private offer, do not use the generic free-trial path. Use the private offer / account-team path instead.

### 0.4 Confirm account-admin access

Open the GCP account console:

```text
https://accounts.gcp.databricks.com
```

Confirm you can:

- create workspaces
- manage account identities
- enable or attach Unity Catalog
- view the account ID

If you cannot access the account console, stop. Workspace-level admin is not enough for the initial platform setup.

### 0.5 Create the classic workspace

In the Databricks account console:

1. Go to **Workspaces**.
2. Click **Create workspace**.
3. Enter a workspace name, for example `abacus-dev`.
4. Select the GCP region.
5. Enter the GCP project ID.
6. Choose networking:
   - **Databricks-managed VPC** for the fastest working setup.
   - **Customer-managed VPC** only if you already know the subnet, firewall, Private Service Connect, and Shared VPC requirements.
7. Click **Create workspace**.
8. Wait until the workspace status is `Running`.
9. Open the workspace from the account console.

The workspace creator needs the required GCP permissions on the workspace project. Databricks recommends owner-level permissions for workspace creation, and states those permissions are used for validation, service enablement, and provisioning.

### 0.6 Secure the workspace buckets

After classic workspace creation, Databricks creates workspace GCS buckets in the project, including root/system storage. Treat these as Databricks platform buckets, not production data landing zones.

Do not store this project's source datasets or operational landing files in DBFS root. This repo expects governed Unity Catalog storage at:

```text
/Volumes/healthcare/bronze/raw_landing/
```

### 0.7 Enable Unity Catalog and create the project catalog

This repo assumes Unity Catalog.

In the account console / workspace:

1. Confirm the workspace is attached to a Unity Catalog metastore.
2. Create or request the catalog:

```sql
CREATE CATALOG IF NOT EXISTS healthcare;
```

3. Grant the bundle deployer or service principal enough access to create schemas, volumes, jobs, and pipelines.

The bundle owns schemas and the managed Bronze landing volume, but the catalog itself is still an account/platform-admin concern.

### 0.8 Decide managed volume vs external GCS volume

Use the managed Bronze landing volume for the current demo deployment:

```text
healthcare.bronze.raw_landing
/Volumes/healthcare/bronze/raw_landing/
```

Use an external GCS volume only if files will be landed by systems outside Databricks into an existing bucket. In that case:

1. Create a GCS bucket/path for landing data.
2. Create a Unity Catalog storage credential for GCP.
3. Create an external location pointing at `gs://<bucket>/<path>`.
4. Grant `CREATE EXTERNAL VOLUME` and volume read/write privileges to the right principals.
5. Bind the external location to the workspace if you need workspace isolation.

For this repo's first GCP deployment, the managed volume is simpler and matches [resources/volumes/volumes.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/resources/volumes/volumes.yml).

### 0.9 Configure file events if production files arrive in GCS

If you later switch to external GCS landing with Auto Loader or file-arrival triggers over external locations, configure GCP file events and Pub/Sub permissions for the storage credential.

This is not required for the default managed-volume demo path, but it matters for production-style ingestion where external systems land files in GCS and Databricks reacts to file arrival.

### 0.10 Prepare local CLI authentication

Install or update Databricks CLI v0.218.0 or later, then configure a GCP workspace profile:

```bash
databricks auth login --host https://<workspace-id>.<region>.gcp.databricks.com --profile gcp-dev
databricks auth profiles
databricks auth env --profile gcp-dev
```

Inside this Codex environment, prefix commands with `rtk`:

```bash
rtk databricks auth profiles
rtk databricks auth env --profile gcp-dev
```

## How the integration works

### 1. Infrastructure layer

Before any pipeline runs, the bundle owns the base Unity Catalog objects:

- schemas from [resources/schemas/schemas.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/resources/schemas/schemas.yml)
- the Bronze landing volume from [resources/volumes/volumes.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/resources/volumes/volumes.yml)

The setup job then does the remaining imperative setup:

- applies grants through [src/notebooks/grants.ipynb](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/notebooks/grants.ipynb)
- creates `healthcare.ml.retrain_decisions` through [src/scripts/setup_retrain_decisions.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/scripts/setup_retrain_decisions.py)

### 2. Data layer

The data path is a standard Bronze -> Silver -> Gold flow:

- Bronze pipeline ingests raw files from `/Volumes/<catalog>/<bronze_schema>/raw_landing/`
- Silver pipeline validates and cleans trusted data
- Gold pipeline builds `claim_features` for ML

Each stage is a DAB pipeline resource:

- [services/etl/bronze/resources/bronze.pipeline.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/etl/bronze/resources/bronze.pipeline.yml)
- [services/etl/silver/resources/silver.pipeline.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/etl/silver/resources/silver.pipeline.yml)
- [services/etl/gold/resources/gold.pipeline.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/etl/gold/resources/gold.pipeline.yml)

Each pipeline publishes its event log into Unity Catalog so observability can read durable event-log tables instead of depending on per-run ownership.

### 3. Verification layer

The recurring verification tasks are Python scripts, not notebooks:

- [src/scripts/verify_bronze.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/scripts/verify_bronze.py)
- [src/scripts/verify_silver.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/scripts/verify_silver.py)

These are real DAG gates:

- Silver does not run until Bronze verification passes
- Gold and analytics do not run until Silver verification passes

If verification fails, the pipeline stops downstream data work.

### 4. Observability layer

Observability is best-effort and non-gating.

The job runs [src/scripts/build_observability.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/scripts/build_observability.py) three times:

- once for Bronze
- once for Silver
- once for Gold

The shared logic lives in [src/analytics/observability_assets.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/analytics/observability_assets.py).

Each run writes the same four `ops_pipeline_*` tables and tags rows with `pipeline_stage` so stage-specific rows can coexist:

- `ops_pipeline_updates`
- `ops_expectation_metrics`
- `ops_user_actions`
- `ops_latest_failures`

Observability tasks depend on pipeline completion, not verifier completion. That is intentional: if a verifier fails, you still want the event log captured.

### 5. Analytics layer

After Silver verification passes, the job runs [src/scripts/build_analytics.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/scripts/build_analytics.py).

That builds and persists operator-facing and dashboard-facing tables from the trusted pipeline outputs.

### 6. Retraining gate

After Gold finishes, the job runs [src/notebooks/check_new_data.ipynb](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/notebooks/check_new_data.ipynb).

That notebook is intentionally thin. It calls [src/ml/retrain_gate.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/ml/retrain_gate.py), which decides whether retraining is needed based on:

- current Gold row count
- current Gold version
- current Gold fingerprint
- champion model fingerprint
- champion feature column metadata

The rules are:

- if no champion exists -> retrain
- if Gold fingerprint changed -> retrain
- if feature columns changed -> retrain
- if champion feature metadata is missing -> retrain
- if Gold has zero rows -> fail the run
- otherwise -> skip retraining

The notebook writes one audit row to `healthcare.ml.retrain_decisions` and publishes Databricks task values.

### 7. Conditional ML training

The next task is a Databricks `condition_task`:

- if `should_retrain == "true"` -> run [scripts/train_denial_model.py](/C:/Users/Krish/Desktop/projects/homeprojectabacus/scripts/train_denial_model.py)
- if `should_retrain == "false"` -> run [src/notebooks/skip_retraining.ipynb](/C:/Users/Krish/Desktop/projects/homeprojectabacus/src/notebooks/skip_retraining.ipynb)

The training script now accepts Databricks-passed CLI args instead of hardcoding `--tune`, and it logs the metadata the retrain gate depends on.

## Jobs in this repo

There are three distinct operational entry points.

### `setup_infrastructure`

Use this once per workspace or when permissions/base objects need to be re-established.

Definition:

- [services/infrastructure/setup/resources/setup_infrastructure.job.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/infrastructure/setup/resources/setup_infrastructure.job.yml)

### `load_sample_data`

Use this only for dev/demo/bootstrap scenarios. It copies the checked-in fixture data into the Bronze landing volume.

Definition:

- [services/infrastructure/load_sample_data/resources/load_sample_data.job.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/services/infrastructure/load_sample_data/resources/load_sample_data.job.yml)

### `etl_ml_pipeline`

This is the production orchestration job. It is triggered by file arrival and handles:

- Bronze refresh
- Bronze verification
- Bronze observability
- Silver refresh
- Silver verification
- Silver observability
- analytics build
- Gold refresh
- Gold observability
- retrain decision
- conditional training or skip

## Before you deploy

### Workspace prerequisites

You need a workspace that can support the bundle shape defined here.

Required:

- Unity Catalog enabled
- a usable catalog for this project, typically `healthcare`
- ability to create schemas, volumes, jobs, and pipelines
- Databricks CLI configured locally
- a GCP node type available in the workspace region; this repo defaults to `n1-standard-4`

Recommended:

- a workspace that supports classic job clusters for this bundle shape
- DBR 17.3 LTS ML for job tasks that load MLflow models; this repo defaults `spark_version` to `17.3.x-cpu-ml-scala2.13`
- enough GCP quota for one driver plus up to four workers per running job or pipeline cluster

### Current repo-specific caveat

During earlier validation on April 29, 2026, the `testing` profile pointed to a workspace that:

- was serverless-only for jobs
- did not contain the `healthcare` catalog

That means bundle validation succeeded, but actual deploy failed in that workspace for environmental reasons. If you hit the same behavior, it is a workspace mismatch, not a local repo syntax problem.

For GCP, also confirm you are not deploying stale AWS-shaped bundle config. The pipeline resources must use `gcp_attributes`, and the target node type must be a valid GCP node type.

Also confirm the exact Spark runtime key in the target workspace:

```bash
databricks clusters spark-versions --profile gcp-dev
```

If the workspace exposes the 17.3 LTS ML runtime under a different key, override the bundle variable:

```bash
databricks bundle validate -t dev --profile gcp-dev --var=spark_version=<workspace-runtime-key>
databricks bundle deploy -t dev --profile gcp-dev --var=spark_version=<workspace-runtime-key>
```

## Databricks CLI setup

### 1. Confirm your CLI profile

Check which profiles exist:

```bash
databricks auth profiles
```

Inspect the profile you want to use:

```bash
databricks auth env --profile gcp-dev
```

If you are running commands inside Codex in this repo, prepend `rtk`:

```bash
rtk databricks auth profiles
rtk databricks auth env --profile gcp-dev
```

### 2. Understand bundle targets vs auth profiles

This repo defines two bundle targets in [databricks.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/databricks.yml):

- `dev`
- `prod`

Those targets control bundle behavior such as mode, variables, and workspace root path.

Your CLI profile controls authentication and the actual workspace you are talking to.

In practice, you will usually use both:

```bash
databricks bundle validate -t dev --profile gcp-dev
```

That means:

- use the bundle's `dev` target settings
- authenticate with the `gcp-dev` Databricks CLI profile

For the new GCP workspace, prefer profile names like:

```bash
databricks bundle validate -t dev --profile gcp-dev
databricks bundle validate -t prod --profile gcp-prod --var=model_version=1
```

## Step-by-step deployment flow

### Step 1: Validate local code first

Run the local manifest validator:

```bash
python -m src.framework.validate_manifests
```

This checks:

- service registry -> manifest existence
- manifest schema
- resource cross-references
- dependency graph cycles
- expected job tasks
- health-check entrypoint paths
- local YAML path resolution

If you use `uv`, the stricter local form is:

```bash
uv run python -m src.framework.validate_manifests
```

### Step 2: Validate the bundle

Validate the dev target:

```bash
databricks bundle validate -t dev --profile gcp-dev
```

Validate the prod target:

```bash
databricks bundle validate -t prod --profile gcp-prod --var=model_version=1
```

What success looks like:

- the CLI prints bundle name, target, workspace path
- the command ends with `Validation OK!`

### Step 3: Check the resolved resources

Use `bundle summary` to confirm the resources the target sees:

```bash
databricks bundle summary -t dev --profile gcp-dev
```

This is the fastest way to answer:

- did the `include:` globs resolve correctly?
- which jobs, pipelines, schemas, and volumes will deploy?
- what names will they have in the workspace?

If `Resources:` is empty or missing expected entries, stop and fix the YAML layout before deploying.

### Step 4: Deploy the bundle

Deploy dev:

```bash
databricks bundle deploy -t dev --profile gcp-dev
```

Deploy prod:

```bash
databricks bundle deploy -t prod --profile prod --var=model_version=<registered-model-version>
```

Notes:

- this repo already sets `bundle.engine: direct`, so deploy does not need Terraform
- prod intentionally requires an explicit `model_version`
- the bundle identity is tied to workspace, bundle name, target, and `workspace.root_path`

### Step 5: Run setup once

After deploy, run the setup job:

```bash
databricks bundle run -t dev --profile gcp-dev setup_infrastructure
```

This should:

- apply grants
- create `healthcare.ml.retrain_decisions`

### Step 6: Load sample data in dev if needed

For a dev/demo workspace, run:

```bash
databricks bundle run -t dev --profile gcp-dev load_sample_data
```

Use this only when you want to seed the checked-in fixture data into the Bronze landing volume.

### Step 7: Run the main orchestration job manually once

Before relying on file arrival, manually trigger the main job:

```bash
databricks bundle run -t dev --profile gcp-dev etl_ml_pipeline
```

Use this first manual run to verify:

- Bronze pipeline refresh works
- Silver verifier gates correctly
- Gold runs
- observability tasks emit rows
- retrain gate behaves as expected

### Step 8: Validate pipelines directly if needed

If you want to validate a pipeline graph without running a full refresh:

```bash
databricks bundle run -t dev --profile gcp-dev --validate-only bronze_pipeline
databricks bundle run -t dev --profile gcp-dev --validate-only silver_pipeline
databricks bundle run -t dev --profile gcp-dev --validate-only gold_pipeline
```

## How to operate this in development

Recommended dev sequence:

1. `databricks auth profiles`
2. `python -m src.framework.validate_manifests`
3. `databricks bundle validate -t dev --profile gcp-dev`
4. `databricks bundle summary -t dev --profile gcp-dev`
5. `databricks bundle deploy -t dev --profile gcp-dev`
6. `databricks bundle run -t dev --profile gcp-dev setup_infrastructure`
7. `databricks bundle run -t dev --profile gcp-dev load_sample_data`
8. `databricks bundle run -t dev --profile gcp-dev etl_ml_pipeline`

## How to operate this in production

Recommended prod sequence:

1. ensure the target workspace has the `healthcare` catalog and the required permissions
2. resolve the MLflow model version you want prod to serve or pin against
3. validate prod:

```bash
databricks bundle validate -t prod --profile prod --var=model_version=<version>
```

4. review resolved resources:

```bash
databricks bundle summary -t prod --profile prod
```

5. deploy:

```bash
databricks bundle deploy -t prod --profile prod --var=model_version=<version>
```

6. run setup once if this workspace is new:

```bash
databricks bundle run -t prod --profile prod setup_infrastructure
```

After that, production should normally rely on file arrival rather than manual job runs.

## How to know the integration is healthy

### Local signals

- `python -m src.framework.validate_manifests` passes
- `databricks bundle validate` passes
- `databricks bundle summary` shows the expected jobs, pipelines, schemas, and volume

### Workspace signals

After a successful run, confirm:

- Bronze, Silver, and Gold pipelines exist
- `etl_ml_pipeline` exists
- `healthcare.ml.retrain_decisions` exists
- observability tables under `healthcare.analytics` exist

Useful checks:

```sql
SELECT pipeline_stage, COUNT(*)
FROM healthcare.analytics.ops_pipeline_updates
GROUP BY pipeline_stage;
```

```sql
SELECT decided_at, should_retrain, reason
FROM healthcare.ml.retrain_decisions
ORDER BY decided_at DESC
LIMIT 10;
```

## Troubleshooting

### `databricks bundle validate` passes but `bundle deploy` fails

This usually means the workspace cannot satisfy the resource shape.

Examples we already hit:

- workspace only supports serverless jobs, but the bundle defines classic job clusters
- workspace does not contain the `healthcare` catalog
- GCP project does not have enough regional quota for the selected node type
- node type is not valid for the workspace's GCP region
- stale AWS fields such as `aws_attributes` remain in a cluster definition

Validation is a config check. Deployment is where workspace capability mismatches show up.

### Deploy fails with a GCP cluster attribute or node-type error

Check:

- [databricks.yml](/C:/Users/Krish/Desktop/projects/homeprojectabacus/databricks.yml) has a GCP node type such as `n1-standard-4`.
- pipeline resources use `gcp_attributes.availability`, not `aws_attributes.availability`.
- the selected GCP region supports the node family.
- the GCP project quota covers the configured autoscale range.

If preemptible capacity is the problem, temporarily switch `PREEMPTIBLE_WITH_FALLBACK_GCP` to `ON_DEMAND_GCP` and redeploy.

### Deploy fails with Terraform signature or download issues

This repo already sets:

```yaml
bundle:
  engine: direct
```

That avoids the Terraform dependency path for new deployments.

### Setup succeeds but retrain decision fails later

Check:

- Gold table exists and is non-empty
- the champion model alias exists if you expect skip behavior
- MLflow metadata was logged during training

If `feature_columns.json` is missing from the champion run, the retrain gate now forces retraining instead of silently skipping.

### Observability tables do not show all three stages

Check:

- Bronze, Silver, and Gold `observe_*` tasks all ran
- each pipeline resource publishes an event log table
- `build_observability.py` was called with the correct `--published-event-log-table` and `--pipeline-stage`

### `bundle summary` looks right but job execution still fails

Then the issue is usually one of:

- cluster/runtime mismatch
- missing catalog or privileges
- missing ML dependencies on the compute runtime
- notebook/script path resolved at deploy time but runtime object permissions are wrong

## Command reference for this repo

Use plain `databricks ...` in your own shell, or `rtk databricks ...` inside this Codex environment.

Profile inspection:

```bash
databricks auth profiles
databricks auth env --profile gcp-dev
```

Bundle validation:

```bash
databricks bundle validate -t dev --profile gcp-dev
databricks bundle validate -t prod --profile gcp-prod --var=model_version=1
```

Bundle summary:

```bash
databricks bundle summary -t dev --profile gcp-dev
```

Deploy:

```bash
databricks bundle deploy -t dev --profile gcp-dev
```

Run setup:

```bash
databricks bundle run -t dev --profile gcp-dev setup_infrastructure
```

Load fixtures:

```bash
databricks bundle run -t dev --profile gcp-dev load_sample_data
```

Run the production graph manually:

```bash
databricks bundle run -t dev --profile gcp-dev etl_ml_pipeline
```

Pipeline graph validation:

```bash
databricks bundle run -t dev --profile gcp-dev --validate-only bronze_pipeline
```

## References

- [docs/integration_plan.md](/C:/Users/Krish/Desktop/projects/homeprojectabacus/docs/integration_plan.md)
- [Databricks on GCP free trial and Marketplace signup](https://docs.databricks.com/gcp/en/getting-started/free-trial)
- [Create a classic Databricks workspace on GCP](https://docs.databricks.com/gcp/en/admin/workspace/create-workspace)
- [Required GCP permissions for workspace creation](https://docs.databricks.com/gcp/en/admin/cloud-configurations/gcp/permissions)
- [Unity Catalog volumes on GCP](https://docs.databricks.com/gcp/en/volumes)
- [Create a GCS external location in Unity Catalog](https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/external-locations-gcs)
- [Declarative Automation Bundles on GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles)
- [Databricks bundle resource reference for GCP cluster attributes](https://docs.databricks.com/gcp/en/dev-tools/bundles/resources)
- [Databricks Apps on GCP](https://docs.databricks.com/gcp/en/dev-tools/databricks-apps/)
- [Models in Unity Catalog on GCP](https://docs.databricks.com/gcp/en/machine-learning/manage-model-lifecycle)
