# HomeProjectAbacus Databricks-on-GCP Deployment Notes: Wins, Losses, Fixes, and Final Learnings

**Project:** `HomeProjectAbacus`  
**Branch:** `integration`  
**Target:** Databricks on GCP, classic-cluster deployment validation  
**Date:** 2026-04-30  
**Purpose:** Capture everything learned while validating the Databricks implementation plan on a paid GCP-backed workspace, including what worked, what failed, why it failed, and what the final working direction is.

---

## 1. Executive summary

The core objective was to test the real implementation plan for `HomeProjectAbacus`, not merely prove that notebooks work on Databricks Free Edition.

Databricks Free Edition was useful for early testing, but it was not enough because the implementation plan depends on real workspace features such as:

- Databricks Asset Bundles.
- Unity Catalog schemas and volumes.
- Classic job clusters.
- Classic Lakeflow / Declarative Pipeline compute.
- Pipeline tasks inside workflows.
- GCP-backed Unity Catalog storage.
- Full orchestration through jobs and pipelines.

The journey exposed three major infrastructure blockers:

1. **GCP SSD quota was too low for Databricks classic compute.**
2. **The first `healthcare` catalog was created on Databricks default storage, which was wrong for classic compute.**
3. **The `e2-highmem-2` single-node setup still exceeded GCP `SSD_TOTAL_GB` quota, even with `enable_elastic_disk: false`.**

The breakthrough came from switching to an **N2-based node with Local SSD**, because Databricks can avoid remote SSD allocation when Local SSD is present.

The final major win: the cluster finally started, Spark started, and the job executed. It then failed due to code issues, which is actually progress because the cloud/bootstrap layer was no longer the blocker.

---

## 2. Final high-level status

| Area | Status | Notes |
|---|---|---|
| Databricks workspace | Working enough to deploy bundles | Paid GCP workspace required. |
| Databricks Asset Bundle | Deployable | Bundle structure exists on `integration`. |
| Unity Catalog catalog | Fixed | `healthcare` now points to your GCS bucket, not Databricks default storage. |
| Classic job compute | Working with N2 + Local SSD | E2 failed due to `SSD_TOTAL_GB`; N2 started Spark. |
| Single-node config | Correct | `num_workers: 0`, `singleNode`, `ResourceClass: SingleNode`. |
| Elastic disk | Disabled for job clusters | Confirmed in failed cluster JSON for E2 attempt. |
| Lakeflow pipeline compute | Not fully proven yet | Needs same N2 + Local SSD-style config. |
| Job run | Started and reached code execution | Failed due to code issue, not infra issue. |
| Cost safety | Still manual vigilance required | Job clusters should terminate, but pipeline clusters in dev mode may stay alive. |

---

## 3. What we initially wanted to test

The goal was **not** just this:

```text
Can Bronze ingestion work on Databricks?
```

That had already been tested on Databricks Free Edition.

The real goal was this:

```text
Can the integration branch implementation plan deploy and run on a real GCP-backed Databricks workspace using classic clusters?
```

That means validating the actual production-like structure:

```text
Databricks Asset Bundle
  -> schemas
  -> volumes
  -> setup_infrastructure job
  -> load_sample_data job
  -> Bronze pipeline
  -> Silver pipeline
  -> Gold pipeline
  -> ETL + ML orchestration job
```

---

## 4. Important repo facts from the `integration` branch

The relevant branch is:

```bash
git checkout integration
```

The `integration` branch contains the actual Databricks Asset Bundle configuration. The top-level bundle file is:

```text
databricks.yml
```

The bundle includes:

```yaml
include:
  - resources/schemas/schemas.yml
  - resources/volumes/volumes.yml
  - services/*/resources/*.yml
  - services/*/*/resources/*.yml
  - services/*/*/*/resources/*.yml
```

The major resources are:

```text
resources/schemas/schemas.yml
resources/volumes/volumes.yml

services/infrastructure/setup/resources/setup_infrastructure.job.yml
services/infrastructure/load_sample_data/resources/load_sample_data.job.yml

services/etl/bronze/resources/bronze.pipeline.yml
services/etl/silver/resources/silver.pipeline.yml
services/etl/gold/resources/gold.pipeline.yml

services/ml/training/resources/training.job.yml
```

The important jobs are:

```text
setup_infrastructure
load_sample_data
etl_ml_pipeline
```

The important pipelines are:

```text
bronze_pipeline
silver_pipeline
gold_pipeline
```

---

## 5. First major lesson: Databricks Free Edition is not enough for this validation

Databricks Free Edition was useful for proving some notebook/data logic, but it does not validate the real implementation plan because the plan requires:

- Classic clusters.
- Classic job compute.
- Lakeflow pipeline resources.
- Real Unity Catalog storage.
- Bundle deployment.
- GCP resource quotas.
- GCP VM / disk provisioning.

So the decision to move to paid Databricks on GCP was justified.

However, the paid path exposed real cloud infra problems that Free Edition hides.

---

## 6. Budget and billing lesson

Before running anything substantial, budget alerts were needed:

```text
₹100
₹500
₹1,000
₹2,000
₹5,000
```

Important: **budget alerts do not cap spend.** They only notify. Google Cloud budgets are warning systems, not hard kill switches.

The practical cost rule became:

```text
Run one thing at a time.
Avoid all-purpose compute.
Use job compute.
Use single-node.
Use no workers.
Use no Photon.
Use --no-tune for the first ML smoke test.
Manually check Compute after every run.
```

---

## 7. Why all-purpose compute was the wrong path

Manual compute creation in the Databricks UI was tempting because it looked like a quick way to test whether compute worked.

But it was not the best path.

Manual all-purpose compute is risky because:

- It can stay running.
- It is not the same as job compute.
- It does not validate the Databricks Asset Bundle.
- It does not validate pipeline tasks.
- It can still hit the same GCP quota errors.
- It increases cost risk.

The correct path became:

```bash
databricks bundle validate -t dev --profile dev
databricks bundle deploy -t dev --profile dev
databricks bundle run setup_infrastructure -t dev --profile dev
```

This uses lifecycle-managed job compute instead of a reusable all-purpose cluster.

---

## 8. Photon decision

Photon was disabled for the first paid test.

Reason:

- The datasets are tiny.
- The goal is deployment validation, not performance benchmarking.
- Photon can restrict compute options.
- Photon can change billing behavior.
- Classic compatibility matters more than acceleration right now.

The target setting became:

```yaml
photon: false
```

for Bronze, Silver, and Gold pipelines.

---

## 9. First compute choice: `e2-highmem-2`

The lowest visible compute option was:

```text
e2-highmem-2
```

This looked attractive because it has:

```text
2 vCPU
16 GB RAM
```

It was reasonable to try because the project’s data is small.

But it failed with:

```text
GCP_RESOURCE_QUOTA_EXCEEDED
Quota 'SSD_TOTAL_GB' exceeded. Limit: 250.0 in region us-east1.
```

At first, this looked like a normal quota issue. But it turned out to be deeper.

---

## 10. GCP quota investigation

The relevant GCP quota was:

```text
SSD_TOTAL_GB
```

In the Google Cloud Console, this appears as:

```text
Persistent disk SSD (GB)
```

The command output showed:

```text
SSD_TOTAL_GB limit = 250 GB
SSD_TOTAL_GB usage = 0 GB
```

This was important because it proved the issue was **not existing disk usage**.

It meant:

```text
Databricks was requesting more than the entire 250 GB SSD quota during cluster launch.
```

Multiple regions were checked:

```text
us-east1
us-central1
us-east4
us-west1
us-west2
```

They all effectively had:

```text
SSD_TOTAL_GB = 250 GB
usage = 0 GB
```

So switching among those US regions would not help.

The quota increase request was blocked with a message similar to:

```text
Based on your service usage history, you are not eligible for a quota increase at this time.
```

That meant the current project/billing account was too new or too restricted for self-service quota increase.

---

## 11. Why 250 GB was not enough

Databricks on GCP provisions multiple disks per compute instance.

Important Databricks behavior:

```text
30 GB boot disk
150 GB container root volume
remote SSD when autoscaling local storage is enabled
```

Remote SSD starts at around:

```text
80 GB
```

So the rough math looked like:

```text
30 GB + 150 GB + 80 GB = 260 GB
```

That is just above the project quota:

```text
250 GB
```

That explained why even a “small” single-node cluster could fail.

---

## 12. Single-node setup

The project was moved to single-node compute.

For job clusters, the correct single-node pattern became:

```yaml
new_cluster:
  spark_version: ${var.spark_version}
  node_type_id: ${var.node_type_id}
  driver_node_type_id: ${var.node_type_id}
  num_workers: 0
  enable_elastic_disk: false

  spark_conf:
    spark.databricks.cluster.profile: singleNode
    spark.master: local[*]

  custom_tags:
    ResourceClass: SingleNode
    Project: homeprojectabacus
    Environment: ${bundle.target}
```

Key meaning:

```text
num_workers: 0
```

means:

```text
driver only
no worker VM
one VM total
```

This was much cheaper than:

```text
1 worker = driver + worker = 2 VMs
```

---

## 13. Job files updated

The following job files were updated to single-node:

```text
services/infrastructure/setup/resources/setup_infrastructure.job.yml
services/infrastructure/load_sample_data/resources/load_sample_data.job.yml
services/ml/training/resources/training.job.yml
```

For the ML training job, the first smoke test should use:

```yaml
- --no-tune
```

instead of:

```yaml
- --tune
```

Reason:

- `--tune` can run Optuna trials.
- That is unnecessary for deployment validation.
- The first paid run should only prove orchestration and execution.

---

## 14. Pipeline files also needed single-node changes

The job files were not enough.

Bronze, Silver, and Gold pipelines create their own pipeline compute, separate from the job cluster.

Therefore these files also needed changes:

```text
services/etl/bronze/resources/bronze.pipeline.yml
services/etl/silver/resources/silver.pipeline.yml
services/etl/gold/resources/gold.pipeline.yml
```

The pipeline cluster block needed:

```yaml
clusters:
  - label: default
    node_type_id: ${var.node_type_id}
    driver_node_type_id: ${var.node_type_id}
    num_workers: 0

    spark_conf:
      spark.databricks.cluster.profile: singleNode
      spark.master: local[*]

    custom_tags:
      ResourceClass: SingleNode
      Project: homeprojectabacus
      Environment: ${bundle.target}
```

---

## 15. `enable_elastic_disk` does not work in pipeline resources

We tried to set:

```yaml
enable_elastic_disk: false
```

inside the pipeline cluster blocks.

Bundle validation produced warnings:

```text
unknown field: enable_elastic_disk
at resources.pipelines.<pipeline>.clusters[0]
```

So the final rule became:

```text
Job clusters:
  enable_elastic_disk: false is allowed.

Pipeline clusters:
  do not use enable_elastic_disk.
```

This is important because the pipeline resources have a more restricted schema than normal cluster resources.

---

## 16. Bundle validation warning about single-node pipelines

The validator warned:

```text
Single node cluster is not correctly configured.
num_workers should be 0 only for single-node clusters.
```

It expected these fields:

```yaml
spark_conf:
  spark.databricks.cluster.profile: singleNode
  spark.master: local[*]

custom_tags:
  ResourceClass: SingleNode
```

So the fix was to add full single-node config to the pipeline cluster blocks, not just `num_workers: 0`.

---

## 17. First failed E2 cluster confirmed the config was applied

The failed cluster JSON showed:

```json
"cluster_source": "JOB",
"node_type_id": "e2-highmem-2",
"driver_node_type_id": "e2-highmem-2",
"num_workers": 0,
"enable_elastic_disk": false,
"spark.databricks.cluster.profile": "singleNode",
"spark.master": "local[*]",
"ResourceClass": "SingleNode"
```

This was a critical learning point.

It proved:

```text
The YAML changes were applied correctly.
The bundle was not ignoring us.
The cluster really was single-node.
Elastic disk really was false.
```

And yet it still failed with:

```text
Quota 'SSD_TOTAL_GB' exceeded. Limit: 250.0 in region us-east1.
```

Therefore the issue was not our config anymore. The issue was the GCP quota environment.

---

## 18. Breakthrough: N2 + Local SSD

The key workaround was switching away from E2.

Why?

E2 does not support Local SSD.

N1, N2, and N2D can support Local SSD. Databricks says that if Local SSD is present, remote SSD can start at `0 GB`. That matters because the quota failure was coming from `SSD_TOTAL_GB`, which covers SSD Persistent Disk and Balanced Persistent Disk, not Local SSD.

The final direction became:

```yaml
node_type_id:
  default: n2-highmem-2
```

or whichever N2/N2D type the Databricks UI actually allowed.

For job clusters:

```yaml
gcp_attributes:
  availability: ON_DEMAND_GCP
  local_ssd_count: 1
  zone_id: AUTO
```

The final working signal was:

```text
Cluster reached "Starting Spark"
Then the job ran
Then it failed due to code issues
```

That is a real infrastructure win.

---

## 19. Unity Catalog problem: default storage was wrong

At one point, the catalog error appeared:

```text
Metastore storage root URL does not exist.
Default Storage is enabled in your account.
You can use the UI to create a new catalog using Default Storage,
or provide a storage location for the catalog.
```

The first attempt created `healthcare` using default storage.

That looked okay in the UI, but it was wrong for this specific test.

The catalog storage location looked like:

```text
gs://databricks-8259552159587810-unitycatalog/...
```

That indicated Databricks-managed default storage.

Problem:

```text
Default storage is for serverless-style access.
Classic compute cannot interact with data assets in default storage.
```

For this project, we need classic compute.

So the fix was to create `healthcare` using a customer-managed GCS bucket.

---

## 20. Correct Unity Catalog setup

The correct architecture became:

```text
GCS bucket in your GCP project
  -> Databricks storage credential
  -> Databricks external location
  -> healthcare catalog MANAGED LOCATION
  -> schemas and volumes from bundle
```

The fixed catalog showed storage under your bucket:

```text
gs://homeprojectabacus-uc-monthhome/unity-catalog/...
```

and Unity Catalog internally expanded it to:

```text
gs://homeprojectabacus-uc-monthhome/unity-catalog/__unitystorage/catalogs/...
```

That is correct.

Unity Catalog adds `__unitystorage/catalogs/<id>` under the declared catalog storage root.

---

## 21. Correct catalog creation pattern

The correct SQL pattern is:

```sql
CREATE CATALOG healthcare
MANAGED LOCATION 'gcs://homeprojectabacus-uc-monthhome/unity-catalog/healthcare';
```

or, depending on accepted URI scheme:

```sql
CREATE CATALOG healthcare
MANAGED LOCATION 'gs://homeprojectabacus-uc-monthhome/unity-catalog/healthcare';
```

The important point is:

```text
It must point to your external location / your GCS bucket.
It must not point to Databricks default storage.
It must not point to __databricks_internal.
```

---

## 22. `__databricks_internal` lesson

When we saw:

```text
Catalog '__databricks_internal' already exists
```

it looked like the bundle might be pointing to the internal catalog.

But `databricks.yml` still had:

```yaml
catalog: healthcare
```

So the better diagnosis was:

```text
The healthcare catalog was created in a default-storage/internal-storage path,
and pipeline creation was colliding with Databricks internal/default storage behavior.
```

The fix was not to use `__databricks_internal`.

The fix was:

```text
Recreate healthcare with proper GCS-backed managed storage.
```

---

## 23. Final `databricks.yml` direction

The catalog variable can remain:

```yaml
variables:
  catalog:
    default: healthcare

targets:
  dev:
    variables:
      catalog: healthcare
```

The node type should use the working N2/N2D family:

```yaml
variables:
  node_type_id:
    default: n2-highmem-2
```

The Spark version can remain:

```yaml
spark_version:
  default: 17.3.x-cpu-ml-scala2.13
```

unless Databricks validation/runtime requires another ML runtime.

---

## 24. Current recommended job cluster block

Use this in:

```text
setup_infrastructure.job.yml
load_sample_data.job.yml
training.job.yml
```

```yaml
new_cluster:
  spark_version: ${var.spark_version}
  node_type_id: ${var.node_type_id}
  driver_node_type_id: ${var.node_type_id}
  num_workers: 0
  enable_elastic_disk: false

  gcp_attributes:
    availability: ON_DEMAND_GCP
    local_ssd_count: 1
    zone_id: AUTO

  spark_conf:
    spark.databricks.cluster.profile: singleNode
    spark.master: local[*]

  custom_tags:
    ResourceClass: SingleNode
    Project: homeprojectabacus
    Environment: ${bundle.target}
```

---

## 25. Current recommended pipeline cluster block

Use this in:

```text
bronze.pipeline.yml
silver.pipeline.yml
gold.pipeline.yml
```

```yaml
clusters:
  - label: default
    node_type_id: ${var.node_type_id}
    driver_node_type_id: ${var.node_type_id}
    num_workers: 0

    gcp_attributes:
      availability: ON_DEMAND_GCP
      local_ssd_count: 1
      zone_id: AUTO

    spark_conf:
      spark.databricks.cluster.profile: singleNode
      spark.master: local[*]

    custom_tags:
      ResourceClass: SingleNode
      Project: homeprojectabacus
      Environment: ${bundle.target}
```

Do not add:

```yaml
enable_elastic_disk: false
```

inside pipeline clusters, because the bundle validator rejects it there.

---

## 26. Job compute shutdown behavior

For job clusters:

```text
setup_infrastructure
load_sample_data
etl_ml_pipeline shared job cluster
```

the expectation is:

```text
cluster starts when the first task starts
cluster terminates after the final task using it completes
```

So after a job succeeds or fails, the job cluster should terminate.

Still verify manually:

```bash
databricks clusters list --profile dev
```

If a cluster is still running unexpectedly:

```bash
databricks clusters delete <cluster-id> --profile dev
```

---

## 27. Pipeline compute shutdown behavior

Lakeflow / Declarative Pipeline compute behaves differently.

In development mode, Databricks can keep pipeline clusters alive for reuse after the update finishes.

Because your target uses:

```yaml
mode: development
```

pipeline clusters may stay alive longer than job clusters.

For cost safety, after every pipeline run:

```text
Check Compute -> Active compute
Terminate anything still running
```

Optional pipeline configuration:

```yaml
configuration:
  pipelines.clusterShutdown.delay: "300s"
```

or:

```yaml
configuration:
  pipelines.clusterShutdown.delay: "5m"
```

This should be applied under each pipeline resource if you want shorter dev-mode cluster reuse.

---

## 28. Deployment order that should be followed from now

Do not run everything at once.

Use this order:

```bash
databricks bundle validate -t dev --profile dev
databricks bundle deploy -t dev --profile dev
```

Then:

```bash
databricks bundle run setup_infrastructure -t dev --profile dev
```

If setup succeeds:

```bash
databricks bundle run load_sample_data -t dev --profile dev
```

Then manually test:

```text
Bronze pipeline
Silver pipeline
Gold pipeline
```

Only after those work:

```bash
databricks bundle run etl_ml_pipeline -t dev --profile dev
```

---

## 29. Things not to run yet

Do not run these until the basic pipeline and code issues are fixed:

```text
Full automatic file-arrival-triggered flow
Optuna tuning
Model serving
Any long-running all-purpose compute
Parallel job runs
Multiple pipelines at once
```

Use:

```yaml
--no-tune
```

for the first ML smoke test.

Restore:

```yaml
--tune
```

only after the full path works.

---

## 30. Development-mode trigger behavior

The `dev` target uses:

```yaml
mode: development
```

This is useful because development mode pauses triggers/schedules by default.

That means the file-arrival trigger should not unexpectedly run in dev unless explicitly unpaused.

Still, do not rely blindly on this. Always check deployed job trigger status before uploading files into watched volumes.

---

## 31. Final wins

### Win 1: Correct branch identified

The real code was on:

```text
integration
```

not `main`.

### Win 2: DAB resources existed

The project already had real deployable resources:

```text
setup_infrastructure
load_sample_data
etl_ml_pipeline
bronze_pipeline
silver_pipeline
gold_pipeline
schemas
volumes
```

### Win 3: Single-node job config was validated

We confirmed the cluster spec really had:

```text
num_workers: 0
enable_elastic_disk: false
singleNode profile
ResourceClass: SingleNode
```

### Win 4: Default-storage catalog issue was fixed

The catalog moved from Databricks-managed default storage to your own GCS bucket.

### Win 5: GCP quota workaround was found

E2 failed.

N2 + Local SSD worked enough to start Spark and execute code.

### Win 6: Cloud issue became code issue

The run reached code execution and failed because of code.

That is progress.

---

## 32. Final losses / blockers encountered

### Loss 1: GCP credit was not a reliable safety net

The available credit was probably not general-purpose Databricks/Marketplace coverage.

Conclusion:

```text
Treat Databricks as real paid spend.
```

### Loss 2: GCP quota increase denied

The project could not raise `SSD_TOTAL_GB` above 250 GB.

Conclusion:

```text
Do not rely on quota increase in this account.
```

### Loss 3: E2 was not viable

Even with single-node and elastic disk disabled, `e2-highmem-2` failed.

Conclusion:

```text
E2 + 250 GB SSD_TOTAL_GB is not enough for this Databricks classic compute setup.
```

### Loss 4: Pipeline YAML has stricter schema

`enable_elastic_disk` was rejected under pipeline clusters.

Conclusion:

```text
Pipeline clusters and job clusters do not support exactly the same fields.
```

### Loss 5: Default storage looked correct but was wrong

Catalog UI showed a managed catalog, but it was backed by Databricks-managed default storage.

Conclusion:

```text
For classic compute, verify the storage path is your GCS bucket.
```

---

## 33. Debugging commands that proved useful

### Validate bundle

```bash
databricks bundle validate -t dev --profile dev
```

### Deploy bundle

```bash
databricks bundle deploy -t dev --profile dev
```

### Run setup job

```bash
databricks bundle run setup_infrastructure -t dev --profile dev
```

### Inspect failed cluster

```bash
databricks clusters get <cluster-id> --profile dev --output json > failed-cluster.json
```

### Check relevant cluster fields

```bash
grep -i "enable_elastic_disk\|num_workers\|node_type_id\|driver_node_type_id\|singleNode\|ResourceClass\|gcp_attributes" failed-cluster.json
```

### Check running clusters

```bash
databricks clusters list --profile dev
```

### Terminate a cluster

```bash
databricks clusters delete <cluster-id> --profile dev
```

### Check GCP SSD quota cleanly

```bash
for r in us-east1 us-central1 us-east4 us-west1 us-west2 asia-south1 asia-southeast1; do
  echo "=== $r ==="
  gcloud compute regions describe "$r" \
    --flatten="quotas[]" \
    --format="csv[no-heading](quotas.metric,quotas.limit,quotas.usage)" \
    | grep '^SSD_TOTAL_GB,'
done
```

### Check SSD and Local SSD quota together

```bash
gcloud compute regions describe us-east1 \
  --flatten="quotas[]" \
  --format="csv[no-heading](quotas.metric,quotas.limit,quotas.usage)" \
  | grep -E '^(SSD_TOTAL_GB|LOCAL_SSD_TOTAL_GB|PREEMPTIBLE_LOCAL_SSD_GB),'
```

---

## 34. Cost-control checklist before every run

Before running:

```text
Confirm no active clusters.
Confirm no all-purpose compute is running.
Confirm only one job/pipeline will run.
Confirm dev target is being used.
Confirm --no-tune is active.
Confirm node type is N2/N2D with Local SSD.
Confirm Photon is false.
Confirm num_workers is 0.
```

After running:

```text
Check Workflows run status.
Check Compute -> Active compute.
Terminate anything left running.
Check GCP Billing.
Check Databricks usage.
```

---

## 35. What to do next

The next phase should be code debugging, not cloud debugging.

Recommended next order:

1. Fix the code issue that caused `setup_infrastructure` to fail.
2. Re-run only `setup_infrastructure`.
3. Confirm cluster terminates.
4. Run `load_sample_data`.
5. Confirm files land in the Unity Catalog volume.
6. Run Bronze pipeline only.
7. Confirm Bronze tables.
8. Run Silver pipeline only.
9. Run Gold pipeline only.
10. Run full `etl_ml_pipeline` with `--no-tune`.

Do not jump directly to full orchestration until setup and sample loading are clean.

---

## 36. Final mental model

The working architecture is now:

```text
GCP project
  -> GCS bucket: homeprojectabacus-uc-monthhome
  -> Databricks storage credential
  -> Databricks external location
  -> healthcare catalog MANAGED LOCATION
  -> DAB-created schemas and volume

Databricks Asset Bundle
  -> setup job: single-node N2 + Local SSD
  -> load sample data job: single-node N2 + Local SSD
  -> Bronze/Silver/Gold pipelines: single-node N2 + Local SSD
  -> ETL/ML job: single-node N2 + Local SSD
```

The major hard lesson:

```text
The implementation plan was not the main problem.
The real blockers were GCP quota, Databricks storage semantics, and classic-compute disk behavior.
```

The major win:

```text
The cloud layer finally ran far enough to expose code issues.
That means the deployment architecture is no longer completely blocked.
```

---

## 37. Reference sources

These are the official references that explain the behavior we observed.

### Databricks on GCP compute storage and Local SSD

- Databricks compute configuration reference for GCP explains that each compute instance gets multiple disks, single-node still provisions all disks on the single node, and remote SSD starts at 80 GB or 0 GB if Local SSD is present.  
  https://docs.databricks.com/gcp/en/compute/configure

### Google Cloud SSD quota

- Google Cloud quota documentation explains that `SSD_TOTAL_GB` corresponds to Persistent disk SSD (GB), and includes SSD Persistent Disk and Balanced Persistent Disk.  
  https://cloud.google.com/compute/resource-usage

### Unity Catalog managed storage

- Databricks managed storage documentation explains catalog-level managed locations and the `__unitystorage/catalogs/<id>` path structure.  
  https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/managed-storage

### GCS external locations

- Databricks GCS external location documentation explains storage credentials and external locations.  
  https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/external-locations

### Default storage limitation

- Databricks default storage documentation explains that interactions with default storage require serverless compute and that classic compute cannot interact with data assets in default storage.  
  https://docs.databricks.com/aws/en/storage/default-storage

### Job compute lifecycle

- Databricks Jobs documentation explains shared job compute lifecycle behavior.  
  https://docs.databricks.com/en/reference/jobs-api-2-1-updates.html

### Lakeflow / DLT pipeline development mode shutdown behavior

- Databricks Lakeflow / DLT documentation explains development-mode cluster reuse and shutdown delay behavior.  
  https://docs.databricks.com/gcp/en/dlt/updates
