# Healthcare Claim Denial Pipeline — Integration & Orchestration Plan

## Executive Summary

The final deployment moves the project from a sequence of manually run notebooks and scripts into a Databricks Asset Bundle (DAB) that deploys Lakeflow Spark Declarative Pipelines, Databricks Jobs, quality gates, ML retraining, and observability as version-controlled resources.

The right target is a DAB-orchestrated multi-task job, but the first draft had several runtime-blocking gaps that have been corrected here. The corrections below are sourced from a verification pass against the actual codebase and current Databricks documentation (Mar–Apr 2026).

Operational gaps that this revised plan now closes:

1. **`scripts/train_denial_model.py:275` hardcodes `main(["--tune"])`.** Any DAB-passed parameters are silently dropped. The script must read `sys.argv[1:]` before the bundle's training task can pass `--catalog`, `--gold-table`, `--registered-model-name`, etc.
2. **`dbutils.jobs.taskValues.set/get` is officially Python-notebook-only.** Therefore `check_new_data` and `skip_retraining` must be **notebook tasks**, not `spark_python_task`s. All other recurring entry points stay as scripts.
3. **Each Lakeflow pipeline must publish its event log to a Unity Catalog table.** Without an `event_log:` block on the pipeline resource, `write_observability_tables()` cannot reliably read pipeline events across runs (the SQL TVF `event_log("<pipeline_id>")` requires per-run owner permissions).
4. **`write_observability_tables()` writes four fixed table names with no stage tag**, so three sequential calls (Bronze, Silver, Gold) overwrite each other. The function needs a `pipeline_stage: str` parameter, append-mode writes, and a `pipeline_stage` column on every persisted row.
5. **Pipelines must use classic clusters with spot fallback, not serverless.** Project preference: predictability + scale.
6. **Job cluster runtime must be `14.3.x-cpu-ml-scala2.12` (DBR 14.3 LTS ML)**, not the non-ML runtime. Otherwise tasks that load the registered model 400 on `s3://dbstorage-*` (see CLAUDE.md "Cluster prerequisites").
7. **`condition_task` syntax must use `condition_task` with `left`, `op`, `right`** — not `if_else_condition_task`.
8. **Bronze and Silver verification must be real DAG gates.** Downstream tasks depend on the verifier task, not on the pipeline refresh task.
9. **Model retraining cannot rely on `training_rows` until the training code logs it.** The retrain gate compares a logged Gold data fingerprint/version against the current Gold table state.
10. **Observability is multi-pipeline aware.** Each event-log collection task receives its specific pipeline ID and stage label.
11. **Production file-arrival triggering and dev fixture loading are separated jobs.** File arrival reacts to externally landed immutable files; fixture copying is a manual/dev bootstrap action.
12. **Setup uses first-class DAB `schemas:` and `volumes:` resources.** The catalog is created out-of-band by the workspace admin; bundle resources own the schemas and the managed volume.
13. **The current ETL code hard-codes `healthcare.*` defaults.** Bundle variables alone do not create true dev/prod isolation until the ETL constants are parameterized; this is explicitly out of scope for the first orchestration cut.

---

## Current State

Today the project is operationally fragmented:

```text
bootstrap_bronze_landing
  -> load_datasets_into_bronze_landing
  -> Bronze Lakeflow pipeline
  -> bronze_verify_and_rbac
  -> bronze_profiling                  (ad hoc — exploration only)
  -> Silver Lakeflow pipeline
  -> silver_validation
  -> observability                     (one notebook, queried per pipeline manually)
  -> claims_exploration                (mixes analytics build + exploration)
  -> Gold Lakeflow pipeline
  -> scripts/train_denial_model.py
  -> sample_prediction                 (demo only)
```

Problems:

- No single deployable resource definition.
- No DAG-level dependency enforcement.
- No conditional ML retraining based on actual Gold data changes.
- No combined observability story across Bronze, Silver, Gold, analytics, and ML.
- Notebooks mix exploratory, one-time setup, destructive reset, and recurring production logic.

---

## Target Architecture

Use Databricks Asset Bundles to define:

- One-time infrastructure setup using DAB-native resources (`schemas:`, `volumes:`) plus a small grants notebook task.
- Three Lakeflow Declarative Pipeline resources: Bronze, Silver, Gold. Each publishes its event log to UC.
- One dev/manual fixture-loading job.
- One production ingestion-to-ML job triggered by file arrival on `/Volumes/healthcare/bronze/raw_landing/`.
- A small set of Python script entrypoints for recurring verification, analytics, and observability; two notebook entrypoints for the retrain gate (because they need to set/read task values).

```text
homeprojectabacus/
  databricks.yml
  resources/
    pipelines/
      bronze.pipeline.yml
      silver.pipeline.yml
      gold.pipeline.yml
    jobs/
      setup_infrastructure.job.yml
      load_sample_data.job.yml
      etl_ml_pipeline.job.yml
    schemas/
      schemas.yml                     # DAB-native schema resources
    volumes/
      volumes.yml                     # DAB-native volume resources
  src/
    scripts/                          # spark_python_task entrypoints
      load_sample_data.py
      verify_bronze.py
      verify_silver.py
      build_analytics.py
      build_observability.py
    notebooks/                        # existing notebook home
      check_new_data.ipynb            # NEW — notebook because it sets task values
      skip_retraining.ipynb           # NEW — notebook because it reads task values
      grants.ipynb                    # NEW — one-time RBAC grants run by setup job
      bootstrap_bronze_landing.ipynb  # existing, still manual
      bronze_verify_and_rbac.ipynb    # split: verify lives in verify_bronze.py, rbac moves to grants.ipynb
      bronze_profiling.ipynb          # exploration-only, manual
      silver_validation.ipynb         # superseded by verify_silver.py for production
      observability.ipynb             # superseded by build_observability.py for production
      claims_exploration.ipynb        # exploration-only; production analytics in build_analytics.py
      sample_prediction.ipynb         # demo, manual
  ETL/
    pipelines/
      bronze/
      silver/
      gold/
  scripts/
    train_denial_model.py             # spark_python_task; needs argv fix (see below)
```

Notebooks remain for:

- One-time/manual setup (`bootstrap_bronze_landing`).
- Exploration (`bronze_profiling`, exploratory cells of `claims_exploration`).
- Destructive reset (kept manual on purpose).
- Demo (`sample_prediction`).
- **Task-value producers/consumers** (`check_new_data`, `skip_retraining`) — required by Databricks because `dbutils.jobs.taskValues.*` is officially Python-notebook-only.
- One-time grants (`grants.ipynb`) run by the setup job.

Recurring production behavior moves to scripts and bundle resources.

---

## Job Topology

### Production Job (`etl_ml_pipeline`)

The production job starts when new immutable files arrive in the Bronze landing volume. It does not copy fixture files into the watched path.

```text
file arrival trigger
  -> run_bronze_pipeline
  -> verify_bronze
  -> run_silver_pipeline
  -> verify_silver
  -> run_gold_pipeline
  -> check_new_data            (notebook_task — sets task values)
  -> should_retrain            (condition_task)
       true  -> train_model
       false -> skip_retraining (notebook_task — reads task values)

parallel after verify_silver:
  -> build_analytics

observability fan-out:
  -> observe_bronze   after run_bronze_pipeline
  -> observe_silver   after run_silver_pipeline
  -> observe_gold     after run_gold_pipeline
```

### Dev Fixture Loading Job (`load_sample_data`)

```text
load_sample_data
  -> optional manual run of etl_ml_pipeline
```

This avoids a circular pattern where the file-arrival job writes files into the path that triggers itself.

### Setup Job (`setup_infrastructure`)

```text
deploy DAB schemas/volumes resources (declarative, not a job task)
  -> grants notebook_task   (one-time RBAC for service principal)
```

The catalog is created out-of-band by the workspace admin (catalogs require a higher privilege than DAB typically holds). Schemas and the `raw_landing` volume are first-class DAB resources, not imperative SQL inside a setup notebook.

---

## Script & Notebook Contract

Every recurring entry point under `src/scripts/` and the new notebooks under `src/notebooks/` follows a single contract so the DAB job is predictable.

- Stdout is PHI-safe. Reference identifiers (claim_id, provider_id) only; no clinical fields, names, or dates of birth.
- Failure: scripts call `sys.exit(1)` on any unmet check; notebooks raise `Exception(...)` so the task fails. Notebook-level `dbutils.notebook.exit("FAIL: ...")` is acceptable when the next task should still see the failure status.
- No widget reads at module import time. Widget defaults are set inside `if __name__ == "__main__"` (scripts) or the first cell (notebooks) so the entry point is DAB-portable and unit-testable locally.
- Verifier scripts emit a single-line PHI-safe summary (`OK: bronze tables=5 rows={...}`) before exit.
- Notebook tasks that publish task values use `dbutils.jobs.taskValues.set(key=..., value=...)` only — no other side effects.

---

## DAB Job Skeleton — `etl_ml_pipeline`

Use `condition_task` (not `if_else_condition_task`). All path references are relative to the YAML file.

```yaml
resources:
  jobs:
    etl_ml_pipeline:
      name: "[${bundle.target}] Healthcare ETL + ML Pipeline"

      trigger:
        file_arrival:
          url: "/Volumes/healthcare/bronze/raw_landing/"
          min_time_between_triggers_seconds: 300
          wait_after_last_change_seconds: 60

      job_clusters:
        - job_cluster_key: shared_cluster
          new_cluster:
            spark_version: "14.3.x-cpu-ml-scala2.12"   # DBR 14.3 LTS ML — required by load_from_registry
            node_type_id: ${var.node_type_id}
            autoscale:
              min_workers: 1
              max_workers: 4

      tasks:
        - task_key: run_bronze_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.bronze_pipeline.id}

        - task_key: verify_bronze
          depends_on:
            - task_key: run_bronze_pipeline
          spark_python_task:
            python_file: ../src/scripts/verify_bronze.py
          job_cluster_key: shared_cluster

        - task_key: observe_bronze
          depends_on:
            - task_key: run_bronze_pipeline
          spark_python_task:
            python_file: ../src/scripts/build_observability.py
            parameters:
              - --published-event-log-table
              - ${var.catalog}.${var.analytics_schema}.bronze_pipeline_event_log
              - --pipeline-stage
              - bronze
          job_cluster_key: shared_cluster

        - task_key: run_silver_pipeline
          depends_on:
            - task_key: verify_bronze
          pipeline_task:
            pipeline_id: ${resources.pipelines.silver_pipeline.id}

        - task_key: verify_silver
          depends_on:
            - task_key: run_silver_pipeline
          spark_python_task:
            python_file: ../src/scripts/verify_silver.py
          job_cluster_key: shared_cluster

        - task_key: observe_silver
          depends_on:
            - task_key: run_silver_pipeline
          spark_python_task:
            python_file: ../src/scripts/build_observability.py
            parameters:
              - --published-event-log-table
              - ${var.catalog}.${var.analytics_schema}.silver_pipeline_event_log
              - --pipeline-stage
              - silver
          job_cluster_key: shared_cluster

        - task_key: build_analytics
          depends_on:
            - task_key: verify_silver
          spark_python_task:
            python_file: ../src/scripts/build_analytics.py
          job_cluster_key: shared_cluster

        - task_key: run_gold_pipeline
          depends_on:
            - task_key: verify_silver
          pipeline_task:
            pipeline_id: ${resources.pipelines.gold_pipeline.id}

        - task_key: observe_gold
          depends_on:
            - task_key: run_gold_pipeline
          spark_python_task:
            python_file: ../src/scripts/build_observability.py
            parameters:
              - --published-event-log-table
              - ${var.catalog}.${var.analytics_schema}.gold_pipeline_event_log
              - --pipeline-stage
              - gold
          job_cluster_key: shared_cluster

        # Notebook task — required because dbutils.jobs.taskValues.set() is Python-notebook-only.
        - task_key: check_new_data
          depends_on:
            - task_key: run_gold_pipeline
          notebook_task:
            notebook_path: ../src/notebooks/check_new_data.ipynb
            base_parameters:
              catalog: ${var.catalog}
              gold_schema: ${var.gold_schema}
              ml_schema: ${var.ml_schema}
              registered_model_name: ${var.catalog}.${var.ml_schema}.claim_denial_model
              champion_alias: champion
          job_cluster_key: shared_cluster

        - task_key: should_retrain
          depends_on:
            - task_key: check_new_data
          condition_task:
            left: "{{tasks.check_new_data.values.should_retrain}}"
            op: EQUAL_TO
            right: "true"

        - task_key: train_model
          depends_on:
            - task_key: should_retrain
              outcome: "true"
          spark_python_task:
            python_file: ../scripts/train_denial_model.py
            parameters:
              - --tune
              - --catalog
              - ${var.catalog}
              - --gold-table
              - ${var.catalog}.${var.gold_schema}.claim_features
              - --registered-model-name
              - ${var.catalog}.${var.ml_schema}.claim_denial_model
              - --champion-alias
              - champion
          job_cluster_key: shared_cluster
          libraries:
            # Versions match pyproject.toml `[dependency-groups] ml`. Keep in sync — do not pin tighter here.
            - pypi:
                package: "xgboost>=2.0,<3.0"
            - pypi:
                package: "scikit-learn>=1.5,<2.0"
            - pypi:
                package: "shap>=0.44,<1.0"
            - pypi:
                package: "optuna>=3.6,<4.0"
            - pypi:
                package: "mlflow>=3.0,<4.0"

        # Notebook task — required because dbutils.jobs.taskValues.get() is Python-notebook-only.
        - task_key: skip_retraining
          depends_on:
            - task_key: should_retrain
              outcome: "false"
          notebook_task:
            notebook_path: ../src/notebooks/skip_retraining.ipynb
          job_cluster_key: shared_cluster
```

Notes:

- `train_model` library versions intentionally use the same ranges as `pyproject.toml` `[dependency-groups] ml` so the bundle tracks the project's ML pinning policy. Do not tighten the pins in this file independently.
- `${var.node_type_id}` is set per target (`dev`, `prod`) in `databricks.yml`. AWS default `i3.xlarge`; document Azure/GCP equivalents if the workspace is multi-cloud.
- **Verification gates the data path; observability does not.** `run_silver_pipeline` depends on `verify_bronze`, and `run_gold_pipeline` / `build_analytics` depend on `verify_silver` — these are real DAG gates. The `observe_bronze` / `observe_silver` / `observe_gold` tasks depend only on their own pipeline run (not on the verifiers), so they collect event-log telemetry even when verification fails. This is intentional: a failed run is exactly when you want the event log captured, and observability is informational, not a gate.
- The condition task uses string comparison; `check_new_data.ipynb` must call `dbutils.jobs.taskValues.set(key="should_retrain", value="true")` (or `"false"`) using exactly those literals.

---

## Pipeline Resources

Each Lakeflow Spark Declarative Pipeline is a DAB pipeline resource. Bronze example below; Silver and Gold follow the same pattern.

```yaml
resources:
  pipelines:
    bronze_pipeline:
      name: "[${bundle.target}] Healthcare Bronze Ingestion"
      catalog: ${var.catalog}
      target: ${var.bronze_schema}
      photon: true
      channel: current

      # Classic clusters with spot fallback (per project preference: predictability + scale > serverless).
      clusters:
        - label: default
          aws_attributes:
            availability: SPOT_WITH_FALLBACK
            spot_bid_price_percent: 100
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: ENHANCED

      libraries:
        - glob:
            include: ../ETL/pipelines/bronze/**

      # Publish event log to UC so build_observability.py can read it durably.
      event_log:
        catalog: ${var.catalog}
        schema: ${var.analytics_schema}
        name: bronze_pipeline_event_log

      permissions:
        - level: CAN_RUN
          group_name: data-engineers
```

Use the same shape for Silver (`target: ${var.silver_schema}`, event log `silver_pipeline_event_log`, libraries glob `../ETL/pipelines/silver/**`) and Gold (`target: ${var.gold_schema}`, event log `gold_pipeline_event_log`, libraries glob `../ETL/pipelines/gold/**`).

The repo path `ETL/` is uppercase — keep it that way; DAB glob includes are case-sensitive on Linux runners.

Important limitation:

- The current ETL modules still import constants that default to `healthcare`, `bronze`, `silver`, `gold` (`src/common/{bronze,silver,gold}_pipeline_config.py`). For the current project phase, target the fixed `healthcare.*` dev deployment and leave the bundle's `var.*` defaults aligned with those values. True dev/prod target isolation is a later parameterization pass where ETL table names read catalog/schema from Spark configuration.

---

## Script Conversion Plan

### `src/scripts/load_sample_data.py`

Purpose:

- Dev/demo only.
- Convert `load_datasets_into_bronze_landing.ipynb`.
- Copy the local fixture datasets into `/Volumes/healthcare/bronze/raw_landing/{claims,providers,diagnosis,cost,policies}/`.

Behavior:

- Fail fast if the target volume does not exist.
- Use `BRONZE_SOURCES` (`src/common/bronze_sources.py:59`) and `POLICY_SOURCE` (`src/common/bronze_sources.py:137`); do not hard-code ad hoc filenames.
- Support overwrite for dev reruns.
- Do not include this task in the production file-arrival job.

### `src/scripts/verify_bronze.py`

Purpose:

- Convert recurring checks from `bronze_verify_and_rbac.ipynb`.

Split point (M3 from the audit):

- **Move to `src/notebooks/grants.ipynb` (run by `setup_infrastructure`)**: every `GRANT` / `REVOKE` statement in the source notebook.
- **Stay in `verify_bronze.py` (run every production cycle)**: required Bronze tables exist; required columns exist; audit columns (`_ingested_at`, etc.) exist; expected fixture tables non-empty in dev; no quarantine table is unexpectedly populated.

Behavior:

- Exit non-zero on any failed quality check.
- Print a single PHI-safe summary line on success.

### `src/scripts/verify_silver.py`

Purpose:

- Convert recurring checks from `silver_validation.ipynb`.

Behavior:

- Call `write_quality_assets()` (`src/analytics/quality_assets.py:118`).
- Verify Silver trusted/quarantine tables exist; expected row-count and diagnostic outputs are present.
- Exit non-zero on failed quality checks.

### `src/scripts/build_analytics.py`

Purpose:

- Replace the production parts of `claims_exploration.ipynb`.

Behavior:

- Call `build_and_persist_claims_assets()` (`src/analytics/claims_analytics.py:532`).
- Depends on `verify_silver` because the analytics layer reads trusted Silver and Bronze reference data.
- Exit non-zero if any required analytics table fails to write.

Naming convention (M8 from the audit): all claims-domain analytics tables are written to `healthcare.analytics.*`. To avoid collisions with operational/observability tables (also in the same schema), continue using the `ops_*` prefix for operational tables and the `claims_*` / `denial_*` prefix for domain analytics — `build_and_persist_claims_assets()` already follows this for the domain side.

### `src/scripts/build_observability.py`

Purpose:

- Replace the production parts of `observability.ipynb` and run it once per pipeline stage.

Behavior:

- Accept `--published-event-log-table` (preferred, durable across runs) or `--pipeline-id` (fallback, requires per-run owner permissions on the pipeline).
- Accept `--pipeline-stage` for `bronze | silver | gold` — passed through to the persistence call.
- Call `write_observability_tables()` (extended; see Code Changes Required below).

### `src/notebooks/check_new_data.ipynb` (notebook — NEW)

Purpose:

- Decide whether model retraining is needed and publish the decision as task values.

Why a notebook: `dbutils.jobs.taskValues.set/get` is officially Python-notebook-only.

Behavior:

- Read current `healthcare.gold.claim_features` (table name from `base_parameters`).
- Compute current training metadata:
  - `training_row_count`
  - Gold Delta table version (via `DESCRIBE HISTORY` LIMIT 1)
  - feature column list (from `src/ml/__init__.py:FEATURE_COLUMNS`)
  - deterministic data fingerprint from stable, non-PHI-safe identifiers and feature values (e.g. `pyspark.sql.functions.hash` over a sorted projection)
- Fetch champion model with MLflow's alias API:
  - `MlflowClient().get_model_version_by_alias(model_name, "champion")`
- Read the champion run's logged metadata (`training_row_count`, `gold_table_version`, `training_data_fingerprint`, `feature_columns`).
- Set task values:
  - `should_retrain`: `"true"` or `"false"` (string literal — the condition_task uses string comparison)
  - `reason`: short PHI-safe reason
  - `current_training_row_count`
  - `current_gold_version`
  - `current_data_fingerprint`

Rules:

- If no champion exists, retrain.
- If current fingerprint differs from champion fingerprint, retrain.
- If feature column list differs, retrain.
- If current row count is zero, fail the notebook (raise) instead of skipping.
- Do not use row count alone as the retrain signal.

### `src/notebooks/skip_retraining.ipynb` (notebook — NEW)

Purpose:

- Emit a PHI-safe log line explaining why retraining was skipped.

Why a notebook: reads task values via `dbutils.jobs.taskValues.get()`.

Behavior:

- Read task values from `check_new_data` (row count, Gold version, reason).
- Print row count, Gold version, and reason to the notebook output.
- Do not access claim-level PHI.

---

## Code Changes Required (prerequisites for the bundle to actually work)

These changes precede bundle deployment because the bundle deploys correctly without them but does not behave correctly.

### `scripts/train_denial_model.py:275` — argv passthrough

```python
# Before:
_rc = main(["--tune"])

# After:
_rc = main(sys.argv[1:] if len(sys.argv) > 1 else ["--tune"])
```

This preserves notebook-style "run with defaults" behavior while letting the DAB pass `--catalog`, `--gold-table`, etc.

### `scripts/train_denial_model.py` and/or `src/ml/train.py` — MLflow metadata logging

Add the following params/tags around the existing `mlflow.log_params(params)` / `mlflow.log_metrics(metrics)` (`src/ml/train.py:271-272`):

- `training_row_count`
- `gold_table_name`
- `gold_table_version`
- `training_data_fingerprint`
- `feature_columns` (use `mlflow.log_dict({"columns": list(FEATURE_COLUMNS)}, "feature_columns.json")`)
- `target_column`
- `release_gate_passed`

Without these, conditional retraining cannot compare current Gold state against the champion's training state.

### `src/analytics/observability_assets.py` — stage-aware writes

Extend `write_observability_tables()`:

```python
def write_observability_tables(
    spark,
    pipeline_id: str | None = None,
    published_event_log_table: str | None = None,
    catalog: str = "healthcare",
    analytics_schema: str = "analytics",
    pipeline_stage: str | None = None,        # NEW — required when called from the bundle
    parallel_writes: bool = True,
    max_parallel_writes: int = 4,
) -> dict[str, str]:
```

Inside, every persisted DataFrame gains a `pipeline_stage = lit(pipeline_stage)` column, and every write becomes append-mode keyed on `(pipeline_stage, update_id)` so Bronze, Silver, and Gold rows accumulate side-by-side instead of overwriting. On first DAB run, drop and recreate the four `ops_*` tables to migrate the schema cleanly (no historical data is lost — they are derived from the event log).

---

## Observability Model

The project has two observability layers:

### Pipeline Observability

Source:

- Lakeflow event logs for Bronze, Silver, and Gold, **published to UC** via the `event_log:` block on each pipeline resource (`healthcare.analytics.{bronze,silver,gold}_pipeline_event_log`).

Tables (single set, stage-tagged):

- `healthcare.analytics.ops_pipeline_updates`
- `healthcare.analytics.ops_expectation_metrics`
- `healthcare.analytics.ops_user_actions`
- `healthcare.analytics.ops_latest_failures`

Required fields (after the C4 fix):

- `pipeline_stage` (`bronze` | `silver` | `gold`)
- `pipeline_id`
- `update_id`
- `update_state`
- `dataset`
- `flow_name`
- `diagnostic_id`
- `event_timestamp`

### Job-Level Observability

Source:

- Databricks Jobs task run metadata (later phase).
- Explicit script log outputs and task values.

Minimum tracked events:

- Bronze verifier status.
- Silver verifier status.
- Analytics build status.
- Gold retrain gate decision (from `check_new_data` task values).
- Model training status and release-gate result.

Job-level observability is a Phase 5 follow-up if needed; the first orchestration commit must capture pipeline event logs per stage.

---

## Infrastructure Setup

The setup job runs once per environment.

**Approach (DAB-native, primary):**

- Catalog `healthcare` is created out-of-band by the workspace admin (catalogs require workspace-admin privilege; the bundle's deploy identity typically does not have it).
- Schemas declared as DAB resources in `resources/schemas/schemas.yml`:
  - `healthcare.bronze`
  - `healthcare.silver`
  - `healthcare.quarantine`
  - `healthcare.gold`
  - `healthcare.analytics`
  - `healthcare.ml`
- Managed volume declared as a DAB resource in `resources/volumes/volumes.yml`:
  - `healthcare.bronze.raw_landing`
- A small `src/notebooks/grants.ipynb` notebook task runs the one-time `GRANT` statements that DAB resource `permissions:` blocks cannot express directly (e.g. cross-schema grants for the cluster service principal).

**Fallback:** if DAB-native schemas/volumes are blocked by workspace permissions, run the existing `src/notebooks/bootstrap_bronze_landing.ipynb` via a `notebook_task` in the setup job. Do not point DAB at `src/notebooks/bootstrap_bronze_landing.py` — that file does not exist.

---

## File Arrival Trigger Guidance

Databricks file arrival triggers monitor a Unity Catalog volume root or subpath, and check subdirectories recursively.

Use:

```text
/Volumes/healthcare/bronze/raw_landing/
```

Important limitations:

- New files trigger runs; overwriting an existing filename does not.
- The path must not contain wildcards.
- The path must not contain external tables or managed locations of catalogs and schemas.
- Keep incoming files immutable.
- In production, upstream systems should land new files with unique names.
- For dev fixture refreshes, use the manual `load_sample_data` job and then manually run the pipeline job if needed.

Operational recommendation:

- **Enable managed file events on the external location backing the volume.** Without file events, the workspace is capped at 50 file-arrival jobs and 10,000 files at the watched path, and trigger latency is bounded by polling. With file events, capacity scales and latency drops to seconds.

Cadence:

- `min_time_between_triggers_seconds: 300` and `wait_after_last_change_seconds: 60` assume hourly-or-faster upstream batches. If the operational cadence is daily, raise to `3600` / `300` to avoid spurious double-fires.

---

## Out of Scope (deliberately)

These notebooks and capabilities are **not** part of the orchestrated production job and remain manual:

- `bronze_profiling.ipynb` — exploratory profiling. Run on demand.
- `claims_exploration.ipynb` — exploratory cells (the production analytics build is in `build_analytics.py`).
- `sample_prediction.ipynb` — demo for stakeholders. Manual.
- ETL constant parameterization (catalog/schema overrides via Spark config). Tracked for a later pass; the first orchestration cut targets the fixed `healthcare.*` deployment.
- `claim_type` feature — remains intentionally deferred (see CLAUDE.md). Do not add as part of orchestration unless a downstream consumer creates a concrete requirement.

---

## Implementation Phases

The phase order is constrained by the prerequisites above. Phase 3 and 4 code changes must precede the bundle skeleton, otherwise the bundle deploys but does not actually work end-to-end.

### Phase 1 — Code prerequisites (no DAB yet)

- `scripts/train_denial_model.py:275` argv passthrough fix.
- MLflow training metadata logging in `src/ml/train.py` (row count, Gold version, fingerprint, feature columns, target column, release gate result).
- `src/analytics/observability_assets.py` — add `pipeline_stage` parameter, append-mode writes, stage column.
- Add unit tests for: argv passthrough; metadata logging; stage-aware observability output schema.

### Phase 2 — Script entry points

- Create `src/scripts/{load_sample_data,verify_bronze,verify_silver,build_analytics,build_observability}.py`.
- Create `src/notebooks/{check_new_data,skip_retraining,grants}.ipynb`.
- Keep setup/reset/exploration/demo notebooks manual.
- Add unit tests where local fakes are practical (especially for `check_new_data`'s should-retrain logic).

### Phase 3 — Bundle skeleton

- Add `databricks.yml` with bundle variables (`catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `analytics_schema`, `ml_schema`, `node_type_id`) and per-target overrides for `dev`/`prod`.
- Add `resources/pipelines/{bronze,silver,gold}.pipeline.yml` (with `event_log:` configured, classic clusters with `SPOT_WITH_FALLBACK`).
- Add `resources/schemas/schemas.yml` and `resources/volumes/volumes.yml` (DAB-native infra).
- Add `resources/jobs/{setup_infrastructure,load_sample_data,etl_ml_pipeline}.job.yml`.

### Phase 4 — Observability integration

- Wire `build_observability.py` to Bronze, Silver, and Gold published event-log tables via `--published-event-log-table` and `--pipeline-stage`.
- Verify stage-aware output: a single `ops_pipeline_updates` row exists per stage per update.
- Add contract tests for stage-aware outputs.

### Phase 5 — Validation

- Run local contract tests:

```bash
uv run pytest -q
```

- Validate bundle config:

```bash
databricks bundle validate
```

- Deploy to dev:

```bash
databricks bundle deploy -t dev
```

- Run setup once.
- Load sample data in dev if needed.
- Run the main pipeline manually once before enabling or relying on file arrival.

---

## Acceptance Criteria

- `databricks bundle validate` passes for every target.
- Bronze, Silver, and Gold pipelines deploy as DAB resources, each publishing its event log to UC.
- The production job has no manual notebook steps in the recurring path (only `check_new_data` and `skip_retraining`, which are recurring notebooks by design).
- Bronze and Silver verification failures stop downstream tasks (real DAG gates).
- Observability captures each pipeline stage separately: `SELECT pipeline_stage, COUNT(*) FROM healthcare.analytics.ops_pipeline_updates GROUP BY pipeline_stage` returns three rows.
- `train_model` task receives DAB-passed parameters (verifies the C1 argv fix).
- Model retraining runs only when the current Gold data fingerprint differs from the champion model metadata, or no champion exists.
- Failed model release gates still block model persistence and registry promotion.
- Job cluster `spark_version` is the DBR ML runtime (`14.3.x-cpu-ml-scala2.12` or later).
- `claim_type` remains deferred.

---

## Deferred: `claim_type`

`claim_type` remains intentionally deferred. Do not add it as part of orchestration unless a downstream Week 5+ consumer creates a concrete requirement for it. Full waterfall lives in CLAUDE.md.
