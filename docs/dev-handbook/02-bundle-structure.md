# Chapter 2: Databricks Asset Bundle Layout

## 2.1 Bundle Root: `databricks.yml`

The bundle is defined at the project root in `databricks.yml`. It forms the single entry point for deploying the entire Healthcare Claim Ops platform to a Databricks workspace.

```yaml
bundle:
  name: healthcare-claim-ops
  engine: direct
```

- **name**: `healthcare-claim-ops` -- used in resource naming conventions.
- **engine**: `direct` -- resources are deployed directly using the Databricks REST API (not Terraform).

## 2.2 Resource Auto-Discovery with `include:` Globs

DAB discovers resource files through glob patterns in the `include:` block:

```yaml
include:
  - resources/schemas/schemas.yml
  - resources/volumes/volumes.yml
  - services/*/resources/*.yml
  - services/*/*/resources/*.yml
  - services/*/*/*/resources/*.yml
```

These five entries match every possible resource definition across the project tree:

| Glob Pattern | Matches |
|---|---|
| `resources/schemas/schemas.yml` | Unity Catalog schema declarations |
| `resources/volumes/volumes.yml` | Managed volume declarations |
| `services/*/resources/*.yml` | Top-level service resources (e.g., `services/etl/resources/*.yml`) |
| `services/*/*/resources/*.yml` | Two-level deep (e.g., `services/ml/training/resources/*.yml`) |
| `services/*/*/*/resources/*.yml` | Three-level deep (future, deeply nested services) |

A resource file is automatically discovered when placed at any of these path depths. No manual registration is required beyond placing the file in the correct directory.

## 2.3 Variables Block

The `variables:` section declares environment-portable parameters:

```yaml
variables:
  catalog:
    default: healthcare
  bronze_schema:
    default: bronze
  silver_schema:
    default: silver
  gold_schema:
    default: gold
  analytics_schema:
    default: analytics
  ml_schema:
    default: ml
  node_type_id:
    default: n2-highmem-2
  spark_version:
    default: 17.3.x-cpu-ml-scala2.13
  model_version:
    description: MLflow registered model version for production serving.
```

| Variable | Default | Purpose |
|----------|---------|---------|
| `catalog` | `healthcare` | Unity Catalog name for all tables |
| `bronze_schema` | `bronze` | Schema name for Bronze tables |
| `silver_schema` | `silver` | Schema name for Silver tables |
| `gold_schema` | `gold` | Schema name for Gold tables |
| `analytics_schema` | `analytics` | Schema name for analytics/observability tables |
| `ml_schema` | `ml` | Schema name for ML model registry objects |
| `node_type_id` | `n2-highmem-2` | Compute node type for Databricks clusters (memory-optimized) |
| `spark_version` | `17.3.x-cpu-ml-scala2.13` | Databricks ML runtime for Spark compatibility |
| `model_version` | _(none in prod)_ | MLflow model version; dev defaults to `"1"`, prod requires explicit `--var` |

## 2.4 Targets: Dev and Prod

Two targets control environment-specific behavior:

### Dev Target (Default)

```yaml
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

- Activated when no `--target` flag is passed.
- `mode: development` allows resource creation without production safeguards.
- `root_path` includes the current user's name, giving each developer an isolated deployment.
- `catalog` is fixed to `healthcare` (see Section 2.6 for limitations).
- `model_version` defaults to `"1"` since dev environments do not query the MLflow registry.

### Prod Target

```yaml
prod:
  mode: production
  workspace:
    profile: prod
    root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
  variables:
    catalog: healthcare
```

- `mode: production` requires explicit acknowledgment and additional safeguards.
- `model_version` has no default -- it must be supplied at deploy time via `--var=model_version=<n>`, resolved by CI/CD pipeline logic.
- Both targets use the same `root_path` pattern; in practice, a CI service principal runs the prod deployment, so `current_user.userName` resolves to the service principal's identity.

## 2.5 Variable Flow: From `databricks.yml` to Spark SQL

The variable substitution follows a three-hop chain:

```
databricks.yml       Job YAML                Python CLI          Spark SQL
${var.catalog}  -->  --catalog ${var.catalog} --> parser arg  -->  spark.table(f"{catalog}.{schema}.{table}")
```

**Hop 1 -- DAB variable resolution**: At bundle deploy time, DAB substitutes `${var.catalog}` into every resource YAML. The value is taken from the active target's variable declarations.

**Hop 2 -- Python argument passing**: Job YAMLs pass DAB-resolved values as CLI parameters to Spark Python tasks. For example, `services/ml/training/resources/training.job.yml`:

```yaml
spark_python_task:
  python_file: ../../../../src/scripts/maybe_retrain_model.py
  parameters:
    - --catalog
    - ${var.catalog}
    - --gold-table
    - ${var.catalog}.${var.gold_schema}.claim_features
    - --registered-model-name
    - ${var.catalog}.${var.ml_schema}.claim_denial_model
```

**Hop 3 -- Python runtime table resolution**: Every script uses `argparse` to parse the CLI parameters into Python variables, then passes them to Spark SQL table references:

```python
# src/common/bronze_pipeline_config.py:37
def table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"

# Usage:
spark.table(table_name(catalog, "gold", "claim_features"))
```

This pattern means no Python code contains hard-coded environment-specific table names. However, there is a known limitation: the ETL pipeline modules (`ETL/pipelines/bronze/*.py`, `ETL/pipelines/silver/*.py`, `ETL/pipelines/gold/*.py`) currently import constants from `src/common/{bronze,silver,gold}_pipeline_config.py` that default to `healthcare`, `bronze`, `silver`, `gold`. True dev/prod isolation would require these constants to read catalog/schema from Spark configuration -- this is tracked in `docs/deferred.md` as a deferred decision.

## 2.6 `experimental.skip_name_prefix_for_schema`

```yaml
experimental:
  skip_name_prefix_for_schema: true
```

**What it does**: Prevents DAB from prepending the bundle name or target name as a prefix to schema names during deployment.

**Why it is needed**: Without this flag, a schema declared as `bronze` in `resources/schemas/schemas.yml` would be created as `healthcare-claim-ops_bronze` or (in dev) `dev_healthcare-claim-ops_bronze`. Since all Python code references `healthcare.bronze.*` verbatim through imported constants, a prefixed schema would cause every table read/write to fail at runtime.

**Trade-off**: Because `skip_name_prefix_for_schema` is `true`, both `dev` and `prod` targets deploy to the same `healthcare` catalog. This is acceptable for the current single-environment deployment. A future multi-environment setup (true dev/prod workspace isolation) would address this through Spark configuration parameterization rather than DAB schema prefixes.

## 2.7 Directory Tree

Below is the complete layout of `services/` and `resources/`:

```
project-root/
  databricks.yml                          # Bundle root
  resources/
    schemas/
      schemas.yml                         # 6 Unity Catalog schemas
    volumes/
      volumes.yml                         # 1 managed volume (raw_landing)
  services/
    manifest.yml                          # Service manifest (dependency graph)
    etl/
      service.yml                         # etl_pipeline service definition (SDP pipeline)
      file_arrival.service.yml            # etl_file_arrival service (triggered job)
      fast_dev.service.yml                # etl_fast_dev service (manual job)
      analytics_observability.service.yml # analytics_observability service (post-ETL)
      resources/
        etl.pipeline.yml                  # The SDP pipeline resource
        etl_file_arrival.job.yml          # File-arrival triggered job resource
        etl_fast_dev.job.yml              # Fast dev job resource
        analytics_observability.job.yml   # Analytics + observability job resource
    ml/
      training/
        service.yml                       # ml_training service definition
        resources/
          training.job.yml                # ML retrain job resource
    infrastructure/
      setup/
        service.yml                       # setup_infrastructure service definition
        resources/
          setup_infrastructure.job.yml    # Setup job resource
```

## 2.8 Service Manifest

The service manifest at `services/manifest.yml` declares the dependency graph:

```yaml
services:
  etl_pipeline:
    type: pipeline
    manifest: services/etl/service.yml
  etl_file_arrival:
    type: job
    manifest: services/etl/file_arrival.service.yml
    depends_on:
      - etl_pipeline
  ml_training:
    type: job
    manifest: services/ml/training/service.yml
    depends_on:
      - etl_pipeline
  setup_infrastructure:
    type: job
    manifest: services/infrastructure/setup/service.yml
```

Each service has a `type` (`pipeline` or `job`), points to its manifest, and optionally declares `depends_on` for ordering. The manifest is consumed by external integration tooling (verify scripts, dashboard generators) and is not used directly by DAB.

## 2.9 Service Definition Files

Each service directory contains a `service.yml` that declares the service metadata and identifies the entry-point DAB resource:

```yaml
# services/etl/service.yml
service:
  name: etl_pipeline
  type: pipeline
  version: "1.0.0"
  description: Consolidated Bronze-Silver-Gold Lakeflow ETL pipeline.
entry_point:
  resource_key: healthcare_etl_pipeline
  resource_type: pipelines
health_check:
  type: script
  entry_point: src/scripts/verify_etl_light.py
```

The `entry_point` links the service abstraction to its DAB resource. The `health_check` (when present) references the verification script that validates the service's output post-execution.

## 2.10 Job YAML Structure

Job resource YAMLs follow a consistent structure. Here is the ML training job as an example:

```yaml
# services/ml/training/resources/training.job.yml
resources:
  jobs:
    ml_retrain_job:
      name: "[${bundle.target}] Healthcare ML Retrain"

      tasks:
        - task_key: maybe_retrain_model
          spark_python_task:
            python_file: ../../../../src/scripts/maybe_retrain_model.py
            parameters:
              - --catalog
              - ${var.catalog}
              - --gold-table
              - ${var.catalog}.${var.gold_schema}.claim_features
              - --registered-model-name
              - ${var.catalog}.${var.ml_schema}.claim_denial_model
              - --champion-alias
              - champion
              - --optuna-trials
              - "50"
              - --random-seed
              - "42"
              - --force
          environment_key: default

      environments:
        - environment_key: default
          spec:
            environment_version: "5"
            dependencies:
              - --editable ${workspace.file_path}
              - xgboost>=2.0,<3.0
              - lightgbm>=4.2,<5.0
              - catboost>=1.2,<2.0
              - scikit-learn>=1.5,<2.0
              - shap>=0.44,<1.0
              - optuna>=3.6,<4.0
              - mlflow>=3.0,<4.0
              - imbalanced-learn>=0.12,<1.0
```

Key structural elements:

- **`tasks`**: Defines one or more task entries. Each task has a `task_key` for dependency chaining and an executor type (`spark_python_task`, `notebook_task`, `pipeline_task`, `run_job_task`, or `condition_task`).
- **`spark_python_task`**: Executes a Python file with CLI parameters. The `python_file` path is relative to the resource YAML file's location.
- **`environment_key`**: References a named environment in the `environments` block.
- **`parameters`**: Array of `--flag value` pairs that become `sys.argv` in the Python script.
- **`environments`**: Declares cluster environment dependencies, using `--editable ${workspace.file_path}` to install the bundle's own Python package plus any additional library dependencies.

The `--force` flag in the ML job parameters defaults to being uncommented, giving developers control over whether to skip the retrain gate and retrain unconditionally.

## 2.11 Pipeline YAML Structure

Pipeline resources (SDP Lakeflow pipelines) follow a different structure:

```yaml
# services/etl/resources/etl.pipeline.yml
resources:
  pipelines:
    healthcare_etl_pipeline:
      name: "[${bundle.target}] Healthcare ETL Pipeline"
      catalog: ${var.catalog}
      target: ${var.gold_schema}
      serverless: true
      channel: current
      libraries:
        - glob:
            include: ../../../ETL/pipelines/bronze/**
        - glob:
            include: ../../../ETL/pipelines/silver/**
        - glob:
            include: ../../../ETL/pipelines/gold/**
      environment:
        dependencies:
          - --editable ${workspace.file_path}
      event_log:
        catalog: ${var.catalog}
        schema: ${var.analytics_schema}
        name: etl_pipeline_event_log
```

- **`catalog` / `target`**: The Unity Catalog and schema where pipeline output tables are materialized.
- **`serverless: true`**: Uses Databricks Serverless DLT (no cluster management).
- **`libraries`**: Glob patterns pointing to directories of Python files. All `.py` files in the matched directories are loaded by the pipeline.
- **`event_log`**: Configures the pipeline event log table in the analytics schema for observability.

## 2.12 How to Add a New Service

Adding a new service follows these steps:

1. **Create the service directory**: `services/<name>/` with a `service.yml` metadata file.

2. **Create the resource file**: `services/<name>/resources/<name>.job.yml` (for jobs) or `services/<name>/resources/<name>.pipeline.yml` (for pipelines). DAB auto-discovers the file through the `include:` globs.

3. **Register in the service manifest** (optional, for integration tooling): Add an entry to `services/manifest.yml`.

4. **Declare Unity Catalog schemas** (if needed): Add a new schema entry to `resources/schemas/schemas.yml`.

5. **Deploy**: Run `databricks bundle deploy --target dev` from the project root.

That is all. The glob-based discovery means no YAML import or registration is needed in `databricks.yml` -- the `services/*/*/resources/*.yml` pattern covers `services/<name>/resources/<resource>.yml` automatically. The three-level pattern `services/*/*/*/resources/*.yml` covers deeper nesting like `services/ml/training/resources/training.job.yml`.
