# Chapter 9: Development How-To

Common developer tasks, with exact steps and code patterns.

## How to add a new ML feature

Adding a new feature column to the Gold table and ML pipeline touches 6 files.

### Step 1: Add Gold ETL computation

File: `ETL/pipelines/gold/gold_claim_features.py`

Add a `.withColumn("new_feature_name", ...)` in the `gold_claim_features()` function, inside the chain after the existing features. Pattern:

```python
.withColumn(
    "new_feature_name",
    F.when(
        F.col("some_column").isNotNull(),
        F.col("numerator").cast("double") / F.col("denominator").cast("double"),
    ).otherwise(F.lit(None).cast("double")),
)
```

If the feature needs a new threshold constant, add it to `src/common/gold_pipeline_config.py` first, then import it.

Add the column name to the final `.select(...)` list at the end of the function.

### Step 2: Add to feature constants

File: `src/ml/__init__.py`

Add `"new_feature_name"` to `FEATURE_COLUMNS` tuple. Keep alphabetical within each group (original, temporal, interaction).

### Step 3: Add fill value and type classification

File: `src/ml/features.py`

Add entry to `DEFAULT_FILL_VALUES`:
```python
"new_feature_name": 0.0,   # for numeric
"new_feature_name": 0,     # for integer/boolean
```

If boolean, add to `BOOLEAN_FEATURES` tuple. If numeric, add to `NUMERIC_FEATURES` tuple.

### Step 4: Update test sample DataFrames

File: `tests/test_ml_contract.py`

Three places need updating:

1. `FeaturePreparationTests.setUpClass()` — `cls.sample_df` (around line 23)
2. `ModelEvaluationTests.setUpClass()` — `feature_data` (around line 233)
3. `PredictionTests.setUpClass()` — `feature_data` (around line 339)
4. `test_coerce_features_casts_all_model_inputs_to_float64` — `raw` dict (around line 443)

Add the new column with realistic random data. Pattern:
```python
"new_feature_name": np.random.uniform(0.0, 1.0, n),
```

### Step 5: Update contract assertions

File: `tests/test_ml_contract.py`

Update `test_feature_columns_constant_is_stable` — change `len(FEATURE_COLUMNS)` count and add to `expected_features` set.

File: `tests/test_gold_contract.py`

Add to `test_gold_pipeline_includes_required_features` list.

### Step 6: Run Gold pipeline first, then train

```
databricks bundle run gold_pipeline -t dev --profile dev
databricks bundle run ml_retrain_job -t dev --profile dev
```

---

## How to add a new ML model

Adding a model candidate like a new boosting library or classifier touches 3 files.

### Step 1: Add default params and training function

File: `src/ml/train.py`

Add a `NEWMODEL_DEFAULT_PARAMS` constant:
```python
NEWMODEL_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "param1": value1,
    "param2": value2,
    "random_state": 42,
}
```

Add a `train_newmodel()` function following the pattern of `train_xgboost()` (`src/ml/train.py:115-135`):
```python
def train_newmodel(
    X_train: Any,
    y_train: Any,
    X_val: Any = None,
    y_val: Any = None,
    params: dict[str, Any] | None = None,
    random_seed: int = 42,
    sample_weight: Any = None,
) -> Any:
    """Fit a NewModel classifier."""
    training_params = {**NEWMODEL_DEFAULT_PARAMS, **(params or {}), "random_state": random_seed}
    model = NewModelClassifier(**training_params)
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_val, y_val)]
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)
    return model
```

### Step 2: Add Optuna builder and tune function

File: `src/ml/train.py`

Add `_build_newmodel_from_trial()` — defines the Optuna search space:
```python
def _build_newmodel_from_trial(trial: Any, random_seed: int = 42) -> Any:
    params = {
        "param1": trial.suggest_int("param1", min_val, max_val),
        "param2": trial.suggest_float("param2", min_val, max_val, log=True),
        "random_state": random_seed,
    }
    return NewModelClassifier(**params)
```

Add `tune_newmodel_optuna()` following `tune_xgboost_optuna()` pattern (`src/ml/train.py:198-226`):

```python
def tune_newmodel_optuna(
    X_train: Any, y_train: Any,
    n_trials: int = 50, random_seed: int = 42, groups: Any = None,
) -> tuple[CalibratedClassifierCV, dict[str, Any]]:
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_newmodel_from_trial, X_train, y_train, random_seed, groups=groups),
        n_trials=n_trials, show_progress_bar=False,
    )
    best_params = dict(study.best_trial.params)
    best_params.update({"random_state": random_seed})
    base = NewModelClassifier(**best_params)
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params
```

Add to `__all__` at the bottom — keep alphabetically sorted.

### Step 3: Wire into training pipeline

File: `src/scripts/train_denial_model.py`

Import the new functions at the top. In `train_pipeline()`, add a training block after the existing CatBoost section:

```python
# --- NewModel ---
if tune:
    newmodel_model, newmodel_params = tune_newmodel_optuna(
        X_train, y_train, n_trials=optuna_trials, random_seed=random_seed, groups=train_groups,
    )
else:
    newmodel_raw = train_newmodel(X_tr, y_tr, random_seed=random_seed, sample_weight=sample_weight_tr)
    newmodel_model = select_best_calibration(newmodel_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
    newmodel_params = dict(NEWMODEL_DEFAULT_PARAMS)
    newmodel_params["random_state"] = random_seed
newmodel_metrics = evaluate_model(newmodel_model, X_test, y_test)
```

Add to `candidates` list. Update `main()` unpacking and print loop.

### Step 4: Add dependency

If the model needs a new package, add to:
- `pyproject.toml` — `ml` dependency group
- `services/ml/training/resources/training.job.yml` — `dependencies` list

### Step 5: Run tests

```
uv run pytest tests/test_ml_contract.py -v
```

---

## How to add a new CLI flag

### Step 1: Add to argument parser

File: `src/scripts/train_denial_model.py` (or whichever script)

In `_parse_args()`:
```python
parser.add_argument(
    "--my-new-flag",
    type=int,
    default=42,
    help="Description of what this flag does.",
)
```

### Step 2: Thread through the pipeline

In `train_pipeline()`, add the parameter:
```python
def train_pipeline(
    ...
    my_new_flag: int = 42,
) -> tuple:
```

Use it where needed.

### Step 3: Wire from main()

In `main()`:
```python
train_pipeline(
    ...
    my_new_flag=args.my_new_flag,
)
```

### Step 4: Add to job YAML (if used in bundle runs)

File: `services/ml/training/resources/training.job.yml`

```yaml
              - --my-new-flag
              - "42"
```

---

## How to add a new ETL pipeline (new layer or parallel pipeline)

### Step 1: Create pipeline Python file

`ETL/pipelines/<layer>/<pipeline_name>.py` using the SDP decorator pattern:

```python
from pyspark import pipelines as dp

@dp.materialized_view(
    name="catalog.schema.table_name",
    refresh_policy="incremental",
    comment="What this pipeline produces.",
    table_properties={...},
)
def my_pipeline():
    source = spark.read.table("source_table")
    return source.withColumn("new_col", ...)
```

### Step 2: Create pipeline resource YAML

`services/<layer>/<name>/resources/<name>.pipeline.yml`:
```yaml
resources:
  pipelines:
    my_new_pipeline:
      name: "[${bundle.target}] My New Pipeline"
      target: "catalog.schema"
      continuous: false
      development: true
      photon: false
      serverless: true
      channel: PREVIEW
      edition: ADVANCED
      libraries:
        - notebook:
            path: /Workspace${workspace.file_path}/ETL/pipelines/<layer>/<pipeline_name>.py
      configuration:
        catalog: ${var.catalog}
      clusters:
        - label: default
          node_type_id: ${var.node_type_id}
          spark_version: ${var.spark_version}
          gcp_attributes:
            availability: PREEMPTIBLE_WITH_FALLBACK_GCP
```

DAB auto-discovers it via the `services/*/*/resources/*.yml` glob.

### Step 3: Deploy and run

```
databricks bundle deploy -t dev --profile dev
databricks bundle run my_new_pipeline -t dev --profile dev
```

---

## How to add a new test

All tests in `tests/`. Contract test pattern is the convention.

### ML tests

File: `tests/test_ml_contract.py`

Each test class corresponds to a module area (FeaturePreparation, ModelTraining, ModelEvaluation, Prediction, RegistryLoad, ReleaseGate). Add a test method:

```python
def test_my_new_behavior(self):
    from src.ml.some_module import some_function
    result = some_function(test_input)
    self.assertEqual(result, expected_output)
```

### Gold tests

File: `tests/test_gold_contract.py`

Source-file contract tests read the Python source file and assert patterns exist. For a new feature:
```python
def test_gold_pipeline_has_my_feature(self):
    source = GOLD_PIPELINE_PATH.read_text(encoding="utf-8")
    self.assertIn("my_new_feature", source)
```

### Run tests

```
uv run pytest tests/test_ml_contract.py::YourTestClass::test_my_new_behavior -v
uv run pytest -q   # full suite
```

---

## How to change a threshold or gate value

All thresholds are constants in their respective `*_config.py` files. Change the constant, tests assert the new value.

| What | File | Constant |
|------|------|----------|
| Release gate thresholds | `src/ml/evaluate.py:22-24` | `DEFAULT_MIN_RECALL_AT_HIGH`, `DEFAULT_MIN_PRECISION`, `DEFAULT_MIN_ROC_AUC` |
| High-risk probability cutoff | `src/ml/evaluate.py:20` and `src/ml/predict.py:21` | `HIGH_RISK_PROBABILITY_THRESHOLD` — must match in both files |
| High cost ratio | `src/common/gold_pipeline_config.py:20` | `HIGH_COST_RATIO_THRESHOLD = 1.5` |
| Severe mismatch floor | `src/common/gold_pipeline_config.py:21` | `HIGH_SEVERITY_EXPECTED_COST_FLOOR = 5000.0` |
| Provider risk minimum count | `src/common/gold_pipeline_config.py:23` | `MIN_PROVIDER_RISK_COUNT = 5` |
| Lookback windows | `src/common/gold_pipeline_config.py:22-25` | `PROVIDER_LOOKBACK_WINDOW_DAYS`, `_60D`, `_90D` |
| Optuna precision floor | `src/ml/train.py:24` | `OPTUNA_PRECISION_FLOOR = 0.70` |
| Latency budget | `src/ml/predict.py:24` | `LATENCY_BUDGET_MS = 150.0` |
| Retrain thresholds | `src/ml/retrain_gate.py:38-39` | `_RETRAIN_ROW_COUNT_MIN_DELTA = 100`, `_RETRAIN_ROW_COUNT_PCT_THRESHOLD = 0.05` |

After changing a constant, run `uv run pytest tests/ -q -k "stable or constant"` to find broken contract assertions.

---

## How to add a new service

A service is a logical grouping of resources (jobs, pipelines) under `services/<name>/`.

1. Create `services/<name>/resources/<name>.job.yml` (or `.pipeline.yml`)
2. DAB auto-discovers it via glob patterns in `databricks.yml:include`
3. If the service has a Python entry point, create `src/scripts/<name>.py`
4. Reference the script in the job YAML as `spark_python_task.python_file`
5. Add any new Python dependencies to both `pyproject.toml` and the job YAML `dependencies`
6. Deploy: `databricks bundle deploy -t dev --profile dev`
7. Run: `databricks bundle run <resource_key> -t dev --profile dev`

No other registration needed — DAB glob discovery handles everything.

---

## File edit quick-reference

| Task | Files to touch |
|------|---------------|
| Add ML feature | `gold_claim_features.py` → `__init__.py` → `features.py` → `test_ml_contract.py` → `test_gold_contract.py` |
| Add ML model | `train.py` → `train_denial_model.py` → `pyproject.toml` → `training.job.yml` |
| Add CLI flag | `train_denial_model.py` → `training.job.yml` |
| Change threshold | Find constant in config file, update, fix tests |
| Add ETL pipeline | Create `.py` + `.pipeline.yml`, DAB auto-discovers |
| Add service | Create `services/<name>/resources/<name>.job.yml` |
| Add test | `tests/test_<layer>_contract.py` |
| Add Python dep | `pyproject.toml` + `training.job.yml` (if used in ML job) |
| Change job schedule/trigger | Edit the `.job.yml` or `.pipeline.yml` resource YAML |
| Change target variable | `databricks.yml` variables block |
