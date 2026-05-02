# Chapter 5: ML Pipeline

This chapter covers the complete machine learning pipeline: feature loading, model training (6 candidates), Optuna hyperparameter tuning, probability calibration, the release gate, MLflow integration, the retrain gate, and how to add a new model.

## 5.1 Architecture Overview

The training flow follows this sequence:

```
Gold table (healthcare.gold.claim_features)
  --> prepare_training_data() --> (X, y) with fill_nulls
  --> stratified_split() --> train/test split (70/30)
  --> For each of 6 model candidates:
      --> Train on 80% of training data, hold out 20% for calibration selection
      --> Optionally: Optuna tuning (50-200 trials, median pruner)
      --> Calibrate via CalibratedClassifierCV (Platt sigmoid)
      --> Evaluate on held-out test split
  --> Sort candidates by (meets_thresholds, recall_at_high, roc_auc)
  --> Best candidate passes release gate?
      YES: pickle to disk, log to MLflow, register in Unity Catalog, move champion alias
      NO:  exit non-zero, no model saved
```

### Key Files

| File | Purpose | Key Exports |
|---|---|---|
| `src/ml/__init__.py` | Feature column definitions | `FEATURE_COLUMNS` (20 features), `TARGET_COLUMN` |
| `src/ml/features.py` | Data loading and preparation | `load_gold_features()`, `prepare_training_data()`, `fill_nulls()`, `stratified_split()`, `temporal_split()`, `BOOLEAN_FEATURES`, `NUMERIC_FEATURES`, `DEFAULT_FILL_VALUES` |
| `src/ml/train.py` | Model training and tuning | `train_xgboost()`, `train_lightgbm()`, `train_catboost()`, `train_logistic_regression()`, `train_voting_ensemble()`, `train_stacking_ensemble()`, `tune_xgboost_optuna()`, `tune_lightgbm_optuna()`, `tune_catboost_optuna()`, `calibrate_classifier()`, `select_best_calibration()`, `train_with_mlflow()` |
| `src/ml/evaluate.py` | Metrics and release gate | `evaluate_model()`, `EvaluationMetrics.meets_thresholds()`, `recall_at_high()`, `find_optimal_threshold()`, `compute_shap_values()` |
| `src/ml/predict.py` | Inference | `load_from_registry()`, `predict_single()`, `predict_batch()`, `RiskLevel` |
| `src/ml/retrain_gate.py` | Retrain decision logic | `decide_retrain()`, `compute_fingerprint()`, `RetrainDecision` |
| `src/scripts/train_denial_model.py` | Training entry point | `train_pipeline()`, `main()` |
| `src/scripts/maybe_retrain_model.py` | Retrain orchestration | Calls `decide_retrain()` then `train_main()` if needed |

## 5.2 Feature Columns

**File:** `src/ml/__init__.py`

```python
FEATURE_COLUMNS = (
    "is_procedure_missing",         "is_amount_missing",
    "amount_to_benchmark_ratio",    "billed_vs_avg_cost",
    "high_cost_flag",               "severity_procedure_mismatch",
    "specialty_diagnosis_mismatch", "provider_location_missing",
    "diagnosis_severity_encoded",   "diagnosis_count",
    "provider_claim_count",         "provider_claim_count_30d",
    "provider_risk_score",          "provider_claim_count_60d",
    "provider_claim_count_90d",     "cost_overbenchmark_and_highseverity",
    "mismatch_and_overbenchmark",   "provider_30d_denial_rate",
    "missing_fields_count",         "low_volume_provider_risk",
)
TARGET_COLUMN = "denial_label"
```

These 20 features are classified as boolean or numeric in `src/ml/features.py:11-35`:

```python
BOOLEAN_FEATURES = (
    "is_procedure_missing", "is_amount_missing", "high_cost_flag",
    "severity_procedure_mismatch", "specialty_diagnosis_mismatch",
    "provider_location_missing",
)
NUMERIC_FEATURES = (
    "amount_to_benchmark_ratio", "billed_vs_avg_cost",
    "diagnosis_severity_encoded", "diagnosis_count", "provider_claim_count",
    "provider_claim_count_30d", "provider_claim_count_60d",
    "provider_claim_count_90d", "provider_risk_score",
    "cost_overbenchmark_and_highseverity", "mismatch_and_overbenchmark",
    "provider_30d_denial_rate", "missing_fields_count",
    "low_volume_provider_risk",
)
```

**Important:** Boolean features are cast to `int` (0/1) during data preparation for model consumption. See `prepare_training_data()` at `src/ml/features.py:82-93`:

```python
def prepare_training_data(df, feature_columns=FEATURE_COLUMNS, target_column=TARGET_COLUMN):
    filled = fill_nulls(df)
    X = filled[list(feature_columns)].copy()
    for col in BOOLEAN_FEATURES:
        if col in X.columns:
            X[col] = X[col].astype(int)
    y = filled[target_column].astype(int)
    return X, y
```

### Default Fill Values

**File:** `src/ml/features.py:37-58`

```python
DEFAULT_FILL_VALUES = {
    "is_procedure_missing": 0,           "is_amount_missing": 0,
    "amount_to_benchmark_ratio": 0.0,    "billed_vs_avg_cost": 0.0,
    "high_cost_flag": 0,                 "severity_procedure_mismatch": 0,
    "specialty_diagnosis_mismatch": 0,   "provider_location_missing": 0,
    "diagnosis_severity_encoded": 0,     "diagnosis_count": 1,
    "provider_claim_count": 0,           "provider_claim_count_30d": 0,
    "provider_claim_count_60d": 0,       "provider_claim_count_90d": 0,
    "provider_risk_score": 0.0,          "cost_overbenchmark_and_highseverity": 0.0,
    "mismatch_and_overbenchmark": 0.0,   "provider_30d_denial_rate": 0.0,
    "missing_fields_count": 0,           "low_volume_provider_risk": 0.0,
}
```

Note: `diagnosis_count` defaults to `1` (not `0`) because a claim with a valid diagnosis code always maps to at least one diagnosis. Filling with `0` would incorrectly represent "no diagnosis" rather than "unknown."

## 5.3 Model Candidates (All 6)

The training pipeline (`train_pipeline()` in `src/scripts/train_denial_model.py:146-315`) trains all 6 candidates, calibrates them, evaluates them, then picks the winner.

### 5.3.1 XGBoost

**Default hyperparameters** (`src/ml/train.py:56-68`):

```python
XGBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "early_stopping_rounds": 50,
    "scale_pos_weight": 2.5,
    "random_state": 42,
}
```

The `scale_pos_weight = 2.5` compensates for the ~70/30 class imbalance (approved/denied) in the synthetic dataset. This prevents the model from biasing toward the majority class and missing the Recall@HIGH gate.

**Optuna search space** (`_build_xgb_from_trial`, `src/ml/train.py:298-314`):

```python
params = {
    "max_depth": trial.suggest_int("max_depth", 3, 10),
    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
    "n_estimators": trial.suggest_int("n_estimators", 50, 300),
    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
    "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
    "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
    "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
}
```

**Tune function:** `tune_xgboost_optuna()` at `src/ml/train.py:485-519`. Runs Optuna, refits and calibrates the best model on the full training set with `cv=3`.

### 5.3.2 LightGBM

**Default hyperparameters** (`src/ml/train.py:70-81`):

```python
LIGHTGBM_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "objective": "binary",
    "metric": "binary_logloss",
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "scale_pos_weight": 2.5,
    "class_weight": "balanced",
    "random_state": 42,
    "verbose": -1,
}
```

Note: LightGBM additionally uses `class_weight="balanced"` alongside `scale_pos_weight` for dual-class-imbalance handling.

**Optuna search space** (`_build_lgb_from_trial`, `src/ml/train.py:317-337`):

```python
params = {
    "num_leaves": trial.suggest_int("num_leaves", 15, 127),
    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
    "n_estimators": trial.suggest_int("n_estimators", 50, 300),
    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
    "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
    "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
    "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
    "lambda_l1": trial.suggest_float("lambda_l1", 0.0, 10.0),
    "lambda_l2": trial.suggest_float("lambda_l2", 0.0, 10.0),
    "min_split_gain": trial.suggest_float("min_split_gain", 0.0, 1.0),
}
```

**Tune function:** `tune_lightgbm_optuna()` at `src/ml/train.py:522-559`.

### 5.3.3 CatBoost

**Default hyperparameters** (`src/ml/train.py:83-93`):

```python
CATBOOST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "objective": "Logloss",
    "eval_metric": "Logloss",
    "learning_rate": 0.1,
    "depth": 6,
    "iterations": 100,
    "scale_pos_weight": 2.5,
    "random_seed": 42,
    "verbose": False,
    "allow_writing_files": False,
}
```

`allow_writing_files: False` is required to prevent CatBoost from creating temporary files on Databricks clusters that have read-only root filesystems.

**Optuna search space** (`_build_catboost_from_trial`, `src/ml/train.py:340-359`):

```python
params = {
    "depth": trial.suggest_int("depth", 4, 10),
    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
    "iterations": trial.suggest_int("iterations", 50, 300),
    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
    "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.6, 1.0),
    "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 50),
    "scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
    "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
}
```

**Tune function:** `tune_catboost_optuna()` at `src/ml/train.py:562-600`.

### 5.3.4 Logistic Regression (Baseline)

**Default hyperparameters** (`src/ml/train.py:95-99`):

```python
LOGREG_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "max_iter": 1000,
    "class_weight": "balanced",
    "random_state": 42,
}
```

This is the simplest model. It is not tuned with Optuna. It serves as a baseline to confirm that the tree-based models are genuinely learning meaningful patterns rather than exploiting noise. If Logistic Regression approaches the tree-model metrics, that is a red flag for data leakage or insufficient signal.

**Train function:** `train_logistic_regression()` at `src/ml/train.py:102-112`.

### 5.3.5 Voting Ensemble

**Implementation** at `src/ml/train.py:212-228`:

```python
def train_voting_ensemble(estimators, X_train, y_train, voting="soft"):
    ensemble = VotingClassifier(estimators=estimators, voting=voting)
    ensemble.fit(X_train, y_train)
    return ensemble
```

Uses **soft voting** (averages predicted probabilities) of the three calibrated tree models: XGBoost + LightGBM + CatBoost. Each base estimator is already wrapped in `CalibratedClassifierCV`, so the soft vote combines meaningful probability distributions rather than raw uncalibrated scores.

### 5.3.6 Stacking Ensemble

**Implementation** at `src/ml/train.py:231-256`:

```python
def train_stacking_ensemble(estimators, X_train, y_train, final_estimator=None, cv=5):
    if final_estimator is None:
        final_estimator = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)
    ensemble = StackingClassifier(
        estimators=estimators,
        final_estimator=final_estimator,
        cv=cv,
    )
    ensemble.fit(X_train, y_train)
    return ensemble
```

The meta-learner (Logistic Regression) trains on out-of-fold predictions from the three base estimators with `cv=5`. This prevents overfitting to base-model idiosyncrasies -- each base estimator is refit on 4/5 folds and predicts the held-out 1/5, so the meta-learner sees predictions on data the base models did not train on.

## 5.4 Training Flow in Detail

**File:** `src/scripts/train_denial_model.py`, function `train_pipeline()` at lines 146-315.

The training pipeline:

1. **Load and prepare data** (lines 165-167):
   ```python
   provider_groups = extract_provider_groups(df)
   X, y = prepare_training_data(df)
   X_train, X_test, y_train, y_test = stratified_split(X, y, random_state=random_seed)
   ```

2. **Create calibration holdout** (lines 174-177):
   ```python
   X_tr, X_cal, y_tr, y_cal = train_test_split(
       X_train, y_train, test_size=0.2, stratify=y_train, random_state=random_seed,
   )
   sample_weight_tr = compute_sample_weights(y_tr)
   ```
   The 80/20 split within training data reserves a calibration validation set for `select_best_calibration()`. `sample_weight` assigns 3x weight to positive (denied) examples.

3. **Train Logistic Regression baseline** (lines 181-185): No tuning, always uses defaults.

4. **Train XGBoost** (lines 187-199): Either tuned (Optuna) or default params.

5. **Train LightGBM** (lines 202-212): Either tuned or default.

6. **Train CatBoost** (lines 214-224): Either tuned or default.

7. **Build ensembles** (lines 227-238): Voting and Stacking from the three calibrated tree models.

8. **Select champion** (lines 240-252):
   ```python
   candidates.sort(
       key=lambda c: (c[3].meets_thresholds(), c[3].recall_at_high, c[3].roc_auc),
       reverse=True,
   )
   best_name, best_model, best_params, best_metrics = candidates[0]
   ```
   Models that pass the gate are always ranked above those that fail. Within each group, higher Recall@HIGH wins; ties broken by ROC-AUC.

9. **Log to MLflow** (lines 259-313): Parameters, metrics, training metadata, feature columns, and (if gate passes) registration with champion alias.

## 5.5 Optuna Tuning

**Objective function** at `src/ml/train.py:362-483`:

The objective maximizes **mean Recall@HIGH across 5-fold StratifiedKFold (or GroupKFold)**:

```python
def _optuna_objective(trial, X_train, y_train, random_seed=42):
    base_estimator = _build_xgb_from_trial(trial, random_seed=random_seed)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
    fold_recalls = []
    fold_precisions = []
    for tr_idx, va_idx in skf.split(X_train, y_train):
        calibrated = CalibratedClassifierCV(base_estimator, method="sigmoid", cv=2)
        calibrated.fit(X_tr, y_tr)
        proba = calibrated.predict_proba(X_va)[:, 1]
        fold_recalls.append(recall_at_high(y_va, proba))
        pred = (proba >= 0.5).astype(int)
        fold_precisions.append(float(precision_score(y_va, pred, zero_division=0)))
    mean_recall = float(np.mean(fold_recalls))
    mean_precision = float(np.mean(fold_precisions))
    if mean_precision < OPTUNA_PRECISION_FLOOR:        # 0.70
        return mean_recall - 2.0 * (OPTUNA_PRECISION_FLOOR - mean_precision)
    return mean_recall
```

Key design choices:
- **Objective: Recall@HIGH (not ROC-AUC).** ROC-AUC is a ranking metric that does not reward concentrating probability mass above the 0.7 threshold. Prior runs hit AUC > 0.94 yet failed Recall@HIGH because scores did not concentrate above 0.7. This objective directly optimizes the gate metric.
- **Soft precision floor:** Trials below 0.70 precision are not discarded (which would give the sampler no gradient signal) but penalized by reducing the score by 2x the shortfall.
- **Inner calibration cv=2:** Each trial does 5 (outer CV) x 2 (inner calibration) = 10 fits. At 50 trials, that is ~500 fits, which completes in minutes on CPU.

**Pruner:** `MedianPruner(n_startup_trials=10, n_warmup_steps=3)` at `src/ml/train.py:499`. This stops poorly-performing trials early based on the running median of intermediate values.

**Default trials:** 50 in the training script; 200 in the production job YAML at `services/ml/training/resources/training.job.yml:19` (via `--optuna-trials 50` -- the YAML uses 50 currently but is designed to be increased).

**Cross-validation:** Uses `StratifiedKFold(n_splits=5)` by default. When `groups` (provider IDs) are available, switches to `GroupKFold(n_splits=5)` to prevent claims from the same provider appearing in both train and validation folds. See `_make_optuna_objective()` at `src/ml/train.py:434-482`.

## 5.6 Calibration

**File:** `src/ml/train.py`

### Why Calibration is Necessary

XGBoost's raw `predict_proba` is uncalibrated -- a predicted probability of 0.7 does not mean the model is "70% confident" the claim will be denied. The probabilities are systematically skewed by the boosting process and the `scale_pos_weight` parameter. Without calibration, the Recall@HIGH metric (which counts claims with probability >= 0.7) would measure an arbitrary cutoff rather than the meaningful "high-confidence denial" tier.

### `calibrate_classifier()`

**Lines 184-209.** Wraps any sklearn-compatible classifier in `CalibratedClassifierCV`:

```python
def calibrate_classifier(estimator, X_train, y_train, method="sigmoid", cv=3):
    calibrator = CalibratedClassifierCV(estimator, method=method, cv=cv)
    calibrator.fit(X_train, y_train)
    return calibrator
```

Uses Platt scaling (`method='sigmoid'`) by default, which fits a logistic regression on the model's output to map scores to well-calibrated probabilities. Sigmoid is preferred over isotonic on the small synthetic dataset because isotonic regression needs more data per fold to fit a stable step function.

### `select_best_calibration()`

**Lines 259-295.** Tries both sigmoid and isotonic calibration on the training data, then picks the one with lower log-loss on the validation holdout:

```python
def select_best_calibration(base_estimator, X_train, y_train, X_val, y_val, cv=3):
    sigmoid_calibrated = calibrate_classifier(base_estimator, X_train, y_train, method="sigmoid", cv=cv)
    sigmoid_loss = float(log_loss(y_val, sigmoid_calibrated.predict_proba(X_val)[:, 1]))
    isotonic_calibrated = calibrate_classifier(base_estimator, X_train, y_train, method="isotonic", cv=cv)
    isotonic_loss = float(log_loss(y_val, isotonic_calibrated.predict_proba(X_val)[:, 1]))
    if isotonic_loss < sigmoid_loss:
        return isotonic_calibrated
    return sigmoid_calibrated
```

This is used for the no-tune path (Logistic Regression, default-param tree models) where there is no Optuna-driven calibration selection.

### Unwrapping for SHAP

When you need the underlying tree model for SHAP feature importance, unwrap via:

```python
inner = model.calibrated_classifiers_[0].estimator
```

This is implemented in `_unwrap_for_shap()` at `src/ml/evaluate.py:138-152`.

## 5.7 Release Gate (THE MOST IMPORTANT SECTION)

**File:** `src/ml/evaluate.py`

Three metrics, ALL must pass for a model to be deployed:

| Gate | Threshold | Definition | Enforced At |
|---|---|---|---|
| **Recall@HIGH** | >= 0.80 | Fraction of truly-denied claims with predicted probability >= 0.7 | `evaluate.py:38-55` |
| **Precision** | >= 0.70 | Precision at default decision threshold (0.5) | `evaluate.py:38-55` |
| **ROC-AUC** | >= 0.85 | Area under the ROC curve | `evaluate.py:38-55` |

### `EvaluationMetrics.meets_thresholds()`

At `src/ml/evaluate.py:38-55`:

```python
@dataclass(frozen=True)
class EvaluationMetrics:
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float
    recall_at_high: float

    def meets_thresholds(
        self,
        min_recall_at_high: float = DEFAULT_MIN_RECALL_AT_HIGH,  # 0.80
        min_precision: float = DEFAULT_MIN_PRECISION,             # 0.70
        min_roc_auc: float = DEFAULT_MIN_ROC_AUC,                 # 0.85
    ) -> bool:
        return (
            self.recall_at_high >= min_recall_at_high
            and self.precision >= min_precision
            and self.roc_auc >= min_roc_auc
        )
```

### What Happens on Gate Failure

In `train_pipeline()` (`train_denial_model.py:357-367`):

```python
if not best_metrics.meets_thresholds():
    failures = []
    if best_metrics.recall_at_high < 0.80:
        failures.append(f"recall_at_high={best_metrics.recall_at_high:.4f} < 0.80")
    if best_metrics.precision < 0.70:
        failures.append(f"precision={best_metrics.precision:.4f} < 0.70")
    if best_metrics.roc_auc < 0.85:
        failures.append(f"roc_auc={best_metrics.roc_auc:.4f} < 0.85")
    print(f"FAIL: Threshold misses: {', '.join(failures)}")
    print("Model NOT saved (release gate blocked promotion).")
    return 1
```

Failing models are NOT pickled, NOT registered in MLflow, and the script exits non-zero (exit code 1). The `champion` alias is never moved.

### Candidate Selection

Candidates are sorted at `train_denial_model.py:248-251`:

```python
candidates.sort(
    key=lambda c: (c[3].meets_thresholds(), c[3].recall_at_high, c[3].roc_auc),
    reverse=True,
)
```

This ensures:
1. Any model that passes the gate beats any model that does not.
2. Among gate-passers, higher Recall@HIGH wins.
3. Ties are broken by ROC-AUC.

### `recall_at_high()` Implementation

At `src/ml/evaluate.py:58-77`:

```python
def recall_at_high(y_true, y_prob, threshold=HIGH_RISK_PROBABILITY_THRESHOLD):
    y_true_arr = np.asarray(y_true)
    y_prob_arr = np.asarray(y_prob)
    positives = y_true_arr == 1
    total_positives = int(positives.sum())
    if total_positives == 0:
        return 0.0
    high_and_positive = int(((y_prob_arr >= threshold) & positives).sum())
    return float(high_and_positive) / float(total_positives)
```

Returns 0.0 when there are no positives in `y_true` (edge case on tiny synthetic test splits -- the metric stays well-defined rather than dividing by zero).

## 5.8 MLflow Integration

**File:** `src/ml/train.py`, function `train_with_mlflow()` at lines 654-746.

### Registry Path

The Unity Catalog 3-level model name is `healthcare.ml.claim_denial_model` (default in `train_denial_model.py:101`). On Databricks, the registry URI is set to `databricks-uc` via `_configure_registry_for_runtime()` at `train.py:638-652`.

### What Gets Logged Per Run

```python
with mlflow.start_run(run_name=model_name):
    mlflow.log_params(params)                                    # All model hyperparameters
    mlflow.log_metrics(metrics)                                  # accuracy, precision, recall, f1, roc_auc, recall_at_high
    mlflow.log_params(training_metadata)                         # training_row_count, gold_table_name, gold_table_version, etc.
    mlflow.log_dict({"columns": list(feature_columns)}, "feature_columns.json")
    mlflow.sklearn.log_model(model, artifact_path="model", signature=..., registered_model_name=...)
```

Training metadata logged:
- `training_row_count` -- number of rows used for training
- `gold_table_name` -- the Gold table source
- `gold_table_version` -- Delta table version at training time
- `training_data_fingerprint` -- SHA-256 fingerprint of the training data
- `feature_columns` -- list of feature column names
- `target_column` -- `"denial_label"`
- `release_gate_passed` -- boolean tag

### Champion Alias

When the gate passes and `registered_model_name` is provided:

```python
client.set_registered_model_alias(
    name=registered_model_name,
    alias=champion_alias,  # "champion"
    version=str(version),
)
```

This moves the `champion` alias to the new model version. Inference callers load via:

```python
load_from_registry(
    name="healthcare.ml.claim_denial_model",
    alias="champion",
)
```

Implemented at `src/ml/predict.py:81-132`. The function uses the documented two-call Databricks loading pattern:

```python
mlflow.set_registry_uri("databricks-uc")
mlflow.pyfunc.load_model(f"models:/{name}@{alias}")
```

### Experiment Naming

Experiment names follow the pattern `claim_denial_{model_name}` (e.g., `claim_denial_xgboost`, `claim_denial_voting_ensemble`).

On Databricks, the name is prefixed with the workspace user path: `/Users/{user}/claim_denial_{model_name}` (see `_resolve_experiment_name()` at `src/ml/train.py:27-54`). The Databricks user is resolved at runtime via `spark.sql("SELECT current_user()")` because the cluster service account (`spark-...`) is not the workspace user. The `MLFLOW_EXPERIMENT_NAME` environment variable overrides everything for explicit control.

## 5.9 Retrain Gate

**File:** `src/ml/retrain_gate.py`

The retrain gate (`decide_retrain()` at lines 275-405) determines whether the model needs retraining. It is called by `src/scripts/maybe_retrain_model.py` before launching the full training pipeline.

### Decision Logic

```
decide_retrain() returns RetrainDecision with:
  1. does champion exist?                          NO  -> retrain (reason: "no champion model found")
  2. champion run found?                           NO  -> retrain (reason: "champion run not found / orphaned")
  3. fingerprint changed?                          YES -> check row count delta
  4.   row count delta >= threshold?               YES -> retrain (reason: "data fingerprint changed")
  5.   row count delta < threshold?                -> skip (reason: "fingerprint changed but delta below threshold")
  6. feature columns changed?                      YES -> retrain (reason: "feature columns changed")
  7. no changes detected                           -> skip (reason: "no data changes")
```

### Fingerprint Computation

`compute_fingerprint()` at `retrain_gate.py:214-247`:

```python
def compute_fingerprint(spark, gold_table, feature_columns):
    columns = sorted(feature_columns)
    frame = spark.table(gold_table).select(*columns)
    row_count = frame.count()
    sample_rows = (
        frame.withColumn("_sample_key",
            F.sha2(F.concat_ws("||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columns]
            ), 256))
        .orderBy(F.col("_sample_key").asc())
        .limit(256).drop("_sample_key").collect()
    )
    payload = {
        "columns": columns,
        "row_count": row_count,
        "rows": [row.asDict(recursive=True) for row in sample_rows],
    }
    return sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
```

The fingerprint combines sorted feature columns, row count, and a hash-ordered sample of 256 rows. This means any data change (new rows, changed values, dropped rows) will produce a different fingerprint.

### Row Count Threshold

At `retrain_gate.py:250-252`:

```python
def _row_count_threshold_exceeded(current_row_count, previous_training_row_count):
    threshold = max(
        _RETRAIN_ROW_COUNT_MIN_DELTA,              # 100 rows
        math.ceil(_RETRAIN_ROW_COUNT_PCT_THRESHOLD  # 5%
                  * previous_training_row_count)
    )
    return abs(current_row_count - previous_training_row_count) >= threshold
```

If the fingerprint changed but fewer than 100 rows (or 5%) of new data has arrived, retraining is skipped as unnecessary churn.

### Orchestration Script

**File:** `src/scripts/maybe_retrain_model.py`

```python
def main(argv=None):
    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()

    if not args.force:
        decision = decide_retrain(
            spark,
            gold_table=args.gold_table,
            feature_columns=list(FEATURE_COLUMNS),
            registered_model_name=args.registered_model_name,
            champion_alias=args.champion_alias,
        )
        print(decision.summary_line())
        if decision.decision_status == "error":
            return 1
        if not decision.should_retrain:
            return 0
    else:
        print("FORCE: skipping retrain-gate check, training unconditionally.")

    # If we get here, training is needed
    train_args = ["--tune", ...]
    return int(train_main(train_args))
```

The script is invoked by the Databricks job defined in `services/ml/training/resources/training.job.yml`. The `--force` flag bypasses the retrain gate entirely for manual retraining.

## 5.10 Inference

**File:** `src/ml/predict.py`

### Risk Tiers

At `predict.py:19-38`:

```python
RISK_THRESHOLD_LOW: Final[float] = 0.3
RISK_THRESHOLD_HIGH: Final[float] = 0.7

class RiskLevel(enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

    @classmethod
    def from_probability(cls, prob: float) -> RiskLevel:
        if prob < RISK_THRESHOLD_LOW:
            return cls.LOW
        if prob < RISK_THRESHOLD_HIGH:
            return cls.MEDIUM
        return cls.HIGH
```

### Single Claim Inference

```python
predict_single(model, feature_dict)
# Returns {"denial_probability": 0.87, "risk_level": "HIGH"}
```

Includes p95 latency tracking against the ARCHITECTURE.md Section 13 budget of 150ms.

### Batch Inference

```python
predict_batch(model, feature_df)
# Returns DataFrame with claim_id, denial_probability, risk_level
```

## 5.11 How to Add a New Model

This walkthrough adds a hypothetical `RandomForest` model to the candidate pool.

### Step 1: Add `*_DEFAULT_PARAMS` constant in `src/ml/train.py`

```python
RANDOMFOREST_DEFAULT_PARAMS: Final[dict[str, Any]] = {
    "n_estimators": 100,
    "max_depth": 10,
    "min_samples_leaf": 5,
    "class_weight": "balanced",
    "random_state": 42,
}
```

Add to `__all__` at the bottom of the file.

### Step 2: Add a `train_*()` function

```python
def train_randomforest(
    X_train, y_train, params=None, random_seed=42, sample_weight=None,
):
    from sklearn.ensemble import RandomForestClassifier

    training_params = {**RANDOMFOREST_DEFAULT_PARAMS, **(params or {}), "random_state": random_seed}
    model = RandomForestClassifier(**training_params)
    fit_kwargs = {}
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = sample_weight
    model.fit(X_train, y_train, **fit_kwargs)
    return model
```

### Step 3: Add `_build_*_from_trial()` for Optuna tuning (optional)

```python
def _build_rf_from_trial(trial, random_seed=42):
    from sklearn.ensemble import RandomForestClassifier
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 50, 300),
        "max_depth": trial.suggest_int("max_depth", 3, 20),
        "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 20),
        "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
        "max_features": trial.suggest_float("max_features", 0.3, 1.0),
        "class_weight": "balanced",
        "random_state": random_seed,
    }
    return RandomForestClassifier(**params)
```

### Step 4: Add `tune_*_optuna()` function (optional)

```python
def tune_randomforest_optuna(X_train, y_train, n_trials=50, random_seed=42, groups=None):
    import optuna
    from optuna.pruners import MedianPruner

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize",
        pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
    )
    study.optimize(
        _make_optuna_objective(_build_rf_from_trial, X_train, y_train, random_seed, groups=groups),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    best_params = dict(study.best_trial.params)
    best_params.update({"class_weight": "balanced", "random_state": random_seed})
    logger.info("Optuna RF best Recall@HIGH: %.4f, params: %s", study.best_value, best_params)
    base = RandomForestClassifier(**best_params)
    calibrated = calibrate_classifier(base, X_train, y_train, method="sigmoid", cv=3)
    return calibrated, best_params
```

### Step 5: Wire into `train_pipeline()` in `train_denial_model.py`

In `train_pipeline()`, add the new model after the existing CatBoost block (around line 225):

```python
# --- RandomForest ---
if tune:
    rf_model, rf_params = tune_randomforest_optuna(
        X_train, y_train, n_trials=optuna_trials, random_seed=random_seed, groups=train_groups,
    )
else:
    rf_raw = train_randomforest(X_tr, y_tr, random_seed=random_seed, sample_weight=sample_weight_tr)
    rf_model = select_best_calibration(rf_raw, X_tr, y_tr, X_cal, y_cal, cv=3)
    rf_params = dict(RANDOMFOREST_DEFAULT_PARAMS)
    rf_params["random_state"] = random_seed
rf_metrics = evaluate_model(rf_model, X_test, y_test)
```

Add to the candidates list:

```python
candidates = [
    # ... existing ...
    ("randomforest", rf_model, rf_params, rf_metrics),
]
```

### Step 6: Import and add to `__all__`

At the top of `train_denial_model.py`, add to imports:

```python
from src.ml.train import (
    # ... existing ...
    RANDOMFOREST_DEFAULT_PARAMS,
    train_randomforest,
    tune_randomforest_optuna,  # if tuning
)
```

In `src/ml/train.py`, add to `__all__`:

```python
__all__ = [
    # ... existing ...
    "RANDOMFOREST_DEFAULT_PARAMS",
    "train_randomforest",
    "tune_randomforest_optuna",
]
```

### Step 7: Add test

In the test file (likely `tests/test_ml_contract.py`), add a test that verifies the new model trains successfully and produces valid metrics:

```python
def test_randomforest_trains_and_produces_valid_metrics(sample_gold_df):
    """RandomForest should train without error and produce deterministic metrics."""
    X, y = prepare_training_data(sample_gold_df)
    X_train, X_test, y_train, y_test = stratified_split(X, y, random_state=42)

    model = train_randomforest(X_train, y_train, random_seed=42)
    calibrated = calibrate_classifier(model, X_train, y_train, method="sigmoid", cv=3)
    metrics = evaluate_model(calibrated, X_test, y_test)

    assert 0.0 <= metrics.roc_auc <= 1.0
    assert 0.0 <= metrics.recall_at_high <= 1.0
    assert 0.0 <= metrics.precision <= 1.0
```
