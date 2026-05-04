# Week 4 Implementation: Machine Learning (Denial Prediction)

## The Big Picture

Week 4's mandate: **"Build ML model to predict claim denial."** The system answers _"Will this claim be denied?"_ by converting clean Silver data into engineered Gold features, training models, evaluating them against a strict release gate, and exposing predictions through a registry-based serving path.

End-to-end flow:

```
Silver Tables → Gold Feature Table → Feature Engineering → Train/Evaluate → Register → Predict
```

---

## Layer 1: The Gold Feature Table (`ETL/pipelines/gold/gold_claim_features.py`)

The Gold ETL doesn't just join tables — it builds **four private intermediate views** before producing the final output. This is a DAG (directed acyclic graph) pattern using Databricks `@dp.materialized_view`: each intermediate can be refreshed independently, and the final view reads from them. This means the rolling-window features don't need to recompute provider lifetime stats every time.

### Step 1 of the doc: "Join Data" → `claims_feature_base` (private intermediate)

Joins `silver_claims` + `silver_providers` + `silver_diagnosis` + `silver_cost` using broadcast joins (providers/diagnosis/cost are small reference tables).

From this single join, it derives **6 base features inline**:

| Feature | Type | What it captures |
|---|---|---|
| `is_procedure_missing` | Boolean | Missing procedure code → suspicious |
| `is_amount_missing` | Boolean | Missing billed amount → incomplete claim |
| `amount_to_benchmark_ratio` | Numeric | billed / expected_cost — how much above benchmark |
| `billed_vs_avg_cost` | Numeric | billed / average_cost — regional comparison |
| `high_cost_flag` | Boolean | True when ratio ≥ 2.5× benchmark |
| `severity_procedure_mismatch` | Boolean | High-severity diagnosis but low-cost procedure |
| `specialty_diagnosis_mismatch` | Boolean | Provider specialty doesn't match diagnosis category |
| `provider_location_missing` | Boolean | Missing provider location |
| `diagnosis_severity_encoded` | Integer | 1 for High severity, 0 for Low |

It also creates `denial_label` (0/1) from the `is_denied` column — this is the **target** the model learns to predict.

> **Security note:** The ETL explicitly drops `claim_status`, `denial_reason_code`, `allowed_amount`, `paid_amount`, and `follow_up_required` — columns that could leak the answer. This is defense-in-depth against data leakage.

### Step 2 of the doc: "Feature Engineering" → provider stats + rolling windows

Two more private intermediates build the temporal/provider features:

**`provider_daily_stats`**: Groups claims by `(provider_id, event_date)` to compute daily claim counts, denial counts, and diagnosis diversity.

**`provider_lifetime_stats`**: Aggregates daily stats into lifetime metrics per provider:
- `provider_claim_count` — total claims ever
- `provider_risk_score` — denial rate (only computed when provider has ≥10 claims, otherwise NULL)

**In the final `gold_claim_features` view**, rolling-window features are computed using Spark window functions:

- `provider_claim_count_30d` — claims in last 30 days
- `provider_claim_count_60d` — claims in last 60 days
- `provider_claim_count_90d` — claims in last 90 days
- `provider_30d_denial_rate` — 30-day denial rate per provider

Plus **interaction/derived features**:
- `cost_overbenchmark_and_highseverity` — `ratio × severity` (amplifies risk when both high)
- `mismatch_and_overbenchmark` — `(mismatch_flags) × ratio` (combined anomaly signal)
- `missing_fields_count` — count of missing fields (data quality proxy)
- `low_volume_provider_risk` — risk for providers with <10 claims (sparse-data risk)

The final output is `healthcare.gold.claim_features` with **33 columns total** — 20 engineered features, plus identifiers, reference data, target label, and audit columns.

---

## Layer 2: Feature Definitions (`src/ml/__init__.py`, `src/ml/features.py`)

The `FEATURE_COLUMNS` constant in `__init__.py` is the single source of truth — it defines exactly 20 features. Every downstream component (training, prediction, evaluation, retrain-gating) imports from this one place. If you add a feature, you add it here once and the entire pipeline updates. This is the "feature contract."

**20 features total**, categorized in `features.py`:

- **6 Boolean features** (`BOOLEAN_FEATURES`): flags like `is_procedure_missing`, `high_cost_flag`, mismatch indicators
- **14 Numeric features** (`NUMERIC_FEATURES`): ratios, counts, rates, and interaction terms

Key functions in `features.py`:

| Function | Purpose |
|---|---|
| `load_gold_features(spark)` | Reads `healthcare.gold.claim_features` into pandas |
| `fill_nulls(df)` | Fills missing values using `DEFAULT_FILL_VALUES` (0 for booleans, 0.0 for ratios, 1 for diagnosis_count) |
| `prepare_training_data(df)` | Null-fill + coerce booleans to int + split X/y |
| `temporal_split(df)` | Split by date (oldest 70% train, newest 30% test) — preserves temporal ordering |
| `stratified_split(X, y)` | Random stratified split — preserves class balance |
| `extract_provider_groups(df)` | Extracts provider IDs for GroupKFold — prevents same-provider claims leaking between train/val |

---

## Layer 3: Model Training (`src/ml/train.py`)

The training pipeline trains **6 candidate models** and picks the best — not just XGBoost. Each tree model is wrapped in `CalibratedClassifierCV` (Platt scaling) so that `predict_proba` returns meaningful probabilities. Without calibration, XGBoost's raw scores are not true probabilities, and a cutoff like 0.7 would land in an arbitrary point in the distribution — breaking the Recall@HIGH gate.

### Models implemented:

| Model | Function | Tuning |
|---|---|---|
| Logistic Regression | `train_logistic_regression()` | Fixed params (baseline) |
| XGBoost | `train_xgboost()` | Optuna (50 trials) |
| LightGBM | `train_lightgbm()` | Optuna (50 trials) |
| CatBoost | `train_catboost()` | Optuna (50 trials) |
| Voting Ensemble | `train_voting_ensemble()` | Soft voting of calibrated trees |
| Stacking Ensemble | `train_stacking_ensemble()` | LogisticRegression meta-learner |

### Why each model was selected

**Logistic Regression** — baseline/sanity check. Simple, interpretable, fast. If a complex ensemble can't beat LR, the engineered features aren't pulling weight. Also doubles as the meta-learner in the stacking ensemble.

**XGBoost** — primary candidate. Industry standard for tabular data. Handles missing values natively, has built-in regularization, and is consistently a top performer in real-world claim denial problems.

**LightGBM** — XGBoost competitor. Leaf-wise tree growth (vs XGBoost's level-wise) means faster training and often better performance on smaller datasets. Different inductive bias catches patterns XGBoost might miss.

**CatBoost** — ordered boosting with native categorical handling. Useful when provider specialty, diagnosis category, or location have high cardinality — CatBoost handles these without manual encoding.

**Voting Ensemble** — combines the three tree models via soft voting (averages their calibrated probabilities). Reduces variance. If one model overfits a quirk, the other two smooth it out.

**Stacking Ensemble** — LR meta-learner learns *how* to weight the three tree models' predictions using out-of-fold data. More sophisticated than voting — can learn "trust XGBoost on high-cost claims, trust LightGBM on low-severity claims."

The bake-off doesn't assume which model wins. It trains all 6, sorts by gate metrics, and promotes whatever comes out on top.

### The Optuna hyperparameter tuning

Optuna is a **Bayesian hyperparameter optimizer**. Instead of blindly trying every combination (grid search) or sampling randomly, it builds a probabilistic model of "which parameter regions produced good scores" and concentrates trials there.

#### How Optuna works in this project

**The loop:** Optuna picks params → builds model → 5-fold CV → scores Recall@HIGH → updates its beliefs → repeats 50× → returns best.

**Step 1 — Define the search space**

`_build_xgb_from_trial` tells Optuna *what* to tune and the valid range for each knob:

```python
"max_depth":        trial.suggest_int("max_depth", 3, 10),
"learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
"n_estimators":     trial.suggest_int("n_estimators", 50, 300),
"scale_pos_weight": trial.suggest_float("scale_pos_weight", 1.0, 15.0),
# ... etc
```

`trial.suggest_*` is the contract — Optuna calls this function each trial, and the trial object injects parameter values. The `log=True` on `learning_rate` means it samples uniformly in log-space (0.01, 0.02, 0.05, 0.1, 0.2, 0.3 are equally likely) — better for parameters that span orders of magnitude.

**Step 2 — Create a study**

```python
study = optuna.create_study(
    direction="maximize",
    pruner=MedianPruner(n_startup_trials=10, n_warmup_steps=3),
)
```

- `direction="maximize"` — highest possible Recall@HIGH wins
- `MedianPruner` — kills bad trials early. After 10 startup trials, at each step (3 warmup steps grace period), if a trial's intermediate score is below the median of all trials at that step, it gets pruned. Saves compute — no point finishing a trial that's clearly losing.

**Step 3 — Run 50 trials**

```python
study.optimize(objective_function, n_trials=50)
```

Each trial:

1. **Optuna samples parameters.** First few trials are random exploration (TPE sampler warmup). After that, Optuna's **TPE (Tree-structured Parzen Estimator)** builds a probabilistic model: "good trials had params in *this* range, bad trials in *that* range." It samples from the good distribution, concentrating search where it matters.

2. **Build the model** with those params.

3. **5-fold cross-validation with Platt calibration** inside each fold. Split train into 5 folds, train on 4/5, calibrate, predict on held-out 1/5, compute Recall@HIGH and Precision. Average across folds. Calibration inside CV is critical — it makes trial-time scores match what the deployed model will actually produce.

4. **Return the score** — mean Recall@HIGH across folds. BUT if mean Precision < 0.70, subtract a penalty: `return mean_recall - 2.0 * (0.70 - mean_precision)`. So a trial with Recall=0.95 but Precision=0.50 scores `0.95 - 0.40 = 0.55` — worse than a balanced trial with Recall=0.82, Precision=0.72 scoring `0.82`. This is a **soft constraint** — the sampler still gets gradient signal, unlike hard rejection which gives zero information.

5. **Optuna updates its internal model** — "that region of parameter space gave score X, adjust beliefs about what's promising."

**Step 4 — Refit the winner**

```python
best_params = study.best_trial.params
# e.g. {"max_depth": 7, "learning_rate": 0.08, "n_estimators": 230, ...}
```

Refit on *all* training data with those best params and full 3-fold calibration — this is the model that gets returned.

**Critical design choice — the objective optimizes Recall@HIGH, not ROC-AUC:**

The Optuna objective runs 5-fold cross-validation with Platt calibration inside each fold. It computes `recall_at_high(y_true, proba)` — the fraction of truly-denied claims that score ≥ 0.7. This is what the release gate measures, so the tuner directly optimizes for it. Previous versions optimized ROC-AUC and hit AUC > 0.94 yet failed Recall@HIGH because the score distribution didn't concentrate above 0.7.

#### Why this beats grid search

Grid search would try 5 values per parameter × 7 parameters = 78,125 combinations. Optuna does 50 trials and typically finds better results because TPE learns which regions are promising and concentrates trials there, rather than blindly covering a grid where 90% of combinations are useless.

#### The search space (XGBoost)

- `max_depth`: 3–10
- `learning_rate`: 0.01–0.3 (log scale)
- `n_estimators`: 50–300
- `subsample`, `colsample_bytree`: 0.6–1.0
- `scale_pos_weight`: 1.0–15.0 (wide range to aggressively rebalance for Recall@HIGH)

### Calibration

Every tree model is wrapped in `CalibratedClassifierCV(method='sigmoid', cv=3)`. The `select_best_calibration` function tries both sigmoid (Platt) and isotonic calibration, picking whichever gives lower log-loss on a held-out calibration set.

### Ensemble methods

- **Voting Ensemble**: Soft voting across XGBoost + LightGBM + CatBoost (all already calibrated)
- **Stacking Ensemble**: 5-fold stacking with LogisticRegression meta-learner — base models predict on out-of-fold data, meta-learner learns to combine them

### MLflow integration (`train_with_mlflow`)

Logs params, metrics, feature columns, and the model artifact. Sets the `champion` alias on the registered model so prediction callers load via `models:/healthcare.ml.claim_denial_model@champion` — no run_id or pickle path needed.

---

## Layer 4: Evaluation (`src/ml/evaluate.py`)

The release gate uses **Recall@HIGH**, not global recall. Why? Because the system only triggers remediation for claims in the HIGH risk tier (probability ≥ 0.7). A model that catches all denied claims at 0.55 probability is useless — those claims never get remediated. Recall@HIGH measures "of the truly denied claims, how many did we surface into the HIGH tier where action actually happens."

### The `EvaluationMetrics` dataclass

Captures: accuracy, precision, recall, f1, roc_auc, **recall_at_high**.

### The Release Gate (`meets_thresholds()`)

Per `ARCHITECTURE.md §13`:
- **Recall@HIGH ≥ 0.80** — 80% of denied claims must land in HIGH risk
- **Precision ≥ 0.70** — ≤30% false positive rate
- **ROC-AUC ≥ 0.85** — strong ranking ability

Gate-failing models are **not pickled or registered**.

### Additional evaluation tools:

| Function | Purpose |
|---|---|
| `recall_at_high(y_true, y_prob)` | Core gate metric |
| `find_optimal_threshold()` | Finds best decision boundary using Youden's J statistic |
| `compute_shap_values()` | SHAP explanations (unwraps calibration for TreeExplainer) |
| `compute_confusion_matrix()` | TN, FP, FN, TP breakdown |
| `compute_psi()` | Population Stability Index (drift detection: PSI ≥ 0.2 = significant shift) |
| `get_top_features()` | Top-N feature importances |
| `generate_evaluation_report()` | Structured dict for the training script |

---

## Layer 5: Prediction (`src/ml/predict.py`)

The prediction module uses a **risk-tier system** (LOW/MEDIUM/HIGH) rather than raw binary classification. Three tiers give the remediation tool operational guidance: LOW claims can be auto-processed, MEDIUM get human review, HIGH get immediate intervention.

Tier cutoffs:
- **LOW**: probability < 0.3
- **MEDIUM**: 0.3 ≤ probability < 0.7
- **HIGH**: probability ≥ 0.7

### Key components:

| Function | Purpose |
|---|---|
| `RiskLevel` enum | `LOW`, `MEDIUM`, `HIGH` with `from_probability()` factory |
| `load_from_registry(name, alias)` | Loads champion from MLflow Registry (`models:/healthcare.ml.claim_denial_model@champion`). Handles Databricks vs local environments with clear error messages |
| `predict_single(model, feature_dict)` | Scores one claim, logs latency (budget: 150ms p95 per §13) |
| `predict_batch(model, feature_df)` | Scores many claims, returns DataFrame with `denial_probability` and `risk_level` |

---

## Layer 6: The Training Script (`src/scripts/train_denial_model.py`)

The script trains **all 6 candidates**, sorts them by `(gate_pass, recall_at_high, roc_auc)`, and promotes only the best. This is a "model bake-off" pattern — you don't guess which algorithm works best for your data; you let the data decide. The LR baseline serves as a sanity check: if a complex ensemble can't beat logistic regression, something is wrong with your feature engineering.

### CLI:

```bash
from src.scripts.train_denial_model import main; main(["--tune"])
```

### Pipeline flow:

1. Load data from `healthcare.gold.claim_features` (Spark) or `--gold-csv` fallback
2. Extract provider groups for GroupKFold
3. `prepare_training_data()` → X, y
4. Stratified split → train (56%), calibration (14%), test (30%)
5. Train all 6 models
6. Sort candidates by `(meets_thresholds, recall_at_high, roc_auc)` descending
7. Log best to MLflow, set champion alias
8. Save pickle to `models/claim_denial_model.pkl` **only if gate passes**
9. Print results table showing every model's metrics and gate status

### Output example:

```
Best model: stacking_ensemble
     LogReg  ROC-AUC: 0.9234  Recall@HIGH: 0.7200  Precision: 0.8200  gate=FAIL
    XGBoost  ROC-AUC: 0.9512  Recall@HIGH: 0.8600  Precision: 0.7800  gate=PASS
   LightGBM  ROC-AUC: 0.9487  Recall@HIGH: 0.8400  Precision: 0.7600  gate=PASS
   CatBoost  ROC-AUC: 0.9456  Recall@HIGH: 0.8300  Precision: 0.7500  gate=PASS
     Voting  ROC-AUC: 0.9534  Recall@HIGH: 0.8700  Precision: 0.7900  gate=PASS
   Stacking  ROC-AUC: 0.9567  Recall@HIGH: 0.8900  Precision: 0.8100  gate=PASS
Best: stacking_ensemble (ROC-AUC: 0.9567, Recall@HIGH: 0.8900, Precision: 0.8100)
PASS: Model meets evaluation thresholds
Model saved to models/claim_denial_model.pkl
```

---

## Layer 7: Smart Retraining (`src/ml/retrain_gate.py`)

Uses a **data-fingerprint-based retrain gate**. Instead of retraining on a schedule, it computes a SHA-256 hash of the Gold table's feature values and compares it to the fingerprint stored in the champion model's MLflow run. Retraining only triggers when the data has actually changed AND the change exceeds minimum thresholds (≥100 rows or ≥5%). This prevents unnecessary retraining when nothing changed — saving compute and preventing model churn.

### `decide_retrain()` logic:

| Condition | Decision |
|---|---|
| No champion exists | **retrain** |
| Champion run missing / orphaned | **retrain** |
| Fingerprint changed + row delta ≥ threshold | **retrain** |
| Fingerprint changed but row delta below threshold | **skip** |
| Feature columns changed | **retrain** |
| Fingerprint same, no column changes | **skip** |

### How the fingerprint actually works (won't fire on 1 new row)

The fingerprint is **not** a hash of the full table — that would change on every insert. Instead, `compute_fingerprint()` uses two independent signals that must both fire:

**1. Deterministic sampling (256 rows max)**

Each row is hashed via `SHA2(concat(feature_values))`, sorted by that hash, and only the top 256 rows are included in the fingerprint payload. A single new row might not even land in the top 256. If it does, it displaces at most one row — a tiny shift in the sample.

**2. Row count threshold (secondary gate)**

Even when the fingerprint *does* change, `decide_retrain()` applies a second check:

```python
if champion_fingerprint != current_fingerprint:
    if not _row_count_threshold_exceeded(current, previous):
        return skip("fingerprint changed but row_count delta below threshold")
```

The threshold is `max(100 rows, 5% of previous count)`. For 1,000 training rows, you'd need ≥50 new rows before retraining fires.

**Design logic**: fingerprint change = "something is different." Row count threshold = "enough is different to justify retraining." Two independent signals prevent retraining on noise — a single corrected claim or one new day of claims won't trigger a full model rebuild.

The `RetrainDecision` dataclass captures the full decision with counts, deltas, and reasons.

---

## Layer 8: Sample Prediction Notebook (`src/notebooks/sample_prediction.ipynb`)

A self-contained Databricks notebook that:

1. Resolves the champion model's dependency spec
2. Loads the model from registry (with local pickle fallback)
3. Pulls sample claims from Gold table (with synthetic fallback)
4. Runs `predict_single` and `predict_batch`
5. Prints WEEK4-formatted output: `Claim ID: 101 / Risk: HIGH (0.82)`
6. Shows risk-tier distribution (LOW / MEDIUM / HIGH counts)

---

## Week 4 Doc ↔ Implementation Mapping

| Doc requirement | Implementation |
|---|---|
| Step 1: Join Data → `gold_claim_base` | `claims_feature_base` private materialized view |
| Step 2: Feature Engineering → `gold_claim_features` | 20 features: 6 boolean + 14 numeric (including rolling windows + interactions) |
| **Cost Features** (billed_amount, billed vs avg_cost, high_cost_flag) | `amount_to_benchmark_ratio`, `billed_vs_avg_cost`, `high_cost_flag` |
| **Provider Features** (specialty, claim_count, risk_score) | `specialty_diagnosis_mismatch`, `provider_claim_count`, `provider_claim_count_30d/60d/90d`, `provider_risk_score`, `provider_30d_denial_rate`, `low_volume_provider_risk` |
| **Diagnosis Features** (count, severity) | `diagnosis_count`, `diagnosis_severity_encoded`, `severity_procedure_mismatch` |
| **Claim Features** (frequency, type) | `cost_overbenchmark_and_highseverity`, `mismatch_and_overbenchmark`, `missing_fields_count` |
| Step 3: Prepare Dataset | `prepare_training_data()`, `fill_nulls()`, `stratified_split()` |
| Step 4: Train Model | 6 candidates: LR, XGBoost, LightGBM, CatBoost, Voting, Stacking |
| Step 5: Predict | `predict_single()`, `predict_batch()` with LOW/MEDIUM/HIGH risk tiers |
| Step 6: Evaluate Model | `EvaluationMetrics` with §13 gate: Recall@HIGH≥0.80, Precision≥0.70, ROC-AUC≥0.85 |
| Step 7: Save Model | `train_with_mlflow()` → MLflow Registry + `models/claim_denial_model.pkl` |
| Output 1: Gold Table | `healthcare.gold.claim_features` |
| Output 2: Trained Model | `healthcare.ml.claim_denial_model@champion` |
| Output 3: Model Performance | Gate metrics + confusion matrix logged in MLflow run |
| Output 4: Feature List | `FEATURE_COLUMNS` in `src/ml/__init__.py` |
| Output 5: Sample Prediction | `sample_prediction.ipynb` — WEEK4-shaped output format |

---

## Key Files Reference

| File | Role |
|---|---|
| `ETL/pipelines/gold/gold_claim_features.py` | Gold feature table ETL (DAG of 4 materialized views) |
| `src/common/gold_pipeline_config.py` | Thresholds, window sizes, table properties |
| `src/ml/__init__.py` | `FEATURE_COLUMNS` + `TARGET_COLUMN` constants (feature contract) |
| `src/ml/features.py` | Data loading, null filling, train/test splitting |
| `src/ml/train.py` | 6 model trainers + Optuna tuners + calibration + ensembles + MLflow |
| `src/ml/evaluate.py` | `EvaluationMetrics`, release gate, SHAP, PSI, confusion matrix |
| `src/ml/predict.py` | `RiskLevel` enum, single/batch prediction, registry loading |
| `src/ml/retrain_gate.py` | Fingerprint-based retrain decision logic |
| `src/scripts/train_denial_model.py` | CLI training script (6-model bake-off) |
| `src/scripts/maybe_retrain_model.py` | Conditional retraining orchestration |
| `src/notebooks/sample_prediction.ipynb` | Demo notebook: load → predict → show results |

---

## What Makes This Production-Grade

Beyond the basic Week 4 requirements, the implementation includes:

- **Probability calibration** — `CalibratedClassifierCV` so 0.7 actually means ~70% confidence
- **Provider GroupKFold** — prevents data leakage where same provider's claims appear in both train and validation
- **Multi-algorithm bake-off** — trains 6 models, data picks the winner
- **Optuna hyperparameter tuning** — 50 trials per algorithm with MedianPruner
- **Gate-optimized objective** — Optuna directly maximizes Recall@HIGH, not a proxy metric
- **MLflow Registry serving** — `models:/name@champion` alias so callers never hardcode run_ids
- **Fingerprint-based retraining** — only retrains when data actually changed
- **PSI drift monitoring** — Population Stability Index for detecting distribution shifts
- **SHAP explanations** — per-claim feature attributions for auditability
- **Latency budget enforcement** — ≤150ms p95 per single-claim prediction
- **Defense-in-depth against leakage** — target-correlated columns explicitly dropped from feature surface
