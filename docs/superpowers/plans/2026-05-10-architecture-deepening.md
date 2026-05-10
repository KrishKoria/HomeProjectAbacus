# Codebase Architecture Deepening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deepen 7 architectural friction points — de-duplicate singletons, unify constants, make cross-domain seams explicit, add validation and missing tests, decompose oversized `train.py`.

**Architecture:** Three batches: Tier 1 quick wins (singleton + constants + unwrap rename), Tier 2 test improvements (feature sync validation + retrain_gate tests + common_contract expansion), Tier 3 decomposition (split train.py into focused modules). All changes backward-compatible — existing import paths preserved.

**Tech Stack:** Python 3.10+, pytest, unittest, unittest.mock, databricks-sdk (mocked)

**Baseline:** `uv run pytest -q` before starting. Note pass/fail count.

---

### Task 1: Extract shared WorkspaceClient singleton

**Files:**
- Create: `src/rag/_workspace_client.py`
- Modify: `src/rag/embeddings.py:9-24`
- Modify: `src/rag/synthesizer.py:10-24`
- Modify: `src/rag/vector_search.py:16-31`
- Create: `tests/test_rag_workspace_client.py`

- [ ] **Step 1: Write tests for the shared singleton**

Create `tests/test_rag_workspace_client.py`:

```python
from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock

from src.rag._workspace_client import get_workspace_client, reset_workspace_client


class TestWorkspaceClient(unittest.TestCase):
    def tearDown(self) -> None:
        reset_workspace_client()

    @patch("src.rag._workspace_client.WorkspaceClient")
    def test_get_creates_client_once(self, mock_ws: MagicMock) -> None:
        mock_ws.return_value = MagicMock()
        c1 = get_workspace_client()
        c2 = get_workspace_client()
        self.assertIs(c1, c2)
        mock_ws.assert_called_once()

    @patch("src.rag._workspace_client.WorkspaceClient")
    def test_reset_creates_new_client(self, mock_ws: MagicMock) -> None:
        mock_ws.return_value = MagicMock()
        c1 = get_workspace_client()
        reset_workspace_client()
        mock_ws.reset_mock()
        c2 = get_workspace_client()
        self.assertIsNot(c1, c2)
        mock_ws.assert_called_once()
```

- [ ] **Step 2: Create `src/rag/_workspace_client.py`**

```python
from __future__ import annotations

from typing import Any

_ws: Any = None


def get_workspace_client() -> Any:
    global _ws
    if _ws is None:
        from databricks.sdk import WorkspaceClient

        _ws = WorkspaceClient()
    return _ws


def reset_workspace_client() -> None:
    global _ws
    _ws = None
```

- [ ] **Step 3: Update `src/rag/embeddings.py`**

Replace lines 9-24 with:
```python
from src.rag._workspace_client import get_workspace_client, reset_workspace_client
```
Update `_get_workspace_client()` → `get_workspace_client()`, `_reset_workspace_client()` → `reset_workspace_client()`.

- [ ] **Step 4: Update `src/rag/synthesizer.py`**

Same pattern — replace lines 10-24 with import, update call sites.

- [ ] **Step 5: Update `src/rag/vector_search.py`**

Same pattern — replace lines 16-31 with import, update call sites.

- [ ] **Step 6: Run tests**

```bash
uv run pytest -q tests/test_rag.py tests/test_rag_workspace_client.py -v
```

- [ ] **Step 7: Commit**

---

### Task 2: Unify duplicated threshold constants

**Files:**
- Modify: `src/ml/__init__.py`
- Modify: `src/ml/evaluate.py:19-22`
- Modify: `src/ml/predict.py:21-22`

- [ ] **Step 1: Add constants to `src/ml/__init__.py`**

Insert after `TARGET_COLUMN`:
```python
RISK_THRESHOLD_LOW: Final[float] = 0.3
HIGH_RISK_PROBABILITY_THRESHOLD: Final[float] = 0.7
RISK_THRESHOLD_HIGH = HIGH_RISK_PROBABILITY_THRESHOLD
```

Update `__all__` to include new names.

- [ ] **Step 2: Update `src/ml/evaluate.py`**

Replace `HIGH_RISK_PROBABILITY_THRESHOLD: Final[float] = 0.7` with `from src.ml import HIGH_RISK_PROBABILITY_THRESHOLD`.

- [ ] **Step 3: Update `src/ml/predict.py`**

Replace `RISK_THRESHOLD_LOW` / `RISK_THRESHOLD_HIGH` definitions with `from src.ml import RISK_THRESHOLD_HIGH, RISK_THRESHOLD_LOW`.

- [ ] **Step 4: Run tests**

```bash
uv run pytest -q tests/test_ml_contract.py tests/test_xai.py
```

- [ ] **Step 5: Commit**

---

### Task 3: Make ML→XAI unwrap seam explicit

**Files:**
- Modify: `src/ml/evaluate.py:143`
- Modify: `src/ml/__init__.py`
- Modify: `src/xai/explainer.py:8,20`

- [ ] **Step 1: Rename `_unwrap_for_shap` → `unwrap_model_for_shap` in `src/ml/evaluate.py`**

Update function definition and all internal call sites. Add to `__all__` in evaluate.py.

- [ ] **Step 2: Export from `src/ml/__init__.py`**

Add `from src.ml.evaluate import unwrap_model_for_shap` and add to `__all__`.

- [ ] **Step 3: Update `src/xai/explainer.py`**

Change `from src.ml.evaluate import _unwrap_for_shap` → `from src.ml import unwrap_model_for_shap`.

- [ ] **Step 4: Run tests**

```bash
uv run pytest -q tests/test_xai.py tests/test_ml_contract.py
```

- [ ] **Step 5: Commit**

---

### Task 4: Add FEATURE_COLUMNS ↔ FEATURE_REASONS sync validation

**Files:**
- Modify: `src/xai/__init__.py`
- Modify: `tests/test_xai.py`

- [ ] **Step 1: Write test and add import-time validation**

Add to `src/xai/__init__.py`:
```python
from src.ml import FEATURE_COLUMNS
from src.xai.feature_reasons import FEATURE_REASONS

_missing = set(FEATURE_COLUMNS) - set(FEATURE_REASONS.keys())
_extra = set(FEATURE_REASONS.keys()) - set(FEATURE_COLUMNS)
if _missing or _extra:
    raise AssertionError(
        f"FEATURE_REASONS out of sync with FEATURE_COLUMNS. "
        f"Missing reasons for: {_missing}. Extra reasons: {_extra}."
    )
```

Add test to `tests/test_xai.py`:
```python
def test_feature_reasons_covers_all_feature_columns(self) -> None:
    from src.ml import FEATURE_COLUMNS
    from src.xai.feature_reasons import FEATURE_REASONS
    missing = set(FEATURE_COLUMNS) - set(FEATURE_REASONS.keys())
    extra = set(FEATURE_REASONS.keys()) - set(FEATURE_COLUMNS)
    self.assertEqual(missing, set(), f"Missing reasons for: {missing}")
    self.assertEqual(extra, set(), f"Extra reasons not in FEATURE_COLUMNS: {extra}")
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest -q tests/test_xai.py
```

- [ ] **Step 3: Commit**

---

### Task 5: Add dedicated `test_retrain_gate.py`

**Files:**
- Create: `tests/test_retrain_gate.py`

- [ ] **Step 1: Write tests for `RetrainDecision`, `compute_fingerprint`, `decide_retrain`**

Test scenarios:
- `RetrainDecision.retrain()` factory — correct fields
- `RetrainDecision.skip()` factory — correct fields
- `RetrainDecision.error()` factory — correct fields
- `compute_fingerprint` — same data → same hash
- `compute_fingerprint` — different data → different hash
- `decide_retrain` — no change → skip
- `decide_retrain` — row count change → retrain
- `decide_retrain` — fingerprint change → retrain
- `decide_retrain` — MLflow error → error decision

All use `unittest.mock` — no Spark dependency.

- [ ] **Step 2: Run tests**

```bash
uv run pytest -q tests/test_retrain_gate.py -v
```

- [ ] **Step 3: Fix mock mismatches if needed against actual signatures**

Inspect `compute_fingerprint` and `decide_retrain` signatures; adjust mocks.

- [ ] **Step 4: Commit**

---

### Task 6: Expand `test_common_contract.py` with behavioral tests

**Files:**
- Modify: `tests/test_common_contract.py`

- [ ] **Step 1: Add test classes for each common module**

Targets:
- `TestDiagnostics` — `format_claimops_diagnostic_id()` valid/invalid domain codes, edge cases
- `TestPhiRegistry` — `is_phi_column()`, `get_phi_columns()`, `get_sensitive_columns()` known/unknown lookups
- `TestLogMessages` — `render_*()` functions with normal/missing/empty inputs
- `TestSilverCleaning` — `normalize_*()` + `parse_*()` with null strings, empty strings, normal values

- [ ] **Step 2: Run tests**

```bash
uv run pytest -q tests/test_common_contract.py -v
```

- [ ] **Step 3: Commit**

---

### Task 7: Decompose `src/ml/train.py` into focused modules

**Files:**
- Create: `src/ml/_train_algorithms.py`
- Create: `src/ml/_train_ensembles.py`
- Create: `src/ml/_train_registry.py`
- Modify: `src/ml/train.py` (becomes re-export hub)
- Modify: `src/scripts/train_denial_model.py` (update imports)
- Modify: `src/scripts/maybe_retrain_model.py` (update imports if needed)

- [ ] **Step 1: Extract algorithm trainers + tuners to `_train_algorithms.py`**

Move from `train.py`:
- `train_xgboost`, `tune_xgboost_optuna`
- `train_lightgbm`, `tune_lightgbm_optuna`
- `train_catboost`, `tune_catboost_optuna`
- `train_logistic_regression`
- `_make_optuna_objective`, `compute_sample_weights`
- `XGBOOST_DEFAULT_PARAMS`, `LIGHTGBM_DEFAULT_PARAMS`, `CATBOOST_DEFAULT_PARAMS`, `LOGREG_DEFAULT_PARAMS`, `OPTUNA_PRECISION_FLOOR`

- [ ] **Step 2: Extract ensemble + calibration to `_train_ensembles.py`**

Move from `train.py`:
- `train_voting_ensemble`, `train_stacking_ensemble`
- `calibrate_classifier`, `select_best_calibration`

- [ ] **Step 3: Extract MLflow + registry ops to `_train_registry.py`**

Move from `train.py`:
- `train_with_mlflow`, `compare_and_promote`
- `select_features_by_importance`

- [ ] **Step 4: Rewrite `src/ml/train.py` as re-export hub**

```python
from __future__ import annotations

from src.ml._train_algorithms import *  # noqa: F401,F403
from src.ml._train_ensembles import *  # noqa: F401,F403
from src.ml._train_registry import *  # noqa: F401,F403
```

- [ ] **Step 5: Update `src/scripts/train_denial_model.py`**

Any imports from `src.ml.train` still work (re-exported). Check that `__all__` in each new module covers everything the script uses.

- [ ] **Step 6: Run tests**

```bash
uv run pytest -q tests/test_ml_contract.py tests/test_retrain_gate.py -v
```

- [ ] **Step 7: Commit**

---

## Spec Coverage

| Spec Requirement | Task |
|-----------------|------|
| De-duplicate WorkspaceClient singleton | Task 1 |
| Unify HIGH_RISK_PROBABILITY_THRESHOLD / RISK_THRESHOLD_HIGH | Task 2 |
| Make _unwrap_for_shap cross-domain seam explicit | Task 3 |
| FEATURE_COLUMNS ↔ FEATURE_REASONS sync validation | Task 4 |
| Dedicated test_retrain_gate.py | Task 5 |
| Expand test_common_contract.py | Task 6 |
| Split train.py into focused modules | Task 7 |
