# Adjudication & Revenue Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add analytics-backed adjudication and revenue reporting tables and expose them on a new Lakeview dashboard page.

**Architecture:** Extend `src/analytics/week2_analytics.py` with new aggregate builders and persistence wiring, then point new dashboard datasets at those managed analytics tables. Keep the dashboard aggregate-only and preserve existing pages.

**Tech Stack:** Python, unittest, optional local PySpark, Lakeview dashboard JSON

---

### Task 1: Add Contract Coverage For New Analytics And Dashboard Surfaces

**Files:**
- Modify: `tests/test_observability_contract.py`
- Test: `tests/test_observability_contract.py`

- [ ] **Step 1: Write the failing tests**

```python
self.assertEqual(
    DASHBOARD_SOURCE_TABLES,
    (
        "claims_provider_joined",
        "claims_diagnosis_joined",
        "claims_by_specialty_summary",
        "claims_by_region_summary",
        "high_cost_claims_summary",
        "week2_dashboard_summary",
        "claims_adjudication_summary",
        "claims_denial_reason_summary",
        "claims_revenue_daily_summary",
    ),
)
```

```python
with dashboard_path.open("r", encoding="utf-8") as handle:
    dashboard = json.load(handle)
self.assertIn("adjudication_kpis", {dataset["name"] for dataset in dashboard["datasets"]})
self.assertIn("Adjudication & Revenue", [page["displayName"] for page in dashboard["pages"]])
```

- [ ] **Step 2: Run the contract test to verify it fails**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: FAIL because the analytics inventory and dashboard JSON do not yet contain the new tables and page.

- [ ] **Step 3: Add minimal supporting imports and helpers in the test file**

```python
import json
```

```python
dashboard_path = PROJECT_ROOT / "src" / "dashboards" / "week2_claims_exploration.lvdash.json"
```

- [ ] **Step 4: Re-run the contract test after implementation**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: PASS

### Task 2: Implement Analytics Builders And Persistence Wiring

**Files:**
- Modify: `src/analytics/week2_analytics.py`
- Modify: `tests/test_observability_contract.py`
- Test: `tests/test_observability_contract.py`

- [ ] **Step 1: Write the failing builder wiring tests**

```python
with patch("src.analytics.week2_analytics.build_claims_adjudication_summary", return_value="adj"), \
     patch("src.analytics.week2_analytics.build_claims_denial_reason_summary", return_value="reason"), \
     patch("src.analytics.week2_analytics.build_claims_revenue_daily_summary", return_value="daily"), \
     patch("src.analytics.week2_analytics.write_managed_table"), \
     patch("src.analytics.week2_analytics.ensure_analytics_schema"):
    persisted = build_and_persist_week2_assets(fake_spark)
```

```python
self.assertIn("claims_adjudication_summary", persisted)
self.assertIn("claims_denial_reason_summary", persisted)
self.assertIn("claims_revenue_daily_summary", persisted)
```

- [ ] **Step 2: Run the analytics wiring test to verify it fails**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: FAIL because the new builder functions are not defined or not wired into `build_and_persist_week2_assets`.

- [ ] **Step 3: Write the minimal analytics implementation**

```python
def build_claims_adjudication_summary(spark, catalog: str, bronze_schema: str):
    claims = spark.table(bronze_table_name(catalog, bronze_schema, "claims"))
    ...
```

```python
outputs = {
    ...,
    "claims_adjudication_summary": build_claims_adjudication_summary(spark, catalog, bronze_schema),
    "claims_denial_reason_summary": build_claims_denial_reason_summary(spark, catalog, bronze_schema),
    "claims_revenue_daily_summary": build_claims_revenue_daily_summary(spark, catalog, bronze_schema),
}
```

- [ ] **Step 4: Run the analytics contract test**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: PASS

### Task 3: Add The Lakeview Adjudication & Revenue Page

**Files:**
- Modify: `src/dashboards/week2_claims_exploration.lvdash.json`
- Test: `tests/test_observability_contract.py`

- [ ] **Step 1: Write the failing dashboard surface assertions**

```python
self.assertIn("denial_reason_mix", {dataset["name"] for dataset in dashboard["datasets"]})
self.assertIn("revenue_funnel", {dataset["name"] for dataset in dashboard["datasets"]})
```

- [ ] **Step 2: Run the contract test to verify it fails**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: FAIL because the dashboard datasets and page are not yet present.

- [ ] **Step 3: Write the minimal dashboard JSON changes**

```json
{
  "name": "adjudication_kpis",
  "displayName": "Adjudication KPIs",
  "queryLines": [
    "SELECT * FROM healthcare.analytics.claims_adjudication_summary\n"
  ]
}
```

```json
{
  "name": "page_adjudication_revenue",
  "displayName": "Adjudication & Revenue",
  "pageType": "PAGE_TYPE_CANVAS",
  "layout": []
}
```

- [ ] **Step 4: Re-run the contract test**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: PASS

### Task 4: Verify The Full Change

**Files:**
- Modify: `src/analytics/week2_analytics.py`
- Modify: `src/dashboards/week2_claims_exploration.lvdash.json`
- Modify: `tests/test_observability_contract.py`

- [ ] **Step 1: Run focused tests**

Run: `rtk pytest tests/test_observability_contract.py -q`
Expected: PASS

- [ ] **Step 2: Run the full test suite**

Run: `rtk pytest -q`
Expected: PASS or an explicit report of any pre-existing failures or skips.

- [ ] **Step 3: Review final diff**

Run: `rtk git diff -- src/analytics/week2_analytics.py src/dashboards/week2_claims_exploration.lvdash.json tests/test_observability_contract.py`
Expected: Only the analytics, dashboard, and contract-test changes required by the spec.
