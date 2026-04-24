from __future__ import annotations

import streamlit as st

from streamlit_app.lib.charts import bar_chart
from streamlit_app.lib.data_access import cached_load_optional_ops_artifact
from streamlit_app.lib.ui import render_metric_card, render_page_header


render_page_header(
    "Data Quality",
    "Row-count reconciliation, missing-field metrics, and the latest warning stream from the local bronze and analytics runs.",
    eyebrow="Operations / Data Quality",
)

row_counts = cached_load_optional_ops_artifact("dataset_row_counts")
quality_metrics = cached_load_optional_ops_artifact("quality_metrics")
warnings = cached_load_optional_ops_artifact("latest_warnings")

with st.sidebar:
    st.markdown("### Quality Filters")
    dataset_options = sorted(quality_metrics["dataset"].dropna().astype(str).unique().tolist()) if not quality_metrics.empty else []
    selected_datasets = st.multiselect("Dataset", dataset_options, default=dataset_options)

if selected_datasets:
    quality_metrics = quality_metrics[quality_metrics["dataset"].isin(selected_datasets)]
    row_counts = row_counts[row_counts["dataset"].isin(selected_datasets)]
    warnings = warnings[warnings["dataset"].isin(selected_datasets)]

metric_columns = st.columns(3)
with metric_columns[0]:
    render_metric_card("Quality Metrics", f"{len(quality_metrics):,}")
with metric_columns[1]:
    render_metric_card("Row Count Records", f"{len(row_counts):,}")
with metric_columns[2]:
    render_metric_card("Warnings", f"{len(warnings):,}")

top_metrics = quality_metrics.sort_values("metric_value", ascending=False, kind="stable").head(12)
st.altair_chart(bar_chart(top_metrics, "metric_name", "metric_value", "Largest Recorded Quality Metrics", horizontal=True), width="stretch")

table_columns = st.columns(2)
with table_columns[0]:
    st.markdown("### Row Count Reconciliation")
    st.dataframe(row_counts.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)
with table_columns[1]:
    st.markdown("### Latest Warnings")
    st.dataframe(warnings.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)

st.markdown("### Metric Detail")
st.dataframe(quality_metrics.sort_values(["dataset", "metric_name"], kind="stable"), width="stretch", hide_index=True)
