from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app.lib.charts import bar_chart, line_chart, severity_bar_chart
from streamlit_app.lib.data_access import (
    ArtifactMissingError,
    available_dimension_values,
    cached_load_analytics_artifact,
    filter_frame_by_date,
)
from streamlit_app.lib.formatting import format_currency, format_integer
from streamlit_app.lib.ui import render_metric_card, render_missing_artifact, render_page_header


render_page_header(
    "Trend Anatomy",
    "Time-based EDA over local analytics outputs, with severity cuts from diagnosis joins and day-grain spend tracking.",
    eyebrow="Analytics / Trends",
)

try:
    trend_df = cached_load_analytics_artifact("week2_dashboard_summary")
    diagnosis_df = cached_load_analytics_artifact("claims_diagnosis_joined")
except ArtifactMissingError as exc:
    render_missing_artifact(f"{exc}. Rebuild the local analytics artifacts before opening this page.")
    st.stop()

date_min = trend_df["claim_date"].min().date()
date_max = trend_df["claim_date"].max().date()
severity_options = available_dimension_values(diagnosis_df, "severity")

with st.sidebar:
    st.markdown("### Trend Filters")
    start_date, end_date = st.date_input("Claim Date Window", value=(date_min, date_max), min_value=date_min, max_value=date_max)
    selected_severities = st.multiselect("Severity", options=severity_options, default=severity_options)

filtered_trend_df = filter_frame_by_date(trend_df, "claim_date", start_date, end_date)
filtered_diagnosis_df = filter_frame_by_date(diagnosis_df, "claim_date", start_date, end_date)
if selected_severities:
    filtered_diagnosis_df = filtered_diagnosis_df[filtered_diagnosis_df["severity"].isin(selected_severities)]

summary_columns = st.columns(3)
with summary_columns[0]:
    render_metric_card("Claim Days", format_integer(len(filtered_trend_df)))
with summary_columns[1]:
    render_metric_card("Claims in Window", format_integer(filtered_trend_df["total_claims"].sum()))
with summary_columns[2]:
    render_metric_card("Window Spend", format_currency(filtered_trend_df["total_billed_amount"].sum()))

line_columns = st.columns(2)
with line_columns[0]:
    st.altair_chart(line_chart(filtered_trend_df, "claim_date", "total_claims", "Total Claims by Day"), width="stretch")
with line_columns[1]:
    st.altair_chart(
        line_chart(filtered_trend_df, "claim_date", "total_billed_amount", "Billed Amount by Day"),
        width="stretch",
    )

severity_summary = (
    filtered_diagnosis_df.groupby(["severity", "category"], dropna=False)
    .size()
    .reset_index(name="claim_count")
    .sort_values("claim_count", ascending=False, kind="stable")
)

bottom_columns = st.columns(2)
with bottom_columns[0]:
    severity_totals = (
        filtered_diagnosis_df.groupby("severity", dropna=False).size().reset_index(name="claim_count").sort_values("claim_count", ascending=False)
    )
    st.altair_chart(
        severity_bar_chart(severity_totals, "severity", "claim_count", "severity", "Claims by Severity"),
        width="stretch",
    )
with bottom_columns[1]:
    top_categories = severity_summary.head(10).copy()
    top_categories["label"] = top_categories["category"].fillna("Unknown")
    st.altair_chart(bar_chart(top_categories, "label", "claim_count", "Top Diagnosis Categories"), width="stretch")

st.markdown("### Severity Detail")
st.dataframe(
    severity_summary.rename(columns={"severity": "Severity", "category": "Category", "claim_count": "Claim Count"}),
    width="stretch",
    hide_index=True,
)
