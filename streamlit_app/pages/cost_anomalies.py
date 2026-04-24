from __future__ import annotations

import streamlit as st

from streamlit_app.lib.charts import bar_chart
from streamlit_app.lib.data_access import (
    ArtifactMissingError,
    available_dimension_values,
    cached_load_analytics_artifact,
    filter_frame_by_date,
)
from streamlit_app.lib.formatting import format_currency, format_integer, format_ratio
from streamlit_app.lib.ui import render_metric_card, render_missing_artifact, render_page_header


render_page_header(
    "Cost Anomalies",
    "Analyst-safe drilldown into claims whose billed amount materially exceeds the expected benchmark.",
    eyebrow="Analytics / Cost Anomalies",
)

try:
    anomalies_df = cached_load_analytics_artifact("high_cost_claims_summary")
except ArtifactMissingError as exc:
    render_missing_artifact(f"{exc}. Rebuild the local analytics artifacts before opening this page.")
    st.stop()

date_min = anomalies_df["claim_date"].min().date()
date_max = anomalies_df["claim_date"].max().date()
region_options = available_dimension_values(anomalies_df, "region")
specialty_options = available_dimension_values(anomalies_df, "specialty")

with st.sidebar:
    st.markdown("### Anomaly Filters")
    start_date, end_date = st.date_input("Claim Date Window", value=(date_min, date_max), min_value=date_min, max_value=date_max)
    ratio_threshold = st.slider("Minimum Ratio", min_value=1.5, max_value=float(anomalies_df["amount_to_benchmark_ratio"].max()), value=1.5, step=0.1)
    selected_regions = st.multiselect("Region", region_options, default=region_options)
    selected_specialties = st.multiselect("Specialty", specialty_options, default=specialty_options)

filtered = filter_frame_by_date(anomalies_df, "claim_date", start_date, end_date)
filtered = filtered[filtered["amount_to_benchmark_ratio"] >= ratio_threshold]
if selected_regions:
    filtered = filtered[filtered["region"].isin(selected_regions)]
if selected_specialties:
    filtered = filtered[filtered["specialty"].isin(selected_specialties)]

metric_columns = st.columns(4)
with metric_columns[0]:
    render_metric_card("Flagged Claims", format_integer(len(filtered)))
with metric_columns[1]:
    render_metric_card("Flagged Spend", format_currency(filtered["billed_amount"].sum()))
with metric_columns[2]:
    render_metric_card("Average Ratio", format_ratio(filtered["amount_to_benchmark_ratio"].mean()))
with metric_columns[3]:
    render_metric_card("Peak Ratio", format_ratio(filtered["amount_to_benchmark_ratio"].max()))

doctor_summary = (
    filtered.groupby("doctor_name", dropna=False)
    .agg(flagged_claims=("claim_id", "size"))
    .reset_index()
    .sort_values("flagged_claims", ascending=False, kind="stable")
)

st.altair_chart(bar_chart(doctor_summary.head(10), "doctor_name", "flagged_claims", "Doctors with the Most Flagged Claims"), width="stretch")

st.markdown("### Claim Drilldown")
st.dataframe(
    filtered.rename(
        columns={
            "claim_id": "Claim ID",
            "doctor_name": "Doctor",
            "specialty": "Specialty",
            "region": "Region",
            "procedure_code": "Procedure",
            "claim_date": "Claim Date",
            "billed_amount": "Billed Amount",
            "expected_cost": "Expected Cost",
            "amount_to_benchmark_ratio": "Ratio",
        }
    ),
    width="stretch",
    hide_index=True,
)
