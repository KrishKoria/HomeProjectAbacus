from __future__ import annotations

import streamlit as st

from streamlit_app.lib.charts import bar_chart
from streamlit_app.lib.data_access import (
    ArtifactMissingError,
    available_dimension_values,
    cached_load_analytics_artifact,
    filter_frame_by_date,
)
from streamlit_app.lib.formatting import format_currency, format_integer
from streamlit_app.lib.ui import render_missing_artifact, render_page_header


render_page_header(
    "Provider & Region",
    "Provider concentration, regional distribution, and doctor-level bill concentration over the local joined claims view.",
    eyebrow="Analytics / Provider & Region",
)

try:
    provider_df = cached_load_analytics_artifact("claims_provider_joined")
except ArtifactMissingError as exc:
    render_missing_artifact(f"{exc}. Rebuild the local analytics artifacts before opening this page.")
    st.stop()

date_min = provider_df["claim_date"].min().date()
date_max = provider_df["claim_date"].max().date()
specialty_options = available_dimension_values(provider_df, "specialty")
region_options = available_dimension_values(provider_df, "region")

with st.sidebar:
    st.markdown("### Provider Filters")
    start_date, end_date = st.date_input("Claim Date Window", value=(date_min, date_max), min_value=date_min, max_value=date_max)
    selected_specialties = st.multiselect("Specialty", specialty_options, default=specialty_options)
    selected_regions = st.multiselect("Region", region_options, default=region_options)

filtered = filter_frame_by_date(provider_df, "claim_date", start_date, end_date)
if selected_specialties:
    filtered = filtered[filtered["specialty"].isin(selected_specialties)]
if selected_regions:
    filtered = filtered[filtered["region"].isin(selected_regions)]

specialty_summary = (
    filtered.groupby("specialty", dropna=False)
    .agg(claim_count=("claim_id", "size"), total_billed_amount=("billed_amount", "sum"))
    .reset_index()
    .sort_values("claim_count", ascending=False, kind="stable")
)
region_summary = (
    filtered.groupby("region", dropna=False)
    .agg(claim_count=("claim_id", "size"), total_billed_amount=("billed_amount", "sum"))
    .reset_index()
    .sort_values("claim_count", ascending=False, kind="stable")
)
doctor_summary = (
    filtered.groupby(["doctor_name", "specialty", "region"], dropna=False)
    .agg(claim_count=("claim_id", "size"), total_billed_amount=("billed_amount", "sum"))
    .reset_index()
    .sort_values(["total_billed_amount", "claim_count"], ascending=[False, False], kind="stable")
)

metric_columns = st.columns(3)
metric_columns[0].metric("Visible Claims", format_integer(len(filtered)))
metric_columns[1].metric("Visible Spend", format_currency(filtered["billed_amount"].sum()))
metric_columns[2].metric("Doctors", format_integer(filtered["doctor_name"].nunique()))

chart_columns = st.columns(2)
with chart_columns[0]:
    st.altair_chart(bar_chart(specialty_summary.head(10), "specialty", "claim_count", "Claim Count by Specialty"), width="stretch")
with chart_columns[1]:
    st.altair_chart(bar_chart(region_summary.head(10), "region", "claim_count", "Claim Count by Region"), width="stretch")

st.markdown("### Doctor Leaderboard")
st.dataframe(
    doctor_summary.rename(
        columns={
            "doctor_name": "Doctor",
            "specialty": "Specialty",
            "region": "Region",
            "claim_count": "Claim Count",
            "total_billed_amount": "Total Billed Amount",
        }
    ),
    width="stretch",
    hide_index=True,
)
