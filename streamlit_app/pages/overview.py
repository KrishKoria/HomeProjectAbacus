from __future__ import annotations

import streamlit as st

from streamlit_app.lib.charts import line_chart
from streamlit_app.lib.data_access import (
    ArtifactMissingError,
    cached_load_analytics_artifact,
    cached_load_optional_ops_artifact,
    filter_frame_by_date,
    latest_record,
)
from streamlit_app.lib.formatting import format_currency, format_integer, format_timestamp
from streamlit_app.lib.ui import render_missing_artifact, render_page_header, render_status_banner


render_page_header(
    "Local Claims Overview",
    "A temporary editorial-style command deck over locally landed bronze and analytics parquet artifacts.",
)

try:
    overview_df = cached_load_analytics_artifact("week2_dashboard_summary")
except ArtifactMissingError as exc:
    render_missing_artifact(f"{exc}. Run the bronze ingest and analytics build commands first.")
    st.stop()

ingest_runs = cached_load_optional_ops_artifact("ingest_runs")
analytics_runs = cached_load_optional_ops_artifact("analytics_runs")
latest_warnings = cached_load_optional_ops_artifact("latest_warnings")
latest_failures = cached_load_optional_ops_artifact("latest_failures")

date_min = overview_df["claim_date"].min().date()
date_max = overview_df["claim_date"].max().date()

with st.sidebar:
    st.markdown("### Overview Filters")
    start_date, end_date = st.date_input("Claim Date Window", value=(date_min, date_max), min_value=date_min, max_value=date_max)

filtered = filter_frame_by_date(overview_df, "claim_date", start_date, end_date)

latest_ingest = latest_record(ingest_runs)
latest_analytics = latest_record(analytics_runs)

render_status_banner(
    f"Latest bronze run: {latest_ingest['status'] if latest_ingest is not None else 'Unavailable'}",
    tone="good" if latest_ingest is not None and latest_ingest["status"] == "SUCCESS" else "warn",
)
render_status_banner(
    f"Latest analytics run: {latest_analytics['status'] if latest_analytics is not None else 'Unavailable'}",
    tone="good" if latest_analytics is not None and latest_analytics["status"] == "SUCCESS" else "warn",
)

metric_columns = st.columns(4)
metric_columns[0].metric("Total Claims", format_integer(filtered["total_claims"].sum()))
metric_columns[1].metric("Total Billed", format_currency(filtered["total_billed_amount"].sum()))
metric_columns[2].metric("Active Providers", format_integer(filtered["active_provider_count"].max()))
metric_columns[3].metric("High-Cost Claims", format_integer(filtered["high_cost_claim_count"].sum()))

chart_columns = st.columns([1.8, 1.2])
with chart_columns[0]:
    st.altair_chart(
        line_chart(filtered, "claim_date", "total_billed_amount", "Billed Amount Over Time"),
        width="stretch",
    )
with chart_columns[1]:
    st.markdown("### Freshness")
    st.write(
        {
            "bronze_updated": format_timestamp(latest_ingest["completed_at"]) if latest_ingest is not None else "Unavailable",
            "analytics_updated": format_timestamp(latest_analytics["completed_at"]) if latest_analytics is not None else "Unavailable",
            "warning_count": int(len(latest_warnings)),
            "failure_count": int(len(latest_failures)),
        }
    )

table_columns = st.columns(2)
with table_columns[0]:
    st.markdown("### Recent Warnings")
    st.dataframe(latest_warnings.head(10), width="stretch", hide_index=True)
with table_columns[1]:
    st.markdown("### Recent Failures")
    st.dataframe(latest_failures.head(10), width="stretch", hide_index=True)
