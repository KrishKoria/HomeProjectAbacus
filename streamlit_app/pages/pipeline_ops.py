from __future__ import annotations

import streamlit as st

from streamlit_app.lib.data_access import cached_load_optional_ops_artifact, latest_record
from streamlit_app.lib.formatting import format_timestamp
from streamlit_app.lib.ui import render_metric_card, render_page_header, render_status_banner


render_page_header(
    "Pipeline Ops",
    "Run history, artifact freshness, and failure visibility for the local bronze-to-analytics workflow.",
    eyebrow="Operations / Pipeline Ops",
)

ingest_runs = cached_load_optional_ops_artifact("ingest_runs")
analytics_runs = cached_load_optional_ops_artifact("analytics_runs")
artifact_inventory = cached_load_optional_ops_artifact("artifact_inventory")
failures = cached_load_optional_ops_artifact("latest_failures")

latest_ingest = latest_record(ingest_runs)
latest_analytics = latest_record(analytics_runs)

if latest_ingest is not None:
    render_status_banner(f"Bronze last completed: {format_timestamp(latest_ingest['completed_at'])}", tone="good")
if latest_analytics is not None:
    render_status_banner(f"Analytics last completed: {format_timestamp(latest_analytics['completed_at'])}", tone="good")
if latest_ingest is None and latest_analytics is None:
    render_status_banner("No local pipeline runs recorded yet.", tone="warn")

metric_columns = st.columns(4)
with metric_columns[0]:
    render_metric_card("Bronze Runs", f"{len(ingest_runs):,}")
with metric_columns[1]:
    render_metric_card("Analytics Runs", f"{len(analytics_runs):,}")
with metric_columns[2]:
    render_metric_card("Tracked Artifacts", f"{len(artifact_inventory):,}")
with metric_columns[3]:
    render_metric_card("Failures", f"{len(failures):,}")

table_columns = st.columns(2)
with table_columns[0]:
    st.markdown("### Bronze Run History")
    st.dataframe(ingest_runs.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)
with table_columns[1]:
    st.markdown("### Analytics Run History")
    st.dataframe(analytics_runs.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)

st.markdown("### Artifact Inventory")
st.dataframe(artifact_inventory.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)

st.markdown("### Failure Log")
st.dataframe(failures.sort_values("timestamp", ascending=False), width="stretch", hide_index=True)
