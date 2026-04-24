from __future__ import annotations

import streamlit as st

try:
    from streamlit_app.bootstrap import ensure_project_root_on_path
except ModuleNotFoundError:
    from bootstrap import ensure_project_root_on_path

ensure_project_root_on_path()

from streamlit_app.lib.ui import inject_base_styles


st.set_page_config(
    page_title="Claims Atlas",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_base_styles()

overview = st.Page("pages/overview.py", title="Overview", icon="📊")
trends = st.Page("pages/trends.py", title="Trends", icon="📈")
provider_region = st.Page("pages/provider_region.py", title="Provider & Region", icon="🏥")
cost_anomalies = st.Page("pages/cost_anomalies.py", title="Cost Anomalies", icon="🚨")
data_quality = st.Page("pages/data_quality.py", title="Data Quality", icon="🔎")
pipeline_ops = st.Page("pages/pipeline_ops.py", title="Pipeline Ops", icon="🧰")

navigation = st.navigation(
    {
        "Analytics": [overview, trends, provider_region, cost_anomalies],
        "Operations": [data_quality, pipeline_ops],
    }
)

navigation.run()
