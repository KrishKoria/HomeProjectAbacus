"""Streamlit app for claim denial prediction with SHAP explanations and RAG policy retrieval.

Databricks-hosted: ``streamlit run app_streamlit.py`` on a Databricks workspace.
"""
from __future__ import annotations

import html
import logging
import os
import re
import time
from typing import Any, Final

import pandas as pd
import streamlit as st
from src.rag.policy_labels import policy_display_name, policy_excerpt_label

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Claim Denial Risk Analyzer",
    page_icon="⚖️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# PHI safety — list of patterns that must never appear in rendered output
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Theme — OKLCH dark palette injected via st.markdown
# ---------------------------------------------------------------------------
_THEME_CSS: str = """\
<style>
/* ===== CSS Custom Properties ===== */
:root {
  --bg-base: oklch(0.14 0.008 60);
  --bg-surface: oklch(0.19 0.01 60);
  --bg-elevated: oklch(0.24 0.012 60);
  --border-subtle: oklch(0.27 0.01 60);
  --border-default: oklch(0.34 0.015 60);
  --text-primary: oklch(0.90 0.005 60);
  --text-secondary: oklch(0.62 0.01 60);
  --text-tertiary: oklch(0.48 0.012 60);

  --accent-red: oklch(0.55 0.20 25);
  --accent-orange: oklch(0.62 0.16 65);
  --accent-green: oklch(0.58 0.18 145);
  --accent-blue: oklch(0.55 0.13 260);
  --accent-teal: oklch(0.55 0.12 200);

  --font-sans: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  --font-mono: "SF Mono", "Cascadia Code", "JetBrains Mono", "Fira Code", monospace;
  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;
}

/* ===== Streamlit Base Overrides ===== */
.stApp {
  background: var(--bg-base);
  color: var(--text-primary);
}

.main .block-container {
  max-width: 1400px;
  padding: 1.5rem 2.5rem;
}

.stApp header {
  background: var(--bg-surface) !important;
  backdrop-filter: none !important;
}

section[data-testid="stSidebar"] {
  display: none;
}

h1, h2, h3, h4 {
  color: var(--text-primary) !important;
  font-family: var(--font-sans);
}

h1 { font-weight: 700; letter-spacing: -0.02em; }
h2 { font-weight: 600; }
h3 { font-weight: 600; }

p, li, label, .stMarkdown {
  color: var(--text-primary);
}

small, .stCaption {
  color: var(--text-secondary) !important;
}

.stButton > button {
  border-radius: var(--radius-sm);
  font-weight: 600;
  font-family: var(--font-sans);
  transition: all 0.15s ease;
}

.stButton > button[kind="primary"] {
  background: var(--accent-blue) !important;
  border-color: var(--accent-blue) !important;
  color: #fff !important;
}

.stButton > button[kind="primary"]:hover {
  background: oklch(0.50 0.14 260) !important;
  border-color: oklch(0.50 0.14 260) !important;
}

div[data-testid="stTextInput"] input {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  color: var(--text-primary);
  border-radius: var(--radius-sm);
}

div[data-testid="stTextInput"] input:focus {
  border-color: var(--accent-blue);
  box-shadow: 0 0 0 1px var(--accent-blue);
}

div[data-testid="stSelectbox"] > div {
  background: var(--bg-surface);
}

.stSelectbox label, .stTextInput label {
  color: var(--text-secondary) !important;
  font-size: 0.85rem;
}

div[data-testid="stAlert"] {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  color: var(--text-primary);
}

div[data-testid="stAlert"] [data-testid="stNotification"] {
  color: var(--text-primary);
}

.stProgress > div {
  background: var(--bg-elevated) !important;
  border-radius: var(--radius-sm) !important;
}

.stProgress > div > div {
  background: var(--accent-blue) !important;
  border-radius: var(--radius-sm) !important;
}

div[data-testid="stExpander"] {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
}

div[data-testid="stExpander"] summary {
  color: var(--text-primary);
  font-weight: 600;
}

div[data-testid="stExpander"] summary:hover {
  color: var(--accent-blue);
}

hr, .stDivider {
  border-color: var(--border-subtle) !important;
}

/* ===== Status Bar ===== */
.status-bar {
  display: flex;
  gap: 1.5rem;
  align-items: center;
  padding: 0.75rem 1rem;
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  margin-bottom: 1.5rem;
}

.status-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  flex-shrink: 0;
}

.status-dot--ok {
  background: var(--accent-green);
  box-shadow: 0 0 5px oklch(0.58 0.18 145 / 0.4);
}

.status-dot--degraded {
  background: var(--accent-orange);
  box-shadow: 0 0 5px oklch(0.62 0.16 65 / 0.4);
}

.status-label {
  font-weight: 600;
  color: var(--text-primary);
}

/* ===== Risk Gauge Section ===== */
.risk-section {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  padding: 1.5rem;
  margin-bottom: 1.25rem;
}

.risk-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
}

.risk-gauge-value {
  font-size: 2.5rem;
  font-weight: 700;
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
  line-height: 1;
}

.risk-gauge-value--low { color: var(--accent-green); }
.risk-gauge-value--medium { color: var(--accent-orange); }
.risk-gauge-value--high { color: var(--accent-red); }

.risk-badge {
  display: inline-block;
  padding: 0.2rem 0.7rem;
  border-radius: var(--radius-sm);
  font-weight: 700;
  font-size: 0.75rem;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.risk-badge--low {
  background: oklch(0.58 0.18 145 / 0.18);
  color: var(--accent-green);
}

.risk-badge--medium {
  background: oklch(0.62 0.16 65 / 0.18);
  color: var(--accent-orange);
}

.risk-badge--high {
  background: oklch(0.55 0.20 25 / 0.20);
  color: var(--accent-red);
}

.risk-meta {
  display: flex;
  gap: 1.5rem;
  align-items: center;
}

.risk-meta-item {
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.risk-meta-item span {
  color: var(--text-primary);
  font-family: var(--font-mono);
}

.risk-bar-track {
  width: 100%;
  height: 6px;
  background: var(--bg-elevated);
  border-radius: 3px;
  margin-top: 0.75rem;
  position: relative;
}

.risk-bar-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.5s ease-out;
}

.risk-bar-fill--low { background: var(--accent-green); }
.risk-bar-fill--medium { background: var(--accent-orange); }
.risk-bar-fill--high { background: var(--accent-red); }

.risk-bar-labels {
  display: flex;
  justify-content: space-between;
  margin-top: 0.3rem;
  font-size: 0.7rem;
  color: var(--text-tertiary);
  font-family: var(--font-mono);
}

/* ===== Feature Breakdown Section ===== */
.feature-section {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  padding: 1.25rem 1.5rem;
  margin-bottom: 1.25rem;
}

.feature-section-header {
  font-weight: 600;
  font-size: 0.85rem;
  color: var(--text-secondary);
  margin-bottom: 1rem;
  letter-spacing: 0.02em;
}

.feature-group-heading {
  font-weight: 700;
  font-size: 0.78rem;
  color: var(--text-secondary);
  margin: 0.75rem 0 0.5rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.feature-row {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-start;
  padding: 0.6rem 0;
  border-bottom: 1px solid var(--border-subtle);
  gap: 0.5rem;
}

.feature-row:last-child {
  border-bottom: none;
}

.feature-icon {
  width: 1.25rem;
  text-align: center;
  flex-shrink: 0;
  font-weight: 700;
  font-size: 0.85rem;
  line-height: 1.4;
}

.feature-icon--inc {
  color: var(--accent-red);
}

.feature-icon--dec {
  color: var(--accent-green);
}

.feature-name {
  font-weight: 600;
  font-size: 0.85rem;
  color: var(--text-primary);
  min-width: 7rem;
}

.feature-value {
  font-size: 0.78rem;
  color: var(--text-secondary);
  font-family: var(--font-mono);
  min-width: 3rem;
}

.feature-bar-wrap {
  flex: 1;
  min-width: 8rem;
  padding-top: 0.35rem;
}

.feature-bar-track {
  width: 100%;
  height: 4px;
  background: var(--bg-elevated);
  border-radius: 2px;
  overflow: hidden;
}

.feature-bar-fill {
  height: 100%;
  border-radius: 2px;
  transition: width 0.5s ease-out;
}

.feature-bar-fill--inc {
  background: var(--accent-red);
}

.feature-bar-fill--dec {
  background: var(--accent-green);
}

.feature-reason {
  font-size: 0.78rem;
  color: var(--text-tertiary);
  line-height: 1.4;
  margin-top: 0.2rem;
  width: 100%;
}

/* ===== Policy Section ===== */
.policy-section {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  padding: 1.25rem 1.5rem;
  margin-bottom: 1.25rem;
}

.policy-source-badge {
  display: inline-block;
  padding: 0.15rem 0.5rem;
  border-radius: var(--radius-sm);
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.03em;
  text-transform: uppercase;
  background: oklch(0.55 0.13 260 / 0.18);
  color: var(--accent-blue);
  margin-left: 0.5rem;
  vertical-align: middle;
}

.policy-narrative {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 1rem;
  margin-bottom: 1rem;
  max-width: 92ch;
}

.policy-narrative-summary {
  color: var(--text-primary);
  font-size: 0.9rem;
  font-weight: 600;
  line-height: 1.55;
}

.policy-narrative-list {
  margin: 0.55rem 0 0;
  padding-left: 1rem;
}

.policy-narrative-list li {
  color: var(--text-secondary);
  font-size: 0.84rem;
  line-height: 1.5;
  margin-bottom: 0.25rem;
}

.policy-narrative-list li:last-child {
  margin-bottom: 0;
}

.policy-card {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 0.8rem 1rem;
  margin-bottom: 0.65rem;
}

.policy-card:last-child {
  margin-bottom: 0;
}

.policy-card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.25rem;
}

.policy-card-title-wrap {
  min-width: 0;
}

.policy-card-title {
  font-weight: 700;
  font-size: 0.84rem;
  color: var(--text-primary);
  line-height: 1.35;
}

.policy-card-meta {
  font-size: 0.74rem;
  color: var(--text-secondary);
  font-family: var(--font-sans);
  margin-top: 0.1rem;
}

.policy-card-relevance {
  font-size: 0.68rem;
  font-weight: 600;
  color: #fff;
  background: var(--accent-blue);
  padding: 0.16rem 0.5rem;
  border-radius: var(--radius-sm);
  font-family: var(--font-mono);
  white-space: nowrap;
}

.policy-card-excerpt {
  font-size: 0.82rem;
  color: var(--text-primary);
  line-height: 1.45;
  font-family: var(--font-sans);
  margin-top: 0.5rem;
}

.policy-card-details {
  margin-top: 0.45rem;
}

.policy-card-details summary {
  color: var(--text-secondary);
  font-size: 0.74rem;
  cursor: pointer;
}

.policy-card-details-text {
  color: var(--text-secondary);
  font-size: 0.8rem;
  line-height: 1.45;
  margin-top: 0.35rem;
}

/* ===== Latency Waterfall Section ===== */
.latency-section {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  padding: 1.25rem 1.5rem;
  margin-bottom: 1.25rem;
}

.latency-row {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0.35rem 0;
}

.latency-label {
  width: 140px;
  text-align: right;
  color: var(--text-secondary);
  font-size: 0.8rem;
  flex-shrink: 0;
}

.latency-track {
  flex: 1;
  height: 18px;
  background: var(--bg-elevated);
  border-radius: 3px;
  overflow: hidden;
}

.latency-bar {
  height: 100%;
  display: flex;
  align-items: center;
  padding: 0 6px;
  min-width: 2px;
  border-radius: 3px;
  font-size: 0.65rem;
  font-weight: 600;
  color: #fff;
}

.latency-bar--blue { background: var(--accent-blue); }
.latency-bar--teal { background: var(--accent-teal); }
.latency-bar--orange { background: var(--accent-orange); }

.latency-ms {
  width: 70px;
  text-align: right;
  color: var(--text-primary);
  font-family: var(--font-mono);
  font-size: 0.78rem;
  font-variant-numeric: tabular-nums;
  flex-shrink: 0;
}

.latency-total {
  font-weight: 700;
  color: var(--text-primary);
  border-top: 1px solid var(--border-subtle);
  margin-top: 0.4rem;
  padding-top: 0.4rem;
}

/* ===== Dataframe Override ===== */
div[data-testid="stDataFrame"] {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
}

div[data-testid="stDataFrame"] th {
  background: var(--bg-elevated) !important;
  color: var(--text-primary) !important;
  font-weight: 600;
}

div[data-testid="stDataFrame"] td {
  color: var(--text-primary) !important;
  background: var(--bg-surface) !important;
}

/* ===== Error state styling ===== */
.stException {
  background: oklch(0.55 0.20 25 / 0.12) !important;
  border: 1px solid oklch(0.55 0.20 25 / 0.25) !important;
  border-radius: var(--radius-md) !important;
}
</style>"""
_FORBIDDEN_PHI_PATTERNS: Final[list[str]] = [
    "patient_id",
    "patient_name",
    "date_of_birth",
    "XXX-XX-XXXX",
]
_DEFAULT_GOLD_TABLE: Final[str] = "healthcare.gold.claim_features"
_DEFAULT_VECTOR_INDEX_NAME: Final[str] = "healthcare.gold.policy_chunks_index"
_DEFAULT_MODEL_NAME: Final[str] = "healthcare.ml.claim_denial_model"
_DEFAULT_MODEL_ALIAS: Final[str] = "champion"
_SAMPLE_CLAIM_LIMIT: Final[int] = 25
_MAX_DETAILS_LEN: Final[int] = 200
_STATUS_CACHE_TTL_SECONDS: Final[int] = 30
_POLICY_EXCERPT_PREVIEW_CHARS: Final[int] = 340



def _inject_theme_css() -> None:
    """Inject the OKLCH dark theme CSS into the Streamlit app."""
    st.markdown(_THEME_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Value-formatting helpers
# ---------------------------------------------------------------------------
_BOOLEAN_FEATURES: frozenset[str] = frozenset({
    "is_procedure_missing",
    "is_amount_missing",
    "high_cost_flag",
    "severity_procedure_mismatch",
    "specialty_diagnosis_mismatch",
    "provider_location_missing",
    "dx_px_compatible",
})


def _format_feature_value(value: object, feature_name: str) -> str:
    """Format a feature value for display, typed by semantics."""
    if value is None:
        return "—"  # em dash
    if isinstance(value, float) and value != value:  # NaN
        return "—"
    if feature_name in _BOOLEAN_FEATURES:
        return "✓" if value else "—"  # check vs dash
    if isinstance(value, (int, float)):
        if "count" in feature_name:
            return f"{int(value):,}"
        if abs(value) >= 100:
            return f"{value:,.1f}"
        if abs(value) >= 1:
            return f"{value:.2f}"
        return f"{value:.4f}"
    return str(value)


def _feature_display_name(name: str) -> str:
    """Convert snake_case feature name to Title Case."""
    return name.replace("_", " ").title()


def _risk_css_class(risk: str) -> str:
    """CSS modifier suffix for the risk level."""
    return {"LOW": "low", "MEDIUM": "medium", "HIGH": "high"}.get(risk, "low")


def _risk_accent_color(risk: str) -> str:
    """OKLCH accent color string for the risk level."""
    return {
        "LOW": "oklch(0.58 0.18 145)",
        "MEDIUM": "oklch(0.62 0.16 65)",
        "HIGH": "oklch(0.55 0.20 25)",
    }.get(risk, "oklch(0.62 0.01 60)")


def _format_policy_relevance_label(value: object) -> str | None:
    """Return a compact relevance label or None when score is unavailable."""
    if value is None:
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score != score:
        return None
    if 0.0 <= score <= 1.0:
        return f"Match {score * 100:.0f}%"
    return f"Score {score:.2f}"


def _policy_excerpt_preview(text: str, max_chars: int = _POLICY_EXCERPT_PREVIEW_CHARS) -> tuple[str, bool]:
    """Return a compact single-line preview and truncation flag."""
    compact = " ".join(text.split())
    if len(compact) <= max_chars:
        return compact, False
    return compact[: max_chars - 3].rstrip() + "...", True


def _policy_narrative_html(text: str) -> str:
    """Render narrative into a concise summary + sentence list."""
    compact = " ".join(text.split())
    if not compact:
        return ""
    sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", compact) if segment.strip()]
    if not sentences:
        sentences = [compact]

    summary_html = html.escape(sentences[0])
    if len(sentences) == 1:
        return (
            '<div class="policy-narrative">'
            f'<div class="policy-narrative-summary">{summary_html}</div>'
            "</div>"
        )

    detail_items = "".join(
        f"<li>{html.escape(sentence)}</li>"
        for sentence in sentences[1:]
    )
    return (
        '<div class="policy-narrative">'
        f'<div class="policy-narrative-summary">{summary_html}</div>'
        f'<ul class="policy-narrative-list">{detail_items}</ul>'
        "</div>"
    )


def _assert_no_phi(text: str, context: str = "") -> None:
    """Raise AssertionError if *text* contains known PHI markers."""
    lower = text.lower()
    for marker in _FORBIDDEN_PHI_PATTERNS:
        if marker.lower() in lower:
            logger.warning("PHI marker %r found in %s output", marker, context)


# ---------------------------------------------------------------------------
# Session state — cached model and retriever survive reruns
# ---------------------------------------------------------------------------
def _init_session() -> None:
    defaults: dict[str, Any] = {
        "model": None,
        "model_loaded": False,
        "model_error": None,
        "retriever": None,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


def _load_model():
    if st.session_state.model_loaded:
        return st.session_state.model
    try:
        from src.ml.predict import load_from_registry

        model = load_from_registry(
            name=_env("CLAIMOPS_MODEL_NAME", _DEFAULT_MODEL_NAME) or _DEFAULT_MODEL_NAME,
            alias=_env("CLAIMOPS_MODEL_ALIAS", _DEFAULT_MODEL_ALIAS) or _DEFAULT_MODEL_ALIAS,
        )
        st.session_state.model = model
        st.session_state.model_loaded = True
        st.session_state.model_error = None
        return model
    except Exception as exc:
        st.session_state.model_error = str(exc)
        return None


def _get_retriever():
    if st.session_state.retriever is not None:
        return st.session_state.retriever
    try:
        from src.rag.vector_search import PolicyRetriever

        retriever = PolicyRetriever(index_name=_vector_index_name())
        st.session_state.retriever = retriever
        return retriever
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _workspace_hostname() -> str:
    host = _env("DATABRICKS_SERVER_HOSTNAME")
    if host:
        return host.replace("https://", "").replace("http://", "").strip("/")
    host = _env("DATABRICKS_HOST")
    return host.replace("https://", "").replace("http://", "").strip("/")


def _gold_table_name() -> str:
    return _env("CLAIMOPS_GOLD_TABLE", _DEFAULT_GOLD_TABLE)


def _vector_index_name() -> str:
    return _env("CLAIMOPS_VECTOR_INDEX_NAME", _DEFAULT_VECTOR_INDEX_NAME)


def _quote_identifier(value: str) -> str:
    clean = value.strip()
    if not clean or "`" in clean:
        raise ValueError(f"Invalid SQL identifier segment: {value!r}")
    return f"`{clean}`"


def _quote_table_name(value: str) -> str:
    segments = [segment for segment in value.split(".") if segment]
    if not segments or len(segments) > 3:
        raise ValueError(f"Invalid table name: {value!r}")
    return ".".join(_quote_identifier(segment) for segment in segments)


def _sql_connection():
    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    server_hostname = _workspace_hostname()
    http_path = _env("CLAIMOPS_SQL_HTTP_PATH", _env("DATABRICKS_HTTP_PATH"))
    if not server_hostname:
        raise ValueError(
            "Missing Databricks workspace host. Set DATABRICKS_SERVER_HOSTNAME or DATABRICKS_HOST."
        )
    if not http_path:
        raise ValueError(
            "Missing SQL warehouse HTTP path. Set CLAIMOPS_SQL_HTTP_PATH or DATABRICKS_HTTP_PATH."
        )

    direct_token = _env("DATABRICKS_TOKEN")
    if direct_token:
        return sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=direct_token,
        )

    client_id = _env("DATABRICKS_CLIENT_ID")
    client_secret = _env("DATABRICKS_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError(
            "Missing app OAuth credentials. Expected DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET."
        )
    oauth_config = Config(
        host=f"https://{server_hostname}",
        client_id=client_id,
        client_secret=client_secret,
    )

    def _credential_provider():
        return oauth_service_principal(oauth_config)

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=_credential_provider,
    )


def _query_sql(query: str, parameters: list[Any] | tuple[Any, ...] | None = None) -> pd.DataFrame:
    with _sql_connection() as connection:
        with connection.cursor() as cursor:
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, parameters)
            rows = cursor.fetchall()
            columns = [str(col[0]) for col in cursor.description or ()]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=120)
def _load_sample_claim_ids(limit: int = _SAMPLE_CLAIM_LIMIT) -> list[str]:
    query = (
        f"SELECT claim_id "
        f"FROM {_quote_table_name(_gold_table_name())} "
        f"WHERE claim_id IS NOT NULL "
        f"ORDER BY claim_id "
        f"LIMIT ?"
    )
    result = _query_sql(query, [int(limit)])
    if result.empty or "claim_id" not in result.columns:
        return []
    return [str(value) for value in result["claim_id"].tolist()]


@st.cache_data(ttl=60)
def _load_claim_features(claim_id: str) -> pd.DataFrame:
    from src.ml import FEATURE_COLUMNS

    projected_columns = ", ".join(_quote_identifier(column) for column in FEATURE_COLUMNS)
    query = (
        f"SELECT claim_id, {projected_columns} "
        f"FROM {_quote_table_name(_gold_table_name())} "
        f"WHERE claim_id = ? "
        f"LIMIT 1"
    )
    return _query_sql(query, [claim_id])


@st.cache_data(ttl=_STATUS_CACHE_TTL_SECONDS)
def _check_gold_connectivity_cached(table_name: str) -> tuple[bool, str]:
    try:
        query = f"SELECT 1 FROM {_quote_table_name(table_name)} LIMIT 1"
        _query_sql(query)
        return True, table_name
    except Exception as exc:
        return False, str(exc)


def _check_gold_connectivity() -> tuple[bool, str]:
    return _check_gold_connectivity_cached(_gold_table_name())


def _check_model_availability() -> tuple[bool, str]:
    model = _load_model()
    if model is not None:
        return True, _env("CLAIMOPS_MODEL_NAME", _DEFAULT_MODEL_NAME)
    error_text = st.session_state.model_error or "Model unavailable"
    return False, error_text


@st.cache_data(ttl=_STATUS_CACHE_TTL_SECONDS)
def _check_vector_search_availability_cached(index_name: str) -> tuple[bool, str]:
    try:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient().vector_search_indexes.get_index(index_name=index_name)
        return True, index_name
    except Exception as exc:
        return False, str(exc)


def _check_vector_search_availability() -> tuple[bool, str]:
    return _check_vector_search_availability_cached(_vector_index_name())


def _summarize_latency(
    feature_lookup_ms: float,
    risk_inference_ms: float,
    shap_ms: float,
    policy_retrieval_ms: float,
    narrative_ms: float,
    total_ms: float,
) -> dict[str, float]:
    return {
        "feature_lookup_ms": feature_lookup_ms,
        "risk_inference_ms": risk_inference_ms,
        "shap_ms": shap_ms,
        "policy_retrieval_ms": policy_retrieval_ms,
        "narrative_ms": narrative_ms,
        "risk_path_ms": feature_lookup_ms + risk_inference_ms,
        "explanation_ms": shap_ms + policy_retrieval_ms + narrative_ms,
        "total_ms": total_ms,
    }


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------
def _render_status_indicators(
    gold_ok: bool, gold_detail: str,
    model_ok: bool, model_detail: str,
    vector_ok: bool, vector_detail: str,
) -> None:
    """Compact inline status bar with colored dots."""
    def _format_status_detail(ok: bool, detail: str) -> str:
        if ok:
            return "Connected"
        return f"Degraded: {detail[:_MAX_DETAILS_LEN]}"

    gold_label = html.escape(_format_status_detail(gold_ok, gold_detail))
    model_label = html.escape(_format_status_detail(model_ok, model_detail))
    vector_label = html.escape(_format_status_detail(vector_ok, vector_detail))

    gold_dot = "status-dot--ok" if gold_ok else "status-dot--degraded"
    model_dot = "status-dot--ok" if model_ok else "status-dot--degraded"
    vector_dot = "status-dot--ok" if vector_ok else "status-dot--degraded"

    st.markdown(
        f"""<div class="status-bar">
  <div class="status-item">
    <div class="status-dot {gold_dot}"></div>
    <span class="status-label">Gold Data</span>
    <span>{gold_label}</span>
  </div>
  <div class="status-item">
    <div class="status-dot {model_dot}"></div>
    <span class="status-label">Model</span>
    <span>{model_label}</span>
  </div>
  <div class="status-item">
    <div class="status-dot {vector_dot}"></div>
    <span class="status-label">Vector Search</span>
    <span>{vector_label}</span>
  </div>
</div>""",
        unsafe_allow_html=True,
    )


def _render_risk_gauge(claim_id: str, prob: float, risk: str, total_ms: float) -> None:
    """Full-width risk section: probability, bar, badge, metadata."""
    css_class = _risk_css_class(risk)
    prob_pct = prob * 100

    st.markdown(
        f"""<div class="risk-section">
  <div class="risk-header">
    <div>
      <div style="font-size:0.8rem;color:var(--text-secondary);margin-bottom:0.25rem">
        Denial Probability
      </div>
      <div class="risk-gauge-value risk-gauge-value--{css_class}">
        {prob_pct:.1f}%
      </div>
      <span class="risk-badge risk-badge--{css_class}">{risk}</span>
    </div>
    <div class="risk-meta">
      <div class="risk-meta-item">
        Claim <span>{html.escape(claim_id)}</span>
      </div>
      <div class="risk-meta-item">
        Total <span>{total_ms:.0f} ms</span>
      </div>
    </div>
  </div>
  <div class="risk-bar-track">
    <div class="risk-bar-fill risk-bar-fill--{css_class}" style="width:{min(prob_pct, 100)}%"></div>
  </div>
  <div class="risk-bar-labels">
    <span>0%</span><span>100%</span>
  </div>
</div>""",
        unsafe_allow_html=True,
    )


def _render_feature_breakdown(
    shap_explanations: list[dict[str, object]],
    feature_dict: dict[str, object],
) -> None:
    """Two-group feature list: Risk Drivers and Risk Mitigators."""
    drivers = [e for e in shap_explanations if e.get("direction") == "increases_risk"]
    mitigators = [e for e in shap_explanations if e.get("direction") == "decreases_risk"]

    html_parts: list[str] = [
        '<div class="feature-section">',
        '<div class="feature-section-header">Key Risk Factors</div>',
    ]

    if drivers:
        html_parts.append('<div class="feature-group-heading">Risk Drivers ↑</div>')
        for entry in drivers:
            html_parts.append(_render_feature_row(entry, feature_dict))

    if mitigators:
        html_parts.append('<div class="feature-group-heading">Risk Mitigators ↓</div>')
        for entry in mitigators:
            html_parts.append(_render_feature_row(entry, feature_dict))

    html_parts.append("</div>")
    st.markdown("\n".join(html_parts), unsafe_allow_html=True)


def _render_feature_row(entry: dict[str, object], feature_dict: dict[str, object]) -> str:
    """Single feature row as an HTML string."""
    feature = str(entry["feature"])
    importance = float(entry["importance"])  # type: ignore[arg-type]
    direction = str(entry["direction"])
    reason = html.escape(str(entry["reason"]))

    _assert_no_phi(reason, f"shap_reason_{feature}")

    actual_value = feature_dict.get(feature)
    formatted_value = html.escape(_format_feature_value(actual_value, feature))
    display_name = _feature_display_name(feature)

    icon_cls = "feature-icon--inc" if direction == "increases_risk" else "feature-icon--dec"
    icon = "↑" if direction == "increases_risk" else "↓"
    bar_cls = "feature-bar-fill--inc" if direction == "increases_risk" else "feature-bar-fill--dec"
    bar_width = min(importance * 100, 100)

    return f"""<div class="feature-row">
  <div class="feature-icon {icon_cls}">{icon}</div>
  <div class="feature-name">{display_name}</div>
  <div class="feature-value">{formatted_value}</div>
  <div class="feature-bar-wrap">
    <div class="feature-bar-track">
      <div class="feature-bar-fill {bar_cls}" style="width:{bar_width:.1f}%"></div>
    </div>
  </div>
  <div class="feature-reason">{reason}</div>
</div>"""


def _render_policy_guidance(rag_result: dict[str, object] | None) -> None:
    """Narrative + compact policy cards."""
    if rag_result is None:
        st.info("Policy retrieval is not available. Check Vector Search connectivity.")
        return

    narrative = rag_result.get("narrative")
    policy_chunks = rag_result.get("policy_chunks", [])
    source = str(rag_result.get("source", "none"))

    source_label = {"llm": "AI-Generated", "template": "Template"}.get(source, source.title())
    source_badge = (
        f'<span class="policy-source-badge">{source_label}</span>'
        if source != "none"
        else ""
    )

    html_parts: list[str] = [
        '<div class="policy-section">',
        f'<div class="feature-section-header">Policy Guidance {source_badge}</div>' if source_badge else '<div class="feature-section-header">Policy Guidance</div>',
    ]

    if narrative:
        narrative_text = str(narrative)
        _assert_no_phi(narrative_text, "rag_narrative")
        html_parts.append(_policy_narrative_html(narrative_text))

    chunks = list(policy_chunks) if isinstance(policy_chunks, (list, tuple)) else []
    if chunks:
        for i, chunk in enumerate(chunks):
            if not isinstance(chunk, dict):
                continue
            full_text_raw = str(chunk.get("chunk_text", ""))
            _assert_no_phi(full_text_raw, f"policy_chunk_{i}")

            policy_name = policy_display_name(chunk.get("document_path"))
            excerpt_label = policy_excerpt_label(chunk.get("chunk_index"))
            relevance_label = _format_policy_relevance_label(chunk.get("relevance_score"))
            preview_text, is_truncated = _policy_excerpt_preview(full_text_raw)

            policy_name_html = html.escape(policy_name)
            excerpt_html = html.escape(excerpt_label)
            preview_html = html.escape(preview_text)
            full_text_html = html.escape(full_text_raw)
            relevance_html = (
                f'<span class="policy-card-relevance">{html.escape(relevance_label)}</span>'
                if relevance_label
                else ""
            )
            details_html = (
                f"""<details class="policy-card-details">
  <summary>Show full excerpt</summary>
  <div class="policy-card-details-text">{full_text_html}</div>
</details>"""
                if is_truncated
                else ""
            )

            html_parts.append(
                f"""<div class="policy-card">
  <div class="policy-card-header">
    <div class="policy-card-title-wrap">
      <div class="policy-card-title">{policy_name_html}</div>
      <div class="policy-card-meta">{excerpt_html}</div>
    </div>
    {relevance_html}
  </div>
  <div class="policy-card-excerpt">{preview_html}</div>
  {details_html}
</div>"""
            )
    elif narrative is None:
        st.info("No matching policy documents found for this claim.")
        html_parts.append("</div>")
        st.markdown("\n".join(html_parts), unsafe_allow_html=True)
        return

    html_parts.append("</div>")
    st.markdown("\n".join(html_parts), unsafe_allow_html=True)


def _render_latency_waterfall(latency: dict[str, float]) -> None:
    """Horizontal bar chart of 5 timing phases."""
    phases: list[tuple[str, float, str]] = [
        ("Feature Lookup", latency.get("feature_lookup_ms", 0), "blue"),
        ("Risk Inference", latency.get("risk_inference_ms", 0), "teal"),
        ("SHAP Explanation", latency.get("shap_ms", 0), "orange"),
        ("Policy Retrieval", latency.get("policy_retrieval_ms", 0), "blue"),
        ("Narrative Synthesis", latency.get("narrative_ms", 0), "teal"),
    ]
    total = latency.get("total_ms", 0)

    if total <= 0:
        st.caption("Timing data unavailable.")
        return

    html_parts: list[str] = [
        '<div class="latency-section">',
        '<div class="feature-section-header">Timing Breakdown</div>',
    ]

    for label, ms, color in phases:
        pct = (ms / total * 100) if total > 0 else 0
        html_parts.append(
            f"""<div class="latency-row">
  <div class="latency-label">{label}</div>
  <div class="latency-track">
    <div class="latency-bar latency-bar--{color}" style="width:{max(pct, 0.5):.1f}%"></div>
  </div>
  <div class="latency-ms">{ms:.0f} ms</div>
</div>"""
        )

    html_parts.append(
        f"""<div class="latency-row latency-total">
  <div class="latency-label">Total</div>
  <div class="latency-track"></div>
  <div class="latency-ms">{total:.0f} ms</div>
</div>"""
    )

    html_parts.append("</div>")
    st.markdown("\n".join(html_parts), unsafe_allow_html=True)


def _render_full_feature_table(
    shap_explanations: list[dict[str, object]],
    feature_dict: dict[str, object],
    all_features: tuple[str, ...],
) -> None:
    """Expandable dataframe with all features, values, SHAP, direction."""
    shap_by_feature: dict[str, tuple[float, str]] = {}
    for entry in shap_explanations:
        feat = str(entry["feature"])
        shap_by_feature[feat] = (
            float(entry["shap_value"]),  # type: ignore[arg-type]
            str(entry["direction"]),
        )

    rows: list[dict[str, object]] = []
    for feat in all_features:
        shap_val, direction = shap_by_feature.get(feat, (None, None))
        rows.append({
            "Feature": _feature_display_name(feat),
            "Value": _format_feature_value(feature_dict.get(feat), feat),
            "SHAP Value": f"{shap_val:.4f}" if shap_val is not None else "—",
            "Direction": direction if direction else "—",
        })

    df = pd.DataFrame(rows)
    with st.expander("All Features (22)", expanded=False):
        st.dataframe(
            df,
            hide_index=True,
            width="stretch",
            column_config={
                "Feature": st.column_config.TextColumn(width="medium"),
                "Value": st.column_config.TextColumn(width="small"),
                "SHAP Value": st.column_config.TextColumn(width="small"),
                "Direction": st.column_config.TextColumn(width="small"),
            },
        )

def main() -> None:
    _init_session()
    _inject_theme_css()

    st.title("Claim Denial Risk Analyzer")
    st.caption("SHAP feature attribution with policy retrieval via Llama 3.3 70B on Databricks")

    # --- Runtime health ---
    gold_ok, gold_detail = _check_gold_connectivity()
    model_ok, model_detail = _check_model_availability()
    vector_ok, vector_detail = _check_vector_search_availability()
    _render_status_indicators(gold_ok, gold_detail, model_ok, model_detail, vector_ok, vector_detail)

    # --- Input ---
    sample_claim_ids: list[str] = []
    sample_error: str | None = None
    try:
        sample_claim_ids = _load_sample_claim_ids()
    except Exception as exc:
        sample_error = str(exc)

    selector_col, input_col = st.columns([1, 2])
    selected_claim = ""
    with selector_col:
        if sample_claim_ids:
            selected_claim = st.selectbox(
                "Sample Claims",
                options=[""] + sample_claim_ids,
                help="Pick a claim ID populated in the Gold table.",
            )
        elif sample_error:
            st.caption(f"Sample list unavailable: {sample_error[:_MAX_DETAILS_LEN]}")
        else:
            st.caption("No sample claims found in Gold yet.")

    with input_col:
        claim_id = st.text_input(
            "Claim ID",
            value=selected_claim,
            placeholder="e.g. CLM-2024-000042",
            help=f"Enter a claim ID from {_gold_table_name()}",
        ).strip()
    predict_clicked = st.button("Analyze Risk", type="primary", disabled=not claim_id)

    if not predict_clicked:
        st.info("Enter a claim ID and click **Analyze Risk** to begin.")
        return

    if not claim_id.strip():
        st.warning("Please enter a claim ID.")
        return

    # --- Model loading ---
    with st.spinner("Loading model from MLflow Registry..."):
        model = _load_model()

    if model is None:
        err = st.session_state.model_error or "unknown error"
        st.error(
            f"Model load failed. Check the MLflow Registry connection.\n\n"
            f"Diagnostic: {err[:200]}"
        )
        return

    # --- Feature fetch ---
    try:
        from src.ml.features import fill_nulls
        from src.ml import FEATURE_COLUMNS
    except ImportError as exc:
        st.error(f"ML module unavailable. Diagnostic: {exc}")
        return

    total_start = time.perf_counter()
    feature_lookup_ms = 0.0
    risk_inference_ms = 0.0
    shap_ms = 0.0
    policy_retrieval_ms = 0.0
    narrative_ms = 0.0

    feature_lookup_start = time.perf_counter()
    try:
        raw = _load_claim_features(claim_id)
        feature_lookup_ms = (time.perf_counter() - feature_lookup_start) * 1000.0
    except Exception as exc:
        feature_lookup_ms = (time.perf_counter() - feature_lookup_start) * 1000.0
        st.error(
            "Feature lookup failed. "
            f"Check SQL warehouse/app permissions and connectivity.\n\nDiagnostic: {str(exc)[:_MAX_DETAILS_LEN]}"
        )
        return

    if raw.empty:
        st.error(
            f"Claim ID **{claim_id}** not found in `{_gold_table_name()}`. "
            f"(lookup took {feature_lookup_ms:.0f} ms)"
        )
        return

    features_df = fill_nulls(raw)
    feature_dict = {c: features_df[c].iloc[0] for c in FEATURE_COLUMNS}

    # --- Prediction ---
    try:
        from src.ml.predict import predict_single

        risk_inference_start = time.perf_counter()
        prediction = predict_single(model, feature_dict)
        risk_inference_ms = (time.perf_counter() - risk_inference_start) * 1000.0
    except Exception as exc:
        st.error(f"Prediction failed. Diagnostic: {exc}")
        return

    prob = prediction["denial_probability"]
    risk = prediction["risk_level"]

    # --- SHAP explanation ---
    shap_start = time.perf_counter()
    try:
        from src.xai.explainer import explain
        import numpy as np

        X_row = np.array([[float(feature_dict.get(c, 0.0)) for c in FEATURE_COLUMNS]])
        shap_explanations = explain(model, X_row, list(FEATURE_COLUMNS), top_n=5)
        shap_ms = (time.perf_counter() - shap_start) * 1000.0
    except Exception as exc:
        shap_ms = (time.perf_counter() - shap_start) * 1000.0
        logger.exception("SHAP explanation failed")
        shap_explanations = []
        st.warning(f"Explanation unavailable (diagnostic: {exc}). Showing prediction only.")
        _assert_no_phi(str(exc), "shap_error")

    # --- RAG retrieval ---
    rag_result: dict[str, Any] | None = None
    try:
        from src.rag.retriever import retrieve_and_explain

        retriever = _get_retriever()
        rag_result = retrieve_and_explain(
            shap_reasons=shap_explanations if shap_explanations else [],
            retriever=retriever,
            top_k=5,
        )
        rag_timing = rag_result.get("timing", {}) if isinstance(rag_result, dict) else {}
        policy_retrieval_ms = float(rag_timing.get("retrieval_ms", 0.0) or 0.0)
        narrative_ms = float(rag_timing.get("synthesis_ms", 0.0) or 0.0)
    except Exception as exc:
        logger.exception("RAG retrieval failed")
        st.warning(f"Policy retrieval unavailable (diagnostic: {exc}).")

    total_ms = (time.perf_counter() - total_start) * 1000.0
    latency = _summarize_latency(
        feature_lookup_ms=feature_lookup_ms,
        risk_inference_ms=risk_inference_ms,
        shap_ms=shap_ms,
        policy_retrieval_ms=policy_retrieval_ms,
        narrative_ms=narrative_ms,
        total_ms=total_ms,
    )
    logger.debug(
        "claim_latency claim_id=%s feature_lookup_ms=%.2f risk_inference_ms=%.2f shap_ms=%.2f "
        "policy_retrieval_ms=%.2f narrative_ms=%.2f risk_path_ms=%.2f explanation_ms=%.2f total_ms=%.2f",
        claim_id,
        latency["feature_lookup_ms"],
        latency["risk_inference_ms"],
        latency["shap_ms"],
        latency["policy_retrieval_ms"],
        latency["narrative_ms"],
        latency["risk_path_ms"],
        latency["explanation_ms"],
        latency["total_ms"],
    )

    # --- Render results ---
    st.divider()

    _render_risk_gauge(claim_id, prob, risk, latency["total_ms"])

    if shap_explanations:
        _render_feature_breakdown(shap_explanations, feature_dict)
    else:
        st.info("No SHAP explanations available for this claim.")

    _render_policy_guidance(rag_result)
    _render_latency_waterfall(latency)

    if feature_dict:
        _render_full_feature_table(shap_explanations, feature_dict, FEATURE_COLUMNS)


if __name__ == "__main__":
    main()
