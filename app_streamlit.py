"""Streamlit app for claim denial prediction with SHAP explanations and RAG policy retrieval.

Databricks-hosted: ``streamlit run app_streamlit.py`` on a Databricks workspace.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Final

import pandas as pd
import streamlit as st

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


def _check_gold_connectivity() -> tuple[bool, str]:
    try:
        query = f"SELECT 1 FROM {_quote_table_name(_gold_table_name())} LIMIT 1"
        _query_sql(query)
        return True, _gold_table_name()
    except Exception as exc:
        return False, str(exc)


def _check_model_availability() -> tuple[bool, str]:
    model = _load_model()
    if model is not None:
        return True, _env("CLAIMOPS_MODEL_NAME", _DEFAULT_MODEL_NAME)
    error_text = st.session_state.model_error or "Model unavailable"
    return False, error_text


def _check_vector_search_availability() -> tuple[bool, str]:
    try:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient().vector_search_indexes.get_index(index_name=_vector_index_name())
        return True, _vector_index_name()
    except Exception as exc:
        return False, str(exc)


def _render_status(label: str, ok: bool, detail: str) -> None:
    if ok:
        st.success(f"{label}: connected")
    else:
        st.warning(f"{label}: degraded ({detail[:_MAX_DETAILS_LEN]})")


def _risk_color(level: str) -> str:
    return {"LOW": "green", "MEDIUM": "orange", "HIGH": "red"}.get(level, "gray")


def _direction_arrow(direction: str) -> str:
    return "↑" if direction == "increases_risk" else "↓"


def _direction_color(direction: str) -> str:
    return "red" if direction == "increases_risk" else "green"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
def main() -> None:
    _init_session()

    st.title("Claim Denial Risk Analyzer")
    st.caption("SHAP explanations + RAG policy retrieval powered by Databricks")

    # --- Runtime health ---
    status_col_1, status_col_2, status_col_3 = st.columns(3)
    gold_ok, gold_detail = _check_gold_connectivity()
    model_ok, model_detail = _check_model_availability()
    vector_ok, vector_detail = _check_vector_search_availability()
    with status_col_1:
        _render_status("Gold Data", gold_ok, gold_detail)
    with status_col_2:
        _render_status("Model", model_ok, model_detail)
    with status_col_3:
        _render_status("Vector Search", vector_ok, vector_detail)

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

    start = time.perf_counter()

    try:
        raw = _load_claim_features(claim_id)
    except Exception as exc:
        st.error(
            "Feature lookup failed. "
            f"Check SQL warehouse/app permissions and connectivity.\n\nDiagnostic: {str(exc)[:_MAX_DETAILS_LEN]}"
        )
        return

    if raw.empty:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        st.error(
            f"Claim ID **{claim_id}** not found in `{_gold_table_name()}`. "
            f"(lookup took {elapsed_ms:.0f} ms)"
        )
        return

    features_df = fill_nulls(raw)
    feature_dict = {c: features_df[c].iloc[0] for c in FEATURE_COLUMNS}

    # --- Prediction ---
    try:
        from src.ml.predict import predict_single

        prediction = predict_single(model, feature_dict)
    except Exception as exc:
        st.error(f"Prediction failed. Diagnostic: {exc}")
        return

    prob = prediction["denial_probability"]
    risk = prediction["risk_level"]

    # --- SHAP explanation ---
    try:
        from src.xai.explainer import explain
        import numpy as np

        X_row = np.array([[float(feature_dict.get(c, 0.0)) for c in FEATURE_COLUMNS]])
        shap_explanations = explain(model, X_row, list(FEATURE_COLUMNS), top_n=5)
    except Exception as exc:
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
    except Exception as exc:
        logger.exception("RAG retrieval failed")
        st.warning(f"Policy retrieval unavailable (diagnostic: {exc}).")

    elapsed_ms = (time.perf_counter() - start) * 1000.0

    # --- Render results ---
    st.divider()

    # Prediction
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Denial Probability", f"{prob * 100:.1f}%")
    with col2:
        st.markdown(
            f"**Risk Level:** "
            f"<span style='color:{_risk_color(risk)};font-weight:bold;font-size:1.2em'>"
            f"{risk}</span>",
            unsafe_allow_html=True,
        )
    with col3:
        st.metric("Latency", f"{elapsed_ms:.0f} ms")

    # Explanation
    st.subheader("Why this assessment?")
    if shap_explanations:
        for entry in shap_explanations:
            reason_text = entry["reason"]
            _assert_no_phi(reason_text, f"shap_reason_{entry['feature']}")

            bar_width = min(entry["importance"] * 100, 100)
            arrow = _direction_arrow(entry["direction"])
            color = _direction_color(entry["direction"])

            st.markdown(
                f"**<span style='color:{color}'>{arrow}</span> "
                f"{reason_text}**",
                unsafe_allow_html=True,
            )
            st.progress(bar_width / 100, text=f"{entry['importance']:.3f}")
    else:
        st.info("No SHAP explanations available for this claim.")

    # Policy
    st.subheader("Policy Guidance")
    if rag_result and rag_result.get("narrative"):
        narrative = rag_result["narrative"]
        _assert_no_phi(narrative, "rag_narrative")
        st.markdown(narrative)

        policy_chunks = rag_result.get("policy_chunks", [])
        if policy_chunks:
            st.caption(f"Retrieved {len(policy_chunks)} policy snippet(s)")
            for i, chunk in enumerate(policy_chunks):
                with st.expander(
                    f"{chunk.get('document_path', 'Unknown')} "
                    f"§{chunk.get('chunk_index', '?')} "
                    f"(relevance: {chunk.get('relevance_score', 0):.2f})"
                ):
                    chunk_text = str(chunk.get("chunk_text", ""))
                    _assert_no_phi(chunk_text, f"policy_chunk_{i}")
                    st.text(chunk_text)
        elif rag_result.get("source") != "none":
            st.info("No matching policy documents found for this claim.")
    else:
        st.info("Policy retrieval is not available. Check Vector Search connectivity.")


if __name__ == "__main__":
    main()
