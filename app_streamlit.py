"""Streamlit app for claim denial prediction with SHAP explanations and RAG policy retrieval.

Databricks-hosted: ``streamlit run app_streamlit.py`` on a Databricks workspace.
"""
from __future__ import annotations

import logging
import time
from typing import Any

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
_FORBIDDEN_PHI_PATTERNS = [
    "patient_id",
    "patient_name",
    "date_of_birth",
    "XXX-XX-XXXX",
]
_PHI_PLACEHOLDER = "[REDACTED]"


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

        model = load_from_registry()
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

        retriever = PolicyRetriever()
        st.session_state.retriever = retriever
        return retriever
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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

    # --- Input ---
    claim_id = st.text_input(
        "Claim ID",
        placeholder="e.g. CLM-2024-000042",
        help="Enter a claim ID from healthcare.gold.claim_features",
    )
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
        from src.ml.features import load_gold_features, fill_nulls
        from src.ml import FEATURE_COLUMNS
        import pandas as pd
    except ImportError as exc:
        st.error(f"ML module unavailable. Diagnostic: {exc}")
        return

    start = time.perf_counter()

    try:
        raw = load_gold_features(claim_id=claim_id)
    except Exception:
        raw = pd.DataFrame()

    if raw.empty:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        st.error(
            f"Claim ID **{claim_id}** not found in `healthcare.gold.claim_features`. "
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
