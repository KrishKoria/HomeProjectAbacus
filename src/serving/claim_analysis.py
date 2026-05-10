from __future__ import annotations

import logging
import math
import time
from typing import Any

import numpy as np
import pandas as pd

from src.ml import FEATURE_COLUMNS
from src.ml.predict import RiskLevel, predict_single
from src.rag.retriever import retrieve_and_explain
from src.rag.vector_search import PolicyRetriever
from src.xai.explainer import explain

logger = logging.getLogger(__name__)


def _risk_level_to_str(level: RiskLevel) -> str:
    mapping = {
        RiskLevel.LOW: "low",
        RiskLevel.MEDIUM: "medium",
        RiskLevel.HIGH: "high",
    }
    return mapping.get(level, "unknown")


def _sanitize(val: Any) -> Any:
    return None if isinstance(val, float) and math.isnan(val) else val


def analyze_claim(
    claim_id: str,
    features: dict[str, Any],
    model: Any = None,
    retriever: PolicyRetriever | None = None,
    top_n_reasons: int = 5,
) -> dict[str, Any]:
    logger.info("Analyzing claim_id=%s", claim_id)

    t0 = time.perf_counter()

    # 1. Prediction
    if model is None:
        from src.ml.predict import load_from_registry
        model = load_from_registry()
    prediction = predict_single(model, features)
    probability = _sanitize(prediction["denial_probability"]) or 0.0
    risk_level = prediction["risk_level"]
    risk_level_str = _risk_level_to_str(risk_level) if isinstance(risk_level, RiskLevel) else str(risk_level)

    # 2. SHAP explanations
    feature_names = list(FEATURE_COLUMNS)
    feature_vec = np.array([features.get(col, 0.0) for col in feature_names], dtype=float)
    shap_results = explain(model, feature_vec, feature_names, top_n=top_n_reasons)

    top_reasons = [
        {
            "feature": r["feature"],
            "value": _sanitize(features.get(r["feature"], None)),
            "shap_value": _sanitize(r["shap_value"]),
            "importance": _sanitize(r["importance"]),
            "description": r["reason"],
            "direction": r["direction"],
        }
        for r in shap_results
    ]

    # 3. Policy guidance via RAG
    rag_result = retrieve_and_explain(shap_results, retriever=retriever)
    policy_chunks = rag_result.get("policy_chunks", [])
    policy_guidance = [
        {
            "document": chunk.get("document_path", ""),
            "excerpt": chunk.get("chunk_text", ""),
            "relevance": chunk.get("score", None),
        }
        for chunk in policy_chunks
    ]

    elapsed = (time.perf_counter() - t0) * 1000.0
    logger.info(
        "analyze_claim completed claim_id=%s latency_ms=%.1f",
        claim_id,
        elapsed,
    )

    return {
        "claimId": claim_id,
        "riskScore": round(float(probability), 4),
        "riskLevel": risk_level_str,
        "predictionLabel": int(round(probability)),
        "topReasons": top_reasons,
        "policyGuidance": policy_guidance,
        "narrative": rag_result.get("narrative", ""),
        "policyCitations": rag_result.get("policy_citations", []),
        "model": "claim_denial_model@champion",
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
