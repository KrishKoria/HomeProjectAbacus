from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.ml.predict import RiskLevel


@pytest.fixture
def mock_model():
    model = MagicMock()
    model.predict_proba.return_value = [[0.3, 0.7]]
    return model


@pytest.fixture
def mock_shap():
    return [
        {
            "feature": "high_cost_flag",
            "importance": 0.25,
            "shap_value": 0.25,
            "reason": "Test reason",
            "direction": "increases_risk",
        }
    ]


@pytest.fixture
def mock_rag():
    return {
        "narrative": "Test narrative",
        "policy_citations": ["Section 1.2"],
        "policy_chunks": [
            {
                "document_path": "policy_01.pdf",
                "chunk_text": "Policy excerpt text",
                "score": 0.85,
            }
        ],
        "source": "template",
    }


@patch("src.serving.claim_analysis.predict_single")
@patch("src.serving.claim_analysis.explain")
@patch("src.serving.claim_analysis.retrieve_and_explain")
def test_analyze_claim(
    mock_retrieve,
    mock_explain,
    mock_predict,
    mock_model,
    mock_shap,
    mock_rag,
):
    mock_predict.return_value = {
        "denial_probability": 0.7,
        "risk_level": RiskLevel.HIGH,
    }
    mock_explain.return_value = mock_shap
    mock_retrieve.return_value = mock_rag

    from src.serving.claim_analysis import analyze_claim

    result = analyze_claim(
        claim_id="C0001",
        features={"high_cost_flag": 1.0, "missing_fields_count": 0.0},
        model=mock_model,
        retriever=None,
    )

    assert result["claimId"] == "C0001"
    assert result["riskLevel"] == "high"
    assert result["riskScore"] == 0.7
    assert len(result["topReasons"]) == 1
    assert result["topReasons"][0]["feature"] == "high_cost_flag"
    assert result["policyGuidance"] == [
        {"document": "policy_01.pdf", "excerpt": "Policy excerpt text", "relevance": 0.85}
    ]
    assert result["narrative"] == "Test narrative"
    assert "model" in result
    assert "generatedAt" in result
    mock_predict.assert_called_once()
    mock_explain.assert_called_once()
    mock_retrieve.assert_called_once()


@patch("src.serving.claim_analysis.predict_single")
@patch("src.serving.claim_analysis.explain")
@patch("src.serving.claim_analysis.retrieve_and_explain")
def test_analyze_claim_no_reasons(
    mock_retrieve,
    mock_explain,
    mock_predict,
    mock_model,
):
    mock_predict.return_value = {
        "denial_probability": 0.3,
        "risk_level": RiskLevel.LOW,
    }
    mock_explain.return_value = []
    mock_retrieve.return_value = {
        "narrative": "No SHAP explanations",
        "policy_citations": [],
        "policy_chunks": [],
        "source": "none",
    }

    from src.serving.claim_analysis import analyze_claim

    result = analyze_claim(
        claim_id="C0002",
        features={"high_cost_flag": 0.0},
        model=mock_model,
        retriever=None,
    )

    assert result["riskLevel"] == "low"
    assert result["riskScore"] == 0.3
    assert result["topReasons"] == []
    assert result["policyGuidance"] == []
