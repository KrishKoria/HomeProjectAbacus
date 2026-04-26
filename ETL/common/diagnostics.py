from __future__ import annotations

from typing import Final


DIAGNOSTIC_DOMAIN_BRONZE: Final[str] = "BRZ"
DIAGNOSTIC_DOMAIN_ANALYTICS: Final[str] = "ANL"
DIAGNOSTIC_DOMAIN_HIPAA: Final[str] = "HIPAA"
DIAGNOSTIC_DOMAIN_OBSERVABILITY: Final[str] = "OBS"
DIAGNOSTIC_DOMAIN_SILVER: Final[str] = "SLV"
DIAGNOSTIC_DOMAIN_QUARANTINE: Final[str] = "QRT"

CLAIMOPS_DOMAINS: Final[frozenset[str]] = frozenset(
    {
        DIAGNOSTIC_DOMAIN_BRONZE,
        DIAGNOSTIC_DOMAIN_ANALYTICS,
        DIAGNOSTIC_DOMAIN_HIPAA,
        DIAGNOSTIC_DOMAIN_OBSERVABILITY,
        DIAGNOSTIC_DOMAIN_SILVER,
        DIAGNOSTIC_DOMAIN_QUARANTINE,
    }
)


def format_claimops_diagnostic_id(domain: str, number: int) -> str:
    """Return a stable diagnostic identifier for logs and dashboards."""
    normalized_domain = domain.upper()
    if normalized_domain not in CLAIMOPS_DOMAINS:
        raise ValueError(f"Unsupported diagnostic domain: {domain}")
    if number < 0 or number > 999:
        raise ValueError("Diagnostic number must be between 0 and 999.")
    return f"CLAIMOPS-{normalized_domain}-{number:03d}"


SILVER_DIAGNOSTIC_IDS: Final[dict[str, dict[str, str]]] = {
    "claims": {
        "missing_claim_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 101),
        "missing_patient_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 102),
        "missing_provider_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 103),
        "missing_diagnosis_code": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 104),
        "invalid_claim_date": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 105),
        "unknown_provider_reference": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 106),
        "unknown_diagnosis_reference": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 107),
        "duplicate_claim_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 108),
        "inconsistent_denial_label": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 109),
    },
    "providers": {
        "missing_provider_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 201),
        "missing_doctor_name": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 202),
        "duplicate_provider_id": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 203),
    },
    "diagnosis": {
        "missing_diagnosis_code": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 301),
        "missing_category": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 302),
        "missing_severity": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 303),
        "invalid_severity": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 304),
        "duplicate_diagnosis_code": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 305),
    },
    "cost": {
        "missing_procedure_code": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 401),
        "invalid_average_cost": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 402),
        "invalid_expected_cost": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 403),
        "missing_region": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 404),
        "duplicate_cost_key": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 405),
    },
    "policy_chunks": {
        "unreadable_pdf": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 501),
        "empty_pdf_text": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 502),
        "duplicate_policy_path": format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 503),
    },
}


def get_silver_diagnostic_id(dataset: str, rule_name: str) -> str:
    """Return a stable diagnostic ID for a dataset/rule pair."""
    dataset_rules = SILVER_DIAGNOSTIC_IDS.get(dataset, {})
    if rule_name in dataset_rules:
        return dataset_rules[rule_name]
    return format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_SILVER, 999)


__all__ = [
    "CLAIMOPS_DOMAINS",
    "DIAGNOSTIC_DOMAIN_ANALYTICS",
    "DIAGNOSTIC_DOMAIN_BRONZE",
    "DIAGNOSTIC_DOMAIN_HIPAA",
    "DIAGNOSTIC_DOMAIN_OBSERVABILITY",
    "DIAGNOSTIC_DOMAIN_QUARANTINE",
    "DIAGNOSTIC_DOMAIN_SILVER",
    "SILVER_DIAGNOSTIC_IDS",
    "format_claimops_diagnostic_id",
    "get_silver_diagnostic_id",
]
