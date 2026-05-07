"""Generate 6 additional synthetic policy PDFs covering the full ML feature space.

Reuses ReportLab helpers from tools.generate_synthetic_policy_pdfs.
Rules are data-driven from reference datasets in datasets/.
Output goes to datasets/policies/ alongside the existing 5 policy PDFs.

Usage:
    uv run python tools/generate_expanded_policy_pdfs.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.platypus import PageBreak, SimpleDocTemplate

# Reuse ReportLab helpers from existing generator
from tools.generate_synthetic_policy_pdfs import (
    add_header_footer,
    build_policy,
    build_styles,
    paragraph_list,
)

ROOT = Path(__file__).resolve().parents[1]
DATASETS_DIR = ROOT / "datasets"
OUTPUT_DIR = DATASETS_DIR / "policies"


def read_dataset(filename: str) -> pd.DataFrame:
    """Read a CSV dataset from datasets/ directory."""
    path = DATASETS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    return pd.read_csv(path)


def generate_dx_px_rules() -> list[tuple[str, str]]:
    """Generate diagnosis-procedure compatibility rules from dx_px_mapping.csv."""
    df = read_dataset("dx_px_mapping.csv")
    rules: list[tuple[str, str]] = []

    for diag_code in sorted(df["diagnosis_code"].unique()):
        diag_rows = df[df["diagnosis_code"] == diag_code]
        compatible = diag_rows[diag_rows["compatible"] == 1]["procedure_code"].tolist()
        incompatible = diag_rows[diag_rows["compatible"] == 0]["procedure_code"].tolist()

        risk_vals = diag_rows["pair_risk_prior"]
        low_risk = risk_vals.min()
        high_risk = risk_vals.max()

        rule_id = f"DXPX-{diag_code[1:]}"

        if compatible and incompatible:
            rules.append((
                rule_id,
                f"{diag_code}: Compatible with {', '.join(compatible)} "
                f"(low risk prior {low_risk:.2f}). "
                f"Incompatible with {', '.join(incompatible)} "
                f"(high risk prior {high_risk:.2f}). "
                f"Incompatible pairs trigger medical-necessity review."
            ))
        elif compatible:
            rules.append((
                rule_id,
                f"{diag_code}: Compatible with {', '.join(compatible)} "
                f"(risk prior {low_risk:.2f}). No incompatibility flags."
            ))
        else:
            rules.append((
                rule_id,
                f"{diag_code}: No compatible procedure pairs in reference. "
                f"All pairs carry elevated risk prior ({high_risk:.2f}). "
                f"Every procedure billed under this diagnosis requires clinical documentation."
            ))

    return rules


def generate_cost_rules() -> list[tuple[str, str]]:
    """Generate procedure cost benchmark rules from cost.csv."""
    df = read_dataset("cost.csv")
    rules: list[tuple[str, str]] = []

    for _, row in df.iterrows():
        proc = row["procedure_code"]
        expected = int(row["expected_cost"])
        region = row["region"]
        threshold_25 = int(expected * 1.25)
        threshold_2x = int(expected * 2)

        rule_id = f"COST-{proc[-1]}"
        rules.append((
            rule_id,
            f"{proc} benchmark expected_cost is {expected:,} "
            f"({region} reference pricing). "
            f"billed_amount above {threshold_25:,} (25% over benchmark) triggers "
            f"OVER_BENCHMARK_REVIEW. billed_amount above {threshold_2x:,} "
            f"(2x benchmark) sets high_cost_flag = 1."
        ))

    return rules


def generate_severity_rules() -> list[tuple[str, str]]:
    """Generate diagnosis severity rules from diagnosis.csv."""
    df = read_dataset("diagnosis.csv")
    rules: list[tuple[str, str]] = []

    high_sev = df[df["severity"] == "High"]
    low_sev = df[df["severity"] == "Low"]

    high_codes = ", ".join(
        f"{row['diagnosis_code']} {row['category']}" for _, row in high_sev.iterrows()
    )
    low_codes = ", ".join(
        f"{row['diagnosis_code']} {row['category']}" for _, row in low_sev.iterrows()
    )

    rules.append((
        "SEV-01",
        f"High-severity diagnoses ({high_codes}) require supporting clinical "
        f"documentation and a procedure aligned with the diagnosis category. "
        f"A high-severity diagnosis paired with a low-intensity procedure "
        f"sets severity_procedure_mismatch = 1 and triggers medical review."
    ))
    rules.append((
        "SEV-02",
        f"Low-severity diagnoses ({low_codes}) should generally map to "
        f"lower-intensity procedures. A low-severity diagnosis with a high-cost "
        f"procedure sets severity_procedure_mismatch = 1 and triggers "
        f"overutilization review."
    ))
    rules.append((
        "SEV-03",
        "A missing diagnosis_code prevents severity assessment. "
        "diagnosis_severity_encoded defaults to 0, and all severity-dependent "
        "features are treated as indeterminate."
    ))

    return rules


def generate_specialty_rules() -> list[tuple[str, str]]:
    """Generate specialty-diagnosis alignment rules from providers_1000.csv."""
    df = read_dataset("providers_1000.csv")
    specialties = sorted(df["specialty"].dropna().unique())

    rules: list[tuple[str, str]] = [
        (
            "SPEC-01",
            f"Provider specialties in this dataset: {', '.join(specialties)}. "
            f"Each specialty has one or more aligned diagnosis categories: "
            f"Cardiology aligns with D10 Heart and D50 Diabetes (cardiac complications); "
            f"Orthopedic aligns with D20 Bone; "
            f"Neurology aligns with D10 Heart (neuro-cardiac) and D50 Diabetes (neuropathy); "
            f"General aligns with D30 Fever, D40 Skin, D60 Cold (low-severity acute care)."
        ),
        (
            "SPEC-02",
            "When provider specialty does not align with the diagnosis category, "
            "specialty_diagnosis_mismatch is set to 1. Mismatched claims require "
            "documentation explaining why a non-aligned specialist performed the service."
        ),
        (
            "SPEC-03",
            "A provider with missing or unknown specialty should be treated as "
            "specialty_diagnosis_mismatch = 1 for conservative risk assessment. "
            "Confirm specialty before submitting the claim to the payer."
        ),
    ]

    return rules


POL_006_DX_PX_COMPAT = {
    "filename": "dx_px_compatibility_policy.pdf",
    "policy_id": "CLAIMOPS-POL-006",
    "title": "Diagnosis-Procedure Compatibility Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Encodes clinical coding compatibility rules between diagnosis_code and "
        "procedure_code. Maps each diagnosis family to its compatible and incompatible "
        "procedures, and defines the denial-risk implications of incompatible coding pairs."
    ),
    "summary": [
        "Each diagnosis_code has a defined set of compatible procedure_codes based on clinical coding guidelines.",
        "A compatible diagnosis-procedure pair has a lower pair_risk_prior and does not trigger medical-necessity review from coding mismatch alone.",
        "An incompatible diagnosis-procedure pair sets dx_px_compatible = 0 and carries an elevated pair_risk_prior, signaling higher denial probability.",
        "Incompatible pairs require supporting documentation explaining why the billed procedure was performed for the given diagnosis.",
    ],
    "rules": generate_dx_px_rules(),
    "remediation": [
        "Before submitting, verify that the selected procedure_code is clinically indicated for the diagnosis_code.",
        "For incompatible pairs, attach chart notes or a letter of medical necessity explaining the clinical rationale.",
        "If the procedure_code was entered in error, correct it and re-run pre-submission validation.",
        "Use diagnosis_code and procedure_code (not patient identifiers or dates) when searching for policy guidance.",
    ],
}

POL_007_TEMPORAL_UTIL = {
    "filename": "temporal_provider_utilization_policy.pdf",
    "policy_id": "CLAIMOPS-POL-007",
    "title": "Temporal Provider Utilization Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Defines how temporal utilization windows (30-day, 60-day, 90-day) and "
        "provider denial rates are used for claim-level risk assessment."
    ),
    "summary": [
        "provider_claim_count_30d measures recent utilization — the number of claims filed by this provider in the trailing 30-day window.",
        "provider_claim_count_60d provides a medium-term utilization signal for trend detection.",
        "provider_claim_count_90d establishes a longer baseline. Deviation from this baseline flags pattern changes.",
        "provider_30d_denial_rate above 30% in the trailing 30 days triggers enhanced claim scrutiny.",
        "low_volume_provider_risk activates when a provider's total claim count falls below statistical reliability thresholds.",
    ],
    "rules": [
        (
            "TEMP-01",
            "provider_claim_count_30d captures short-term utilization. "
            "Fewer than 3 claims in the trailing 30 days produces an unstable risk signal. "
            "Use 60d and 90d windows as supplementary context."
        ),
        (
            "TEMP-02",
            "provider_claim_count_60d provides medium-term trend detection. "
            "A sharp increase from 30d to 60d may indicate a practice-pattern change "
            "or seasonal variation. A sharp decrease may indicate reduced patient volume."
        ),
        (
            "TEMP-03",
            "provider_claim_count_90d establishes the long-term baseline. "
            "Claims from providers whose 30d count deviates more than 50% from their "
            "90d average should be reviewed for utilization anomalies."
        ),
        (
            "TEMP-04",
            "provider_30d_denial_rate measures the proportion of denied claims "
            "in the trailing 30 days. A denial rate exceeding 30% is elevated and "
            "triggers enhanced review for every subsequent claim from that provider "
            "until the rate normalizes."
        ),
        (
            "TEMP-05",
            "low_volume_provider_risk is set to 1 when a provider has fewer than "
            "10 total claims in the reference period. Low-volume providers have "
            "statistically unreliable denial-rate estimates. Their claims are assessed "
            "primarily on dx-px compatibility and cost signals rather than provider history."
        ),
    ],
    "remediation": [
        "For low-volume providers, rely on coding and cost signals rather than historical denial patterns.",
        "For providers with elevated 30d denial rates, flag all pending claims for pre-submission review.",
        "For providers with unusual temporal patterns, request a utilization summary before adjudication.",
        "Do not include provider identifier or claim counts in LLM prompts. Use temporal descriptors only.",
    ],
}

POL_008_SPECIALTY_ALIGN = {
    "filename": "specialty_diagnosis_alignment_policy.pdf",
    "policy_id": "CLAIMOPS-POL-008",
    "title": "Specialty-Diagnosis Alignment Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Defines expected alignment between provider specialty and diagnosis category. "
        "Specialty-diagnosis mismatches are a standard medical-necessity gate and "
        "directly inform the severity_procedure_mismatch and specialty_diagnosis_mismatch features."
    ),
    "summary": [
        "Each provider specialty has a defined set of diagnosis categories it typically manages.",
        "When a provider bills a procedure for a diagnosis outside their specialty alignment, specialty_diagnosis_mismatch is set to 1.",
        "A high-severity diagnosis paired with a low-intensity procedure triggers severity_procedure_mismatch regardless of specialty alignment.",
        "Specialty alignment complements, but does not replace, diagnosis-procedure compatibility review.",
    ],
    "rules": generate_specialty_rules(),
    "remediation": [
        "Verify the provider's specialty against the billed diagnosis before payer submission.",
        "If specialty mismatch is identified, request documentation from the referring or supervising physician.",
        "If the diagnosis was coded in error, correct the diagnosis_code and re-run validation.",
        "For legitimate cross-specialty care (e.g., cardiologist managing diabetes complications), attach a supporting note.",
    ],
}

POL_009_MISSING_DATA = {
    "filename": "missing_data_field_triage_policy.pdf",
    "policy_id": "CLAIMOPS-POL-009",
    "title": "Missing Data Field Triage Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Defines the triage priority for claims submitted with missing required fields. "
        "Directly informs is_procedure_missing, is_amount_missing, provider_location_missing, "
        "and the composite missing_fields_count feature."
    ),
    "summary": [
        "Missing critical fields prevent claim validation and must be resolved before payer submission.",
        "missing_fields_count aggregates the total number of absent required fields on a single claim.",
        "procedure_code is the highest-priority field — without it, no coding, pricing, or medical review is possible.",
        "billed_amount is the second-priority field — without it, no cost benchmarking or payment calculation is possible.",
        "provider_location is operational metadata — missing location does not block processing but prevents regional benchmarking.",
    ],
    "rules": [
        (
            "MISS-01",
            "missing_fields_count = 0: All required fields present. Standard processing."
        ),
        (
            "MISS-02",
            "missing_fields_count = 1: Single field missing. Route to field-specific "
            "remediation queue. Priority determined by which field is missing."
        ),
        (
            "MISS-03",
            "missing_fields_count >= 2: Multiple fields missing. High-priority case. "
            "Route to supervisor review. Do not submit to payer until all critical "
            "fields are resolved."
        ),
        (
            "MISS-04",
            "is_procedure_missing = 1: procedure_code is blank. Claim cannot be priced, "
            "coded, or medically reviewed. HIGHEST priority. Route to coding review "
            "immediately. Expected denial reason: MISSING_PROCEDURE."
        ),
        (
            "MISS-05",
            "is_amount_missing = 1: billed_amount is blank. Claim cannot be benchmarked "
            "against expected_cost or average_cost. SECOND priority. Route to charge "
            "entry review. Expected denial reason: MISSING_BILLED_AMOUNT."
        ),
        (
            "MISS-06",
            "provider_location_missing = 1: provider location is blank. Operational "
            "data quality issue. Impute as Unknown for analytics. LOWEST priority. "
            "Does not independently cause denial but contributes to missing_fields_count."
        ),
    ],
    "remediation": [
        "Triage claims by missing_fields_count first, then by which specific fields are absent.",
        "Resolve procedure_code gaps before billed_amount gaps — coding must happen before pricing.",
        "For claims missing both procedure_code and billed_amount, assign to a senior analyst.",
        "After field completion, re-run the full pre-submission validation pipeline.",
    ],
}

POL_010_COST_SEVERITY = {
    "filename": "cost_severity_interaction_policy.pdf",
    "policy_id": "CLAIMOPS-POL-010",
    "title": "Cost-Severity Interaction Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Defines how billed_amount interacts with diagnosis severity to produce "
        "compound risk signals. Covers amount_to_benchmark_ratio, high_cost_flag, "
        "cost_overbenchmark_and_highseverity, and mismatch_and_overbenchmark features."
    ),
    "summary": [
        "amount_to_benchmark_ratio compares billed_amount to the procedure-specific expected_cost. A ratio above 1.25 triggers review.",
        "high_cost_flag is set to 1 when billed_amount exceeds 2x the procedure expected_cost.",
        "cost_overbenchmark_and_highseverity combines over-benchmark charges with a high-severity diagnosis — the strongest cost-based risk signal.",
        "mismatch_and_overbenchmark combines specialty-diagnosis mismatch with over-benchmark charges — dual misalignment is a high-confidence denial predictor.",
    ],
    "rules": generate_cost_rules() + [
        (
            "COST-INT-07",
            "high_cost_flag = 1 when billed_amount exceeds 2x the procedure "
            "expected_cost. High-cost claims undergo mandatory secondary review "
            "regardless of other risk signals."
        ),
        (
            "COST-INT-08",
            "cost_overbenchmark_and_highseverity = 1 when BOTH amount_to_benchmark_ratio > 1.25 "
            "AND diagnosis_severity_encoded indicates High severity. "
            "This intersection triggers the most intensive manual-review protocol "
            "because high-cost, high-severity claims carry the greatest financial "
            "and clinical risk."
        ),
        (
            "COST-INT-09",
            "mismatch_and_overbenchmark = 1 when BOTH specialty_diagnosis_mismatch = 1 "
            "AND amount_to_benchmark_ratio > 1.25. Dual misalignment — clinical AND "
            "financial — is the strongest single denial predictor in the model. "
            "These claims require both clinical documentation AND cost justification."
        ),
    ],
    "remediation": [
        "For over-benchmark claims, verify the billed_amount against the charge master and contract rates.",
        "For cost-overbenchmark-and-highseverity claims, require both clinical notes AND a cost outlier explanation.",
        "For mismatch-and-overbenchmark claims, escalate to medical director review before any payer submission.",
        "Do not include billed_amount or expected_cost in LLM prompts. Use over benchmark or within benchmark descriptors.",
    ],
}

POL_011_PROVIDER_RISK = {
    "filename": "provider_risk_stratification_policy.pdf",
    "policy_id": "CLAIMOPS-POL-011",
    "title": "Provider Risk Stratification Policy",
    "effective": "2026-04-25",
    "purpose": (
        "Defines provider risk tiers based on claim volume and historical denial patterns. "
        "Covers provider_risk_score, provider_claim_count, and low_volume_provider_risk features."
    ),
    "summary": [
        "provider_risk_score aggregates denial frequency, charge outliers, and practice-pattern metrics into a single risk score.",
        "Provider risk is stratified into tiers based on claim volume and denial history.",
        "Micro-volume and low-volume providers have statistically unreliable risk scores and are assessed primarily on claim-level signals.",
        "High-volume providers with elevated risk scores signal systemic documentation or coding issues that may affect all their claims.",
    ],
    "rules": [
        (
            "RISK-01",
            "provider_risk_score baseline is 0.15 (synthetic prior for unknown providers). "
            "Scores between 0.15 and 0.30 are within normal range. "
            "Scores above 0.30 are elevated and trigger enhanced review for all claims from that provider."
        ),
        (
            "RISK-02",
            "provider_risk_score above 0.50 is high risk. Claims from high-risk "
            "providers receive the most intensive review tier: mandatory clinical "
            "documentation check, cost benchmark comparison, and dx-px compatibility "
            "verification before payer submission."
        ),
        (
            "RISK-03",
            "Micro-volume tier: provider_claim_count < 5. Individual risk scores are "
            "unstable due to insufficient data. Claims are assessed using dx-px "
            "compatibility, cost benchmarks, and completeness signals rather than "
            "provider history. low_volume_provider_risk = 1."
        ),
        (
            "RISK-04",
            "Low-volume tier: provider_claim_count between 5 and 20. Risk scores "
            "should be used with caution. Supplement provider-history signals with "
            "claim-level coding and cost signals. low_volume_provider_risk = 1."
        ),
        (
            "RISK-05",
            "Adequate-volume tier: provider_claim_count between 20 and 100. "
            "Risk score estimates are moderately reliable. Use provider history "
            "as one signal among several."
        ),
        (
            "RISK-06",
            "High-volume tier: provider_claim_count > 100. Risk score estimates are "
            "statistically reliable. Provider history carries full weight in the "
            "denial-risk model alongside claim-level signals."
        ),
    ],
    "remediation": [
        "For micro-volume providers, focus remediation on claim-level completeness and coding accuracy.",
        "For high-volume, high-risk providers, initiate a provider education or documentation improvement program.",
        "Recompute provider risk scores weekly to capture changing practice patterns.",
        "Do not include provider identifiers, claim counts, or risk scores in LLM prompts.",
    ],
}

EXPANDED_POLICIES = [
    POL_006_DX_PX_COMPAT,
    POL_007_TEMPORAL_UTIL,
    POL_008_SPECIALTY_ALIGN,
    POL_009_MISSING_DATA,
    POL_010_COST_SEVERITY,
    POL_011_PROVIDER_RISK,
]


def generate() -> list[Path]:
    """Generate all 6 expanded policy PDFs. Returns list of output paths."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    styles = build_styles()
    generated: list[Path] = []

    for policy in EXPANDED_POLICIES:
        path = OUTPUT_DIR / str(policy["filename"])
        doc = SimpleDocTemplate(
            str(path),
            pagesize=LETTER,
            rightMargin=0.72 * inch,
            leftMargin=0.72 * inch,
            topMargin=0.65 * inch,
            bottomMargin=0.7 * inch,
            title=str(policy["title"]),
            author="ClaimOps Abacus Demo",
            subject="Synthetic non-PHI policy corpus — expanded",
        )
        story = build_policy(policy, styles)
        # Remove trailing PageBreak (build_policy adds one)
        if story and isinstance(story[-1], PageBreak):
            story = story[:-1]
        doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
        generated.append(path)

    return generated


if __name__ == "__main__":
    for generated_path in generate():
        print(generated_path.relative_to(ROOT))
