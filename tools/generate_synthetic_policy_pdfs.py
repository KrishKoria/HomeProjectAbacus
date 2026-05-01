from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "datasets" / "policies"


POLICIES = [
    {
        "filename": "claim_submission_completeness_policy.pdf",
        "policy_id": "CLAIMOPS-POL-001",
        "title": "Claim Submission Completeness Policy",
        "effective": "2026-04-25",
        "purpose": (
            "Defines minimum data completeness requirements for synthetic pre-submission "
            "claim validation in the ClaimOps Abacus demo environment."
        ),
        "summary": [
            "Every claim must include claim_id, patient_id, provider_id, diagnosis_code, procedure_code, billed_amount, and date before payer submission.",
            "Claims missing procedure_code must be held for billing analyst correction and flagged with denial_reason_code MISSING_PROCEDURE.",
            "Claims missing billed_amount must be held for amount reconciliation and flagged with denial_reason_code MISSING_BILLED_AMOUNT.",
            "Claims missing both procedure_code and billed_amount are high priority remediation cases because they cannot be priced or medically reviewed.",
        ],
        "rules": [
            ("COMP-01", "procedure_code is required for all submitted claims. A blank procedure_code prevents coding validation and should not be sent to the payer."),
            ("COMP-02", "billed_amount is required for all submitted claims. A blank billed_amount prevents allowed_amount comparison and payment calculation."),
            ("COMP-03", "diagnosis_code is required for clinical context. A blank diagnosis_code must be reviewed before the claim can receive a policy-backed explanation."),
            ("COMP-04", "date must be present and parseable. Service dates are PHI in production logs and must not be included in RAG prompts."),
        ],
        "remediation": [
            "Route missing procedure_code cases to coding review.",
            "Route missing billed_amount cases to charge entry review.",
            "If multiple critical fields are missing, prioritize completion before medical necessity review.",
            "Do not include patient_id, service date, or billed amount in policy retrieval prompts.",
        ],
    },
    {
        "filename": "medical_necessity_by_diagnosis_policy.pdf",
        "policy_id": "CLAIMOPS-POL-002",
        "title": "Medical Necessity by Diagnosis Policy",
        "effective": "2026-04-25",
        "purpose": (
            "Maps the synthetic diagnosis families used by this project to expected "
            "procedure review behavior for denial-risk scoring."
        ),
        "summary": [
            "High-severity diagnosis categories D10 Heart, D20 Bone, and D50 Diabetes require stronger documentation and a clinically aligned procedure_code.",
            "Low-severity diagnosis categories D30 Fever, D40 Skin, and D60 Cold should generally map to lower-intensity procedures unless documentation supports escalation.",
            "A high-severity diagnosis paired with a low-intensity procedure may require medical review because the clinical story is incomplete.",
            "A low-severity diagnosis paired with a high-cost procedure may require overutilization review.",
        ],
        "rules": [
            ("MED-01", "D10 Heart claims commonly align with PROC2 or PROC5 when specialist intervention or advanced testing is documented."),
            ("MED-02", "D20 Bone claims commonly align with PROC3 or PROC4 when imaging, fracture care, or orthopedic treatment is documented."),
            ("MED-03", "D50 Diabetes claims commonly align with PROC1, PROC2, or PROC6 depending on acuity and monitoring needs."),
            ("MED-04", "D30 Fever, D40 Skin, and D60 Cold are low-severity categories. High-cost procedures for these categories require clear supporting documentation."),
            ("MED-05", "A claim with diagnosis_code missing should not receive a confident medical necessity explanation until diagnosis context is corrected."),
        ],
        "remediation": [
            "Ask the billing analyst to confirm whether the selected procedure_code matches the diagnosis category.",
            "For D10, D20, and D50 claims, require chart notes supporting severity and selected procedure intensity.",
            "For D30, D40, and D60 claims with PROC5 or unusually high billed_amount, request supervisor review.",
            "Use diagnosis category and denial reason in RAG queries; exclude patient identifiers and service dates.",
        ],
    },
    {
        "filename": "procedure_cost_benchmark_policy.pdf",
        "policy_id": "CLAIMOPS-POL-003",
        "title": "Procedure Cost Benchmark Policy",
        "effective": "2026-04-25",
        "purpose": (
            "Documents synthetic benchmark behavior for comparing billed_amount to "
            "expected_cost in the project cost reference dataset."
        ),
        "summary": [
            "The expected_cost column is the reference benchmark for pre-submission cost reasonableness review.",
            "A billed_amount materially above expected_cost should be flagged for overbilling review before payer submission.",
            "A missing billed_amount cannot be benchmarked and should be corrected before cost-risk scoring.",
            "Benchmark review is a risk signal, not a final coverage or payment determination.",
        ],
        "rules": [
            ("COST-01", "PROC1 benchmark expected_cost is 5000 for Delhi reference pricing."),
            ("COST-02", "PROC2 benchmark expected_cost is 15000 for Mumbai reference pricing."),
            ("COST-03", "PROC3 benchmark expected_cost is 9000 for Bangalore reference pricing."),
            ("COST-04", "PROC4 benchmark expected_cost is 8000 for Hyderabad reference pricing."),
            ("COST-05", "PROC5 benchmark expected_cost is 25000 for Chennai reference pricing."),
            ("COST-06", "PROC6 benchmark expected_cost is 1200 for Ahmedabad reference pricing."),
            ("COST-07", "If billed_amount is greater than expected_cost by more than 25 percent, flag OVER_BENCHMARK_REVIEW for analyst confirmation."),
        ],
        "remediation": [
            "Confirm the correct procedure_code before comparing against the benchmark.",
            "If the billed amount is high because of documented complexity, attach supporting notes before submission.",
            "If the procedure_code was selected incorrectly, correct the code and re-run validation.",
            "Do not send billed_amount to the LLM. Use non-PHI descriptors such as over benchmark or missing amount.",
        ],
    },
    {
        "filename": "provider_documentation_policy.pdf",
        "policy_id": "CLAIMOPS-POL-004",
        "title": "Provider Documentation Policy",
        "effective": "2026-04-25",
        "purpose": (
            "Defines provider-reference checks for synthetic claim validation and "
            "documents how provider metadata should be handled."
        ),
        "summary": [
            "provider_id must resolve to a provider reference record before a claim is considered complete.",
            "Provider specialty should be consistent with the diagnosis category and procedure intensity.",
            "Missing provider location is an operational data-quality issue and should be imputed to Unknown in trusted Silver provider data.",
            "Provider names and locations are operational data in this synthetic dataset, but access should still be controlled.",
        ],
        "rules": [
            ("PROV-01", "A claim with provider_id not present in the provider reference table should be flagged PROVIDER_NOT_FOUND."),
            ("PROV-02", "A high-severity diagnosis with a specialty mismatch should be flagged for medical review."),
            ("PROV-03", "Missing provider location should not block trusted claim processing when provider identity is otherwise resolvable."),
            ("PROV-04", "Provider metadata should be used for operational routing and specialty checks, not as patient PHI."),
        ],
        "remediation": [
            "Verify provider_id against the provider master data before payer submission.",
            "If specialty mismatch is found, request coder review or provider documentation.",
            "If location is missing, use Unknown in analytics and avoid dropping otherwise valid provider rows.",
            "Keep policy explanations focused on specialty alignment and documentation requirements.",
        ],
    },
    {
        "filename": "denial_reason_remediation_policy.pdf",
        "policy_id": "CLAIMOPS-POL-005",
        "title": "Denial Reason Remediation Policy",
        "effective": "2026-04-25",
        "purpose": (
            "Standardizes synthetic denial reason interpretation and remediation "
            "recommendations for analyst-facing explanations."
        ),
        "summary": [
            "MISSING_PROCEDURE means the claim lacks a billable procedure_code and should not be submitted until coding is complete.",
            "MISSING_BILLED_AMOUNT means the claim lacks a charge amount and cannot be evaluated for payment or benchmark reasonableness.",
            "MEDICAL_REVIEW means the diagnosis, procedure, provider specialty, or cost pattern requires clinical or coding confirmation.",
            "NONE means no synthetic denial reason is currently assigned, but standard completeness checks still apply.",
        ],
        "rules": [
            ("DENY-01", "For MISSING_PROCEDURE, return a remediation step asking the analyst to add or correct procedure_code."),
            ("DENY-02", "For MISSING_BILLED_AMOUNT, return a remediation step asking the analyst to confirm the billed amount from charge entry."),
            ("DENY-03", "For MEDICAL_REVIEW, return a remediation step asking for diagnosis-procedure-supporting documentation."),
            ("DENY-04", "For NONE, do not invent a denial reason. Report that no synthetic denial reason was present and cite any remaining validation warnings separately."),
            ("DENY-05", "Every explanation must cite the policy_id, rule_id, and document title used to support the recommendation."),
        ],
        "remediation": [
            "Rank missing critical fields before medical necessity or cost review.",
            "Provide one concrete next action per denial reason.",
            "Separate policy-backed findings from model-risk signals so analysts can see what is deterministic.",
            "Never include patient_id, claim_id, date, or billed_amount in LLM prompts or policy-search text.",
        ],
    },
]


def build_styles() -> dict[str, ParagraphStyle]:
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="DocTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            spaceAfter=10,
            textColor=colors.HexColor("#233044"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="SectionHeading",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            spaceBefore=10,
            spaceAfter=6,
            textColor=colors.HexColor("#31576f"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="Body",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=13,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Small",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=8,
            leading=10,
            textColor=colors.HexColor("#555555"),
        )
    )
    return styles


def add_header_footer(canvas, doc) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#666666"))
    canvas.drawString(0.72 * inch, 0.45 * inch, "Synthetic policy corpus - non-PHI - for ClaimOps Abacus demo use")
    canvas.drawRightString(7.75 * inch, 0.45 * inch, f"Page {doc.page}")
    canvas.restoreState()


def paragraph_list(items: list[str], styles: dict[str, ParagraphStyle]) -> list[Paragraph]:
    return [Paragraph(f"- {item}", styles["Body"]) for item in items]


def build_policy(policy: dict[str, object], styles: dict[str, ParagraphStyle]) -> list[object]:
    story: list[object] = []
    story.append(Paragraph(str(policy["title"]), styles["DocTitle"]))
    story.append(Paragraph("Synthetic Training Policy - Non-PHI", styles["SectionHeading"]))
    story.append(Paragraph(str(policy["purpose"]), styles["Body"]))

    metadata = [
        ["Policy ID", str(policy["policy_id"])],
        ["Effective Date", str(policy["effective"])],
        ["Applies To", "Synthetic claims, provider, diagnosis, and cost datasets in this repository"],
        ["Source Type", "Locally generated demo policy; not a real payer coverage policy"],
    ]
    table = Table(metadata, colWidths=[1.35 * inch, 5.2 * inch])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8eef3")),
                ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#233044")),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b7c4cf")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.extend([table, Spacer(1, 0.12 * inch)])

    story.append(Paragraph("Policy Summary", styles["SectionHeading"]))
    story.extend(paragraph_list(policy["summary"], styles))

    story.append(Paragraph("Rules", styles["SectionHeading"]))
    rule_rows = [["Rule ID", "Requirement"]]
    for rule_id, text in policy["rules"]:
        rule_rows.append([rule_id, Paragraph(text, styles["Body"])])
    rules_table = Table(rule_rows, colWidths=[0.92 * inch, 5.65 * inch], repeatRows=1)
    rules_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#31576f")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#c6d0d8")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(rules_table)

    story.append(Paragraph("Remediation Guidance", styles["SectionHeading"]))
    story.extend(paragraph_list(policy["remediation"], styles))

    story.append(Paragraph("RAG Safety Note", styles["SectionHeading"]))
    story.append(
        Paragraph(
            "This document contains only synthetic policy text. It intentionally avoids patient names, patient identifiers, service dates, and real billed amounts. Use policy_id, rule_id, denial reason, diagnosis category, and procedure status as retrieval metadata.",
            styles["Small"],
        )
    )
    story.append(PageBreak())
    return story


def generate() -> list[Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    styles = build_styles()
    generated: list[Path] = []
    for policy in POLICIES:
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
            subject="Synthetic non-PHI policy corpus",
        )
        story = build_policy(policy, styles)
        if story and isinstance(story[-1], PageBreak):
            story = story[:-1]
        doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
        generated.append(path)
    return generated


if __name__ == "__main__":
    for generated_path in generate():
        print(generated_path.relative_to(ROOT))
