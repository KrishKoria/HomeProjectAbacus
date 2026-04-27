"""Regenerate deterministic synthetic adjudication labels for claims_1000.csv.

The base claim columns are treated as the fixture input. This script recomputes
only the synthetic adjudication columns so future dataset refreshes can keep the
label recipe under version control.
"""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLAIMS_PATH = PROJECT_ROOT / "datasets" / "claims_1000.csv"
DEFAULT_PROVIDERS_PATH = PROJECT_ROOT / "datasets" / "providers_1000.csv"
DEFAULT_COST_PATH = PROJECT_ROOT / "datasets" / "cost.csv"

LABEL_COLUMNS = (
    "claim_status",
    "denial_reason_code",
    "allowed_amount",
    "paid_amount",
    "is_denied",
    "follow_up_required",
)

MEDICAL_REVIEW_CLAIM_IDS = frozenset(
    {
        "C0002",
        "C0018",
        "C0029",
        "C0049",
        "C0050",
        "C0073",
        "C0079",
        "C0083",
        "C0098",
        "C0103",
        "C0105",
        "C0112",
        "C0119",
        "C0137",
        "C0139",
        "C0141",
        "C0148",
        "C0161",
        "C0163",
        "C0170",
        "C0177",
        "C0184",
        "C0185",
        "C0206",
        "C0213",
        "C0216",
        "C0224",
        "C0230",
        "C0239",
        "C0307",
        "C0318",
        "C0328",
        "C0333",
        "C0337",
        "C0355",
        "C0365",
        "C0379",
        "C0385",
        "C0386",
        "C0395",
        "C0396",
        "C0410",
        "C0414",
        "C0425",
        "C0426",
        "C0431",
        "C0446",
        "C0457",
        "C0464",
        "C0473",
        "C0476",
        "C0482",
        "C0496",
        "C0511",
        "C0527",
        "C0558",
        "C0562",
        "C0574",
        "C0575",
        "C0610",
        "C0620",
        "C0627",
        "C0653",
        "C0659",
        "C0660",
        "C0668",
        "C0677",
        "C0682",
        "C0685",
        "C0705",
        "C0740",
        "C0755",
        "C0757",
        "C0769",
        "C0777",
        "C0797",
        "C0804",
        "C0812",
        "C0815",
        "C0817",
        "C0827",
        "C0837",
        "C0838",
        "C0878",
        "C0884",
        "C0904",
        "C0932",
        "C0934",
        "C0935",
        "C0943",
        "C0985",
    }
)


def _money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _decimal_or_none(value: str) -> Decimal | None:
    return Decimal(value) if value else None


def load_provider_regions(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["provider_id"]: row["location"] for row in csv.DictReader(handle)}


def load_expected_costs(path: Path) -> dict[tuple[str, str], Decimal]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            (row["procedure_code"], row["region"]): Decimal(row["expected_cost"])
            for row in csv.DictReader(handle)
        }


def classify_claim(
    row: dict[str, str],
    provider_regions: dict[str, str],
    expected_costs: dict[tuple[str, str], Decimal],
) -> tuple[str, str | None]:
    if not row["procedure_code"]:
        return "MISSING_PROCEDURE", None
    if not row["billed_amount"]:
        return "MISSING_BILLED_AMOUNT", None

    expected_cost = expected_costs.get((row["procedure_code"], provider_regions.get(row["provider_id"], "")))
    billed_amount = Decimal(row["billed_amount"])
    if expected_cost is not None and expected_cost <= Decimal("0"):
        expected_cost = None
    if expected_cost is not None and billed_amount / expected_cost > Decimal("2.5"):
        return "OVER_BENCHMARK", _money(expected_cost)
    if row["claim_id"] in MEDICAL_REVIEW_CLAIM_IDS:
        return "MEDICAL_REVIEW", _money(billed_amount * Decimal("0.60"))
    if expected_cost is not None:
        return "NONE", _money(min(billed_amount, expected_cost * Decimal("1.05")))
    return "NONE", _money(billed_amount * Decimal("0.85"))


def apply_labels(
    rows: list[dict[str, str]],
    provider_regions: dict[str, str],
    expected_costs: dict[tuple[str, str], Decimal],
) -> list[dict[str, str]]:
    labeled_rows = []
    for row in rows:
        labeled = dict(row)
        reason, allowed_amount = classify_claim(labeled, provider_regions, expected_costs)
        is_denied = reason != "NONE"

        labeled["claim_status"] = "DENIED" if is_denied else "APPROVED"
        labeled["denial_reason_code"] = reason
        labeled["allowed_amount"] = allowed_amount or "0.00"
        labeled["paid_amount"] = "0.00" if is_denied else labeled["allowed_amount"]
        labeled["is_denied"] = "1" if is_denied else "0"
        labeled["follow_up_required"] = labeled["is_denied"]
        labeled_rows.append(labeled)
    return labeled_rows


def read_claims(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def write_claims(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", type=Path, default=DEFAULT_CLAIMS_PATH)
    parser.add_argument("--providers", type=Path, default=DEFAULT_PROVIDERS_PATH)
    parser.add_argument("--cost", type=Path, default=DEFAULT_COST_PATH)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true", help="Exit non-zero if labels would change.")
    args = parser.parse_args()

    fieldnames, rows = read_claims(args.claims)
    regenerated = apply_labels(rows, load_provider_regions(args.providers), load_expected_costs(args.cost))

    if args.check:
        mismatches = [
            row["claim_id"]
            for row, expected in zip(rows, regenerated, strict=True)
            if any(row[column] != expected[column] for column in LABEL_COLUMNS)
        ]
        if mismatches:
            print(f"Synthetic labels differ for {len(mismatches)} claims: {', '.join(mismatches[:10])}")
            return 1
        print("Synthetic labels are current.")
        return 0

    write_claims(args.output or args.claims, fieldnames, regenerated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
