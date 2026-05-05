"""Generate deterministic synthetic Dx-Px mapping reference table for ICD-CPT compatibility.

This script creates ``dx_px_mapping.csv`` and emits one row per
``(diagnosis_code, procedure_code)`` pair with:

1. ``compatible`` (0/1): whether coding rules consider the pair valid.
2. ``procedure_category`` (str): coarse category for the procedure.
3. ``pair_risk_prior`` (float): synthetic denial-risk prior derived from rules.

`pair_risk_prior` is intentionally label-independent and must never be computed
from the current training labels. This avoids leakage while preserving a
granular code-pair signal in the training project.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH: Final[Path] = PROJECT_ROOT / "datasets" / "dx_px_mapping.csv"

COLUMNS: Final[tuple[str, ...]] = (
    "diagnosis_code",
    "procedure_code",
    "compatible",
    "procedure_category",
    "pair_risk_prior",
)

# Clinical compatibility rules for synthetic codes.
# These map each diagnosis_code to the procedure_codes that are medically indicated.
# In production, replace with CMS MCD coverage article crosswalk.
COMPATIBILITY_RULES: Final[dict[str, set[str]]] = {
    "D10": {"PROC2", "PROC5"},          # Heart → Cardiac surgery or Advanced (not Basic/General)
    "D20": {"PROC3", "PROC4"},          # Bone → Orthopedic procedures
    "D30": {"PROC1", "PROC6"},          # Fever → General or Basic routine
    "D40": {"PROC1", "PROC6"},          # Skin → General or Basic routine
    "D50": {"PROC2", "PROC5"},          # Diabetes → Cardiac (complex management) or Advanced
    "D60": {"PROC6"},                   # Cold → Basic routine only
}

# Procedure category assignment — coarse grouping for category-compatibility checks.
# Maps procedure_code to a clinical category label.
PROCEDURE_CATEGORIES: Final[dict[str, str]] = {
    "PROC1": "General",
    "PROC2": "Cardiac",
    "PROC3": "General",
    "PROC4": "Orthopedic",
    "PROC5": "Advanced",
    "PROC6": "Basic",
}

# All known synthetic codes — used to generate the full Cartesian product.
ALL_DIAGNOSIS_CODES: Final[tuple[str, ...]] = ("D10", "D20", "D30", "D40", "D50", "D60")
ALL_PROCEDURE_CODES: Final[tuple[str, ...]] = ("PROC1", "PROC2", "PROC3", "PROC4", "PROC5", "PROC6")

BASELINE_COMPATIBLE_RISK: Final[float] = 0.18
BASELINE_INCOMPATIBLE_RISK: Final[float] = 0.64
HIGH_SEVERITY_RISK_BONUS: Final[float] = 0.11
ADVANCED_PROC_RISK_BONUS: Final[float] = 0.09
HIGH_SEVERITY_CODES: Final[frozenset[str]] = frozenset({"D10", "D20", "D50"})
ADVANCED_PROCEDURES: Final[frozenset[str]] = frozenset({"PROC2", "PROC5"})


def _compute_pair_risk_prior(
    diagnosis_code: str,
    procedure_code: str,
    compatible: int,
) -> float:
    """Compute a deterministic synthetic risk prior for a Dx-Px pair."""
    risk = BASELINE_COMPATIBLE_RISK if compatible == 1 else BASELINE_INCOMPATIBLE_RISK
    if diagnosis_code in HIGH_SEVERITY_CODES:
        risk += HIGH_SEVERITY_RISK_BONUS
    if procedure_code in ADVANCED_PROCEDURES:
        risk += ADVANCED_PROC_RISK_BONUS
    return round(min(0.95, max(0.05, risk)), 4)


def generate_rows() -> list[dict[str, str | int | float]]:
    """Generate one row per (diagnosis_code, procedure_code) pair."""
    rows: list[dict[str, str | int | float]] = []
    for dx in ALL_DIAGNOSIS_CODES:
        compatible_procs = COMPATIBILITY_RULES.get(dx, set())
        for px in ALL_PROCEDURE_CODES:
            compatible = 1 if px in compatible_procs else 0
            pair_risk_prior = _compute_pair_risk_prior(dx, px, compatible)

            rows.append({
                "diagnosis_code": dx,
                "procedure_code": px,
                "compatible": compatible,
                "procedure_category": PROCEDURE_CATEGORIES[px],
                "pair_risk_prior": pair_risk_prior,
            })
    return rows


def write_mapping(path: Path, rows: list[dict[str, str | int | float]]) -> None:
    """Write the mapping table to CSV."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if output would change.",
    )
    args = parser.parse_args()

    rows = generate_rows()

    if args.check:
        if not args.output.exists():
            print("dx_px_mapping.csv does not exist — would be created.")
            return 1

        with args.output.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = tuple(reader.fieldnames or ())
            existing = list(reader)

        if fieldnames != COLUMNS:
            print(f"Column mismatch: existing={fieldnames} expected={COLUMNS}")
            return 1

        if len(existing) != len(rows):
            print(
                f"Row count mismatch: existing={len(existing)} expected={len(rows)}"
            )
            return 1

        for expected, actual in zip(rows, existing, strict=True):
            for col in COLUMNS:
                expected_val = str(expected[col])
                actual_val = actual[col].strip()
                if expected_val != actual_val:
                    print(
                        f"Mismatch at ({expected['diagnosis_code']}, {expected['procedure_code']})"
                        f" column={col}: expected={expected_val!r} actual={actual_val!r}"
                    )
                    return 1
        print("dx_px_mapping.csv is current.")
        return 0

    write_mapping(args.output, rows)
    print(f"Wrote {len(rows)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
