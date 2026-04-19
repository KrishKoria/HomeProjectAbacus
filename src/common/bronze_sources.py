from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Final


@dataclass(frozen=True, slots=True)
class BronzeSource:
    """Dataset metadata for the bronze/raw landing zone bootstrap flow.

    phi_columns lists the column names that contain Protected Health Information
    as defined by 45 CFR § 164.514(b)(2). This is a risk analysis artifact
    per § 164.308(a)(1)(ii)(A). Columns listed here must be encrypted at rest
    in production per § 164.312(a)(2)(iv).
    """

    local_filename: str
    volume_subdirectory: str
    expected_row_count: int
    phi_columns: frozenset[str] = field(default_factory=frozenset)

    @property
    def local_path(self) -> PurePosixPath:
        """Project-relative source path for the dataset file."""

        return PurePosixPath("datasets") / self.local_filename

    @property
    def has_phi(self) -> bool:
        """True if any column in this dataset contains PHI."""
        return bool(self.phi_columns)


BRONZE_SOURCES: Final[dict[str, BronzeSource]] = {
    "claims": BronzeSource(
        local_filename="claims_1000.csv",
        volume_subdirectory="claims",
        expected_row_count=1000,
        # PHI columns per 45 CFR § 164.514(b)(2):
        #   patient_id     — § 164.514(b)(2)(ii)  unique identifying code
        #   diagnosis_code — § 164.514(b)(2)(xvi) health condition linked to patient
        #   billed_amount  — § 164.514(b)(2)      financial health information
        #   date           — § 164.514(b)(2)(iv)  date of health service (except year)
        phi_columns=frozenset({"patient_id", "diagnosis_code", "billed_amount", "date"}),
    ),
    "providers": BronzeSource(
        local_filename="providers_1000.csv",
        volume_subdirectory="providers",
        expected_row_count=21,
        # No PHI — provider identity (doctor_name, location) is operational data,
        # not individually identifiable health information per § 164.501.
        phi_columns=frozenset(),
    ),
    "diagnosis": BronzeSource(
        local_filename="diagnosis.csv",
        volume_subdirectory="diagnosis",
        expected_row_count=6,
        # No PHI — standalone diagnosis code reference table (D10=Heart, High).
        # diagnosis_code is medical terminology without patient linkage; it becomes
        # PHI only when combined with patient_id in the claims table (§ 164.501).
        phi_columns=frozenset(),
    ),
    "cost": BronzeSource(
        local_filename="cost.csv",
        volume_subdirectory="cost",
        expected_row_count=6,
        # No PHI — procedure cost benchmarks are operational reference data.
        phi_columns=frozenset(),
    ),
}

DATASET_KEYS: Final[tuple[str, ...]] = tuple(BRONZE_SOURCES)

__all__ = ["BronzeSource", "BRONZE_SOURCES", "DATASET_KEYS"]
