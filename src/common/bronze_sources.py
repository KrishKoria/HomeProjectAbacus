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
    required_columns: tuple[str, ...]
    canonical_dataset: str
    source_profile: str = "current_fixture"
    phi_columns: frozenset[str] = field(default_factory=frozenset)

    @property
    def local_path(self) -> PurePosixPath:
        """Project-relative source path for the dataset file."""

        return PurePosixPath("datasets") / self.local_filename

    @property
    def has_phi(self) -> bool:
        """True if any column in this dataset contains PHI."""
        return bool(self.phi_columns)


@dataclass(frozen=True, slots=True)
class PolicySource:
    """Landing zone metadata for insurance policy PDF documents.

    Policy documents differ from the four CSV datasets: they are binary files
    (PDFs), have no fixed row count, and contain no PHI. The Bronze pipeline
    (bronze_policies.py) ingests them with cloudFiles binaryFile format.
    Text extraction and chunking happen in Silver via pdfplumber.

    No PHI — per architecture Assumption A-04 and HIPAA § 164.501, policy
    documents are insurance billing policy text, not patient health information.
    """

    volume_subdirectory: str

    @property
    def has_phi(self) -> bool:
        """Policy documents never contain PHI."""
        return False


BRONZE_SOURCES: Final[dict[str, BronzeSource]] = {
    "claims": BronzeSource(
        local_filename="claims_1000.csv",
        volume_subdirectory="claims",
        expected_row_count=1000,
        required_columns=(
            "claim_id",
            "patient_id",
            "provider_id",
            "diagnosis_code",
            "procedure_code",
            "billed_amount",
            "date",
            "claim_status",
            "denial_reason_code",
            "allowed_amount",
            "paid_amount",
            "is_denied",
            "follow_up_required",
        ),
        canonical_dataset="claims",
        # PHI columns per 45 CFR § 164.514(b)(2):
        #   patient_id     — § 164.514(b)(2)(ii)  unique identifying code
        #   diagnosis_code — § 164.514(b)(2)(xvi) health condition linked to patient
        #   billed_amount  — § 164.514(b)(2)      financial health information
        #   date           — § 164.514(b)(2)(iv)  date of health service (except year)
        phi_columns=frozenset(
            {
                "patient_id",
                "diagnosis_code",
                "billed_amount",
                "date",
                "claim_status",
                "denial_reason_code",
                "allowed_amount",
                "paid_amount",
                "is_denied",
                "follow_up_required",
            }
        ),
    ),
    "providers": BronzeSource(
        local_filename="providers_1000.csv",
        volume_subdirectory="providers",
        expected_row_count=21,
        required_columns=("provider_id", "doctor_name", "specialty", "location"),
        canonical_dataset="providers",
        # No PHI — provider identity (doctor_name, location) is operational data,
        # not individually identifiable health information per § 164.501.
        phi_columns=frozenset(),
    ),
    "diagnosis": BronzeSource(
        local_filename="diagnosis.csv",
        volume_subdirectory="diagnosis",
        expected_row_count=6,
        required_columns=("diagnosis_code", "category", "severity"),
        canonical_dataset="diagnosis",
        # No PHI — standalone diagnosis code reference table (D10=Heart, High).
        # diagnosis_code is medical terminology without patient linkage; it becomes
        # PHI only when combined with patient_id in the claims table (§ 164.501).
        phi_columns=frozenset(),
    ),
    "cost": BronzeSource(
        local_filename="cost.csv",
        volume_subdirectory="cost",
        expected_row_count=6,
        required_columns=("procedure_code", "average_cost", "expected_cost", "region"),
        canonical_dataset="cost",
        # No PHI — procedure cost benchmarks are operational reference data.
        phi_columns=frozenset(),
    ),
}

DATASET_KEYS: Final[tuple[str, ...]] = tuple(BRONZE_SOURCES)

# Policy PDF landing zone — separate from BRONZE_SOURCES because PDFs are binary files
# with no row count and no PHI, unlike the four CSV datasets above.
# Used by the bootstrap notebook to create the policies/ volume folder.
POLICY_SOURCE: Final[PolicySource] = PolicySource(volume_subdirectory="policies")

__all__ = ["BronzeSource", "BRONZE_SOURCES", "DATASET_KEYS", "PolicySource", "POLICY_SOURCE"]
