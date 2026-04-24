"""
HIPAA PHI Column Registry — Abacus Insights Bronze Layer
=========================================================
Machine-readable inventory of Protected Health Information (PHI) columns
across all Bronze Delta tables.

Compliance basis
----------------
45 CFR § 164.308(a)(1)(ii)(A) — Risk Analysis: requires an accurate and thorough
    assessment of potential risks to the confidentiality of ePHI, including
    knowing WHERE ePHI resides across all systems.

45 CFR § 164.312(a)(2)(iv) — Encryption and Decryption (addressable): columns
    listed in PHI_COLUMNS must be encrypted at rest in production using
    Databricks column masking (AES-256).

PHI definition per § 164.501
-----------------------------
PHI is "individually identifiable health information" — health information that
identifies, or could be used to identify, an individual. This requires linkage
to a specific person. Reference/lookup tables without patient linkage do NOT
contain PHI, even if they store medical codes (see diagnosis table note below).

"""

from __future__ import annotations

from typing import Final


# ---------------------------------------------------------------------------
# PHI column registry
# Maps fully-qualified Bronze table name → frozenset of PHI column names.
#
# PHI identifier categories per 45 CFR § 164.514(b)(2) Safe Harbor:
#   (ii)   unique identifying codes                   → patient_id
#   (iv)   dates directly related to an individual    → date (claim submission date)
#   (xvi)  health condition linked to a patient        → diagnosis_code (in claims only)
#   (xvii) financial health information               → billed_amount
# ---------------------------------------------------------------------------

PHI_COLUMNS: Final[dict[str, frozenset[str]]] = {
    # Claims table: contains patient-linked health information.
    # Every PHI column here is linked to a specific patient via patient_id.
    "healthcare.bronze.claims": frozenset({
        # § 164.514(b)(2)(ii) — unique identifying code for the patient
        "patient_id",
        # § 164.514(b)(2)(xvi) — medical condition linked to patient_id
        "diagnosis_code",
        # § 164.514(b)(2)(xvii) — financial health information
        "billed_amount",
        # § 164.514(b)(2)(iv) — claim submission date is a date directly
        "date",
        # related to an individual's health event (except year = PHI)
    }),

    # Diagnosis reference table: maps codes to categories (D10=Heart, High).
    # No patient linkage exists in this table → NOT PHI per § 164.501.
    # diagnosis_code becomes PHI only when joined to patient_id in the claims table.
    "healthcare.bronze.diagnosis": frozenset(),

    # Providers reference table: provider identity and business location.
    # HIPAA § 164.501 defines PHI as health information of PATIENTS, not providers.
    # Provider names and business addresses are operational data, not PHI.
    "healthcare.bronze.providers": frozenset(),

    # Cost benchmark reference table: procedure cost benchmarks by region.
    # Pure operational reference data — no individual or patient linkage.
    "healthcare.bronze.cost": frozenset(),
}


# ---------------------------------------------------------------------------
# PHI-adjacent columns — not PHI but require access control because they
# can be combined with PHI to reconstruct a patient record.
# ---------------------------------------------------------------------------

SENSITIVE_COLUMNS: Final[dict[str, frozenset[str]]] = {
    "healthcare.bronze.claims": frozenset({
        "claim_id",       # PHI-adjacent: unique claim identifier — can link to patient record
        "provider_id",    # PHI-adjacent: links claim to provider — operational but access-controlled
        "procedure_code",  # PHI-adjacent: treatment type linked to patient health event
    }),
    "healthcare.bronze.diagnosis": frozenset(),
    "healthcare.bronze.providers": frozenset(),
    "healthcare.bronze.cost": frozenset(),
}


def get_phi_columns(table_name: str) -> frozenset[str]:
    """Return the PHI columns for a fully-qualified Bronze table name.

    Returns an empty frozenset if the table has no PHI columns or is unknown.
    """
    return PHI_COLUMNS.get(table_name, frozenset())


def get_sensitive_columns(table_name: str) -> frozenset[str]:
    """Return PHI-adjacent columns for a fully-qualified Bronze table name."""
    return SENSITIVE_COLUMNS.get(table_name, frozenset())


def is_phi_column(table_name: str, column_name: str) -> bool:
    """Return True if the given column is PHI in the specified table."""
    return column_name in PHI_COLUMNS.get(table_name, frozenset())


__all__ = [
    "PHI_COLUMNS",
    "SENSITIVE_COLUMNS",
    "get_phi_columns",
    "get_sensitive_columns",
    "is_phi_column",
]
