from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Final


@dataclass(frozen=True, slots=True)
class BronzeSource:
    """Dataset metadata for the bronze/raw landing zone bootstrap flow."""

    local_filename: str
    volume_subdirectory: str
    expected_row_count: int

    @property
    def local_path(self) -> PurePosixPath:
        """Project-relative source path for the dataset file."""

        return PurePosixPath("datasets") / self.local_filename


BRONZE_SOURCES: Final[dict[str, BronzeSource]] = {
    "claims": BronzeSource(
        local_filename="claims_1000.csv",
        volume_subdirectory="claims",
        expected_row_count=1000,
    ),
    "providers": BronzeSource(
        local_filename="providers_1000.csv",
        volume_subdirectory="providers",
        expected_row_count=21,
    ),
    "diagnosis": BronzeSource(
        local_filename="diagnosis.csv",
        volume_subdirectory="diagnosis",
        expected_row_count=6,
    ),
    "cost": BronzeSource(
        local_filename="cost.csv",
        volume_subdirectory="cost",
        expected_row_count=6,
    ),
}

DATASET_KEYS: Final[tuple[str, ...]] = tuple(BRONZE_SOURCES)

__all__ = ["BronzeSource", "BRONZE_SOURCES", "DATASET_KEYS"]
