from __future__ import annotations

from src.ml import FEATURE_COLUMNS
from src.xai.feature_reasons import FEATURE_REASONS

_missing = set(FEATURE_COLUMNS) - set(FEATURE_REASONS.keys())
_extra = set(FEATURE_REASONS.keys()) - set(FEATURE_COLUMNS)
if _missing or _extra:
    raise AssertionError(
        f"FEATURE_REASONS out of sync with FEATURE_COLUMNS. "
        f"Missing reasons for: {_missing}. Extra reasons: {_extra}."
    )

from src.xai.explainer import explain  # noqa: E402

__all__ = ["FEATURE_REASONS", "explain"]
