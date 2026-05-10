from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Final

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import DataframeSplitInput

_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
_PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.ml import FEATURE_COLUMNS  # noqa: E402

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _build_synthetic_features() -> dict[str, Any]:
    return {col: 0.0 for col in FEATURE_COLUMNS}


def _verify_response(response: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    predictions = response.get("predictions", [])
    if not predictions:
        errors.append("Missing predictions array in response")
        return errors

    result = predictions[0]
    if "riskScore" not in result:
        errors.append("Missing riskScore in prediction")
    if "riskLevel" not in result:
        errors.append("Missing riskLevel in prediction")
    if "topReasons" not in result or not result["topReasons"]:
        errors.append("Missing or empty topReasons (SHAP)")
    if "policyGuidance" not in result:
        errors.append("Missing policyGuidance (RAG)")
    return errors


def main() -> None:
    endpoint_name = _env("CLAIMOPS_ANALYSIS_ENDPOINT", "claim-denial-analysis")
    features = _build_synthetic_features()

    payload = DataframeSplitInput(
        columns=list(FEATURE_COLUMNS),
        data=[[features[col] for col in FEATURE_COLUMNS]],
    )

    w = WorkspaceClient()
    logger.info("Calling endpoint %s with synthetic data...", endpoint_name)
    response = w.serving_endpoints.query(
        name=endpoint_name,
        dataframe_split=payload,
    )

    response_data: dict[str, Any] = response.as_dict()  # type: ignore[union-attr]
    errors = _verify_response(response_data)
    if errors:
        for err in errors:
            logger.error("Verification failed: %s", err)
        sys.exit(1)

    logger.info("Endpoint verification passed for %s", endpoint_name)


if __name__ == "__main__":
    main()
