from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Final

import mlflow
import pandas as pd
from mlflow.models import ModelSignature
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)
from mlflow.types.schema import ColSpec, DataType, Schema

_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
_PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.ml import FEATURE_COLUMNS  # noqa: E402
from src.ml.train import _resolve_experiment_name  # noqa: E402
from src.serving.claim_analysis import analyze_claim  # noqa: E402

logger = logging.getLogger(__name__)

_DEFAULT_DEPENDENCY_MODEL_NAME: Final = "healthcare.ml.claim_denial_model"
_DEFAULT_REGISTERED_MODEL_NAME: Final = "healthcare.ml.claim_denial_analysis"
_DEFAULT_MODEL_ALIAS: Final = "champion"


class ClaimAnalysisWrapper(mlflow.pyfunc.PythonModel):
    """Pyfunc wrapper for the claim-analysis serving endpoint.

    Accepts ``dataframe_split`` input format:
    ``{"dataframe_split": {"columns": [...], "data": [[...]]}}``

    Returns ``{"predictions": [{...}]}``.
    """

    def __init__(self) -> None:
        super().__init__()
        self._model: Any = None

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        if context.artifacts and "model" in context.artifacts:
            self._model = mlflow.sklearn.load_model(context.artifacts["model"])

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: pd.DataFrame,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for _, row in model_input.iterrows():
            features = {col: row.get(col) for col in FEATURE_COLUMNS}
            claim_id = str(row.get("claim_id", "unknown"))
            result = analyze_claim(claim_id, features, model=self._model)
            results.append(result)
        return results


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _build_signature() -> ModelSignature:
    input_schema = Schema(
        [ColSpec(DataType.double, name=col) for col in FEATURE_COLUMNS]
        + [ColSpec(DataType.string, name="claim_id")]
    )
    output_schema = Schema(
        [
            ColSpec(DataType.string, name="claimId"),
            ColSpec(DataType.double, name="riskScore"),
            ColSpec(DataType.string, name="riskLevel"),
            ColSpec(DataType.long, name="predictionLabel"),
            ColSpec(DataType.string, name="topReasons"),
            ColSpec(DataType.string, name="policyGuidance"),
            ColSpec(DataType.string, name="narrative"),
            ColSpec(DataType.string, name="policyCitations"),
            ColSpec(DataType.string, name="model"),
            ColSpec(DataType.string, name="generatedAt"),
        ]
    )
    return ModelSignature(inputs=input_schema, outputs=output_schema)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register claim analysis pyfunc wrapper model"
    )
    parser.add_argument(
        "--model-name",
        default=_env("CLAIMOPS_REGISTERED_MODEL_NAME", _DEFAULT_REGISTERED_MODEL_NAME),
        help="Registered model name for the analysis wrapper",
    )
    parser.add_argument(
        "--model-alias",
        default=_env("CLAIMOPS_MODEL_ALIAS", _DEFAULT_MODEL_ALIAS),
        help="Alias to assign to the new model version",
    )
    parser.add_argument(
        "--dependency-model-name",
        default=_env("CLAIMOPS_MODEL_NAME", _DEFAULT_DEPENDENCY_MODEL_NAME),
        help="Registered denial model the wrapper depends on",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    registered_model_name: str = args.model_name
    model_alias: str = args.model_alias
    dependency_model_name: str = args.dependency_model_name

    experiment_name = registered_model_name.rsplit(".", 1)[-1]

    mlflow.set_experiment(_resolve_experiment_name(experiment_name))
    with mlflow.start_run(run_name=experiment_name) as run:
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=ClaimAnalysisWrapper(),
            artifacts={
                "model": f"models:/{dependency_model_name}@{model_alias}",
            },
            signature=_build_signature(),
            pip_requirements=[
                "mlflow",
                "pandas",
                "numpy",
                "scikit-learn",
                "xgboost>=2.0,<3.0",
                "lightgbm>=4.2,<5.0",
                "catboost>=1.2,<2.0",
                "shap",
                "databricks-sdk>=0.28,<1.0",
                "databricks-vectorsearch>=0.67,<1.0",
            ],
            code_paths=[str(_PROJECT_ROOT / "src")],
            resources=[
                DatabricksVectorSearchIndex(index_name="healthcare.gold.policy_chunks_index"),
                DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
                DatabricksServingEndpoint(endpoint_name="databricks-gte-large-en"),
            ],
        )

        model_uri = f"runs:/{run.info.run_id}/model"
        result = mlflow.register_model(
            model_uri=model_uri,
            name=registered_model_name,
        )

        client = mlflow.tracking.MlflowClient()
        client.set_registered_model_alias(
            name=registered_model_name,
            alias=model_alias,
            version=result.version,
        )

        logger.info(
            "Registered model %s version %s with alias %s",
            registered_model_name,
            result.version,
            model_alias,
        )


if __name__ == "__main__":
    main()
