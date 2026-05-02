from __future__ import annotations

import argparse

from src.ml import FEATURE_COLUMNS
from src.ml.retrain_gate import decide_retrain
from src.scripts.train_denial_model import main as train_main


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run retrain gate and train only when needed.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument(
        "--gold-table", default="healthcare.gold.claim_features")
    parser.add_argument("--registered-model-name",
                        default="healthcare.ml.claim_denial_model")
    parser.add_argument("--champion-alias", default="champion")
    parser.add_argument("--optuna-trials", type=int, default=10)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the retrain-gate check and train unconditionally.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()

    if not args.force:
        decision = decide_retrain(
            spark,
            gold_table=args.gold_table,
            feature_columns=list(FEATURE_COLUMNS),
            registered_model_name=args.registered_model_name,
            champion_alias=args.champion_alias,
        )
        print(decision.summary_line())

        if decision.decision_status == "error":
            return 1
        if not decision.should_retrain:
            return 0
    else:
        print("FORCE: skipping retrain-gate check, training unconditionally.")

    train_args = [
        "--tune",
        "--optuna-trials",
        str(args.optuna_trials),
        "--random-seed",
        str(args.random_seed),
        "--catalog",
        args.catalog,
        "--gold-table",
        args.gold_table,
        "--registered-model-name",
        args.registered_model_name,
        "--champion-alias",
        args.champion_alias,
    ]
    return int(train_main(train_args))


if __name__ == "__main__":
    import traceback

    _rc = 1
    try:
        _rc = main()
    except Exception:
        traceback.print_exc()
    finally:
        from pyspark.sql import SparkSession

        try:
            SparkSession.builder.getOrCreate().stop()
        except Exception:
            pass
    if _rc != 0:
        raise RuntimeError(f"Training pipeline failed with exit code {_rc}")
