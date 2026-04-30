from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Final


_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the retrain decision audit table.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--ml-schema", default="ml")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {args.catalog}.{args.ml_schema}.retrain_decisions (
            decided_at TIMESTAMP,
            should_retrain STRING,
            reason STRING,
            current_row_count BIGINT,
            current_gold_version BIGINT,
            current_fingerprint STRING,
            champion_run_id STRING
        )
        USING DELTA
        """
    )
    print(HealthCheckResult("setup_infrastructure", True, "retrain_decisions table ready").summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
