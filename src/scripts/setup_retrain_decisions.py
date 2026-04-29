from __future__ import annotations

import argparse

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
    raise SystemExit(main())
