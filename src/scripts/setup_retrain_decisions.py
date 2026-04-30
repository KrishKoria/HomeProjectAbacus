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

_MIGRATION_COLUMNS: Final[dict[str, str]] = {
    "decision_status": "STRING",
    "error_detail": "STRING",
    "previous_training_row_count": "BIGINT",
    "row_count_delta": "BIGINT",
    "row_count_delta_pct": "DOUBLE",
    "current_gold_object_type": "STRING",
    "current_gold_last_altered": "STRING",
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or migrate the retrain decision audit table.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--ml-schema", default="ml")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    table_fqn = f"{args.catalog}.{args.ml_schema}.retrain_decisions"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            decided_at TIMESTAMP,
            decision_status STRING,
            should_retrain STRING,
            reason STRING,
            error_detail STRING,
            current_row_count BIGINT,
            current_gold_version BIGINT,
            current_gold_object_type STRING,
            current_gold_last_altered STRING,
            current_fingerprint STRING,
            champion_run_id STRING,
            previous_training_row_count BIGINT,
            row_count_delta BIGINT,
            row_count_delta_pct DOUBLE
        )
        USING DELTA
        """
    )

    existing = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()}
    missing = [
        (col_name, col_type)
        for col_name, col_type in _MIGRATION_COLUMNS.items()
        if col_name not in existing
    ]

    if missing:
        add_clauses = ", ".join(
            f"{col_name} {col_type}" for col_name, col_type in missing
        )
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({add_clauses})")
        migrated_names = ", ".join(col_name for col_name, _col_type in missing)
        result = f"retrain_decisions table ready (migrated: {migrated_names})"
    else:
        result = "retrain_decisions table ready"

    print(HealthCheckResult("setup_infrastructure", True, result).summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
