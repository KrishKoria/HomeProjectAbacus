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

from src.common.bronze_pipeline_config import AUDIT_COLUMNS, bronze_table_name
from src.common.bronze_sources import BRONZE_SOURCES, POLICY_SOURCE
from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Bronze pipeline outputs.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--schema", default="bronze")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    tables = {**BRONZE_SOURCES, POLICY_SOURCE.volume_subdirectory: POLICY_SOURCE}
    row_counts: dict[str, int] = {}
    for dataset, source in tables.items():
        table_fqn = bronze_table_name(dataset, catalog=args.catalog, schema=args.schema)
        dataframe = spark.table(table_fqn)
        row_count = int(dataframe.count())
        if row_count <= 0:
            print(HealthCheckResult("bronze", False, f"empty_table={dataset}").summary_line())
            return 1
        row_counts[dataset] = row_count
        if dataset in BRONZE_SOURCES:
            required_columns = set(BRONZE_SOURCES[dataset].required_columns)
            if not required_columns.issubset(set(dataframe.columns)):
                print(HealthCheckResult("bronze", False, f"missing_columns={dataset}").summary_line())
                return 1
        if not set(AUDIT_COLUMNS).issubset(set(dataframe.columns)):
            print(HealthCheckResult("bronze", False, f"missing_audit_columns={dataset}").summary_line())
            return 1

    print(HealthCheckResult("bronze", True, f"tables={len(row_counts)} rows={row_counts}").summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
