from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Final

_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
_PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.analytics.quality_assets import WEEK3_DATASETS, write_quality_assets
from src.common.silver_pipeline_config import quarantine_table_name, silver_table_name
from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Silver and quarantine outputs.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--quarantine-schema", default="quarantine")
    parser.add_argument("--analytics-schema", default="analytics")
    parser.add_argument(
        "--emit-quality-assets",
        action="store_true",
        help="Persist quality assets (off by default for ETL hot path).",
    )
    parser.add_argument("--upstream-status", default="success")
    return parser.parse_args(argv)


def _normalize_state(value: str | None) -> str:
    return (value or "").strip().lower()


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    if args.emit_quality_assets and _normalize_state(args.upstream_status) != "success":
        print(
            HealthCheckResult(
                "silver",
                True,
                (
                    "quality_assets_skipped_due_upstream_status "
                    f"upstream_status={_normalize_state(args.upstream_status) or 'unknown'}"
                ),
            ).summary_line()
        )
        return 0

    spark = SparkSession.builder.getOrCreate()
    trusted_counts: dict[str, int] = {}
    for dataset in WEEK3_DATASETS:
        trusted = spark.table(silver_table_name(args.catalog, dataset, args.silver_schema))
        quarantined = spark.table(quarantine_table_name(args.catalog, dataset, args.quarantine_schema))
        if dataset == "claims":
            trusted_count = int(trusted.count())
            quarantine_count = int(quarantined.count())
            total_count = trusted_count + quarantine_count
            if trusted_count <= 0:
                print(HealthCheckResult("silver", False, "trusted_claims_empty").summary_line())
                return 1
            quarantine_ratio = quarantine_count / total_count
            if quarantine_ratio > 0.50:
                print(
                    HealthCheckResult(
                        "silver",
                        False,
                        f"claims_quarantine_ratio_exceeded quarantine={quarantine_count} "
                        f"trusted={trusted_count} ratio={quarantine_ratio:.4f}",
                    ).summary_line()
                )
                return 1
            trusted_counts[dataset] = total_count
            continue

        trusted_has_rows = trusted.limit(1).count() > 0
        if not trusted_has_rows:
            print(HealthCheckResult("silver", False, f"trusted_empty={dataset}").summary_line())
            return 1
        trusted_counts[dataset] = 1

    if args.emit_quality_assets:
        write_quality_assets(
            spark,
            catalog=args.catalog,
            silver_schema=args.silver_schema,
            quarantine_schema=args.quarantine_schema,
            analytics_schema=args.analytics_schema,
        )
    print(HealthCheckResult("silver", True, f"datasets={len(trusted_counts)}").summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
