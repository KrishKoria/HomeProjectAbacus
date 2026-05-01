from __future__ import annotations

import argparse

from src.analytics.quality_assets import WEEK3_DATASETS, write_quality_assets
from src.common.silver_pipeline_config import quarantine_table_name, silver_table_name
from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Silver and quarantine outputs.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--quarantine-schema", default="quarantine")
    parser.add_argument("--analytics-schema", default="analytics")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    trusted_counts: dict[str, int] = {}
    for dataset in WEEK3_DATASETS:
        trusted = spark.table(silver_table_name(args.catalog, dataset, args.silver_schema))
        quarantined = spark.table(quarantine_table_name(args.catalog, dataset, args.quarantine_schema))
        trusted_count = int(trusted.count())
        quarantine_count = int(quarantined.count())
        total_count = trusted_count + quarantine_count

        if trusted_count <= 0 and dataset == "claims":
            print(HealthCheckResult("silver", False, "trusted_claims_empty").summary_line())
            return 1

        if dataset == "claims" and total_count > 0:
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
