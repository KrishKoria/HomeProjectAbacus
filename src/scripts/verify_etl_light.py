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

from src.common.bronze_pipeline_config import bronze_table_name
from src.common.bronze_sources import BRONZE_SOURCES
from src.common.gold_pipeline_config import gold_table_name
from src.common.silver_pipeline_config import quarantine_table_name, silver_table_name
from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lightweight ETL health checks.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--quarantine-schema", default="quarantine")
    parser.add_argument("--pipeline-result", default="SUCCESS")
    return parser.parse_args(argv)


def _has_rows(dataframe) -> bool:
    return dataframe.limit(1).count() > 0


def _normalize_state(value: str | None) -> str:
    return (value or "").strip().lower()


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    pipeline_result = _normalize_state(args.pipeline_result)
    if pipeline_result != "success":
        print(
            HealthCheckResult(
                "etl",
                True,
                f"verify_skipped_due_pipeline_state pipeline_result={pipeline_result or 'unknown'}",
            ).summary_line()
        )
        return 0

    spark = SparkSession.builder.getOrCreate()

    for dataset in BRONZE_SOURCES:
        bronze_fqn = bronze_table_name(dataset, catalog=args.catalog, schema=args.bronze_schema)
        if not _has_rows(spark.table(bronze_fqn)):
            print(HealthCheckResult("etl", False, f"empty_bronze_table={dataset}").summary_line())
            return 1

    for dataset in ("claims", "providers", "diagnosis", "cost", "policy_chunks"):
        silver_fqn = silver_table_name(args.catalog, dataset, args.silver_schema)
        if not _has_rows(spark.table(silver_fqn)):
            print(HealthCheckResult("etl", False, f"empty_silver_table={dataset}").summary_line())
            return 1

    trusted_claims = spark.table(silver_table_name(args.catalog, "claims", args.silver_schema))
    quarantined_claims = spark.table(quarantine_table_name(args.catalog, "claims", args.quarantine_schema))
    trusted_count = int(trusted_claims.count())
    quarantine_count = int(quarantined_claims.count())
    total = trusted_count + quarantine_count

    if trusted_count <= 0:
        print(HealthCheckResult("etl", False, "trusted_claims_empty").summary_line())
        return 1

    if total > 0:
        quarantine_ratio = quarantine_count / total
        if quarantine_ratio > 0.50:
            print(
                HealthCheckResult(
                    "etl",
                    False,
                    f"claims_quarantine_ratio_exceeded quarantine={quarantine_count} "
                    f"trusted={trusted_count} ratio={quarantine_ratio:.4f}",
                ).summary_line()
            )
            return 1

    gold_fqn = gold_table_name(args.catalog, "claim_features", args.gold_schema)
    if not _has_rows(spark.table(gold_fqn)):
        print(HealthCheckResult("etl", False, "gold_claim_features_empty").summary_line())
        return 1

    print(HealthCheckResult("etl", True, "light_checks_passed").summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
