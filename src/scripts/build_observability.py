from __future__ import annotations

import argparse
import logging

from src.analytics.observability_assets import write_observability_tables
from src.framework import HealthCheckResult


logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build observability tables from pipeline event logs.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--analytics-schema", default="analytics")
    parser.add_argument("--pipeline-id", default=None)
    parser.add_argument("--published-event-log-table", default=None)
    parser.add_argument("--pipeline-stage", choices=("bronze", "silver", "gold"), required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    try:
        persisted = write_observability_tables(
            spark,
            pipeline_id=args.pipeline_id,
            published_event_log_table=args.published_event_log_table,
            catalog=args.catalog,
            analytics_schema=args.analytics_schema,
            pipeline_stage=args.pipeline_stage,
        )
    except Exception:
        logger.warning("Observability build failed", exc_info=True)
        print(f"FAIL: observability - stage={args.pipeline_stage}")
        return 1

    result = HealthCheckResult(
        service_name="observability",
        healthy=True,
        message=f"stage={args.pipeline_stage} tables={len(persisted)}",
    )
    print(result.summary_line())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
