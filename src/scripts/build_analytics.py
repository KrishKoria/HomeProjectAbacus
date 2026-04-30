from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Final


_SCRIPT_PATH: Final[Path] = Path(
    globals().get("__file__", sys._getframe().f_code.co_filename)
).resolve()
PROJECT_ROOT: Final[Path] = _SCRIPT_PATH.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.claims_analytics import build_and_persist_claims_assets
from src.framework import HealthCheckResult

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analytics tables from trusted Silver inputs.")
    parser.add_argument("--catalog", default="healthcare")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--analytics-schema", default="analytics")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    try:
        persisted = build_and_persist_claims_assets(
            spark,
            catalog=args.catalog,
            bronze_schema=args.bronze_schema,
            analytics_schema=args.analytics_schema,
        )
    except Exception:
        logger.warning("Analytics build failed", exc_info=True)
        print("FAIL: analytics - analytics build failed")
        return 1

    result = HealthCheckResult(
        service_name="analytics",
        healthy=True,
        message=f"tables={len(persisted)}",
        details={key: value for key, value in persisted.items()},
    )
    print(result.summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
