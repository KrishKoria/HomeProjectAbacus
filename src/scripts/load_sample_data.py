from __future__ import annotations

import argparse
import sys
from pathlib import Path
_PROJECT_ROOT = Path(globals().get("__file__", sys._getframe().f_code.co_filename)).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.bronze_pipeline_config import add_common_databricks_args, bronze_volume_root
from src.common.bronze_sources import BRONZE_SOURCES, POLICY_SOURCE
from src.framework import HealthCheckResult


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy fixture datasets into the Bronze landing volume.")
    add_common_databricks_args(parser)
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--volume", default="raw_landing")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def _dbutils(spark):
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    dbutils = _dbutils(spark)
    volume_root = bronze_volume_root(catalog=args.catalog, schema=args.schema, volume=args.volume)

    try:
        dbutils.fs.ls(volume_root)
    except Exception:
        print(f"FAIL: load_sample_data - missing volume {volume_root}")
        return 1

    copied = 0
    overwrite_flag = str(args.overwrite).lower()
    for source in BRONZE_SOURCES.values():
        local_path = _PROJECT_ROOT / source.local_path
        target_dir = f"{volume_root}/{source.volume_subdirectory}"
        target_path = f"{target_dir}/{local_path.name}"
        dbutils.fs.mkdirs(target_dir)
        dbutils.fs.cp(f"file:{local_path}", target_path, args.overwrite)
        copied += 1

    policy_dir = _PROJECT_ROOT / "datasets" / POLICY_SOURCE.volume_subdirectory
    target_policy_dir = f"{volume_root}/{POLICY_SOURCE.volume_subdirectory}"
    dbutils.fs.mkdirs(target_policy_dir)
    for policy_file in sorted(policy_dir.glob("*.pdf")):
        dbutils.fs.cp(f"file:{policy_file}", f"{target_policy_dir}/{policy_file.name}", args.overwrite)
        copied += 1

    result = HealthCheckResult(
        service_name="load_sample_data",
        healthy=True,
        message=f"files={copied} overwrite={overwrite_flag}",
    )
    print(result.summary_line())
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
