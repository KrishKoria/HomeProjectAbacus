from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.local_pipeline.bronze_ingest import run_local_bronze_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest local CSV datasets into Bronze parquet artifacts.")
    parser.add_argument("--source-root", type=Path, default=None, help="Override the source datasets directory.")
    parser.add_argument("--data-root", type=Path, default=None, help="Override the generated data directory.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_local_bronze_ingest(
        source_root=args.source_root if args.source_root is not None else PROJECT_ROOT / "datasets",
        data_root=args.data_root if args.data_root is not None else PROJECT_ROOT / "data",
    )
    print(json.dumps(summary, default=str, indent=2))


if __name__ == "__main__":
    main()
