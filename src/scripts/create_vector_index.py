#!/usr/bin/env python3
"""Create or update the Databricks Vector Search delta-sync index.

Usage::

    python src/scripts/create_vector_index.py                          # create/update
    python src/scripts/create_vector_index.py --endpoint-name <name>   # custom endpoint
    python src/scripts/create_vector_index.py --dry-run                # validate only
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

DEFAULT_SOURCE_TABLE = "healthcare.gold.policy_chunks"
DEFAULT_ENDPOINT_NAME = "healthcare.gold.policy_chunks_index"
DEFAULT_EMBEDDING_COLUMN = "embedding_vector"
DEFAULT_PRIMARY_KEY = "chunk_id"
EMBEDDING_DIM = 768

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or update a Databricks Vector Search delta-sync index."
    )
    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help="Fully-qualified Delta table name (default: %(default)s)",
    )
    parser.add_argument(
        "--endpoint-name",
        default=DEFAULT_ENDPOINT_NAME,
        help="Vector Search endpoint name (default: %(default)s)",
    )
    parser.add_argument(
        "--embedding-column",
        default=DEFAULT_EMBEDDING_COLUMN,
        help="Column containing the embedding vector (default: %(default)s)",
    )
    parser.add_argument(
        "--primary-key",
        default=DEFAULT_PRIMARY_KEY,
        help="Primary key column for delta sync (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without creating the index",
    )
    return parser


def _validate_table(spark, table_name: str) -> bool:
    try:
        spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
        return True
    except Exception:
        logger.warning("Cannot read table %s; does it exist?", table_name)
        return False


def _configure_index(args: argparse.Namespace) -> dict:
    """Build the Vector Search index configuration payload."""
    return {
        "name": args.endpoint_name,
        "endpoint_name": args.endpoint_name,
        "primary_key": args.primary_key,
        "index_type": "DELTA_SYNC",
        "source_table": args.source_table,
        "delta_sync_index_spec": {
            "source_table": args.source_table,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {
                    "name": args.embedding_column,
                    "embedding_dimension": EMBEDDING_DIM,
                }
            ],
        },
    }


def create_vector_index(args: argparse.Namespace) -> None:
    """Create or update the Vector Search index via the Databricks REST API."""
    config = _configure_index(args)

    if args.dry_run:
        print("Dry run — would create/update index with this config:")
        print(json.dumps(config, indent=2))
        return

    try:
        from databricks.vector_search.client import VectorSearchClient
    except ImportError:
        logger.error(
            "databricks-vector-sdk is not installed. "
            "Install with: pip install databricks-vector-sdk"
        )
        sys.exit(1)

    client = VectorSearchClient(disable_notice=True)

    existing = client.get_index(args.endpoint_name)
    if existing:
        logger.info("Updating existing Vector Search index: %s", args.endpoint_name)
    else:
        logger.info("Creating new Vector Search index: %s", args.endpoint_name)

    try:
        client.create_delta_sync_index(
            endpoint_name=args.endpoint_name,
            source_table_name=args.source_table,
            index_name=args.endpoint_name,
            primary_key=args.primary_key,
            embedding_source_column=args.embedding_column,
            embedding_dimension=EMBEDDING_DIM,
            pipeline_type="TRIGGERED",
        )
        logger.info(
            "Vector Search index %s is being provisioned. "
            "Delta sync will populate the index automatically.",
            args.endpoint_name,
        )
    except Exception as exc:
        logger.error("Failed to create/update Vector Search index: %s", exc)
        sys.exit(1)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    create_vector_index(args)


if __name__ == "__main__":
    main()
