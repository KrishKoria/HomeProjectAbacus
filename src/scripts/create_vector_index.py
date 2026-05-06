#!/usr/bin/env python3
"""Create or update the Databricks Vector Search delta-sync index.

Usage::

    python src/scripts/create_vector_index.py                          # create/update
    python src/scripts/create_vector_index.py --mv-source-table <fqn> # custom MV source
    python src/scripts/create_vector_index.py --endpoint-name <name>   # custom endpoint
    python src/scripts/create_vector_index.py --index-name <name>      # custom index
    python src/scripts/create_vector_index.py --dry-run                # validate only
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys

DEFAULT_SOURCE_TABLE = "healthcare.gold.policy_chunks_vs"
DEFAULT_MV_SOURCE_TABLE = "healthcare.gold.policy_chunks"
DEFAULT_ENDPOINT_NAME = "healthcare_policy_vector_endpoint"
DEFAULT_INDEX_NAME = "healthcare.gold.policy_chunks_index"
DEFAULT_QUERY_MODEL_ENDPOINT = "databricks-gte-large-en"
DEFAULT_EMBEDDING_COLUMN = "embedding_vector"
DEFAULT_PRIMARY_KEY = "chunk_id"
EMBEDDING_DIM = 1024
PIPELINE_TYPE = "TRIGGERED"
UC_FQN_PATTERN = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
RESOURCE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")

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
        "--mv-source-table",
        default=DEFAULT_MV_SOURCE_TABLE,
        help="Fully-qualified MV source table (default: %(default)s)",
    )
    parser.add_argument(
        "--endpoint-name",
        default=DEFAULT_ENDPOINT_NAME,
        help="Vector Search endpoint name (default: %(default)s)",
    )
    parser.add_argument(
        "--index-name",
        default=DEFAULT_INDEX_NAME,
        help="Vector Search index name (default: %(default)s)",
    )
    parser.add_argument(
        "--query-model-endpoint",
        default=DEFAULT_QUERY_MODEL_ENDPOINT,
        help="Serving endpoint used for query_text embedding (default: %(default)s)",
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


def _configure_index(args: argparse.Namespace) -> dict:
    """Build the Vector Search index configuration payload."""
    return {
        "endpoint_name": args.endpoint_name,
        "index_name": args.index_name,
        "source_table": args.source_table,
        "mv_source_table": args.mv_source_table,
        "primary_key": args.primary_key,
        "embedding_column": args.embedding_column,
        "query_model_endpoint": args.query_model_endpoint,
        "embedding_dimension": EMBEDDING_DIM,
        "pipeline_type": PIPELINE_TYPE,
    }


def _validate_uc_fqn(name: str, label: str) -> None:
    if not UC_FQN_PATTERN.fullmatch(name):
        raise ValueError(
            f"{label} must be a three-part Unity Catalog name (catalog.schema.table): {name!r}"
        )


def _validate_resource_name(name: str, label: str) -> None:
    if not RESOURCE_NAME_PATTERN.fullmatch(name):
        raise ValueError(f"{label} contains unsupported characters: {name!r}")


def _quote_sql_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _quote_uc_fqn(table_name: str) -> str:
    return ".".join(_quote_sql_identifier(part) for part in table_name.split("."))


def _validate_config(config: dict) -> None:
    _validate_uc_fqn(config["source_table"], "source_table")
    _validate_uc_fqn(config["mv_source_table"], "mv_source_table")
    _validate_uc_fqn(config["index_name"], "index_name")
    _validate_resource_name(config["endpoint_name"], "endpoint_name")
    _validate_resource_name(config["query_model_endpoint"], "query_model_endpoint")
    _validate_resource_name(config["primary_key"], "primary_key")
    _validate_resource_name(config["embedding_column"], "embedding_column")


def _endpoint_exists(client, endpoint_name: str) -> bool:
    if hasattr(client, "endpoint_exists"):
        try:
            return bool(client.endpoint_exists(endpoint_name=endpoint_name))
        except TypeError:
            return bool(client.endpoint_exists(endpoint_name))
    if hasattr(client, "get_endpoint"):
        try:
            endpoint = client.get_endpoint(endpoint_name=endpoint_name)
        except TypeError:
            endpoint = client.get_endpoint(endpoint_name)
        except Exception as exc:
            if _is_not_found_exception(exc):
                return False
            raise
        return endpoint is not None
    response = client.list_endpoints()
    endpoints = response.get("endpoints", []) if isinstance(response, dict) else response
    for endpoint in endpoints:
        name = endpoint.get("name") if isinstance(endpoint, dict) else getattr(endpoint, "name", None)
        if name == endpoint_name:
            return True
    return False


def _ensure_endpoint(client, endpoint_name: str) -> str:
    if _endpoint_exists(client, endpoint_name):
        return endpoint_name
    logger.info("Creating Vector Search endpoint: %s", endpoint_name)
    client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
    return endpoint_name


def _ensure_cdf_delta_source(mv_table: str, delta_table: str, primary_key: str) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    source_columns = spark.table(mv_table).columns
    if primary_key not in source_columns:
        raise ValueError(f"Primary key column {primary_key!r} not found in {mv_table}")

    # Create an empty target once with CDF enabled; future runs only MERGE changes.
    quoted_mv_table = _quote_uc_fqn(mv_table)
    quoted_delta_table = _quote_uc_fqn(delta_table)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {quoted_delta_table} "
        f"AS SELECT * FROM {quoted_mv_table} WHERE 1 = 0"
    )
    spark.sql(
        f"ALTER TABLE {quoted_delta_table} "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)"
    )

    non_pk_columns = [column for column in source_columns if column != primary_key]
    update_assignments = ", ".join(
        f"target.`{column}` = source.`{column}`" for column in non_pk_columns
    )
    changed_predicate = " OR ".join(
        f"NOT(target.`{column}` <=> source.`{column}`)" for column in non_pk_columns
    )
    insert_columns = ", ".join(f"`{column}`" for column in source_columns)
    insert_values = ", ".join(f"source.`{column}`" for column in source_columns)

    merge_sql = [
        f"MERGE INTO {quoted_delta_table} AS target",
        f"USING {quoted_mv_table} AS source",
        f"ON target.`{primary_key}` = source.`{primary_key}`",
    ]
    if changed_predicate:
        merge_sql.append(f"WHEN MATCHED AND ({changed_predicate}) THEN UPDATE SET {update_assignments}")
    merge_sql.append(f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})")
    merge_sql.append("WHEN NOT MATCHED BY SOURCE THEN DELETE")

    logger.info("Refreshing vector source table %s from %s via MERGE", delta_table, mv_table)
    try:
        spark.sql("\n".join(merge_sql))
    except Exception as exc:
        # Older runtimes may not support MERGE delete branch syntax.
        if "NOT MATCHED BY SOURCE" not in str(exc):
            raise
        logger.warning(
            "MERGE delete branch unsupported; applying fallback delete pass for %s",
            delta_table,
            exc_info=True,
        )
        spark.sql(
            f"DELETE FROM {quoted_delta_table} AS target "
            f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_mv_table} AS source "
            f"WHERE source.`{primary_key}` = target.`{primary_key}`)"
        )
        spark.sql("\n".join(merge_sql[:-1]))


def _is_not_found_exception(exc: Exception) -> bool:
    error_text = str(exc).lower()
    error_class = str(getattr(exc, "errorClass", "")).upper()
    return (
        "not found" in error_text
        or "does not exist" in error_text
        or "resource_does_not_exist" in error_text
        or error_class in {"RESOURCE_DOES_NOT_EXIST", "TABLE_OR_VIEW_NOT_FOUND"}
    )


def _existing_index(client, index_name: str):
    if hasattr(client, "index_exists"):
        if not client.index_exists(index_name=index_name):
            return None
        return client.get_index(index_name=index_name)
    try:
        return client.get_index(index_name=index_name)
    except Exception as exc:
        if not _is_not_found_exception(exc):
            raise
        return None


def _create_index(client, config: dict) -> None:
    client.create_delta_sync_index(
        endpoint_name=config["endpoint_name"],
        source_table_name=config["source_table"],
        index_name=config["index_name"],
        primary_key=config["primary_key"],
        embedding_dimension=config["embedding_dimension"],
        embedding_vector_column=config["embedding_column"],
        model_endpoint_name_for_query=config["query_model_endpoint"],
        pipeline_type=config["pipeline_type"],
    )


def _sync_existing_index(index, index_name: str) -> None:
    logger.info("Index already exists; syncing: %s", index_name)
    index.sync()


def _dry_run_output(config: dict) -> str:
    payload = {
        "action": "create_or_sync",
        "endpoint_name": config["endpoint_name"],
        "index_name": config["index_name"],
        "source_table": config["source_table"],
        "mv_source_table": config["mv_source_table"],
        "primary_key": config["primary_key"],
        "embedding_column": config["embedding_column"],
        "query_model_endpoint": config["query_model_endpoint"],
        "embedding_dimension": config["embedding_dimension"],
        "pipeline_type": config["pipeline_type"],
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def create_vector_index(args: argparse.Namespace) -> None:
    """Create or update the Vector Search index via the Databricks REST API."""
    config = _configure_index(args)
    _validate_config(config)

    if args.dry_run:
        print("Dry run — would create or sync index with this config:")
        print(_dry_run_output(config))
        return

    try:
        from databricks.vector_search.client import VectorSearchClient
    except ImportError:
        logger.error(
            "databricks-vectorsearch is not installed. "
            "Install with: pip install databricks-vectorsearch"
        )
        sys.exit(1)

    client = VectorSearchClient(disable_notice=True)

    _ensure_cdf_delta_source(
        mv_table=config["mv_source_table"],
        delta_table=config["source_table"],
        primary_key=config["primary_key"],
    )

    config["endpoint_name"] = _ensure_endpoint(client, config["endpoint_name"])

    existing_index = _existing_index(
        client=client,
        index_name=config["index_name"],
    )
    if existing_index is not None:
        try:
            _sync_existing_index(existing_index, config["index_name"])
            logger.info("Vector Search index sync requested: %s", config["index_name"])
            return
        except Exception as exc:
            logger.error("Failed to sync existing Vector Search index: %s", exc)
            sys.exit(1)

    logger.info(
        "Creating Vector Search delta-sync index: %s (endpoint=%s)",
        config["index_name"],
        config["endpoint_name"],
    )
    try:
        _create_index(client, config)
        created_index = client.get_index(index_name=config["index_name"])
        created_index.sync()
        logger.info(
            "Vector Search index %s created and sync requested.",
            config["index_name"],
        )
    except Exception as exc:
        logger.error("Failed to create Vector Search index: %s", exc)
        sys.exit(1)


def _legacy_payload(args: argparse.Namespace) -> dict:
    """Compatibility helper retained for older tests and external callers."""
    return {
        "name": args.index_name,
        "endpoint_name": args.endpoint_name,
        "index_name": args.index_name,
        "primary_key": args.primary_key,
        "index_type": "DELTA_SYNC",
        "source_table": args.source_table,
        "mv_source_table": args.mv_source_table,
        "query_model_endpoint": args.query_model_endpoint,
        "delta_sync_index_spec": {
            "source_table": args.source_table,
            "pipeline_type": PIPELINE_TYPE,
            "embedding_source_columns": [
                {
                    "name": args.embedding_column,
                    "embedding_dimension": EMBEDDING_DIM,
                }
            ],
        },
    }


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
