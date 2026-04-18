# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap the governed bronze landing zone
# MAGIC
# MAGIC This notebook is safe to re-run. It creates a Unity Catalog catalog, schema,
# MAGIC and managed volume, applies optional grants for comma-separated principal
# MAGIC lists, creates the dataset folders declared in the bronze manifest, and
# MAGIC finishes with inspection cells you can re-run after uploading the CSV files.

# COMMAND ----------
from __future__ import annotations

from pathlib import Path
from typing import Iterable
import sys


# COMMAND ----------
def _repo_root() -> Path:
    """Locate the synced repo root so the notebook can import shared code."""

    search_roots: list[Path] = []

    if "__file__" in globals():
        search_roots.append(Path(__file__).resolve().parent)

    search_roots.append(Path.cwd().resolve())

    seen: set[Path] = set()
    for root in search_roots:
        for candidate in (root, *root.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / "src" / "common" / "bronze_sources.py").exists():
                return candidate

    raise ModuleNotFoundError(
        "Could not locate repo root containing src/common/bronze_sources.py. "
        "Run this notebook from the synced project repo."
    )


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.common.bronze_sources import BRONZE_SOURCES


# COMMAND ----------
def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _quote_qualified_identifier(*parts: str) -> str:
    return ".".join(_quote_identifier(part) for part in parts)


def _split_principals(raw_value: str) -> list[str]:
    return [principal.strip() for principal in raw_value.split(",") if principal.strip()]


def _path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def _show_records(records: list[dict[str, object]]) -> None:
    frame = spark.createDataFrame(records)
    if "display" in globals():
        display(frame)
    else:
        frame.show(truncate=False)


# COMMAND ----------
dbutils.widgets.text("catalog", "healthcare", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("volume", "raw_landing", "Volume")
dbutils.widgets.text(
    "read_principals",
    "",
    "Read principals (comma-separated)",
)
dbutils.widgets.text(
    "write_principals",
    "",
    "Write principals (comma-separated)",
)

catalog_name = dbutils.widgets.get("catalog").strip()
schema_name = dbutils.widgets.get("schema").strip()
volume_name = dbutils.widgets.get("volume").strip()
read_principals = _split_principals(dbutils.widgets.get("read_principals"))
write_principals = _split_principals(dbutils.widgets.get("write_principals"))

if not catalog_name or not schema_name or not volume_name:
    raise ValueError("Catalog, schema, and volume widget values are required.")

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
volume_identifier = _quote_qualified_identifier(catalog_name, schema_name, volume_name)


# COMMAND ----------
setup_sql = [
    f"CREATE CATALOG IF NOT EXISTS {_quote_identifier(catalog_name)}",
    f"CREATE SCHEMA IF NOT EXISTS {_quote_qualified_identifier(catalog_name, schema_name)}",
    f"CREATE VOLUME IF NOT EXISTS {volume_identifier}",
]

for statement in setup_sql:
    print(f"Executing: {statement}")
    spark.sql(statement)

print(f"Managed volume is available at {volume_path}")


# COMMAND ----------
def _grant_volume_access(
    principals: Iterable[str],
    *,
    volume_privileges: Iterable[str],
) -> list[dict[str, str]]:
    executed_grants: list[dict[str, str]] = []

    for principal in principals:
        statements = [
            (
                f"GRANT USE CATALOG ON CATALOG {_quote_identifier(catalog_name)} "
                f"TO {_quote_identifier(principal)}"
            ),
            (
                f"GRANT USE SCHEMA ON SCHEMA "
                f"{_quote_qualified_identifier(catalog_name, schema_name)} "
                f"TO {_quote_identifier(principal)}"
            ),
        ]
        statements.extend(
            f"GRANT {privilege} ON VOLUME {volume_identifier} TO {_quote_identifier(principal)}"
            for privilege in volume_privileges
        )

        for statement in statements:
            print(f"Executing: {statement}")
            spark.sql(statement)
            executed_grants.append({"principal": principal, "statement": statement})

    return executed_grants


executed_grants = []
executed_grants.extend(
    _grant_volume_access(read_principals, volume_privileges=["READ VOLUME"])
)
executed_grants.extend(
    _grant_volume_access(write_principals, volume_privileges=["READ VOLUME", "WRITE VOLUME"])
)

if executed_grants:
    _show_records(executed_grants)
else:
    print("No principal widgets were populated, so no GRANT statements were applied.")


# COMMAND ----------
mkdir_results: list[dict[str, object]] = []

for dataset_key, source in BRONZE_SOURCES.items():
    dataset_path = f"{volume_path}/{source.volume_subdirectory}"
    created = dbutils.fs.mkdirs(dataset_path)
    mkdir_results.append(
        {
            "dataset": dataset_key,
            "path": dataset_path,
            "mkdirs_returned": bool(created),
        }
    )

_show_records(mkdir_results)


# COMMAND ----------
folder_rows: list[dict[str, str]] = []

for entry in dbutils.fs.ls(volume_path):
    folder_rows.append(
        {
            "name": entry.name,
            "path": entry.path,
            "is_dir": str(entry.isDir()),
        }
    )

_show_records(folder_rows)

expected_subdirectories = {source.volume_subdirectory for source in BRONZE_SOURCES.values()}
actual_subdirectories = {row["name"].rstrip("/") for row in folder_rows if row["is_dir"] == "True"}
missing_subdirectories = sorted(expected_subdirectories - actual_subdirectories)

if missing_subdirectories:
    raise AssertionError(
        f"Expected landing folders were not created: {', '.join(missing_subdirectories)}"
    )

print("All manifest-declared bronze landing folders exist.")


# COMMAND ----------
verification_rows: list[dict[str, object]] = []
mismatched_datasets: list[str] = []

for dataset_key, source in BRONZE_SOURCES.items():
    dataset_file = f"{volume_path}/{source.volume_subdirectory}/{source.local_filename}"

    if not _path_exists(dataset_file):
        verification_rows.append(
            {
                "dataset": dataset_key,
                "file": dataset_file,
                "expected_rows": source.expected_row_count,
                "actual_rows": None,
                "status": "pending-upload",
            }
        )
        continue

    actual_rows = spark.read.option("header", True).csv(dataset_file).count()
    status = "matched" if actual_rows == source.expected_row_count else "row-count-mismatch"

    if status != "matched":
        mismatched_datasets.append(dataset_key)

    verification_rows.append(
        {
            "dataset": dataset_key,
            "file": dataset_file,
            "expected_rows": source.expected_row_count,
            "actual_rows": actual_rows,
            "status": status,
        }
    )

_show_records(verification_rows)

if mismatched_datasets:
    raise AssertionError(
        "Uploaded dataset row counts did not match the manifest for: "
        + ", ".join(sorted(mismatched_datasets))
    )

pending_uploads = [row["dataset"] for row in verification_rows if row["status"] == "pending-upload"]
if pending_uploads:
    print(
        "Dataset files are not in the volume yet for: "
        + ", ".join(pending_uploads)
        + ". Upload the CSVs, then rerun this verification cell."
    )
else:
    print("All uploaded dataset row counts match the bronze manifest.")
