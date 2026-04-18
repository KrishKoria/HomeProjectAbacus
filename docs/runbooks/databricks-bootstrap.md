# Databricks bronze landing bootstrap runbook

## Purpose

Use this runbook to bootstrap the governed raw landing zone for the healthcare demo data in Databricks. The process creates a Unity Catalog catalog, schema, and managed volume, then prepares one folder per source dataset so newcomers can upload the four CSV files into a predictable landing layout.

The bootstrap notebook lives at `src/notebooks/bootstrap_bronze_landing.py`.

## What this setup creates

When run with the default widget values, the notebook creates:

- Catalog: `healthcare`
- Schema: `bronze`
- Managed volume: `raw_landing`
- Volume path: `/Volumes/healthcare/bronze/raw_landing`
- Dataset folders:
  - `/Volumes/healthcare/bronze/raw_landing/claims`
  - `/Volumes/healthcare/bronze/raw_landing/providers`
  - `/Volumes/healthcare/bronze/raw_landing/diagnosis`
  - `/Volumes/healthcare/bronze/raw_landing/cost`

## Prerequisites

Before you run the notebook, make sure you have:

- A Databricks workspace with Unity Catalog enabled.
- Permission to create or use the target catalog, schema, and volume.
- A compute resource that can run Python notebook cells with `spark` and `dbutils`.
- The project synced into Databricks Repos, or an equivalent repo layout that still contains `src/common/bronze_sources.py` next to the notebook. The notebook discovers the repo root dynamically and expects that shared manifest to exist.
- Access to the four source CSV files from this repo:
  - `datasets/claims_1000.csv`
  - `datasets/providers_1000.csv`
  - `datasets/diagnosis.csv`
  - `datasets/cost.csv`
- Optional: the Databricks principals that should receive read-only or read/write access to the volume.

## Dataset-to-folder map

Upload each CSV into the matching subdirectory with the exact filename shown below.

| Dataset key | Local file | Target folder | Target file path | Expected rows |
|---|---|---|---|---:|
| `claims` | `datasets/claims_1000.csv` | `claims` | `/Volumes/healthcare/bronze/raw_landing/claims/claims_1000.csv` | 1000 |
| `providers` | `datasets/providers_1000.csv` | `providers` | `/Volumes/healthcare/bronze/raw_landing/providers/providers_1000.csv` | 21 |
| `diagnosis` | `datasets/diagnosis.csv` | `diagnosis` | `/Volumes/healthcare/bronze/raw_landing/diagnosis/diagnosis.csv` | 6 |
| `cost` | `datasets/cost.csv` | `cost` | `/Volumes/healthcare/bronze/raw_landing/cost/cost.csv` | 6 |

## Widget configuration

The notebook defines five widgets.

| Widget | Default value | Required | What it controls |
|---|---|---|---|
| `catalog` | `healthcare` | Yes | Unity Catalog catalog name to create or reuse. |
| `schema` | `bronze` | Yes | Schema inside the chosen catalog. |
| `volume` | `raw_landing` | Yes | Managed volume name inside the schema. |
| `read_principals` | empty | No | Comma-separated principals that receive `USE CATALOG`, `USE SCHEMA`, and `READ VOLUME`. |
| `write_principals` | empty | No | Comma-separated principals that receive `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, and `WRITE VOLUME`. |

### Recommended first run

For a first bootstrap run, keep the defaults unless your workspace already standardizes on different object names.

- `catalog = healthcare`
- `schema = bronze`
- `volume = raw_landing`
- `read_principals = ` (leave blank unless you already know the readers)
- `write_principals = ` (leave blank unless you already know the uploaders)

### Principal formatting

If you do populate principal widgets, use a comma-separated list with no special quoting, for example:

- `read_principals = analysts, data-science-team`
- `write_principals = data-engineering, ingestion-service`

Blank principal widgets are valid. In that case the notebook skips all `GRANT` statements.

## Run the bootstrap notebook

1. Open `src/notebooks/bootstrap_bronze_landing.py` from the synced Databricks repo.
2. Attach the notebook to a cluster or serverless compute that has access to Unity Catalog.
3. Confirm or override the widget values.
4. Run all cells.
5. Review the printed SQL statements and the inspection tables at the end of the notebook.

The notebook is designed to be safe to rerun. It uses `CREATE ... IF NOT EXISTS`, recreates missing folder structure with `dbutils.fs.mkdirs`, and can be rerun after file upload to refresh the verification table.

## Upload or copy the CSV files into the volume

After the notebook creates the volume and folders, place each CSV in its matching subdirectory.

### Option A: Upload through Catalog Explorer or the volume browser

Use this option when the CSV files are on your laptop or workstation.

1. In Databricks, open **Catalog Explorer**.
2. Navigate to `healthcare` → `bronze` → `raw_landing`.
3. Open each folder created by the notebook (`claims`, `providers`, `diagnosis`, `cost`).
4. Upload the matching file into that folder:
   - `claims/claims_1000.csv`
   - `providers/providers_1000.csv`
   - `diagnosis/diagnosis.csv`
   - `cost/cost.csv`
5. Refresh the view and confirm the files appear under the correct folder names.

### Option B: Copy from workspace-accessible storage

Use this option when the files already exist in a workspace-accessible path.

Example pattern:

```python
base_volume = "/Volumes/healthcare/bronze/raw_landing"
dbutils.fs.cp("file:/path/to/claims_1000.csv", f"{base_volume}/claims/claims_1000.csv")
dbutils.fs.cp("file:/path/to/providers_1000.csv", f"{base_volume}/providers/providers_1000.csv")
dbutils.fs.cp("file:/path/to/diagnosis.csv", f"{base_volume}/diagnosis/diagnosis.csv")
dbutils.fs.cp("file:/path/to/cost.csv", f"{base_volume}/cost/cost.csv")
```

If you use a non-local source such as DBFS or another mounted location, replace the `file:/...` source with the appropriate path.

## Verify success

### Notebook runtime signals

After the initial run, use the notebook outputs as the primary success signal:

- The setup cell prints successful `CREATE CATALOG`, `CREATE SCHEMA`, and `CREATE VOLUME` statements.
- The grant cell either shows executed `GRANT` statements or prints that no principal widgets were populated.
- The folder creation cell shows one row per dataset folder.
- The folder inspection cell confirms the expected subdirectories exist.
- The final verification cell shows one row per dataset with:
  - `status = pending-upload` before files are present
  - `status = matched` after the correct files are uploaded and row counts match the manifest

A fully successful data upload ends with:

- all four datasets present in the verification table
- `actual_rows` matching `expected_rows`
- the final message: `All uploaded dataset row counts match the bronze manifest.`

### Catalog Explorer inspection

You can also verify the landing zone visually in Databricks:

1. Open **Catalog Explorer**.
2. Navigate to `healthcare` → `bronze`.
3. Confirm the `raw_landing` volume exists.
4. Open the volume and confirm these folders exist:
   - `claims`
   - `providers`
   - `diagnosis`
   - `cost`
5. Open each folder and confirm the matching CSV file is present.

### Optional SQL checks

If you want an explicit SQL confirmation, run checks such as:

```sql
SHOW VOLUMES IN healthcare.bronze;
DESCRIBE VOLUME healthcare.bronze.raw_landing;
```

Those checks complement, but do not replace, the notebook’s final row-count verification cell.

## Troubleshooting

### `ValueError: Catalog, schema, and volume widget values are required.`

One or more required widgets were blank.

Fix:
- Populate `catalog`, `schema`, and `volume`.
- Rerun the notebook.

### `ModuleNotFoundError` about `src/common/bronze_sources.py`

The notebook could not locate the synced repo root.

Fix:
- Run the notebook from the project’s synced Databricks repo rather than from an isolated exported file.
- Confirm the repo still contains `src/common/bronze_sources.py`.

### Permission or privilege errors on `CREATE` or `GRANT`

Your current identity may not be allowed to create catalogs, schemas, volumes, or grant access.

Fix:
- Retry with an identity that has Unity Catalog admin or delegated privileges.
- If the objects already exist, use widget values that point at approved existing objects.
- Leave principal widgets blank if grants should be handled separately.

### Error about a missing managed location

Some workspaces require the catalog to have managed storage configured before a managed volume can be created.

Fix:
- Use an existing catalog with a valid managed location.
- Or ask a platform admin to configure the catalog’s managed storage, then rerun the notebook.

### `Expected landing folders were not created`

The volume exists, but one or more dataset subdirectories were not present after the folder-creation step.

Fix:
- Rerun the `dbutils.fs.mkdirs` cell and the folder inspection cell.
- Confirm the notebook is targeting the expected `catalog`, `schema`, and `volume` values.
- Check that the volume path resolves to `/Volumes/<catalog>/<schema>/<volume>`.

### Final verification shows `pending-upload`

This is expected until the CSV files are uploaded into the correct subdirectories.

Fix:
- Upload or copy the four CSV files into their matching folders.
- Keep the exact filenames from the dataset map.
- Rerun only the final verification cell after the upload completes.

### Final verification shows `row-count-mismatch`

A file exists, but its row count does not match the manifest.

Fix:
- Confirm you uploaded the correct file into the correct folder.
- Replace any accidentally edited or partial CSV.
- Check that duplicate files were not appended together.
- Rerun the final verification cell after replacing the file.

## Operational notes

- The notebook is intentionally rerunnable; use that to recover from partial setup or after grant changes.
- The last verification cell is also meant to be rerun after file uploads, so `pending-upload` is a normal intermediate state.
- Keep the filenames and folder names aligned with the manifest, because downstream bronze ingestion expects the governed layout shown in this runbook.
