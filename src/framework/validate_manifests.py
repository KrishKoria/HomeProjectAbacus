from __future__ import annotations

from collections import defaultdict, deque
from pathlib import Path
from typing import Final

import yaml


PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
SERVICES_ROOT: Final[Path] = PROJECT_ROOT / "services"
REGISTRY_PATH: Final[Path] = SERVICES_ROOT / "manifest.yml"
RESOURCE_TYPE_KEYS: Final[dict[str, str]] = {
    "apps": "apps",
    "jobs": "jobs",
    "model_serving_endpoints": "model_serving_endpoints",
    "pipelines": "pipelines",
    "script": "script",
}
REQUIRED_SERVICE_FIELDS: Final[tuple[str, ...]] = (
    "service.name",
    "service.type",
    "service.version",
    "service.description",
    "entry_point.resource_key",
    "entry_point.resource_type",
)
ENTRY_PATH_KEYS: Final[tuple[str, ...]] = (
    "python_file",
    "notebook_path",
    "source_code_path",
    "path",
)
GLOB_INCLUDE_KEY: Final[str] = "include"
JOB_TASK_EXPECTATIONS: Final[dict[str, tuple[str, ...]]] = {
    "load_sample_data": ("load_sample_data",),
    "ml_training": (
        "run_bronze_pipeline",
        "verify_bronze",
        "observe_bronze",
        "run_silver_pipeline",
        "verify_silver",
        "observe_silver",
        "build_analytics",
        "run_gold_pipeline",
        "observe_gold",
        "check_new_data",
        "should_retrain",
        "train_model",
        "skip_retraining",
    ),
    "setup_infrastructure": ("apply_grants", "create_retrain_decisions"),
}
PIPELINE_SERVICE_TO_TASK: Final[dict[str, str]] = {
    "etl_bronze": "run_bronze_pipeline",
    "etl_silver": "run_silver_pipeline",
    "etl_gold": "run_gold_pipeline",
}


def _read_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a YAML mapping at the top level")
    return data


def _get_path(data: dict, dotted_path: str):
    current = data
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _normalize_local_path(path_text: str, resource_path: Path) -> Path | None:
    if not path_text or "${" in path_text:
        return None
    if path_text.startswith("/"):
        return None
    normalized = Path(path_text.replace("/", "\\"))
    return (resource_path.parent / normalized).resolve()


def _iter_resource_paths(payload, resource_path: Path):
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in ENTRY_PATH_KEYS and isinstance(value, str):
                yield key, value, resource_path
            if key == "glob" and isinstance(value, dict):
                include_value = value.get(GLOB_INCLUDE_KEY)
                if isinstance(include_value, str):
                    yield f"glob.{GLOB_INCLUDE_KEY}", include_value, resource_path
            yield from _iter_resource_paths(value, resource_path)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_resource_paths(item, resource_path)


def _resource_definitions(resource_files: list[Path]) -> dict[str, dict[str, Path]]:
    definitions: dict[str, dict[str, Path]] = defaultdict(dict)
    for resource_file in resource_files:
        payload = _read_yaml(resource_file)
        resources = payload.get("resources", {})
        if not isinstance(resources, dict):
            raise ValueError(f"{resource_file}: resources must be a mapping")
        for resource_type, entries in resources.items():
            if isinstance(entries, dict):
                for resource_key in entries:
                    definitions[resource_type][resource_key] = resource_file
    return definitions


def _registry_entries() -> dict[str, dict]:
    payload = _read_yaml(REGISTRY_PATH)
    services = payload.get("services", {})
    if not isinstance(services, dict):
        raise ValueError(f"{REGISTRY_PATH}: services must be a mapping")
    return services


def _validate_registry_exists(entries: dict[str, dict]) -> list[str]:
    errors: list[str] = []
    for service_name, entry in entries.items():
        manifest_text = entry.get("manifest")
        if not isinstance(manifest_text, str):
            errors.append(f"{service_name}: registry entry missing manifest path")
            continue
        manifest_path = (PROJECT_ROOT / manifest_text).resolve()
        if not manifest_path.exists():
            errors.append(f"{service_name}: manifest does not exist at {manifest_path}")
    return errors


def _validate_manifest_schema(entries: dict[str, dict]) -> list[str]:
    errors: list[str] = []
    valid_resource_types = tuple(RESOURCE_TYPE_KEYS)
    for service_name, entry in entries.items():
        manifest_path = (PROJECT_ROOT / entry["manifest"]).resolve()
        manifest = _read_yaml(manifest_path)
        for field_name in REQUIRED_SERVICE_FIELDS:
            if _get_path(manifest, field_name) in (None, ""):
                errors.append(f"{manifest_path}: missing required field {field_name}")
        resource_type = _get_path(manifest, "entry_point.resource_type")
        if resource_type not in valid_resource_types:
            errors.append(
                f"{manifest_path}: entry_point.resource_type must be one of {valid_resource_types}"
            )
        manifest_name = _get_path(manifest, "service.name")
        if manifest_name != service_name:
            errors.append(
                f"{manifest_path}: service.name={manifest_name!r} does not match registry key {service_name!r}"
            )
    return errors


def _validate_resource_cross_check(entries: dict[str, dict], resource_files: list[Path]) -> list[str]:
    errors: list[str] = []
    definitions = _resource_definitions(resource_files)
    for service_name, entry in entries.items():
        manifest_path = (PROJECT_ROOT / entry["manifest"]).resolve()
        manifest = _read_yaml(manifest_path)
        resource_type = _get_path(manifest, "entry_point.resource_type")
        resource_key = _get_path(manifest, "entry_point.resource_key")
        if resource_type == "script":
            continue
        expected_key = RESOURCE_TYPE_KEYS[resource_type]
        if resource_key not in definitions.get(expected_key, {}):
            errors.append(
                f"{manifest_path}: resource {expected_key}.{resource_key} not found in service resources"
            )
    return errors


def _validate_dag(entries: dict[str, dict]) -> list[str]:
    indegree: dict[str, int] = {name: 0 for name in entries}
    graph: dict[str, list[str]] = defaultdict(list)
    errors: list[str] = []
    for service_name, entry in entries.items():
        depends_on = entry.get("depends_on", [])
        if depends_on is None:
            continue
        if not isinstance(depends_on, list):
            errors.append(f"{service_name}: depends_on must be a list")
            continue
        for dependency in depends_on:
            if dependency not in entries:
                errors.append(f"{service_name}: unknown dependency {dependency}")
                continue
            graph[dependency].append(service_name)
            indegree[service_name] += 1
    queue = deque(name for name, count in indegree.items() if count == 0)
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for downstream in graph[current]:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                queue.append(downstream)
    if visited != len(entries):
        errors.append("services/manifest.yml: dependency graph contains a cycle")
    return errors


def _validate_job_task_consistency(entries: dict[str, dict], resource_files: list[Path]) -> list[str]:
    errors: list[str] = []
    definitions = _resource_definitions(resource_files)
    jobs = definitions.get("jobs", {})
    for service_name, expected_tasks in JOB_TASK_EXPECTATIONS.items():
        if service_name not in entries:
            continue
        manifest_path = (PROJECT_ROOT / entries[service_name]["manifest"]).resolve()
        manifest = _read_yaml(manifest_path)
        if _get_path(manifest, "entry_point.resource_type") != "jobs":
            continue
        job_key = _get_path(manifest, "entry_point.resource_key")
        job_file = jobs.get(job_key)
        if job_file is None:
            continue
        payload = _read_yaml(job_file)
        job_resource = payload["resources"]["jobs"][job_key]
        task_keys = {
            task.get("task_key")
            for task in job_resource.get("tasks", [])
            if isinstance(task, dict) and task.get("task_key")
        }
        missing = sorted(set(expected_tasks) - task_keys)
        if missing:
            errors.append(f"{job_file}: missing expected tasks for {service_name}: {missing}")

    training_entry = entries.get("ml_training")
    if training_entry:
        manifest = _read_yaml((PROJECT_ROOT / training_entry["manifest"]).resolve())
        job_key = _get_path(manifest, "entry_point.resource_key")
        job_file = jobs.get(job_key)
        if job_file is not None:
            payload = _read_yaml(job_file)
            job_resource = payload["resources"]["jobs"][job_key]
            depends_map = {
                task["task_key"]: [
                    dependency["task_key"]
                    for dependency in task.get("depends_on", [])
                    if isinstance(dependency, dict) and "task_key" in dependency
                ]
                for task in job_resource.get("tasks", [])
                if isinstance(task, dict) and "task_key" in task
            }
            if "run_silver_pipeline" in depends_map and "verify_bronze" not in depends_map["run_silver_pipeline"]:
                errors.append(f"{job_file}: run_silver_pipeline must depend on verify_bronze")
            for task_key in ("run_gold_pipeline", "build_analytics"):
                if task_key in depends_map and "verify_silver" not in depends_map[task_key]:
                    errors.append(f"{job_file}: {task_key} must depend on verify_silver")
            for service_name, task_key in PIPELINE_SERVICE_TO_TASK.items():
                depends_on = set(entries.get(service_name, {}).get("depends_on", []))
                if service_name == "etl_bronze":
                    continue
                parent_service = "etl_bronze" if service_name == "etl_silver" else "etl_silver"
                parent_task = "verify_bronze" if service_name == "etl_silver" else "verify_silver"
                if parent_service in depends_on and parent_task not in depends_map.get(task_key, []):
                    errors.append(
                        f"{job_file}: registry dependency {parent_service}->{service_name} is not reflected by {task_key}"
                    )
    return errors


def _validate_health_checks(entries: dict[str, dict]) -> list[str]:
    errors: list[str] = []
    for entry in entries.values():
        manifest_path = (PROJECT_ROOT / entry["manifest"]).resolve()
        manifest = _read_yaml(manifest_path)
        health_check = manifest.get("health_check")
        if not isinstance(health_check, dict):
            continue
        if health_check.get("type") != "script":
            continue
        entry_point = health_check.get("entry_point")
        if not isinstance(entry_point, str):
            errors.append(f"{manifest_path}: script health_check.entry_point must be a string")
            continue
        target_path = (PROJECT_ROOT / entry_point).resolve()
        if not target_path.exists():
            errors.append(f"{manifest_path}: health-check entry point does not exist: {target_path}")
    return errors


def _validate_local_paths(resource_files: list[Path]) -> list[str]:
    errors: list[str] = []
    for resource_file in resource_files:
        payload = _read_yaml(resource_file)
        for key_name, raw_path, source_file in _iter_resource_paths(payload, resource_file):
            resolved = _normalize_local_path(raw_path, source_file)
            if resolved is None:
                continue
            if "*" in raw_path:
                matches = list(source_file.parent.glob(raw_path.replace("/", "\\")))
                if not matches:
                    errors.append(f"{source_file}: {key_name} path does not match any files: {raw_path}")
                continue
            if not resolved.exists():
                errors.append(f"{source_file}: {key_name} path does not exist: {raw_path}")
    return errors


def _resource_files() -> list[Path]:
    return sorted(SERVICES_ROOT.glob("*/*/resources/*.yml"))


def validate_manifests() -> list[str]:
    entries = _registry_entries()
    resource_files = _resource_files()
    errors: list[str] = []
    errors.extend(_validate_registry_exists(entries))
    errors.extend(_validate_manifest_schema(entries))
    errors.extend(_validate_resource_cross_check(entries, resource_files))
    errors.extend(_validate_dag(entries))
    errors.extend(_validate_job_task_consistency(entries, resource_files))
    errors.extend(_validate_health_checks(entries))
    errors.extend(_validate_local_paths(resource_files))
    return errors


def main() -> int:
    errors = validate_manifests()
    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    print("OK: service manifests and bundle resource references are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
