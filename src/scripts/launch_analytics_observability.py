from __future__ import annotations

import argparse
import logging

from databricks.sdk import WorkspaceClient

from src.common.diagnostics import DIAGNOSTIC_DOMAIN_ANALYTICS, DIAGNOSTIC_DOMAIN_OBSERVABILITY, format_claimops_diagnostic_id
from src.framework import HealthCheckResult


logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch analytics/observability job asynchronously.")
    parser.add_argument("--analytics-job-id", type=int, default=None)
    parser.add_argument("--analytics-job-name", default=None)
    parser.add_argument("--parent-job-name", default="manual")
    parser.add_argument("--parent-run-id", default="0")
    parser.add_argument("--pipeline-stage", default="etl")
    parser.add_argument("--pipeline-result", default="unknown")
    parser.add_argument("--verify-result", default="unknown")
    return parser.parse_args(argv)


def _normalize_state(value: str | None) -> str:
    return (value or "").strip().lower()


def _resolve_job_id(workspace: WorkspaceClient, job_name: str) -> int:
    matching = [job for job in workspace.jobs.list(name=job_name) if job.job_id is not None]
    if not matching:
        diag_id = format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_ANALYTICS, 201)
        raise ValueError(f"[{diag_id}] analytics job not found: {job_name}")
    # Prefer the latest job ID if duplicate names exist.
    return max(int(job.job_id) for job in matching)


def _compute_upstream_status(pipeline_result: str, verify_result: str) -> str:
    if _normalize_state(pipeline_result) == "success" and _normalize_state(verify_result) == "success":
        return "success"
    return "failure"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    upstream_status = _compute_upstream_status(args.pipeline_result, args.verify_result)
    try:
        workspace = WorkspaceClient()
        if args.analytics_job_id is not None:
            job_id = args.analytics_job_id
        else:
            if args.analytics_job_name is None:
                diag_id = format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_ANALYTICS, 202)
                raise ValueError(
                    f"[{diag_id}] either --analytics-job-id or "
                    "--analytics-job-name is required"
                )
            job_id = _resolve_job_id(workspace, args.analytics_job_name)
        launched = workspace.jobs.run_now(
            job_id=job_id,
            job_parameters={
                "upstream_status": upstream_status,
                "parent_job_name": args.parent_job_name,
                "parent_run_id": str(args.parent_run_id),
                "pipeline_stage": args.pipeline_stage,
            },
        )
    except Exception:
        logger.warning(
            "[%s] Failed to launch analytics observability job",
            format_claimops_diagnostic_id(DIAGNOSTIC_DOMAIN_OBSERVABILITY, 201),
            exc_info=True,
        )
        print(HealthCheckResult("etl", False, "launch_analytics_observability_failed").summary_line())
        return 1

    print(
        HealthCheckResult(
            "etl",
            True,
            (
                "launched_analytics_observability "
                f"job_name={args.analytics_job_name} "
                f"job_id={job_id} run_id={launched.run_id} "
                f"upstream_status={upstream_status}"
            ),
        ).summary_line()
    )
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc != 0:
        raise SystemExit(_rc)
