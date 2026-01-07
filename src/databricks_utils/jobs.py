"""Utilities for working with Databricks jobs."""

from typing import Optional, Dict, Any, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, RunState


def wait_for_run(
    client: WorkspaceClient,
    run_id: int,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 30
) -> Run:
    """
    Wait for a job run to complete.

    Args:
        client: WorkspaceClient instance
        run_id: ID of the job run to wait for
        timeout_seconds: Maximum time to wait in seconds (default: 1 hour)
        poll_interval_seconds: How often to poll for status (default: 30 seconds)

    Returns:
        Run: The completed run object

    Raises:
        TimeoutError: If the run doesn't complete within timeout_seconds

    Examples:
        >>> client = get_workspace_client()
        >>> run = client.jobs.run_now(job_id=123)
        >>> completed_run = wait_for_run(client, run.run_id)
        >>> print(f"Run completed with state: {completed_run.state.life_cycle_state}")
    """
    import time

    elapsed = 0
    while elapsed < timeout_seconds:
        run = client.jobs.get_run(run_id=run_id)

        if run.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            return run

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise TimeoutError(
        f"Job run {run_id} did not complete within {timeout_seconds} seconds"
    )


def get_run_output(client: WorkspaceClient, run_id: int) -> Optional[Dict[str, Any]]:
    """
    Get the output of a completed job run.

    Args:
        client: WorkspaceClient instance
        run_id: ID of the job run

    Returns:
        Dict containing the run output, or None if no output available

    Examples:
        >>> client = get_workspace_client()
        >>> output = get_run_output(client, run_id=12345)
        >>> if output:
        ...     print(output)
    """
    try:
        run_output = client.jobs.get_run_output(run_id=run_id)
        return {
            "notebook_output": getattr(run_output, "notebook_output", None),
            "sql_output": getattr(run_output, "sql_output", None),
            "dbt_output": getattr(run_output, "dbt_output", None),
            "logs": getattr(run_output, "logs", None),
            "error": getattr(run_output, "error", None),
        }
    except Exception:
        return None


def list_failed_runs(
    client: WorkspaceClient,
    job_id: Optional[int] = None,
    limit: int = 25
) -> List[Run]:
    """
    List recent failed job runs.

    Args:
        client: WorkspaceClient instance
        job_id: Optional job ID to filter by
        limit: Maximum number of runs to return

    Returns:
        List of failed Run objects

    Examples:
        >>> client = get_workspace_client()
        >>> failed_runs = list_failed_runs(client, job_id=123)
        >>> for run in failed_runs:
        ...     print(f"Run {run.run_id} failed: {run.state.state_message}")
    """
    kwargs = {"limit": limit, "completed_only": True}
    if job_id:
        kwargs["job_id"] = job_id

    runs = list(client.jobs.list_runs(**kwargs))

    return [
        run for run in runs
        if run.state.result_state == "FAILED"
    ]
