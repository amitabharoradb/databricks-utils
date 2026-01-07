"""Core utilities for Databricks pipeline operations."""

from __future__ import annotations

import json
from typing import Dict, Any, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.pipelines import (
        PipelineSpec,
        PipelineStateInfo,
        PipelineUpdate,
    )


def create(
    client: WorkspaceClient,
    spec: Union[Dict[str, Any], str],
    **kwargs
) -> str:
    """
    Create a Databricks pipeline from a JSON specification.

    Args:
        client: WorkspaceClient instance
        spec: Pipeline specification as a dictionary or JSON string
        **kwargs: Additional arguments to override spec values

    Returns:
        str: Pipeline ID of the created pipeline

    Examples:
        >>> from databricks_utils import get_workspace_client
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> client = get_workspace_client()
        >>>
        >>> # Using a dictionary
        >>> spec = {
        ...     "name": "my_pipeline",
        ...     "storage": "/mnt/pipeline-storage",
        ...     "target": "dev",
        ...     "libraries": [
        ...         {"notebook": {"path": "/Workspace/notebooks/pipeline_notebook"}}
        ...     ]
        ... }
        >>> pipeline_id = pl.create(client, spec)
        >>>
        >>> # Using a JSON string
        >>> json_spec = '''
        ... {
        ...     "name": "my_pipeline",
        ...     "storage": "/mnt/pipeline-storage",
        ...     "configuration": {"source": "demo"}
        ... }
        ... '''
        >>> pipeline_id = pl.create(client, json_spec)
    """
    # Parse spec if it's a string
    if isinstance(spec, str):
        spec = json.loads(spec)

    # Merge kwargs into spec
    spec.update(kwargs)

    # Create the pipeline
    result = client.pipelines.create(**spec)

    return result.pipeline_id


def dry_run(
    client: "WorkspaceClient",
    pipeline_id: str,
    refresh_selection: Optional[List[str]] = None,
    full_refresh_selection: Optional[List[str]] = None,
    validate_only: bool = True,
) -> str:
    """
    Run an existing pipeline in dry-run/validation mode.

    This starts a pipeline update that validates the pipeline without actually
    processing data or materializing tables. Useful for testing pipeline logic
    and dependencies before running a full update.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the existing pipeline to run in dry-run mode
        refresh_selection: Optional list of tables to validate (if not all)
        full_refresh_selection: Optional list of tables to validate with full refresh
        validate_only: If True, only validates without processing data (default: True)

    Returns:
        str: Update ID for the dry-run update

    Examples:
        >>> from databricks_utils import get_workspace_client
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> client = get_workspace_client()
        >>>
        >>> # Run dry-run on an existing pipeline
        >>> update_id = pl.dry_run(client, pipeline_id="abc123")
        >>> print(f"Dry-run update started: {update_id}")
        >>>
        >>> # Wait for validation to complete
        >>> result = pl.wait_for_update(client, pipeline_id="abc123", update_id=update_id)
        >>> print(f"Validation result: {result.state}")
        >>>
        >>> # Dry-run specific tables only
        >>> update_id = pl.dry_run(
        ...     client,
        ...     pipeline_id="abc123",
        ...     refresh_selection=["bronze_table", "silver_table"]
        ... )
        >>>
        >>> # Dry-run with full refresh for specific tables
        >>> update_id = pl.dry_run(
        ...     client,
        ...     pipeline_id="abc123",
        ...     full_refresh_selection=["bronze_customers"]
        ... )
    """
    kwargs = {
        "pipeline_id": pipeline_id,
    }

    # Add refresh selections if provided
    if refresh_selection:
        kwargs["refresh_selection"] = refresh_selection

    if full_refresh_selection:
        kwargs["full_refresh_selection"] = full_refresh_selection

    # Add validate_only flag if supported
    # Note: This parameter validates pipeline logic without materializing data
    if validate_only:
        kwargs["validate_only"] = validate_only

    # Start the dry-run update
    result = client.pipelines.start_update(**kwargs)
    return result.update_id


def update(
    client: WorkspaceClient,
    pipeline_id: str,
    spec: Optional[Union[Dict[str, Any], str]] = None,
    **kwargs
) -> None:
    """
    Update an existing pipeline configuration.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline to update
        spec: Pipeline specification updates as dictionary or JSON string
        **kwargs: Additional arguments to override spec values

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> # Update pipeline configuration
        >>> pl.update(
        ...     client,
        ...     pipeline_id="abc123",
        ...     spec={"target": "prod", "continuous": True}
        ... )
    """
    update_spec = {}

    if spec:
        if isinstance(spec, str):
            update_spec = json.loads(spec)
        else:
            update_spec = spec.copy()

    update_spec.update(kwargs)
    update_spec["pipeline_id"] = pipeline_id

    client.pipelines.update(**update_spec)


def create_or_update(
    client: "WorkspaceClient",
    spec: Union[Dict[str, Any], str],
    **kwargs
) -> tuple[str, bool]:
    """
    Create a pipeline if it doesn't exist, or update it if it does (by name).

    This function searches for an existing pipeline by name. If found, it updates
    the pipeline configuration. If not found, it creates a new pipeline.

    Args:
        client: WorkspaceClient instance
        spec: Pipeline specification as a dictionary or JSON string
        **kwargs: Additional arguments to override spec values

    Returns:
        tuple[str, bool]: A tuple of (pipeline_id, was_created) where:
            - pipeline_id: The ID of the created or updated pipeline
            - was_created: True if pipeline was created, False if it was updated

    Examples:
        >>> from databricks_utils import get_workspace_client
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> client = get_workspace_client()
        >>>
        >>> # First time - creates the pipeline
        >>> spec = {
        ...     "name": "my_pipeline",
        ...     "storage": "/mnt/pipeline-storage",
        ...     "target": "dev",
        ...     "libraries": [{"notebook": {"path": "/Workspace/notebooks/etl"}}]
        ... }
        >>> pipeline_id, was_created = pl.create_or_update(client, spec)
        >>> print(f"Pipeline {pipeline_id}, created={was_created}")
        Pipeline abc123, created=True
        >>>
        >>> # Second time - updates the existing pipeline
        >>> spec["target"] = "prod"  # Change target
        >>> pipeline_id, was_created = pl.create_or_update(client, spec)
        >>> print(f"Pipeline {pipeline_id}, created={was_created}")
        Pipeline abc123, created=False
        >>>
        >>> # Use with kwargs to override spec
        >>> pipeline_id, was_created = pl.create_or_update(
        ...     client,
        ...     spec,
        ...     continuous=True
        ... )
    """
    # Parse spec if it's a string
    if isinstance(spec, str):
        parsed_spec = json.loads(spec)
    else:
        parsed_spec = spec.copy()

    # Merge kwargs into spec
    parsed_spec.update(kwargs)

    # Get pipeline name from spec
    pipeline_name = parsed_spec.get("name")
    if not pipeline_name:
        raise ValueError("Pipeline spec must include 'name' field")

    # Search for existing pipeline by name
    try:
        all_pipelines = list(client.pipelines.list_pipelines())
        existing_pipeline = None

        for pipeline in all_pipelines:
            if pipeline.name == pipeline_name:
                existing_pipeline = pipeline
                break

        if existing_pipeline:
            # Pipeline exists - update it
            pipeline_id = existing_pipeline.pipeline_id

            # Update the pipeline
            update_spec = parsed_spec.copy()
            update_spec["pipeline_id"] = pipeline_id

            client.pipelines.update(**update_spec)

            return pipeline_id, False  # Not created, was updated

        else:
            # Pipeline doesn't exist - create it
            result = client.pipelines.create(**parsed_spec)
            return result.pipeline_id, True  # Was created

    except Exception as e:
        # If listing fails, try to create (will fail if exists)
        raise RuntimeError(f"Failed to determine if pipeline exists: {str(e)}")


def delete(
    client: WorkspaceClient,
    pipeline_id: str
) -> None:
    """
    Delete a pipeline.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline to delete

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>> pl.delete(client, pipeline_id="abc123")
    """
    client.pipelines.delete(pipeline_id=pipeline_id)


def start(
    client: WorkspaceClient,
    pipeline_id: str,
    full_refresh: bool = False,
    refresh_selection: Optional[List[str]] = None,
) -> str:
    """
    Start a pipeline update.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline to start
        full_refresh: Whether to perform a full refresh
        refresh_selection: List of tables to refresh (if not all)

    Returns:
        str: Update ID for the started pipeline update

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> # Start a normal update
        >>> update_id = pl.start(client, pipeline_id="abc123")
        >>>
        >>> # Start with full refresh
        >>> update_id = pl.start(client, pipeline_id="abc123", full_refresh=True)
        >>>
        >>> # Refresh specific tables
        >>> update_id = pl.start(
        ...     client,
        ...     pipeline_id="abc123",
        ...     refresh_selection=["bronze_table", "silver_table"]
        ... )
    """
    kwargs = {"pipeline_id": pipeline_id}

    if full_refresh:
        kwargs["full_refresh"] = full_refresh

    if refresh_selection:
        kwargs["refresh_selection"] = refresh_selection

    result = client.pipelines.start_update(**kwargs)
    return result.update_id


def stop(
    client: WorkspaceClient,
    pipeline_id: str
) -> None:
    """
    Stop a running pipeline.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline to stop

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>> pl.stop(client, pipeline_id="abc123")
    """
    client.pipelines.stop(pipeline_id=pipeline_id)


def get_pipeline(
    client: WorkspaceClient,
    pipeline_id: str
) -> PipelineStateInfo:
    """
    Get pipeline details and current state.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline

    Returns:
        PipelineStateInfo: Current pipeline state and configuration

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> pipeline = pl.get_pipeline(client, pipeline_id="abc123")
        >>> print(f"Pipeline: {pipeline.name}")
        >>> print(f"State: {pipeline.state}")
        >>> print(f"Latest update: {pipeline.latest_updates[0].state if pipeline.latest_updates else 'None'}")
    """
    return client.pipelines.get(pipeline_id=pipeline_id)


def list_pipelines(
    client: WorkspaceClient,
    filter: Optional[str] = None,
    max_results: int = 100
) -> List[PipelineStateInfo]:
    """
    List all pipelines in the workspace.

    Args:
        client: WorkspaceClient instance
        filter: Optional filter string (e.g., "name LIKE 'prod_%'")
        max_results: Maximum number of pipelines to return

    Returns:
        List of PipelineStateInfo objects

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> # List all pipelines
        >>> all_pipelines = pl.list_pipelines(client)
        >>> for p in all_pipelines:
        ...     print(f"{p.name}: {p.state}")
        >>>
        >>> # Filter pipelines
        >>> prod_pipelines = pl.list_pipelines(
        ...     client,
        ...     filter="name LIKE 'prod_%'"
        ... )
    """
    kwargs = {"max_results": max_results}
    if filter:
        kwargs["filter"] = filter

    return list(client.pipelines.list_pipelines(**kwargs))


def get_pipeline_updates(
    client: WorkspaceClient,
    pipeline_id: str,
    max_results: int = 25
) -> List[PipelineUpdate]:
    """
    Get the update history for a pipeline.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline
        max_results: Maximum number of updates to return

    Returns:
        List of PipelineUpdate objects

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> updates = pl.get_pipeline_updates(client, pipeline_id="abc123")
        >>> for update in updates:
        ...     print(f"Update {update.update_id}: {update.state}")
        ...     if update.state.value == "FAILED":
        ...         print(f"  Error: {update.cause}")
    """
    return list(client.pipelines.list_updates(
        pipeline_id=pipeline_id,
        max_results=max_results
    ))


def wait_for_update(
    client: WorkspaceClient,
    pipeline_id: str,
    update_id: str,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 30
) -> PipelineUpdate:
    """
    Wait for a pipeline update to complete.

    Args:
        client: WorkspaceClient instance
        pipeline_id: ID of the pipeline
        update_id: ID of the update to wait for
        timeout_seconds: Maximum time to wait in seconds (default: 1 hour)
        poll_interval_seconds: How often to poll for status (default: 30 seconds)

    Returns:
        PipelineUpdate: The completed update object

    Raises:
        TimeoutError: If the update doesn't complete within timeout_seconds

    Examples:
        >>> from databricks_utils import pipelines as pl
        >>>
        >>> # Start an update and wait for completion
        >>> update_id = pl.start(client, pipeline_id="abc123")
        >>> completed = pl.wait_for_update(
        ...     client,
        ...     pipeline_id="abc123",
        ...     update_id=update_id
        ... )
        >>> print(f"Update completed with state: {completed.state}")
    """
    import time

    elapsed = 0
    while elapsed < timeout_seconds:
        update = client.pipelines.get_update(
            pipeline_id=pipeline_id,
            update_id=update_id
        )

        # Terminal states
        if update.state.value in ["COMPLETED", "FAILED", "CANCELED"]:
            return update

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise TimeoutError(
        f"Pipeline update {update_id} did not complete within {timeout_seconds} seconds"
    )
