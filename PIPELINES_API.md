# Pipelines Module API Reference

The `pipelines` module provides utilities for working with Databricks Pipelines (Delta Live Tables / Lakeflow).

## Installation

```bash
pip install databricks-utils
# or
uv add databricks-utils
```

## Import

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl
```

## Available Functions

### `pl.dry_run(client, pipeline_id, refresh_selection=None, full_refresh_selection=None, validate_only=True)`

Run an existing pipeline in dry-run/validation mode without processing data.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the existing pipeline to validate
- `refresh_selection` (list, optional): List of tables to validate (if not all)
- `full_refresh_selection` (list, optional): List of tables to validate with full refresh
- `validate_only` (bool): If True, only validates without processing data (default: True)

**Returns:**
- `str`: Update ID for the dry-run update

**Example:**
```python
client = get_workspace_client()

# Basic dry-run validation
update_id = pl.dry_run(client, pipeline_id="abc123")

# Wait for validation to complete
result = pl.wait_for_update(client, "abc123", update_id)
if result.state.value == "COMPLETED":
    print("✓ Validation passed!")
else:
    print("✗ Validation failed:", result.state)

# Dry-run specific tables only
update_id = pl.dry_run(
    client,
    pipeline_id="abc123",
    refresh_selection=["bronze_table", "silver_table"]
)
```

---

### `pl.create(client, spec, **kwargs)`

Create a new pipeline from a specification.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `spec` (dict or str): Pipeline specification
- `**kwargs`: Additional fields to override spec values

**Returns:**
- `str`: Pipeline ID of the created pipeline

**Example:**
```python
client = get_workspace_client()

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/pipeline-storage",
    "target": "dev",
    "libraries": [{"notebook": {"path": "/Workspace/notebooks/etl"}}]
}

pipeline_id = pl.create(client, spec)
```

---

### `pl.update(client, pipeline_id, spec, **kwargs)`

Update an existing pipeline configuration.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline to update
- `spec` (dict or str, optional): Pipeline specification updates
- `**kwargs`: Additional fields to override spec values

**Returns:**
- None

**Example:**
```python
pl.update(
    client,
    pipeline_id="abc123",
    spec={"target": "prod", "continuous": True}
)
```

---

### `pl.create_or_update(client, spec, **kwargs)`

Create a pipeline if it doesn't exist, or update it if it does (idempotent operation).

Searches for an existing pipeline by name. If found, updates its configuration. If not found, creates a new pipeline. This is perfect for CI/CD deployments where you want idempotent operations.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `spec` (dict or str): Pipeline specification
- `**kwargs`: Additional fields to override spec values

**Returns:**
- `tuple[str, bool]`: A tuple of (pipeline_id, was_created) where:
  - `pipeline_id`: The ID of the created or updated pipeline
  - `was_created`: True if pipeline was created, False if it was updated

**Example:**
```python
client = get_workspace_client()

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/pipeline-storage",
    "target": "dev",
    "libraries": [{"notebook": {"path": "/Workspace/notebooks/etl"}}]
}

# First time - creates the pipeline
pipeline_id, was_created = pl.create_or_update(client, spec)
print(f"Pipeline {pipeline_id}, created={was_created}")  # created=True

# Second time - updates the existing pipeline
spec["target"] = "prod"
pipeline_id, was_created = pl.create_or_update(client, spec)
print(f"Pipeline {pipeline_id}, created={was_created}")  # created=False

# Perfect for CI/CD deployments - safe to run multiple times
def deploy_pipeline(environment: str):
    spec = {
        "name": f"etl_pipeline_{environment}",
        "storage": f"/mnt/{environment}-storage",
        "target": environment,
        "libraries": [{"notebook": {"path": f"/Workspace/{environment}/etl"}}]
    }
    pipeline_id, was_created = pl.create_or_update(client, spec)
    action = "Created" if was_created else "Updated"
    print(f"{action} pipeline: {pipeline_id}")
    return pipeline_id
```

---

### `pl.start(client, pipeline_id, full_refresh=False, refresh_selection=None)`

Start a pipeline update.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline to start
- `full_refresh` (bool): Whether to perform a full refresh
- `refresh_selection` (list, optional): List of tables to refresh

**Returns:**
- `str`: Update ID for the started pipeline update

**Example:**
```python
# Normal update
update_id = pl.start(client, pipeline_id="abc123")

# Full refresh
update_id = pl.start(client, pipeline_id="abc123", full_refresh=True)

# Refresh specific tables
update_id = pl.start(
    client,
    pipeline_id="abc123",
    refresh_selection=["bronze_table", "silver_table"]
)
```

---

### `pl.stop(client, pipeline_id)`

Stop a running pipeline.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline to stop

**Returns:**
- None

**Example:**
```python
pl.stop(client, pipeline_id="abc123")
```

---

### `pl.delete(client, pipeline_id)`

Delete a pipeline.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline to delete

**Returns:**
- None

**Example:**
```python
pl.delete(client, pipeline_id="abc123")
```

---

### `pl.get_pipeline(client, pipeline_id)`

Get pipeline details and current state.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline

**Returns:**
- `PipelineStateInfo`: Current pipeline state and configuration

**Example:**
```python
pipeline = pl.get_pipeline(client, pipeline_id="abc123")
print(f"Pipeline: {pipeline.name}")
print(f"State: {pipeline.state}")
```

---

### `pl.list_pipelines(client, filter=None, max_results=100)`

List all pipelines in the workspace.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `filter` (str, optional): Filter string (e.g., "name LIKE 'prod_%'")
- `max_results` (int): Maximum number of pipelines to return

**Returns:**
- `list[PipelineStateInfo]`: List of pipeline state objects

**Example:**
```python
# List all pipelines
pipelines = pl.list_pipelines(client)
for p in pipelines:
    print(f"{p.name}: {p.state}")

# Filter pipelines
prod_pipelines = pl.list_pipelines(
    client,
    filter="name LIKE 'prod_%'"
)
```

---

### `pl.get_pipeline_updates(client, pipeline_id, max_results=25)`

Get the update history for a pipeline.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline
- `max_results` (int): Maximum number of updates to return

**Returns:**
- `list[PipelineUpdate]`: List of pipeline update objects

**Example:**
```python
updates = pl.get_pipeline_updates(client, pipeline_id="abc123")
for update in updates:
    print(f"Update {update.update_id}: {update.state}")
```

---

### `pl.wait_for_update(client, pipeline_id, update_id, timeout_seconds=3600, poll_interval_seconds=30)`

Wait for a pipeline update to complete.

**Parameters:**
- `client` (WorkspaceClient): Databricks workspace client
- `pipeline_id` (str): ID of the pipeline
- `update_id` (str): ID of the update to wait for
- `timeout_seconds` (int): Maximum time to wait (default: 3600)
- `poll_interval_seconds` (int): Polling interval (default: 30)

**Returns:**
- `PipelineUpdate`: The completed update object

**Raises:**
- `TimeoutError`: If the update doesn't complete within timeout_seconds

**Example:**
```python
update_id = pl.start(client, pipeline_id="abc123")

completed = pl.wait_for_update(
    client,
    pipeline_id="abc123",
    update_id=update_id,
    timeout_seconds=1800  # 30 minutes
)
print(f"Update completed with state: {completed.state}")
```

---

## Complete Workflow Example

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

# Get client
client = get_workspace_client()

# Define pipeline spec
spec = {
    "name": "production_pipeline",
    "storage": "/mnt/prod-storage",
    "target": "prod",
    "continuous": False,
    "libraries": [
        {"notebook": {"path": "/Workspace/pipelines/bronze"}},
        {"notebook": {"path": "/Workspace/pipelines/silver"}},
    ],
    "clusters": [
        {
            "label": "default",
            "num_workers": 4,
        }
    ],
}

# 1. Create pipeline
pipeline_id = pl.create(client, spec)
print(f"Created pipeline: {pipeline_id}")

# 2. Run dry-run validation first
print("Running dry-run validation...")
dry_run_update_id = pl.dry_run(client, pipeline_id)

# 3. Wait for validation to complete
validation_result = pl.wait_for_update(
    client,
    pipeline_id=pipeline_id,
    update_id=dry_run_update_id,
    timeout_seconds=600
)

if validation_result.state.value != "COMPLETED":
    print(f"Validation failed: {validation_result.state}")
    exit(1)

print("✓ Validation passed!")

# 4. Start actual pipeline update
update_id = pl.start(client, pipeline_id)

# 5. Wait for completion
completed_update = pl.wait_for_update(
    client,
    pipeline_id=pipeline_id,
    update_id=update_id
)

print(f"Pipeline update completed: {completed_update.state}")

# 6. Get pipeline status
pipeline = pl.get_pipeline(client, pipeline_id)
print(f"Pipeline state: {pipeline.state}")
```

## Examples

See the `examples/` directory for more examples:
- `examples/quick_start.py` - Basic usage
- `examples/pipeline_example.py` - Complete pipeline operations
- `examples/create_or_update_example.py` - Idempotent pipeline deployment
- `examples/dry_run_example.py` - Dry-run validation examples
- `examples/import_styles_demo.py` - Different import style demonstrations
