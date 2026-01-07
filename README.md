# databricks-utils

Utilities to simplify Databricks SDK integration

## Installation

### Using pip

```bash
# Install from local directory (for development)
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Install from git repository (once published)
pip install git+https://github.com/yourusername/databricks-utils.git
```

### Using uv

```bash
# Add to your project
uv add databricks-utils

# Add from local directory
uv add --editable .

# Add with dev dependencies
uv add --extra dev databricks-utils
```

**Important Note**: The package is installed as `databricks-utils` (with hyphen), but imported in Python code as `databricks_utils` (with underscore):

```bash
# Installation uses hyphen
pip install databricks-utils
uv add databricks-utils
```

```python
# Import uses underscore - both styles work:
from databricks_utils import pipelines as pl          # Style 1
import databricks_utils.pipelines as pl               # Style 2

from databricks_utils import get_workspace_client
```

Both import styles are supported and equivalent. See [IMPORT_GUIDE.md](IMPORT_GUIDE.md) for details.

## Features

- **Client Utilities**: Simplified WorkspaceClient creation and connection validation
- **Job Utilities**: Wait for job runs, get outputs, and list failed runs
- **Workspace Utilities**: List, export, and import notebooks
- **Pipeline Utilities**: Create and manage Databricks pipelines (Delta Live Tables/Lakeflow)
  - Create pipelines from JSON specifications
  - Idempotent create_or_update for CI/CD deployments
  - Dry-run validation before production updates
  - Start, stop, and monitor pipeline updates

## Usage

### Client Setup

```python
from databricks_utils import get_workspace_client, validate_connection

# Using default profile from ~/.databrickscfg
client = get_workspace_client()

# Using specific profile
client = get_workspace_client(profile="prod")

# Using explicit credentials
client = get_workspace_client(
    host="https://your-workspace.cloud.databricks.com",
    token="your-token"
)

# Validate connection
if validate_connection(client):
    print("Connected successfully!")
```

### Working with Jobs

```python
from databricks_utils import wait_for_run, get_run_output, list_failed_runs

# Run a job and wait for completion
run = client.jobs.run_now(job_id=123)
completed_run = wait_for_run(client, run.run_id)

# Get run output
output = get_run_output(client, run.run_id)
print(output)

# List recent failed runs
failed_runs = list_failed_runs(client, job_id=123, limit=10)
for run in failed_runs:
    print(f"Run {run.run_id} failed: {run.state.state_message}")
```

### Working with Workspace Objects

```python
from databricks_utils import list_notebooks, export_notebook, import_notebook

# List notebooks in a directory
notebooks = list_notebooks(client, "/Users/user@example.com", recursive=True)
for nb in notebooks:
    print(f"{nb.path}: {nb.object_type}")

# Export notebook to local filesystem
export_notebook(
    client,
    workspace_path="/Users/user@example.com/my_notebook",
    local_path="./my_notebook.py",
    format="SOURCE"
)

# Import notebook from local filesystem
import_notebook(
    client,
    local_path="./my_notebook.py",
    workspace_path="/Users/user@example.com/imported_notebook",
    language="PYTHON",
    overwrite=True
)
```

### Working with Pipelines

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()

# Create a pipeline from a JSON specification (dictionary)
spec = {
    "name": "my_pipeline",
    "storage": "/mnt/pipeline-storage",
    "target": "dev",
    "continuous": False,
    "libraries": [
        {"notebook": {"path": "/Workspace/notebooks/etl_notebook"}}
    ],
    "clusters": [
        {
            "label": "default",
            "num_workers": 2
        }
    ]
}

pipeline_id = pl.create(client, spec)
print(f"Created pipeline: {pipeline_id}")

# Idempotent create or update (creates if doesn't exist, updates if it does)
pipeline_id, was_created = pl.create_or_update(client, spec)
if was_created:
    print(f"Created new pipeline: {pipeline_id}")
else:
    print(f"Updated existing pipeline: {pipeline_id}")

# Run dry-run validation on existing pipeline before actual run
dry_run_update_id = pl.dry_run(client, pipeline_id)
dry_run_result = pl.wait_for_update(client, pipeline_id, dry_run_update_id)

if dry_run_result.state.value == "COMPLETED":
    print("✓ Validation passed! Running actual update...")
    update_id = pl.start(client, pipeline_id)
else:
    print("✗ Validation failed:", dry_run_result.state)

# Create a pipeline from a JSON string
json_spec = '''
{
    "name": "gold_layer_pipeline",
    "storage": "/mnt/dlt-storage",
    "target": "prod",
    "configuration": {
        "source_database": "silver",
        "target_database": "gold"
    }
}
'''
pipeline_id = pl.create(client, json_spec)

# Start a pipeline update
update_id = pl.start(client, pipeline_id=pipeline_id)

# Wait for the update to complete
completed_update = pl.wait_for_update(
    client,
    pipeline_id=pipeline_id,
    update_id=update_id
)
print(f"Pipeline update completed: {completed_update.state}")

# Start with full refresh
update_id = pl.start(client, pipeline_id=pipeline_id, full_refresh=True)

# Refresh specific tables only
update_id = pl.start(
    client,
    pipeline_id=pipeline_id,
    refresh_selection=["bronze_customers", "bronze_orders"]
)

# Get pipeline details
pipeline = pl.get_pipeline(client, pipeline_id=pipeline_id)
print(f"Pipeline: {pipeline.name}, State: {pipeline.state}")

# List all pipelines
pipelines = pl.list_pipelines(client)
for p in pipelines:
    print(f"{p.name}: {p.state}")

# Get pipeline update history
updates = pl.get_pipeline_updates(client, pipeline_id=pipeline_id)
for update in updates:
    print(f"Update {update.update_id}: {update.state}")

# Update pipeline configuration
pl.update(
    client,
    pipeline_id=pipeline_id,
    spec={"continuous": True, "target": "prod"}
)

# Stop a running pipeline
pl.stop(client, pipeline_id=pipeline_id)

# Delete a pipeline
pl.delete(client, pipeline_id=pipeline_id)
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/databricks-utils.git
cd databricks-utils

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
# OR with uv
uv sync --extra dev
```

### Running Tests

```bash
# Run tests with pytest
pytest

# Run with coverage
pytest --cov=databricks_utils --cov-report=html
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type check
mypy src/
```

## Requirements

- Python >= 3.8
- databricks-sdk >= 0.18.0

## License

See LICENSE file for details.
