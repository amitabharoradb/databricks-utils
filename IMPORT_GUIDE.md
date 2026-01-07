# Import Guide for databricks-utils

## Package Name vs Import Name

- **Package name (for installation)**: `databricks-utils` (with hyphen)
- **Import name (in Python code)**: `databricks_utils` (with underscore)

```bash
# Install with hyphen
pip install databricks-utils
uv add databricks-utils
```

```python
# Import with underscore
from databricks_utils import pipelines as pl
import databricks_utils.pipelines as pl
```

## Both Import Styles Are Supported

The package supports both Python import styles:

### Style 1: `from databricks_utils import pipelines as pl`

```python
from databricks_utils import pipelines as pl

# Use pipelines functions
result = pl.dry_run(spec)
pipeline_id = pl.create(client, spec)
```

### Style 2: `import databricks_utils.pipelines as pl`

```python
import databricks_utils.pipelines as pl

# Use pipelines functions
result = pl.dry_run(spec)
pipeline_id = pl.create(client, spec)
```

**Both styles import the exact same module** and can be used interchangeably.

## Complete Examples

### Example 1: Using Style 1

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/storage",
    "libraries": [{"notebook": {"path": "/notebook"}}]
}

# Validate first
result = pl.dry_run(spec)
if result["valid"]:
    # Create pipeline
    pipeline_id = pl.create(client, spec)
    # Start it
    update_id = pl.start(client, pipeline_id)
```

### Example 2: Using Style 2

```python
from databricks_utils import get_workspace_client
import databricks_utils.pipelines as pl

client = get_workspace_client()

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/storage",
    "libraries": [{"notebook": {"path": "/notebook"}}]
}

# Validate first
result = pl.dry_run(spec)
if result["valid"]:
    # Create pipeline
    pipeline_id = pl.create(client, spec)
    # Start it
    update_id = pl.start(client, pipeline_id)
```

### Example 3: Direct Function Imports

You can also import specific functions directly:

```python
from databricks_utils.pipelines import dry_run, create, start
from databricks_utils import get_workspace_client

client = get_workspace_client()

spec = {"name": "pipeline", "storage": "/mnt/storage"}

# Use functions directly
result = dry_run(spec)
if result["valid"]:
    pipeline_id = create(client, spec)
    update_id = start(client, pipeline_id)
```

## Why Use Underscore Instead of Hyphen?

Python does not allow hyphens in import statements because they're interpreted as the minus operator. This is a Python language restriction, not a package design choice.

```python
# ❌ INVALID - Python syntax error
from databricks-utils import pipelines

# ✓ VALID - Use underscore
from databricks_utils import pipelines
```

This naming pattern (hyphen for package, underscore for import) is standard in Python:
- `pip install scikit-learn` → `import sklearn`
- `pip install python-dateutil` → `import dateutil`
- `pip install beautifulsoup4` → `import bs4`
- `pip install databricks-utils` → `import databricks_utils`

## Available Functions

When you import `pipelines as pl`, you get access to:

- `pl.dry_run(spec)` - Validate pipeline spec (no SDK required)
- `pl.create(client, spec)` - Create pipeline
- `pl.update(client, pipeline_id, spec)` - Update pipeline
- `pl.delete(client, pipeline_id)` - Delete pipeline
- `pl.start(client, pipeline_id)` - Start pipeline
- `pl.stop(client, pipeline_id)` - Stop pipeline
- `pl.get_pipeline(client, pipeline_id)` - Get pipeline details
- `pl.list_pipelines(client)` - List all pipelines
- `pl.get_pipeline_updates(client, pipeline_id)` - Get update history
- `pl.wait_for_update(client, pipeline_id, update_id)` - Wait for completion

## Working Without databricks-sdk

The `dry_run` function works without `databricks-sdk` installed:

```python
# Works even without databricks-sdk
from databricks_utils import pipelines as pl

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/storage"
}

result = pl.dry_run(spec)
print(f"Valid: {result['valid']}")
print(f"Errors: {result['errors']}")
print(f"Warnings: {result['warnings']}")
```

All other functions require `databricks-sdk` to be installed.
