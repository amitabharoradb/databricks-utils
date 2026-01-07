# create_or_update Guide

## Overview

The `create_or_update` function provides an **idempotent** way to deploy Databricks pipelines. It automatically detects whether a pipeline exists (by name) and either creates it or updates it accordingly.

This is the recommended approach for CI/CD deployments and infrastructure-as-code scenarios where you want safe, repeatable deployments.

## Function Signature

```python
def create_or_update(
    client: WorkspaceClient,
    spec: Union[Dict[str, Any], str],
    **kwargs
) -> tuple[str, bool]
```

## Parameters

- **client** (WorkspaceClient): Databricks workspace client instance
- **spec** (dict or str): Pipeline specification as dictionary or JSON string
- **\*\*kwargs**: Additional fields to override spec values

## Returns

- **tuple[str, bool]**: A tuple containing:
  - **pipeline_id** (str): The ID of the created or updated pipeline
  - **was_created** (bool): True if pipeline was created, False if it was updated

## Basic Usage

### Simple Create or Update

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()

spec = {
    "name": "my_pipeline",
    "storage": "/mnt/pipeline-storage",
    "target": "dev",
    "libraries": [
        {"notebook": {"path": "/Workspace/notebooks/etl"}}
    ]
}

# First time - creates the pipeline
pipeline_id, was_created = pl.create_or_update(client, spec)
if was_created:
    print(f"Created new pipeline: {pipeline_id}")
else:
    print(f"Updated existing pipeline: {pipeline_id}")
```

### Updating Configuration

```python
# Modify the spec and run again - updates the existing pipeline
spec["target"] = "prod"
spec["continuous"] = True

pipeline_id, was_created = pl.create_or_update(client, spec)
# was_created will be False if pipeline already existed
```

## Why Use create_or_update?

### ✅ Idempotent Deployments

Safe to run multiple times - won't create duplicates or fail if pipeline exists:

```python
# Run this script multiple times - it's safe!
spec = {"name": "prod_pipeline", ...}
pipeline_id, was_created = pl.create_or_update(client, spec)
```

### ✅ CI/CD Integration

Perfect for automated deployments:

```python
def deploy_to_environment(env: str):
    spec = {
        "name": f"etl_{env}",
        "storage": f"/mnt/{env}-storage",
        "target": env,
        ...
    }

    pipeline_id, was_created = pl.create_or_update(client, spec)

    action = "CREATED" if was_created else "UPDATED"
    print(f"[{env.upper()}] Pipeline {action}: {pipeline_id}")

    return pipeline_id

# Deploy to all environments
for env in ["dev", "staging", "prod"]:
    deploy_to_environment(env)
```

### ✅ Infrastructure as Code

Version control your pipeline specs and apply them:

```python
# pipeline_config.json
{
    "name": "production_etl",
    "storage": "/mnt/prod-storage",
    "target": "prod",
    ...
}

# deploy.py
import json
with open("pipeline_config.json") as f:
    spec = json.load(f)

pipeline_id, was_created = pl.create_or_update(client, spec)
```

## Complete Deployment Workflow

Here's a production-ready deployment pattern:

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

def deploy_pipeline(spec: dict, validate: bool = True) -> str:
    """
    Deploy pipeline with optional validation.

    Args:
        spec: Pipeline specification
        validate: Whether to run dry-run validation

    Returns:
        str: Pipeline ID
    """
    client = get_workspace_client()

    # Step 1: Create or update pipeline
    print(f"Deploying pipeline '{spec['name']}'...")
    pipeline_id, was_created = pl.create_or_update(client, spec)

    action = "Created" if was_created else "Updated"
    print(f"  ✓ {action} pipeline: {pipeline_id}")

    # Step 2: Validate if requested
    if validate:
        print("  Running validation...")
        dry_run_id = pl.dry_run(client, pipeline_id)
        result = pl.wait_for_update(
            client,
            pipeline_id,
            dry_run_id,
            timeout_seconds=600
        )

        if result.state.value == "COMPLETED":
            print("  ✓ Validation passed!")
        else:
            print(f"  ✗ Validation failed: {result.state}")
            raise RuntimeError("Pipeline validation failed")

    # Step 3: Get final state
    pipeline = pl.get_pipeline(client, pipeline_id)
    print(f"  Pipeline state: {pipeline.state}")

    return pipeline_id


# Usage
spec = {
    "name": "production_etl",
    "storage": "/mnt/prod-storage",
    "target": "prod",
    "continuous": True,
    "libraries": [
        {"notebook": {"path": "/Workspace/prod/bronze"}},
        {"notebook": {"path": "/Workspace/prod/silver"}},
        {"notebook": {"path": "/Workspace/prod/gold"}},
    ]
}

pipeline_id = deploy_pipeline(spec, validate=True)
print(f"\n✓ Deployment successful: {pipeline_id}")
```

## Using with JSON Specs

```python
# Load from JSON file
import json

with open("pipeline_spec.json") as f:
    spec_dict = json.load(f)

pipeline_id, was_created = pl.create_or_update(client, spec_dict)

# Or pass JSON string directly
json_spec = '''
{
    "name": "my_pipeline",
    "storage": "/mnt/storage",
    "target": "dev"
}
'''

pipeline_id, was_created = pl.create_or_update(client, json_spec)
```

## Using kwargs to Override

```python
base_spec = {
    "name": "etl_pipeline",
    "storage": "/mnt/storage",
    "libraries": [...]
}

# Override specific fields
pipeline_id, was_created = pl.create_or_update(
    client,
    base_spec,
    target="prod",        # Override target
    continuous=True,      # Override continuous
)
```

## Error Handling

```python
try:
    pipeline_id, was_created = pl.create_or_update(client, spec)
    print(f"Success: {pipeline_id}")

except ValueError as e:
    print(f"Invalid spec: {e}")
    # Missing required field (e.g., 'name')

except RuntimeError as e:
    print(f"Deployment error: {e}")
    # Failed to determine if pipeline exists

except Exception as e:
    print(f"Unexpected error: {e}")
```

## Best Practices

### 1. Always Include Pipeline Name

The `name` field is required for lookup:

```python
spec = {
    "name": "my_pipeline",  # REQUIRED for create_or_update
    "storage": "/mnt/storage",
    ...
}
```

### 2. Use Consistent Naming

Use a clear naming convention for environment-specific pipelines:

```python
# Good naming patterns
f"etl_{environment}"           # etl_dev, etl_prod
f"{project}_{environment}"     # finance_dev, finance_prod
f"{team}_{pipeline}_{env}"     # data_ingestion_prod
```

### 3. Version Control Your Specs

Store pipeline specs in version control:

```
project/
├── pipelines/
│   ├── dev.json
│   ├── staging.json
│   └── prod.json
└── deploy.py
```

### 4. Validate After Updates

Always validate after updates, especially in production:

```python
pipeline_id, was_created = pl.create_or_update(client, spec)

if not was_created:  # If updated, validate
    dry_run_id = pl.dry_run(client, pipeline_id)
    result = pl.wait_for_update(client, pipeline_id, dry_run_id)
    assert result.state.value == "COMPLETED", "Validation failed"
```

### 5. Log Deployment Actions

Track what changed:

```python
import logging

logging.info(f"Deploying pipeline: {spec['name']}")
pipeline_id, was_created = pl.create_or_update(client, spec)

if was_created:
    logging.info(f"Created new pipeline: {pipeline_id}")
else:
    logging.info(f"Updated existing pipeline: {pipeline_id}")
```

## Comparison: create vs create_or_update

| Feature | `create` | `create_or_update` |
|---------|----------|-------------------|
| Creates new pipeline | Yes | Yes (if doesn't exist) |
| Updates existing | No (fails) | Yes (if exists) |
| Idempotent | No | Yes |
| Returns was_created flag | No | Yes |
| CI/CD friendly | No | Yes |
| Use case | One-time creation | Deployments/IaC |

## Examples

See [examples/create_or_update_example.py](examples/create_or_update_example.py) for comprehensive examples including:
- First deployment (create)
- Subsequent deployments (update)
- CI/CD patterns
- Multi-environment deployment
- Error handling

## See Also

- [PIPELINES_API.md](PIPELINES_API.md) - Complete API reference
- [DRY_RUN_GUIDE.md](DRY_RUN_GUIDE.md) - Validation guide
- [examples/create_or_update_example.py](examples/create_or_update_example.py) - Detailed examples
