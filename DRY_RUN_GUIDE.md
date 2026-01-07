# Dry Run Guide for Databricks Pipelines

## Overview

The `dry_run` function runs an existing Databricks pipeline in validation mode, allowing you to test the pipeline logic and dependencies **without actually processing data or materializing tables**.

This is useful for:
- Validating pipeline configurations before running production updates
- Testing pipeline logic and dependencies
- Catching errors early without consuming compute resources
- Ensuring pipelines will succeed before processing large datasets

## Function Signature

```python
def dry_run(
    client: WorkspaceClient,
    pipeline_id: str,
    refresh_selection: Optional[List[str]] = None,
    full_refresh_selection: Optional[List[str]] = None,
    validate_only: bool = True,
) -> str
```

## Parameters

- **client** (WorkspaceClient): Databricks workspace client instance
- **pipeline_id** (str): ID of the existing pipeline to validate
- **refresh_selection** (list, optional): List of specific tables to validate (if not all)
- **full_refresh_selection** (list, optional): List of tables to validate with full refresh logic
- **validate_only** (bool): If True, only validates without processing data (default: True)

## Returns

- **str**: Update ID for the dry-run validation update

## Basic Usage

### Simple Validation

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()

# Run dry-run validation
update_id = pl.dry_run(client, pipeline_id="abc123")

# Wait for validation to complete
result = pl.wait_for_update(client, "abc123", update_id)

if result.state.value == "COMPLETED":
    print("✓ Validation passed!")
else:
    print("✗ Validation failed:", result.state)
```

### Validate Specific Tables

```python
# Validate only specific tables
update_id = pl.dry_run(
    client,
    pipeline_id="abc123",
    refresh_selection=["bronze_customers", "silver_customers", "gold_summary"]
)

result = pl.wait_for_update(client, "abc123", update_id)
print(f"Validation state: {result.state}")
```

### Validate with Full Refresh Logic

```python
# Test full refresh logic for specific tables
update_id = pl.dry_run(
    client,
    pipeline_id="abc123",
    full_refresh_selection=["bronze_orders"]
)

result = pl.wait_for_update(client, "abc123", update_id)
print(f"Full refresh validation state: {result.state}")
```

## Complete Workflow: Validate Before Running

This is a recommended pattern for production pipelines:

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()
pipeline_id = "production-pipeline-id"

# Step 1: Run dry-run validation
print("Step 1: Validating pipeline...")
dry_run_update_id = pl.dry_run(client, pipeline_id)

# Step 2: Wait for validation
validation_result = pl.wait_for_update(
    client,
    pipeline_id=pipeline_id,
    update_id=dry_run_update_id,
    timeout_seconds=600  # 10 minutes
)

# Step 3: Check validation result
if validation_result.state.value == "COMPLETED":
    print("✓ Validation passed!")

    # Step 4: Run actual pipeline update
    print("Step 2: Running actual pipeline update...")
    actual_update_id = pl.start(client, pipeline_id)

    # Step 5: Wait for actual update
    actual_result = pl.wait_for_update(
        client,
        pipeline_id=pipeline_id,
        update_id=actual_update_id,
        timeout_seconds=3600  # 1 hour
    )

    print(f"✓ Pipeline update completed: {actual_result.state}")

else:
    print(f"✗ Validation failed: {validation_result.state}")
    if hasattr(validation_result, 'cause'):
        print(f"Error details: {validation_result.cause}")
    print("Aborting actual update due to validation failure.")
```

## CI/CD Integration

Use dry-run in your CI/CD pipelines to catch errors before deployment:

```python
import sys
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

def validate_pipeline(pipeline_id: str) -> bool:
    """Validate a pipeline and return True if valid, False otherwise."""
    client = get_workspace_client()

    # Run validation
    update_id = pl.dry_run(client, pipeline_id)

    # Wait for result
    result = pl.wait_for_update(
        client,
        pipeline_id=pipeline_id,
        update_id=update_id,
        timeout_seconds=600
    )

    # Return success/failure
    return result.state.value == "COMPLETED"


if __name__ == "__main__":
    pipeline_id = sys.argv[1]

    if validate_pipeline(pipeline_id):
        print("✓ Pipeline validation passed")
        sys.exit(0)
    else:
        print("✗ Pipeline validation failed")
        sys.exit(1)
```

## Error Handling

```python
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()

try:
    # Run dry-run
    update_id = pl.dry_run(client, pipeline_id="abc123")

    # Wait for completion
    result = pl.wait_for_update(client, "abc123", update_id)

    if result.state.value == "COMPLETED":
        print("✓ Validation successful")
    elif result.state.value == "FAILED":
        print("✗ Validation failed")
        if hasattr(result, 'cause'):
            print(f"Failure reason: {result.cause}")
    elif result.state.value == "CANCELED":
        print("⚠ Validation was canceled")

except TimeoutError:
    print("✗ Validation timed out")
except Exception as e:
    print(f"✗ Error during validation: {e}")
```

## Best Practices

1. **Always validate before production updates**: Run dry-run validation before starting actual pipeline updates in production.

2. **Set appropriate timeouts**: Validation can take time, especially for complex pipelines. Set timeouts based on your pipeline complexity:
   ```python
   result = pl.wait_for_update(client, pipeline_id, update_id, timeout_seconds=1800)
   ```

3. **Validate specific tables for faster feedback**: If you only changed specific tables, validate just those:
   ```python
   update_id = pl.dry_run(client, pipeline_id, refresh_selection=["changed_table"])
   ```

4. **Check validation results**: Don't just check if validation completed - also check for warnings or issues:
   ```python
   result = pl.wait_for_update(client, pipeline_id, update_id)
   if result.state.value == "COMPLETED":
       # Check for any warnings or notices
       updates = pl.get_pipeline_updates(client, pipeline_id)
       latest = updates[0]
       # Examine latest update for any issues
   ```

5. **Integrate with CI/CD**: Make validation a required step in your deployment pipeline.

## Comparison: dry_run vs start

| Feature | `dry_run` | `start` |
|---------|-----------|---------|
| Processes data | No | Yes |
| Materializes tables | No | Yes |
| Validates logic | Yes | Yes |
| Validates dependencies | Yes | Yes |
| Consumes compute | Minimal | Full |
| Use case | Testing/validation | Production runs |

## See Also

- [PIPELINES_API.md](PIPELINES_API.md) - Complete API reference
- [examples/dry_run_example.py](examples/dry_run_example.py) - Detailed examples
- [examples/pipeline_example.py](examples/pipeline_example.py) - Complete pipeline workflow
