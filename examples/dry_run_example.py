"""Example usage of the dry_run function for running pipelines in validation mode."""

from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl


def main():
    # Get workspace client
    client = get_workspace_client()

    # Assume you have an existing pipeline
    pipeline_id = "your-pipeline-id-here"  # Replace with actual pipeline ID

    # Example 1: Basic dry-run (validation only)
    print("=" * 70)
    print("Example 1: Basic dry-run - validate pipeline without processing data")
    print("=" * 70)

    # Start a dry-run update that validates the pipeline
    update_id = pl.dry_run(client, pipeline_id)
    print(f"✓ Dry-run update started: {update_id}")

    # Wait for the validation to complete
    print("  Waiting for validation to complete...")
    result = pl.wait_for_update(
        client,
        pipeline_id=pipeline_id,
        update_id=update_id,
        timeout_seconds=600  # 10 minutes
    )

    print(f"  Validation state: {result.state}")
    if hasattr(result, 'cause') and result.cause:
        print(f"  Details: {result.cause}")

    # Example 2: Dry-run specific tables only
    print("\n" + "=" * 70)
    print("Example 2: Dry-run specific tables")
    print("=" * 70)

    # Validate only specific tables
    update_id = pl.dry_run(
        client,
        pipeline_id=pipeline_id,
        refresh_selection=["bronze_customers", "silver_customers"]
    )
    print(f"✓ Dry-run started for selected tables: {update_id}")

    result = pl.wait_for_update(client, pipeline_id, update_id)
    print(f"  Validation state: {result.state}")

    # Example 3: Dry-run with full refresh for specific tables
    print("\n" + "=" * 70)
    print("Example 3: Dry-run with full refresh selection")
    print("=" * 70)

    # Validate with full refresh logic for specific tables
    update_id = pl.dry_run(
        client,
        pipeline_id=pipeline_id,
        full_refresh_selection=["bronze_orders"]
    )
    print(f"✓ Dry-run with full refresh started: {update_id}")

    result = pl.wait_for_update(client, pipeline_id, update_id)
    print(f"  Validation state: {result.state}")

    # Example 4: Complete workflow - validate before running
    print("\n" + "=" * 70)
    print("Example 4: Complete workflow - validate then run")
    print("=" * 70)

    # First, validate the pipeline
    print("Step 1: Running dry-run validation...")
    dry_run_update_id = pl.dry_run(client, pipeline_id)
    dry_run_result = pl.wait_for_update(
        client,
        pipeline_id,
        dry_run_update_id,
        timeout_seconds=600
    )

    if dry_run_result.state.value == "COMPLETED":
        print("  ✓ Validation passed!")

        # If validation succeeded, run the actual pipeline
        print("\nStep 2: Running actual pipeline update...")
        actual_update_id = pl.start(client, pipeline_id)
        print(f"  ✓ Actual update started: {actual_update_id}")

        # Wait for completion
        actual_result = pl.wait_for_update(
            client,
            pipeline_id,
            actual_update_id,
            timeout_seconds=3600  # 1 hour
        )
        print(f"  ✓ Update completed: {actual_result.state}")
    else:
        print(f"  ✗ Validation failed: {dry_run_result.state}")
        if hasattr(dry_run_result, 'cause'):
            print(f"  Error details: {dry_run_result.cause}")
        print("  Skipping actual update due to validation failure.")

    # Example 5: Get pipeline information
    print("\n" + "=" * 70)
    print("Example 5: Get pipeline and update information")
    print("=" * 70)

    # Get pipeline details
    pipeline = pl.get_pipeline(client, pipeline_id)
    print(f"Pipeline: {pipeline.name}")
    print(f"State: {pipeline.state}")

    # Get recent updates
    updates = pl.get_pipeline_updates(client, pipeline_id, max_results=5)
    print(f"\nRecent updates:")
    for update in updates:
        print(f"  - Update {update.update_id}: {update.state}")


if __name__ == "__main__":
    main()
