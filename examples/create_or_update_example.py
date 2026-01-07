"""Example usage of create_or_update for idempotent pipeline deployment."""

from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl


def main():
    # Get workspace client
    client = get_workspace_client()

    # Define pipeline specification
    spec = {
        "name": "example_pipeline",
        "storage": "/mnt/pipeline-storage",
        "target": "dev",
        "continuous": False,
        "libraries": [
            {"notebook": {"path": "/Workspace/notebooks/bronze_ingestion"}},
            {"notebook": {"path": "/Workspace/notebooks/silver_transformation"}},
        ],
        "clusters": [
            {
                "label": "default",
                "num_workers": 2,
            }
        ],
    }

    # Example 1: First deployment - creates the pipeline
    print("=" * 70)
    print("Example 1: First deployment - creates pipeline")
    print("=" * 70)

    pipeline_id, was_created = pl.create_or_update(client, spec)

    if was_created:
        print(f"✓ Pipeline created: {pipeline_id}")
    else:
        print(f"✓ Pipeline updated: {pipeline_id}")

    # Example 2: Second deployment - updates the existing pipeline
    print("\n" + "=" * 70)
    print("Example 2: Second deployment - updates existing pipeline")
    print("=" * 70)

    # Modify the spec (e.g., change target or add configuration)
    spec["target"] = "staging"
    spec["configuration"] = {
        "source_database": "raw_data",
        "target_database": "processed_data"
    }

    pipeline_id, was_created = pl.create_or_update(client, spec)

    if was_created:
        print(f"✓ Pipeline created: {pipeline_id}")
    else:
        print(f"✓ Pipeline updated: {pipeline_id}")
        print("  Configuration changes applied!")

    # Example 3: Using kwargs to override spec values
    print("\n" + "=" * 70)
    print("Example 3: Using kwargs to override spec")
    print("=" * 70)

    # You can override spec values using kwargs
    pipeline_id, was_created = pl.create_or_update(
        client,
        spec,
        continuous=True,  # Override continuous flag
        target="prod"     # Override target
    )

    print(f"✓ Pipeline {'created' if was_created else 'updated'}: {pipeline_id}")
    print("  Applied overrides: continuous=True, target=prod")

    # Example 4: Idempotent deployment workflow
    print("\n" + "=" * 70)
    print("Example 4: Idempotent deployment workflow")
    print("=" * 70)

    # This workflow is safe to run multiple times
    deployment_spec = {
        "name": "production_etl_pipeline",
        "storage": "/mnt/prod-pipeline-storage",
        "target": "prod",
        "continuous": True,
        "libraries": [
            {"notebook": {"path": "/Workspace/production/bronze"}},
            {"notebook": {"path": "/Workspace/production/silver"}},
            {"notebook": {"path": "/Workspace/production/gold"}},
        ],
        "clusters": [
            {
                "label": "default",
                "num_workers": 4,
                "node_type_id": "i3.xlarge",
            }
        ],
    }

    print("Step 1: Deploy or update pipeline...")
    pipeline_id, was_created = pl.create_or_update(client, deployment_spec)

    action = "created" if was_created else "updated"
    print(f"  ✓ Pipeline {action}: {pipeline_id}")

    print("\nStep 2: Validate with dry-run...")
    dry_run_update_id = pl.dry_run(client, pipeline_id)
    validation_result = pl.wait_for_update(
        client,
        pipeline_id,
        dry_run_update_id,
        timeout_seconds=600
    )

    if validation_result.state.value == "COMPLETED":
        print("  ✓ Validation passed!")

        print("\nStep 3: Start pipeline if needed...")
        # Get current pipeline state
        pipeline = pl.get_pipeline(client, pipeline_id)

        if pipeline.state == "IDLE":
            print("  Pipeline is idle, starting update...")
            update_id = pl.start(client, pipeline_id)
            print(f"  ✓ Update started: {update_id}")
        else:
            print(f"  Pipeline is already running (state: {pipeline.state})")
    else:
        print(f"  ✗ Validation failed: {validation_result.state}")

    # Example 5: JSON string spec
    print("\n" + "=" * 70)
    print("Example 5: Using JSON string spec")
    print("=" * 70)

    json_spec = '''
    {
        "name": "json_pipeline",
        "storage": "/mnt/json-storage",
        "target": "dev",
        "libraries": [
            {"notebook": {"path": "/Workspace/notebooks/json_etl"}}
        ]
    }
    '''

    pipeline_id, was_created = pl.create_or_update(client, json_spec)
    print(f"✓ Pipeline {'created' if was_created else 'updated'}: {pipeline_id}")

    # Example 6: CI/CD deployment pattern
    print("\n" + "=" * 70)
    print("Example 6: CI/CD deployment pattern")
    print("=" * 70)

    def deploy_pipeline(environment: str):
        """Deploy pipeline to specified environment."""
        spec = {
            "name": f"etl_pipeline_{environment}",
            "storage": f"/mnt/{environment}-pipeline-storage",
            "target": environment,
            "libraries": [
                {"notebook": {"path": f"/Workspace/{environment}/etl"}}
            ],
        }

        print(f"\nDeploying to {environment}...")
        pipeline_id, was_created = pl.create_or_update(client, spec)

        action = "Created" if was_created else "Updated"
        print(f"  {action} pipeline: {pipeline_id}")

        # Validate deployment
        print(f"  Validating {environment} pipeline...")
        dry_run_id = pl.dry_run(client, pipeline_id)
        result = pl.wait_for_update(client, pipeline_id, dry_run_id, timeout_seconds=300)

        if result.state.value == "COMPLETED":
            print(f"  ✓ {environment.upper()} deployment successful!")
            return pipeline_id
        else:
            print(f"  ✗ {environment.upper()} validation failed!")
            return None

    # Deploy to multiple environments
    for env in ["dev", "staging", "prod"]:
        deploy_pipeline(env)

    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
