"""Example usage of databricks_utils pipelines module."""

from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl


def main():
    # Get a workspace client
    client = get_workspace_client()

    # Example 1: Create a pipeline with a dictionary spec
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
                "node_type_id": "i3.xlarge",
            }
        ],
        "configuration": {
            "source_path": "/mnt/raw-data",
            "checkpoint_location": "/mnt/checkpoints",
        },
    }

    # Create the pipeline
    pipeline_id = pl.create(client, spec)
    print(f"Created pipeline: {pipeline_id}")

    # Example 2: Create a pipeline with a JSON string
    json_spec = """
    {
        "name": "json_pipeline",
        "storage": "/mnt/dlt-storage",
        "target": "prod",
        "continuous": true,
        "libraries": [
            {"notebook": {"path": "/Workspace/notebooks/pipeline_logic"}}
        ]
    }
    """
    pipeline_id = pl.create(client, json_spec)
    print(f"Created pipeline from JSON: {pipeline_id}")

    # Start the pipeline
    update_id = pl.start(client, pipeline_id)
    print(f"Started pipeline update: {update_id}")

    # Wait for completion
    completed_update = pl.wait_for_update(
        client,
        pipeline_id=pipeline_id,
        update_id=update_id,
        timeout_seconds=1800,  # 30 minutes
    )
    print(f"Pipeline update completed with state: {completed_update.state}")

    # Get pipeline status
    pipeline = pl.get_pipeline(client, pipeline_id)
    print(f"Pipeline {pipeline.name} is in state: {pipeline.state}")

    # List all pipelines
    all_pipelines = pl.list_pipelines(client)
    print(f"\nFound {len(all_pipelines)} pipelines:")
    for p in all_pipelines:
        print(f"  - {p.name}: {p.state}")

    # Get update history
    updates = pl.get_pipeline_updates(client, pipeline_id)
    print(f"\nPipeline has {len(updates)} updates:")
    for update in updates:
        print(f"  - Update {update.update_id}: {update.state}")

    # Update pipeline configuration
    pl.update(
        client,
        pipeline_id=pipeline_id,
        spec={"continuous": False, "target": "staging"},
    )
    print("Updated pipeline configuration")

    # Stop the pipeline if running
    pl.stop(client, pipeline_id)
    print("Stopped pipeline")


if __name__ == "__main__":
    main()
