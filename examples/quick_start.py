"""Quick start example for databricks_utils package."""

# Note: Install the package first with:
#   pip install databricks-utils
# or
#   uv add databricks-utils
#
# Then import using underscore (not hyphen):

from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

# Get a client
client = get_workspace_client()

# Create a pipeline from JSON spec
spec = {
    "name": "my_pipeline",
    "storage": "/mnt/pipeline-storage",
    "target": "dev",
    "libraries": [
        {"notebook": {"path": "/Workspace/notebooks/my_notebook"}}
    ]
}

# Use the pipelines module
pipeline_id = pl.create(client, spec)
print(f"Created pipeline: {pipeline_id}")

# Start the pipeline
update_id = pl.start(client, pipeline_id)
print(f"Started update: {update_id}")
