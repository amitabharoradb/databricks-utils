"""Utilities to simplify Databricks SDK integration."""

__version__ = "0.1.0"

# Always available: pipelines module (dry_run doesn't need databricks-sdk)
from databricks_utils import pipelines

__all__ = [
    "__version__",
    "pipelines",
]

# Optional imports that require databricks-sdk
try:
    from databricks_utils.client import get_workspace_client, validate_connection
    from databricks_utils.jobs import wait_for_run, get_run_output, list_failed_runs
    from databricks_utils.workspace import (
        list_notebooks,
        export_notebook,
        import_notebook,
    )

    __all__.extend([
        # Client utilities
        "get_workspace_client",
        "validate_connection",
        # Job utilities
        "wait_for_run",
        "get_run_output",
        "list_failed_runs",
        # Workspace utilities
        "list_notebooks",
        "export_notebook",
        "import_notebook",
    ])
except ImportError:
    # databricks-sdk not installed, only pipelines.dry_run will work
    pass
