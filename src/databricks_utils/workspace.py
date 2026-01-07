"""Utilities for working with Databricks workspace objects."""

from typing import Optional, List
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo, ObjectType


def list_notebooks(
    client: WorkspaceClient,
    path: str,
    recursive: bool = False
) -> List[ObjectInfo]:
    """
    List notebooks in a workspace path.

    Args:
        client: WorkspaceClient instance
        path: Workspace path to list notebooks from
        recursive: Whether to list notebooks recursively

    Returns:
        List of ObjectInfo for notebooks

    Examples:
        >>> client = get_workspace_client()
        >>> notebooks = list_notebooks(client, "/Users/user@example.com")
        >>> for nb in notebooks:
        ...     print(f"{nb.path}: {nb.object_type}")
    """
    try:
        objects = list(client.workspace.list(path=path))
        notebooks = [
            obj for obj in objects
            if obj.object_type == ObjectType.NOTEBOOK
        ]

        if recursive:
            directories = [
                obj for obj in objects
                if obj.object_type == ObjectType.DIRECTORY
            ]
            for directory in directories:
                notebooks.extend(
                    list_notebooks(client, directory.path, recursive=True)
                )

        return notebooks
    except Exception:
        return []


def export_notebook(
    client: WorkspaceClient,
    workspace_path: str,
    local_path: str,
    format: str = "SOURCE"
) -> bool:
    """
    Export a notebook from workspace to local filesystem.

    Args:
        client: WorkspaceClient instance
        workspace_path: Path to notebook in workspace
        local_path: Local filesystem path to save notebook
        format: Export format (SOURCE, HTML, JUPYTER, DBC)

    Returns:
        bool: True if export successful, False otherwise

    Examples:
        >>> client = get_workspace_client()
        >>> success = export_notebook(
        ...     client,
        ...     "/Users/user@example.com/my_notebook",
        ...     "./local_notebook.py"
        ... )
    """
    try:
        from databricks.sdk.service.workspace import ExportFormat

        format_enum = ExportFormat[format.upper()]

        exported = client.workspace.export(
            path=workspace_path,
            format=format_enum
        )

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        if exported.content:
            import base64
            content = base64.b64decode(exported.content)
            with open(local_path, "wb") as f:
                f.write(content)
            return True

        return False
    except Exception:
        return False


def import_notebook(
    client: WorkspaceClient,
    local_path: str,
    workspace_path: str,
    language: Optional[str] = None,
    overwrite: bool = False
) -> bool:
    """
    Import a notebook from local filesystem to workspace.

    Args:
        client: WorkspaceClient instance
        local_path: Local filesystem path to notebook
        workspace_path: Path to save notebook in workspace
        language: Notebook language (PYTHON, SCALA, SQL, R)
        overwrite: Whether to overwrite existing notebook

    Returns:
        bool: True if import successful, False otherwise

    Examples:
        >>> client = get_workspace_client()
        >>> success = import_notebook(
        ...     client,
        ...     "./local_notebook.py",
        ...     "/Users/user@example.com/my_notebook",
        ...     language="PYTHON"
        ... )
    """
    try:
        import base64
        from databricks.sdk.service.workspace import ImportFormat, Language

        with open(local_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")

        kwargs = {
            "path": workspace_path,
            "content": content,
            "format": ImportFormat.SOURCE,
            "overwrite": overwrite,
        }

        if language:
            kwargs["language"] = Language[language.upper()]

        client.workspace.import_(**kwargs)
        return True
    except Exception:
        return False
