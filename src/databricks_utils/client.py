"""Databricks client utilities."""

from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


def get_workspace_client(
    host: Optional[str] = None,
    token: Optional[str] = None,
    profile: Optional[str] = None,
    **kwargs
) -> WorkspaceClient:
    """
    Create a WorkspaceClient with simplified configuration.

    Args:
        host: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
        token: Personal access token for authentication
        profile: Profile name from ~/.databrickscfg
        **kwargs: Additional configuration options passed to WorkspaceClient

    Returns:
        WorkspaceClient: Configured Databricks workspace client

    Examples:
        >>> # Using default profile from ~/.databrickscfg
        >>> client = get_workspace_client()

        >>> # Using specific profile
        >>> client = get_workspace_client(profile="prod")

        >>> # Using explicit credentials
        >>> client = get_workspace_client(
        ...     host="https://your-workspace.cloud.databricks.com",
        ...     token="your-token"
        ... )
    """
    config_kwargs = {}

    if host:
        config_kwargs["host"] = host
    if token:
        config_kwargs["token"] = token
    if profile:
        config_kwargs["profile"] = profile

    config_kwargs.update(kwargs)

    return WorkspaceClient(**config_kwargs)


def validate_connection(client: WorkspaceClient) -> bool:
    """
    Validate that the client can connect to the workspace.

    Args:
        client: WorkspaceClient instance to validate

    Returns:
        bool: True if connection is successful, False otherwise

    Examples:
        >>> client = get_workspace_client()
        >>> if validate_connection(client):
        ...     print("Connected successfully!")
    """
    try:
        # Try to get current user info as a connection test
        client.current_user.me()
        return True
    except Exception:
        return False
