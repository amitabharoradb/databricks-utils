"""Tests for client utilities."""

import pytest
from databricks_utils.client import get_workspace_client, validate_connection


def test_get_workspace_client_default():
    """Test creating client with default configuration."""
    # This will use default authentication from environment/config
    # In a real test, you'd mock the WorkspaceClient
    client = get_workspace_client()
    assert client is not None


def test_get_workspace_client_with_profile():
    """Test creating client with specific profile."""
    # This would use a profile from ~/.databrickscfg
    # In a real test, you'd mock the configuration
    client = get_workspace_client(profile="test")
    assert client is not None


def test_get_workspace_client_with_credentials():
    """Test creating client with explicit credentials."""
    client = get_workspace_client(
        host="https://test.cloud.databricks.com",
        token="test-token"
    )
    assert client is not None
    assert client.config.host == "https://test.cloud.databricks.com"


# Add more tests as needed
# Note: Real tests would use mocking to avoid actual API calls
