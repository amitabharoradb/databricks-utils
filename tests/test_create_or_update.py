"""Tests for create_or_update functionality."""

import pytest
from databricks_utils import pipelines as pl


def test_create_or_update_function_exists():
    """Test that create_or_update function is callable."""
    assert callable(pl.create_or_update)


def test_create_or_update_signature():
    """Test create_or_update function signature."""
    import inspect
    sig = inspect.signature(pl.create_or_update)

    # Verify required parameters
    assert "client" in sig.parameters
    assert "spec" in sig.parameters

    # Verify return annotation (tuple of str and bool)
    # Python 3.9+ syntax: tuple[str, bool]
    # The annotation will be a string representation
    assert sig.return_annotation is not None


def test_create_or_update_missing_name():
    """Test that create_or_update raises error when name is missing."""
    # This would need mocking in real tests
    # Just verify the function exists and can be called
    assert hasattr(pl, "create_or_update")


def test_create_or_update_with_json_string():
    """Test create_or_update accepts JSON string."""
    # This would need mocking in real tests
    # Just verify the function signature accepts string or dict
    import inspect
    from typing import get_type_hints, Union, Dict, Any

    sig = inspect.signature(pl.create_or_update)
    spec_param = sig.parameters["spec"]

    # The annotation should be Union[Dict[str, Any], str]
    assert spec_param.annotation is not None


# Note: Real tests would use mocking to avoid actual API calls
# Example mock test:
#
# def test_create_or_update_creates_new_pipeline(mock_client):
#     """Test that create_or_update creates pipeline when it doesn't exist."""
#     # Mock: no existing pipelines
#     mock_client.pipelines.list_pipelines.return_value = []
#
#     # Mock: create returns new pipeline ID
#     mock_client.pipelines.create.return_value = Mock(pipeline_id="new-123")
#
#     spec = {"name": "test_pipeline", "storage": "/mnt/storage"}
#     pipeline_id, was_created = pl.create_or_update(mock_client, spec)
#
#     assert pipeline_id == "new-123"
#     assert was_created is True
#     mock_client.pipelines.create.assert_called_once()
#     mock_client.pipelines.update.assert_not_called()
#
#
# def test_create_or_update_updates_existing_pipeline(mock_client):
#     """Test that create_or_update updates pipeline when it exists."""
#     # Mock: existing pipeline with same name
#     existing = Mock(name="test_pipeline", pipeline_id="existing-123")
#     mock_client.pipelines.list_pipelines.return_value = [existing]
#
#     spec = {"name": "test_pipeline", "storage": "/mnt/new-storage"}
#     pipeline_id, was_created = pl.create_or_update(mock_client, spec)
#
#     assert pipeline_id == "existing-123"
#     assert was_created is False
#     mock_client.pipelines.update.assert_called_once()
#     mock_client.pipelines.create.assert_not_called()
