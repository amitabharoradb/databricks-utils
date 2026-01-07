"""Tests for pipeline utilities."""

import pytest
from databricks_utils import pipelines as pl


def test_pipelines_module_import():
    """Test that pipelines module can be imported."""
    assert pl is not None
    assert hasattr(pl, "create")
    assert hasattr(pl, "update")
    assert hasattr(pl, "delete")
    assert hasattr(pl, "start")
    assert hasattr(pl, "stop")
    assert hasattr(pl, "get_pipeline")
    assert hasattr(pl, "list_pipelines")
    assert hasattr(pl, "get_pipeline_updates")
    assert hasattr(pl, "wait_for_update")


def test_create_function_exists():
    """Test that create function is callable."""
    assert callable(pl.create)


def test_create_with_dict_spec():
    """Test create function signature with dict spec."""
    # This would need mocking in a real test
    # Just verifying the function exists and has correct signature
    import inspect
    sig = inspect.signature(pl.create)
    assert "client" in sig.parameters
    assert "spec" in sig.parameters


def test_create_with_json_spec():
    """Test create function can accept JSON string."""
    import inspect
    sig = inspect.signature(pl.create)
    # Verify spec parameter exists (can be dict or string)
    assert "spec" in sig.parameters


# Add more tests as needed
# Note: Real tests would use mocking to avoid actual API calls
