"""Tests for pipeline dry_run functionality."""

import pytest
from databricks_utils import pipelines as pl


def test_dry_run_function_exists():
    """Test that dry_run function is callable."""
    assert callable(pl.dry_run)


def test_dry_run_signature():
    """Test dry_run function signature."""
    import inspect
    sig = inspect.signature(pl.dry_run)

    # Verify required parameters
    assert "client" in sig.parameters
    assert "pipeline_id" in sig.parameters

    # Verify optional parameters
    assert "refresh_selection" in sig.parameters
    assert "full_refresh_selection" in sig.parameters
    assert "validate_only" in sig.parameters

    # Verify defaults
    assert sig.parameters["refresh_selection"].default is None
    assert sig.parameters["full_refresh_selection"].default is None
    assert sig.parameters["validate_only"].default is True


def test_dry_run_return_type():
    """Test that dry_run is documented to return a string (update_id)."""
    import inspect
    sig = inspect.signature(pl.dry_run)

    # Check return annotation
    assert sig.return_annotation == str


# Note: Real tests would use mocking to avoid actual API calls
# Example mock test:
# def test_dry_run_calls_api(mock_client):
#     """Test that dry_run calls the correct API method."""
#     mock_client.pipelines.start_update.return_value = Mock(update_id="test-123")
#
#     update_id = pl.dry_run(mock_client, "pipeline-123")
#
#     assert update_id == "test-123"
#     mock_client.pipelines.start_update.assert_called_once()
#     call_kwargs = mock_client.pipelines.start_update.call_args[1]
#     assert call_kwargs["pipeline_id"] == "pipeline-123"
#     assert call_kwargs["validate_only"] is True
