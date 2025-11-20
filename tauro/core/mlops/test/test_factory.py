"""
Tests for MLOps Factory Pattern (P0: Local vs Databricks Mode Support).

Validates that factories correctly instantiate storage backends based on
execution mode configuration.
"""
import pytest
from unittest.mock import MagicMock

from tauro.core.mlops.factory import (
    StorageBackendFactory,
    ExperimentTrackerFactory,
    ModelRegistryFactory,
)
from tauro.core.mlops.storage import LocalStorageBackend, DatabricksStorageBackend


class MockContext:
    """Mock Context for testing factory pattern."""

    def __init__(self, mode: str, global_settings: dict = None):
        self.execution_mode = mode
        self.global_settings = global_settings or {}


class TestStorageBackendFactory:
    """Tests for StorageBackendFactory."""

    def test_create_local_backend_default(self):
        """Test creating local backend with default path."""
        context = MockContext(mode="local")

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, LocalStorageBackend)
        assert "mlruns" in str(backend.base_path)

    def test_create_local_backend_custom_path(self):
        """Test creating local backend with custom path."""
        context = MockContext(mode="local", global_settings={"mlops_path": "/custom/mlruns"})

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, LocalStorageBackend)
        # Normalize path for cross-platform comparison
        assert str(backend.base_path).replace("\\", "/").endswith("custom/mlruns")

    def test_create_local_backend_override_path(self):
        """Test creating local backend with override path."""
        context = MockContext(mode="local")

        backend = StorageBackendFactory.create_from_context(context, base_path="/override/path")

        assert isinstance(backend, LocalStorageBackend)
        # Normalize path for cross-platform comparison
        assert str(backend.base_path).replace("\\", "/").endswith("override/path")

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_create_databricks_backend(self, monkeypatch):
        """Test creating Databricks backend from context config."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        context = MockContext(
            mode="databricks",
            global_settings={
                "databricks": {
                    "host": "${DATABRICKS_HOST}",
                    "token": "${DATABRICKS_TOKEN}",
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                }
            },
        )

        # Mock databricks import to avoid dependency
        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, DatabricksStorageBackend)
        assert backend.catalog == "test_catalog"
        assert backend.schema == "test_schema"

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_create_databricks_backend_distributed_alias(self, monkeypatch):
        """Test that 'distributed' mode creates Databricks backend."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        context = MockContext(
            mode="distributed",
            global_settings={
                "databricks": {
                    "host": "${DATABRICKS_HOST}",
                    "token": "${DATABRICKS_TOKEN}",
                    "catalog": "main",
                    "schema": "ml_tracking",
                }
            },
        )

        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, DatabricksStorageBackend)
        assert backend.catalog == "main"
        assert backend.schema == "ml_tracking"

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_create_databricks_backend_with_kwargs(self, monkeypatch):
        """Test creating Databricks backend with kwargs override."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        context = MockContext(mode="databricks")

        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        backend = StorageBackendFactory.create_from_context(
            context,
            catalog="override_catalog",
            schema="override_schema",
            workspace_url="https://override.databricks.com",
            token="override_token",
        )

        assert isinstance(backend, DatabricksStorageBackend)
        assert backend.catalog == "override_catalog"
        assert backend.schema == "override_schema"
        assert backend.workspace_url == "https://override.databricks.com"
        assert backend.token == "override_token"

    def test_create_backend_invalid_mode(self):
        """Test that invalid mode raises ValueError."""
        context = MockContext(mode="invalid_mode")

        with pytest.raises(ValueError, match="Invalid execution mode"):
            StorageBackendFactory.create_from_context(context)

    def test_create_backend_no_mode_defaults_to_local(self):
        """Test that missing mode defaults to local."""
        context = MockContext(mode=None)

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, LocalStorageBackend)


class TestExperimentTrackerFactory:
    """Tests for ExperimentTrackerFactory."""

    def test_from_context_local(self):
        """Test creating ExperimentTracker in local mode."""
        context = MockContext(mode="local")

        tracker = ExperimentTrackerFactory.from_context(context)

        assert tracker is not None
        assert isinstance(tracker.storage, LocalStorageBackend)
        assert tracker.tracking_path == "experiment_tracking"
        assert tracker.metric_buffer_size == 100
        assert tracker.auto_flush_metrics is True

    def test_from_context_local_custom_params(self):
        """Test creating ExperimentTracker with custom parameters."""
        context = MockContext(mode="local", global_settings={"mlops_path": "/custom/path"})

        tracker = ExperimentTrackerFactory.from_context(
            context,
            tracking_path="custom_tracking",
            metric_buffer_size=50,
            auto_flush_metrics=False,
        )

        assert tracker is not None
        assert isinstance(tracker.storage, LocalStorageBackend)
        assert tracker.tracking_path == "custom_tracking"
        assert tracker.metric_buffer_size == 50
        assert tracker.auto_flush_metrics is False

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_from_context_databricks(self, monkeypatch):
        """Test creating ExperimentTracker in Databricks mode."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        context = MockContext(
            mode="databricks",
            global_settings={
                "databricks": {
                    "host": "${DATABRICKS_HOST}",
                    "token": "${DATABRICKS_TOKEN}",
                    "catalog": "main",
                    "schema": "ml_tracking",
                }
            },
        )

        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        tracker = ExperimentTrackerFactory.from_context(context)

        assert tracker is not None
        assert isinstance(tracker.storage, DatabricksStorageBackend)
        assert tracker.tracking_path == "experiment_tracking"


class TestModelRegistryFactory:
    """Tests for ModelRegistryFactory."""

    def test_from_context_local(self):
        """Test creating ModelRegistry in local mode."""
        context = MockContext(mode="local")

        registry = ModelRegistryFactory.from_context(context)

        assert registry is not None
        assert isinstance(registry.storage, LocalStorageBackend)
        assert registry.registry_path == "model_registry"

    def test_from_context_local_custom_params(self):
        """Test creating ModelRegistry with custom parameters."""
        context = MockContext(mode="local", global_settings={"mlops_path": "/custom/path"})

        registry = ModelRegistryFactory.from_context(context, registry_path="custom_registry")

        assert registry is not None
        assert isinstance(registry.storage, LocalStorageBackend)
        assert registry.registry_path == "custom_registry"

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_from_context_databricks(self, monkeypatch):
        """Test creating ModelRegistry in Databricks mode."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        context = MockContext(
            mode="databricks",
            global_settings={
                "databricks": {
                    "host": "${DATABRICKS_HOST}",
                    "token": "${DATABRICKS_TOKEN}",
                    "catalog": "main",
                    "schema": "ml_tracking",
                }
            },
        )

        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        registry = ModelRegistryFactory.from_context(context)

        assert registry is not None
        assert isinstance(registry.storage, DatabricksStorageBackend)
        assert registry.registry_path == "model_registry"


class TestConvenienceAliases:
    """Tests for convenience function aliases."""

    def test_create_storage_backend_alias(self):
        """Test create_storage_backend convenience function."""
        from tauro.core.mlops.factory import create_storage_backend

        context = MockContext(mode="local")

        backend = create_storage_backend(context)

        assert isinstance(backend, LocalStorageBackend)

    def test_create_experiment_tracker_alias(self):
        """Test create_experiment_tracker convenience function."""
        from tauro.core.mlops.factory import create_experiment_tracker

        context = MockContext(mode="local")

        tracker = create_experiment_tracker(context)

        assert tracker is not None
        assert isinstance(tracker.storage, LocalStorageBackend)

    def test_create_model_registry_alias(self):
        """Test create_model_registry convenience function."""
        from tauro.core.mlops.factory import create_model_registry

        context = MockContext(mode="local")

        registry = create_model_registry(context)

        assert registry is not None
        assert isinstance(registry.storage, LocalStorageBackend)


class TestModeDetection:
    """Tests for mode detection edge cases."""

    def test_mode_case_insensitive(self):
        """Test that mode detection is case-insensitive."""
        for mode in ["LOCAL", "Local", "local"]:
            context = MockContext(mode=mode)
            backend = StorageBackendFactory.create_from_context(context)
            assert isinstance(backend, LocalStorageBackend)

    @pytest.mark.skipif(
        True, reason="Databricks tests require databricks-sql-connector (optional dependency)"
    )
    def test_distributed_alias_case_insensitive(self, monkeypatch):
        """Test that 'distributed' alias works case-insensitively."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        mock_databricks = MagicMock()
        monkeypatch.setattr("tauro.core.mlops.storage.databricks", mock_databricks, raising=False)

        for mode in ["DISTRIBUTED", "Distributed", "distributed"]:
            context = MockContext(
                mode=mode,
                global_settings={
                    "databricks": {
                        "host": "https://test.databricks.com",
                        "token": "test_token",
                        "catalog": "main",
                        "schema": "ml_tracking",
                    }
                },
            )
            backend = StorageBackendFactory.create_from_context(context)
            assert isinstance(backend, DatabricksStorageBackend)

    def test_empty_global_settings(self):
        """Test that empty global_settings doesn't crash."""
        context = MockContext(mode="local", global_settings={})

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, LocalStorageBackend)

    def test_none_global_settings(self):
        """Test that None global_settings doesn't crash."""
        context = MockContext(mode="local")
        context.global_settings = None

        backend = StorageBackendFactory.create_from_context(context)

        assert isinstance(backend, LocalStorageBackend)
