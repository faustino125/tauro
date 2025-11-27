"""Unit tests for protocols module.

Tests the Protocol interfaces to ensure they define the correct contracts
and can be used for type checking with structural subtyping.
"""

import pytest
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import datetime

from engine.mlops.protocols import (
    StorageBackendProtocol,
    StorageMetadataProtocol,
    ExperimentTrackerProtocol,
    ExperimentProtocol,
    RunProtocol,
    RunStatusProtocol,
    ModelRegistryProtocol,
    ModelVersionProtocol,
    ModelMetadataProtocol,
    ModelStageProtocol,
    EventEmitterProtocol,
    EventCallback,
    LockProtocol,
    SerializableProtocol,
    ValidatorProtocol,
    MLOpsContextProtocol,
    BaseConfig,
)
from engine.mlops.events import EventType


class TestStorageBackendProtocol:
    """Tests for StorageBackendProtocol interface."""

    def test_protocol_defines_required_methods(self):
        """Verify protocol has expected method signatures."""
        assert hasattr(StorageBackendProtocol, "write_dataframe")
        assert hasattr(StorageBackendProtocol, "read_dataframe")
        assert hasattr(StorageBackendProtocol, "write_json")
        assert hasattr(StorageBackendProtocol, "read_json")
        assert hasattr(StorageBackendProtocol, "write_artifact")
        assert hasattr(StorageBackendProtocol, "read_artifact")
        assert hasattr(StorageBackendProtocol, "exists")
        assert hasattr(StorageBackendProtocol, "delete")
        assert hasattr(StorageBackendProtocol, "list_paths")
        assert hasattr(StorageBackendProtocol, "get_stats")

    def test_mock_implementation_conforms(self):
        """Test that a mock class conforms to protocol."""

        class MockStorage:
            """Mock storage for testing protocol conformance."""

            def write_dataframe(
                self, df: Any, path: str, mode: str = "overwrite"
            ) -> StorageMetadataProtocol:
                """Write dataframe mock."""
                return StorageMetadataProtocol(path=path, created_at="", updated_at="")

            def read_dataframe(self, path: str) -> Any:
                """Read dataframe mock."""
                return None

            def write_json(
                self, data: Dict[str, Any], path: str, mode: str = "overwrite"
            ) -> StorageMetadataProtocol:
                """Write JSON mock."""
                return StorageMetadataProtocol(path=path, created_at="", updated_at="")

            def read_json(self, path: str) -> Dict[str, Any]:
                """Read JSON mock."""
                return {}

            def write_artifact(
                self, artifact_path: str, destination: str, mode: str = "overwrite"
            ) -> StorageMetadataProtocol:
                """Write artifact mock."""
                return StorageMetadataProtocol(path=destination, created_at="", updated_at="")

            def read_artifact(self, path: str, local_destination: str) -> None:
                """Read artifact mock."""
                pass  # Mock implementation

            def exists(self, path: str) -> bool:
                """Exists mock."""
                return True

            def delete(self, path: str) -> None:
                """Delete mock."""
                pass  # Mock implementation

            def list_paths(self, prefix: str) -> List[str]:
                """List paths mock."""
                return []

            def get_stats(self) -> Dict[str, Any]:
                """Get stats mock."""
                return {}

        storage = MockStorage()
        # Should be usable as StorageBackendProtocol
        assert isinstance(storage, StorageBackendProtocol)


class TestExperimentTrackerProtocol:
    """Tests for ExperimentTrackerProtocol interface."""

    def test_protocol_defines_experiment_methods(self):
        """Verify protocol has experiment tracking methods."""
        assert hasattr(ExperimentTrackerProtocol, "create_experiment")
        assert hasattr(ExperimentTrackerProtocol, "start_run")
        assert hasattr(ExperimentTrackerProtocol, "end_run")
        assert hasattr(ExperimentTrackerProtocol, "get_run")
        assert hasattr(ExperimentTrackerProtocol, "list_runs")

    def test_protocol_defines_logging_methods(self):
        """Verify protocol has logging methods."""
        assert hasattr(ExperimentTrackerProtocol, "log_metric")
        assert hasattr(ExperimentTrackerProtocol, "log_parameter")
        assert hasattr(ExperimentTrackerProtocol, "log_artifact")


class TestModelRegistryProtocol:
    """Tests for ModelRegistryProtocol interface."""

    def test_protocol_defines_model_methods(self):
        """Verify protocol has model registration methods."""
        assert hasattr(ModelRegistryProtocol, "register_model")
        assert hasattr(ModelRegistryProtocol, "get_model_version")
        assert hasattr(ModelRegistryProtocol, "get_model_by_stage")
        assert hasattr(ModelRegistryProtocol, "list_models")
        assert hasattr(ModelRegistryProtocol, "list_model_versions")

    def test_protocol_defines_version_methods(self):
        """Verify protocol has version management methods."""
        assert hasattr(ModelRegistryProtocol, "promote_model")
        assert hasattr(ModelRegistryProtocol, "download_artifact")
        assert hasattr(ModelRegistryProtocol, "delete_model_version")


class TestEventEmitterProtocol:
    """Tests for EventEmitterProtocol interface."""

    def test_protocol_defines_event_methods(self):
        """Verify protocol has event methods."""
        assert hasattr(EventEmitterProtocol, "on")
        assert hasattr(EventEmitterProtocol, "off")
        assert hasattr(EventEmitterProtocol, "emit")


class TestLockProtocol:
    """Tests for LockProtocol interface."""

    def test_protocol_defines_lock_methods(self):
        """Verify protocol has lock management methods."""
        assert hasattr(LockProtocol, "acquire")
        assert hasattr(LockProtocol, "release")
        assert hasattr(LockProtocol, "is_acquired")

    def test_protocol_defines_context_manager(self):
        """Verify protocol supports context manager pattern."""
        assert hasattr(LockProtocol, "__enter__")
        assert hasattr(LockProtocol, "__exit__")


class TestEventType:
    """Tests for EventType enum."""

    def test_event_type_values(self):
        """Verify EventType enum has expected values."""
        assert EventType.EXPERIMENT_CREATED == "experiment.created"
        assert EventType.RUN_STARTED == "run.started"
        assert EventType.RUN_ENDED == "run.ended"
        assert EventType.MODEL_REGISTERED == "model.registered"
        assert EventType.MODEL_PROMOTED == "model.promoted"

    def test_all_event_types_are_strings(self):
        """Verify all event types are string values."""
        for event_type in EventType:
            assert isinstance(event_type.value, str)


class TestModelStageProtocol:
    """Tests for ModelStageProtocol enum."""

    def test_stage_values(self):
        """Verify model stages have expected values."""
        assert ModelStageProtocol.STAGING == "Staging"
        assert ModelStageProtocol.PRODUCTION == "Production"
        assert ModelStageProtocol.ARCHIVED == "Archived"


class TestRunStatusProtocol:
    """Tests for RunStatusProtocol enum."""

    def test_status_values(self):
        """Verify run status values."""
        assert RunStatusProtocol.RUNNING == "RUNNING"
        assert RunStatusProtocol.COMPLETED == "COMPLETED"
        assert RunStatusProtocol.FAILED == "FAILED"
        assert RunStatusProtocol.SCHEDULED == "SCHEDULED"


class TestBaseConfig:
    """Tests for BaseConfig dataclass."""

    def test_base_config_to_dict(self):
        """Test BaseConfig to_dict method."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class TestConfig(BaseConfig):
            name: str
            value: int

        config = TestConfig(name="test", value=42)
        result = config.to_dict()

        assert result == {"name": "test", "value": 42}

    def test_base_config_from_dict(self):
        """Test BaseConfig from_dict class method."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class TestConfig(BaseConfig):
            name: str
            value: int

        data = {"name": "test", "value": 42}
        config = TestConfig.from_dict(data)

        assert config.name == "test"
        assert config.value == 42


class TestProtocolInteroperability:
    """Test protocol interoperability and typing."""

    def test_protocols_are_runtime_checkable(self):
        """Verify protocols can be used for isinstance checks at runtime."""
        # All main protocols should be runtime checkable
        assert hasattr(StorageBackendProtocol, "_is_protocol")
        assert hasattr(ExperimentTrackerProtocol, "_is_protocol")
        assert hasattr(ModelRegistryProtocol, "_is_protocol")
        assert hasattr(LockProtocol, "_is_protocol")
        assert hasattr(EventEmitterProtocol, "_is_protocol")

    def test_storage_metadata_protocol_is_dataclass(self):
        """Test StorageMetadataProtocol is a proper dataclass."""
        metadata = StorageMetadataProtocol(
            path="/test/path",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
            size_bytes=1024,
            format="parquet",
        )

        assert metadata.path == "/test/path"
        assert metadata.size_bytes == 1024
        assert metadata.format == "parquet"

    def test_mock_event_callback_conforms(self):
        """Test that a function can be used as EventCallback."""
        events_received = []

        def my_callback(event_type: str, data: Dict[str, Any]) -> None:
            events_received.append((event_type, data))

        # Should be callable with expected signature
        my_callback("test.event", {"key": "value"})

        assert len(events_received) == 1
        assert events_received[0][0] == "test.event"
        assert events_received[0][1] == {"key": "value"}
