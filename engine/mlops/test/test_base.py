"""Unit tests for base module.

Tests the base classes, mixins, and utility functions
used across the MLOps framework.
"""

import pytest
import time
import os
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from engine.mlops.base import (
    ComponentState,
    ComponentStats,
    BaseMLOpsComponent,
    IndexManagerMixin,
    ValidationMixin,
    PathManager,
    now_iso,
    parse_iso,
    age_seconds,
)


class TestComponentState:
    """Tests for ComponentState enum."""

    def test_all_states_defined(self):
        """Verify all component states exist."""
        assert ComponentState.CREATED is not None
        assert ComponentState.INITIALIZED is not None
        assert ComponentState.RUNNING is not None
        assert ComponentState.STOPPED is not None
        assert ComponentState.ERROR is not None

    def test_state_values(self):
        """Test state string values."""
        assert ComponentState.CREATED.value == "created"
        assert ComponentState.INITIALIZED.value == "initialized"
        assert ComponentState.RUNNING.value == "running"
        assert ComponentState.STOPPED.value == "stopped"
        assert ComponentState.ERROR.value == "error"


class TestComponentStats:
    """Tests for ComponentStats dataclass."""

    def test_stats_creation(self):
        """Test creating component stats."""
        stats = ComponentStats(
            operations=100,
            errors=5,
            last_operation_time="2024-01-01T00:00:00Z",
            total_duration_seconds=150.5,
        )

        assert stats.operations == 100
        assert stats.errors == 5
        assert stats.total_duration_seconds == 150.5

    def test_error_rate(self):
        """Test error rate calculation."""
        stats = ComponentStats(operations=100, errors=10)

        assert stats.error_rate == 0.1

    def test_error_rate_zero_operations(self):
        """Test error rate with zero operations."""
        stats = ComponentStats(operations=0, errors=0)

        assert stats.error_rate == 0.0

    def test_average_duration(self):
        """Test average duration calculation."""
        stats = ComponentStats(operations=10, total_duration_seconds=100.0)

        assert stats.average_duration == 10.0

    def test_average_duration_zero_operations(self):
        """Test average duration with zero operations."""
        stats = ComponentStats(operations=0, total_duration_seconds=0)

        assert stats.average_duration == 0.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stats = ComponentStats(
            operations=100,
            errors=5,
            last_operation_time="2024-01-01T00:00:00Z",
            total_duration_seconds=150.5,
        )

        result = stats.to_dict()

        assert result["operations"] == 100
        assert result["errors"] == 5
        assert "error_rate" in result
        assert "average_duration" in result


class ConcreteComponent(BaseMLOpsComponent):
    """Concrete implementation for testing BaseMLOpsComponent."""

    def initialize(self) -> None:
        """Initialize the component."""
        self._state = ComponentState.INITIALIZED

    def shutdown(self) -> None:
        """Shutdown the component."""
        self._state = ComponentState.STOPPED


class TestBaseMLOpsComponent:
    """Tests for BaseMLOpsComponent base class."""

    def test_component_creation(self):
        """Test creating a component."""
        component = ConcreteComponent(name="test_component")

        assert component.name == "test_component"
        assert component.state == ComponentState.CREATED

    def test_component_initialization(self):
        """Test component initialization."""
        component = ConcreteComponent(name="test")
        component.initialize()

        assert component.state == ComponentState.INITIALIZED

    def test_component_shutdown(self):
        """Test component shutdown."""
        component = ConcreteComponent(name="test")
        component.initialize()
        component.shutdown()

        assert component.state == ComponentState.STOPPED

    def test_record_operation(self):
        """Test recording an operation."""
        component = ConcreteComponent(name="test")

        component.record_operation(duration=1.5)

        stats = component.get_stats()
        assert stats.operations == 1
        assert stats.total_duration_seconds >= 1.5

    def test_record_error(self):
        """Test recording an error."""
        component = ConcreteComponent(name="test")

        component.record_operation(duration=1.0)
        component.record_error(Exception("Test error"))

        stats = component.get_stats()
        assert stats.errors == 1

    def test_get_stats(self):
        """Test getting component statistics."""
        component = ConcreteComponent(name="test")

        for _ in range(5):
            component.record_operation(duration=1.0)
        component.record_error(Exception("Error"))

        stats = component.get_stats()

        assert stats.operations == 5
        assert stats.errors == 1
        assert stats.error_rate == 0.2

    def test_component_context_manager(self):
        """Test using component as context manager."""
        component = ConcreteComponent(name="test")

        with component as ctx:
            assert ctx.state == ComponentState.INITIALIZED

        assert component.state == ComponentState.STOPPED


class IndexedStorage(IndexManagerMixin):
    """Test implementation using IndexManagerMixin."""

    def __init__(self):
        self._index = {}
        super().__init__()

    def _get_index_path(self, index_type: str) -> str:
        return f"indexes/{index_type}.json"

    def _read_index(self, path: str) -> dict:
        return self._index.get(path, {})

    def _write_index(self, path: str, data: dict) -> None:
        self._index[path] = data


class TestIndexManagerMixin:
    """Tests for IndexManagerMixin."""

    def test_get_empty_index(self):
        """Test getting an empty index."""
        storage = IndexedStorage()

        index = storage.get_index("models")

        assert index == {}

    def test_update_index(self):
        """Test updating an index."""
        storage = IndexedStorage()

        storage.update_index("models", "model_1", {"name": "test"})

        index = storage.get_index("models")
        assert "model_1" in index
        assert index["model_1"]["name"] == "test"

    def test_remove_from_index(self):
        """Test removing from index."""
        storage = IndexedStorage()

        storage.update_index("models", "model_1", {"name": "test"})
        storage.remove_from_index("models", "model_1")

        index = storage.get_index("models")
        assert "model_1" not in index

    def test_list_index_keys(self):
        """Test listing index keys."""
        storage = IndexedStorage()

        storage.update_index("models", "model_1", {"name": "test1"})
        storage.update_index("models", "model_2", {"name": "test2"})

        keys = storage.list_index_keys("models")

        assert "model_1" in keys
        assert "model_2" in keys


class ValidatedClass(ValidationMixin):
    """Test implementation using ValidationMixin."""

    pass


class TestValidationMixin:
    """Tests for ValidationMixin."""

    def test_validate_required(self):
        """Test validating required fields."""
        obj = ValidatedClass()

        # Should pass
        obj.validate_required(name="test", value=123)

        # Should raise
        with pytest.raises(ValueError, match="required"):
            obj.validate_required(name=None, value="test")

    def test_validate_type(self):
        """Test type validation."""
        obj = ValidatedClass()

        # Should pass
        obj.validate_type("test", str, "value")
        obj.validate_type(123, int, "number")

        # Should raise
        with pytest.raises(TypeError):
            obj.validate_type("test", int, "value")

    def test_validate_range(self):
        """Test range validation."""
        obj = ValidatedClass()

        # Should pass
        obj.validate_range(5, 0, 10, "value")

        # Should raise - below minimum
        with pytest.raises(ValueError, match="must be"):
            obj.validate_range(-1, 0, 10, "value")

        # Should raise - above maximum
        with pytest.raises(ValueError, match="must be"):
            obj.validate_range(11, 0, 10, "value")

    def test_validate_not_empty(self):
        """Test non-empty validation."""
        obj = ValidatedClass()

        # Should pass
        obj.validate_not_empty("test", "value")
        obj.validate_not_empty([1, 2, 3], "list")

        # Should raise
        with pytest.raises(ValueError, match="cannot be empty"):
            obj.validate_not_empty("", "value")

        with pytest.raises(ValueError, match="cannot be empty"):
            obj.validate_not_empty([], "list")

    def test_validate_pattern(self):
        """Test pattern validation."""
        obj = ValidatedClass()

        # Should pass
        obj.validate_pattern("test_name", r"^[a-z_]+$", "name")

        # Should raise
        with pytest.raises(ValueError, match="does not match"):
            obj.validate_pattern("Test Name", r"^[a-z_]+$", "name")


class TestPathManager:
    """Tests for PathManager class."""

    def test_path_building(self):
        """Test building paths."""
        manager = PathManager(base_path="/mlops")

        path = manager.build_path("models", "my_model", "v1")

        assert path == "/mlops/models/my_model/v1"

    def test_path_joining(self):
        """Test joining paths."""
        manager = PathManager(base_path="/mlops")

        path = manager.join("models", "artifacts")

        assert path == "/mlops/models/artifacts"

    def test_get_relative_path(self):
        """Test getting relative path."""
        manager = PathManager(base_path="/mlops")

        relative = manager.get_relative("/mlops/models/test")

        assert relative == "models/test"

    def test_ensure_path_exists(self):
        """Test ensuring path exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PathManager(base_path=tmpdir)

            path = manager.build_path("subdir", "nested")
            manager.ensure_path(path)

            assert os.path.exists(path)

    def test_list_subdirectories(self):
        """Test listing subdirectories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PathManager(base_path=tmpdir)

            # Create subdirectories
            os.makedirs(os.path.join(tmpdir, "dir1"))
            os.makedirs(os.path.join(tmpdir, "dir2"))

            # Create a file (should not be listed)
            with open(os.path.join(tmpdir, "file.txt"), "w") as f:
                f.write("test")

            subdirs = manager.list_subdirectories(tmpdir)

            assert "dir1" in subdirs
            assert "dir2" in subdirs
            assert "file.txt" not in subdirs


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_now_iso(self):
        """Test ISO timestamp generation."""
        timestamp = now_iso()

        # Should be parseable
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        assert dt is not None

        # Should be close to current time
        now = datetime.now(timezone.utc)
        diff = abs((now - dt).total_seconds())
        assert diff < 1  # Within 1 second

    def test_parse_iso(self):
        """Test ISO timestamp parsing."""
        timestamp = "2024-01-15T10:30:00Z"

        dt = parse_iso(timestamp)

        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30

    def test_age_seconds(self):
        """Test age calculation in seconds."""
        # Create a timestamp from 60 seconds ago
        past = datetime.now(timezone.utc).replace(microsecond=0)
        past_iso = (past.isoformat() + "Z").replace("+00:00Z", "Z")

        age = age_seconds(past_iso)

        # Should be approximately 0 (we just created it)
        assert age >= 0
        assert age < 2  # Allow small timing variance

    def test_age_seconds_old_timestamp(self):
        """Test age calculation for old timestamp."""
        from datetime import timedelta

        # Create timestamp from 1 hour ago
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        past_iso = past.strftime("%Y-%m-%dT%H:%M:%SZ")

        age = age_seconds(past_iso)

        # Should be approximately 3600 seconds
        assert age >= 3500  # Allow some variance
        assert age < 3700


class TestComponentLifecycle:
    """Integration tests for component lifecycle management."""

    def test_full_lifecycle(self):
        """Test complete component lifecycle."""
        component = ConcreteComponent(name="lifecycle_test")

        # Initial state
        assert component.state == ComponentState.CREATED

        # Initialize
        component.initialize()
        assert component.state == ComponentState.INITIALIZED

        # Perform operations
        for i in range(5):
            component.record_operation(duration=0.1 * i)

        # Check stats
        stats = component.get_stats()
        assert stats.operations == 5

        # Shutdown
        component.shutdown()
        assert component.state == ComponentState.STOPPED

    def test_error_handling_in_lifecycle(self):
        """Test error handling during lifecycle."""
        component = ConcreteComponent(name="error_test")
        component.initialize()

        # Simulate errors
        for _ in range(3):
            component.record_operation(duration=1.0)
        for _ in range(2):
            component.record_error(Exception("Test"))

        stats = component.get_stats()
        assert stats.operations == 3
        assert stats.errors == 2
        assert stats.error_rate > 0
