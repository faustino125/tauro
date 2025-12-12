"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Tests for SparkSessionManager improvements - Phase 1
"""
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from core.config.session import SparkSessionManager, SparkSessionFactory


class TestSparkSessionManager:
    """Test suite for SparkSessionManager lifecycle management."""

    def setup_method(self):
        """Reset state before each test."""
        SparkSessionManager._sessions.clear()
        SparkSessionManager._session_metadata.clear()
        SparkSessionManager._cleanup_registered = False

    def teardown_method(self):
        """Cleanup after each test."""
        SparkSessionManager.cleanup_all()

    def test_session_reuse(self):
        """Test that sessions are reused for same configuration."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            # First call - should create
            session1 = SparkSessionManager.get_or_create_session(mode="local")
            assert mock_create.call_count == 1

            # Second call - should reuse
            session2 = SparkSessionManager.get_or_create_session(mode="local")
            assert mock_create.call_count == 1  # No additional creation
            assert session1 is session2

    def test_different_configs_create_different_sessions(self):
        """Test that different configurations create separate sessions."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session1 = Mock()
            mock_session1.version = "3.5.0"
            mock_session2 = Mock()
            mock_session2.version = "3.5.0"
            mock_create.side_effect = [mock_session1, mock_session2]

            session1 = SparkSessionManager.get_or_create_session(
                mode="local", ml_config={"spark.executor.memory": "2g"}
            )
            session2 = SparkSessionManager.get_or_create_session(
                mode="local", ml_config={"spark.executor.memory": "4g"}
            )

            assert mock_create.call_count == 2
            assert session1 is not session2

    def test_force_new_session(self):
        """Test that force_new creates a new session even if one exists."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session1 = Mock()
            mock_session1.version = "3.5.0"
            mock_session2 = Mock()
            mock_session2.version = "3.5.0"
            mock_create.side_effect = [mock_session1, mock_session2]

            session1 = SparkSessionManager.get_or_create_session(mode="local")
            session2 = SparkSessionManager.get_or_create_session(mode="local", force_new=True)

            assert mock_create.call_count == 2
            assert session1 is not session2

    def test_session_cleanup(self):
        """Test that cleanup properly stops sessions."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            SparkSessionManager.get_or_create_session(mode="local")
            assert len(SparkSessionManager._sessions) == 1

            SparkSessionManager.cleanup_all()
            assert len(SparkSessionManager._sessions) == 0
            mock_session.stop.assert_called_once()

    def test_stale_session_cleanup(self):
        """Test that stale sessions are cleaned up."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            # Create session
            SparkSessionManager.get_or_create_session(mode="local")

            # Manually set old timestamp
            for key in SparkSessionManager._session_metadata:
                SparkSessionManager._session_metadata[key]["last_accessed"] = (
                    time.time() - 7200
                )  # 2 hours ago

            # Cleanup stale (max age 1 hour)
            cleaned = SparkSessionManager.cleanup_stale_sessions(max_age=3600)

            assert cleaned == 1
            assert len(SparkSessionManager._sessions) == 0

    def test_invalid_session_recreation(self):
        """Test that invalid sessions are recreated."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session1 = Mock()
            mock_session1.version = "3.5.0"
            mock_session2 = Mock()
            mock_session2.version = "3.5.0"
            mock_create.side_effect = [mock_session1, mock_session2]

            SparkSessionManager.get_or_create_session(mode="local")

            for key in SparkSessionManager._session_metadata:
                SparkSessionManager._session_metadata[key]["last_accessed"] = (
                    time.time() - 7200
                )  # 2 hours ago

            SparkSessionManager.get_or_create_session(mode="local")

            assert mock_create.call_count == 2
            mock_session1.stop.assert_called_once()

    def test_get_session_info(self):
        """Test session info retrieval."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            SparkSessionManager.get_or_create_session(mode="local")

            info = SparkSessionManager.get_session_info()

            assert len(info) == 1
            session_key = list(info.keys())[0]
            assert info[session_key]["mode"] == "local"
            assert info[session_key]["is_valid"] is True
            assert "age_seconds" in info[session_key]
            assert "last_access_seconds_ago" in info[session_key]

    def test_session_key_generation(self):
        """Test that session keys are generated correctly."""
        key1 = SparkSessionManager._generate_session_key("local", None)
        key2 = SparkSessionManager._generate_session_key("local", None)
        assert key1 == key2

        key3 = SparkSessionManager._generate_session_key("local", {"spark.executor.memory": "2g"})
        assert key1 != key3

    def test_cleanup_on_exit_registered(self):
        """Test that cleanup handler is registered."""
        with patch("atexit.register") as mock_register:
            SparkSessionManager._cleanup_registered = False
            SparkSessionManager._register_cleanup()
            mock_register.assert_called_once_with(SparkSessionManager.cleanup_all)

    def test_access_time_update(self):
        """Test that access time is updated on reuse."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            # Create session
            SparkSessionManager.get_or_create_session(mode="local")

            # Get initial access time
            key = list(SparkSessionManager._session_metadata.keys())[0]
            initial_time = SparkSessionManager._session_metadata[key]["last_accessed"]

            # Wait a bit and access again
            time.sleep(0.1)
            SparkSessionManager.get_or_create_session(mode="local")

            # Access time should be updated
            updated_time = SparkSessionManager._session_metadata[key]["last_accessed"]
            assert updated_time > initial_time


class TestExecutorTimeouts:
    """Test suite for executor timeout improvements."""

    def test_base_executor_default_timeout(self):
        """Test that BaseExecutor has default timeout configured."""
        from core.exec.executor import BaseExecutor

        mock_context = Mock()
        mock_context.is_ml_layer = False
        mock_context.global_settings = {}

        with patch("core.exec.executor.InputLoader"):
            with patch("core.exec.executor.DataOutputManager"):
                executor = BaseExecutor(mock_context)

                assert hasattr(executor, "timeout_seconds")
                assert executor.timeout_seconds == BaseExecutor.DEFAULT_TIMEOUT_SECONDS

    def test_base_executor_custom_timeout(self):
        """Test that BaseExecutor respects custom timeout from settings."""
        from core.exec.executor import BaseExecutor

        mock_context = Mock()
        mock_context.is_ml_layer = False
        mock_context.global_settings = {"execution_timeout_seconds": 7200}  # 2 hours

        with patch("core.exec.executor.InputLoader"):
            with patch("core.exec.executor.DataOutputManager"):
                executor = BaseExecutor(mock_context)

                assert executor.timeout_seconds == 7200

    def test_base_executor_max_timeout_limit(self):
        """Test that BaseExecutor enforces maximum timeout."""
        from core.exec.executor import BaseExecutor

        mock_context = Mock()
        mock_context.is_ml_layer = False
        mock_context.global_settings = {"execution_timeout_seconds": 200000}  # Exceeds max

        with patch("core.exec.executor.InputLoader"):
            with patch("core.exec.executor.DataOutputManager"):
                executor = BaseExecutor(mock_context)

                assert executor.timeout_seconds == BaseExecutor.MAX_TIMEOUT_SECONDS

    def test_node_executor_timeout(self):
        """Test that NodeExecutor has timeout configured."""
        from core.exec.node_executor import NodeExecutor

        mock_context = Mock()
        mock_context.is_ml_layer = False
        mock_context.global_settings = {}

        with patch("core.exec.node_executor.get_default_resource_pool"):
            executor = NodeExecutor(
                context=mock_context, input_loader=Mock(), output_manager=Mock(), max_workers=4
            )

            assert hasattr(executor, "node_timeout")
            assert executor.node_timeout == NodeExecutor.DEFAULT_NODE_TIMEOUT


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
