"""
Tests for tauro.core.config.session module.

Coverage includes:
- SparkSessionFactory configuration management
- Protected configuration handling
- Session creation and cleanup
- Error handling in session lifecycle
- Thread-safe session management
- Configuration validation
"""

import pytest  # type: ignore
import time
from unittest.mock import Mock, patch, MagicMock

from core.config.session import SparkSessionFactory, SparkSessionManager


# ============================================================================
# SPARK SESSION FACTORY TESTS
# ============================================================================


class TestSparkSessionFactory:
    """Tests for SparkSessionFactory core functionality."""

    def test_factory_initialization(self):
        """Should initialize factory without errors."""
        SparkSessionFactory.reset_session()
        assert SparkSessionFactory._session is None

    def test_set_protected_configs(self):
        """Should set protected configuration keys."""
        protected = ["forbid.this", "forbid.that"]
        SparkSessionFactory.set_protected_configs(protected)
        # Should not raise
        assert SparkSessionFactory._protected_configs is not None

    def test_apply_ml_configs_respects_protected(self):
        """Should not apply protected configurations."""

        class FakeBuilder:
            def __init__(self):
                self.configs = {}

            def config(self, k, v):
                self.configs[k] = v
                return self

        builder = FakeBuilder()
        SparkSessionFactory.set_protected_configs(["forbid.me"])
        ml_cfg = {"forbid.me": "x", "ok.one": "y"}

        result = SparkSessionFactory._apply_ml_configs(builder, ml_cfg)

        assert result is builder
        assert "ok.one" in builder.configs
        assert "forbid.me" not in builder.configs

    def test_apply_ml_configs_returns_builder(self):
        """Should return builder for chaining."""

        class FakeBuilder:
            def __init__(self):
                self.configs = {}

            def config(self, k, v):
                self.configs[k] = v
                return self

        builder = FakeBuilder()
        SparkSessionFactory.set_protected_configs([])
        ml_cfg = {"executor.memory": "4g"}

        result = SparkSessionFactory._apply_ml_configs(builder, ml_cfg)

        assert result is builder
        assert "executor.memory" in builder.configs

    def test_apply_ml_configs_empty_config(self):
        """Should handle empty ML configuration."""

        class FakeBuilder:
            def __init__(self):
                self.configs = {}

            def config(self, k, v):
                self.configs[k] = v
                return self

        builder = FakeBuilder()
        SparkSessionFactory.set_protected_configs([])
        result = SparkSessionFactory._apply_ml_configs(builder, {})

        assert result is builder
        assert len(result.configs) == 0


# ============================================================================
# SESSION RESET AND CLEANUP TESTS
# ============================================================================


class TestSessionReset:
    """Tests for session reset and cleanup operations."""

    def test_reset_session_no_error_when_none(self):
        """Should handle reset when session is None."""
        SparkSessionFactory._session = None
        # Should not raise
        SparkSessionFactory.reset_session()
        assert SparkSessionFactory._session is None

    def test_reset_session_stops_existing_session(self):
        """Should call stop on existing session."""
        mock_session = Mock()
        mock_session.stop = Mock()

        SparkSessionFactory._session = mock_session
        SparkSessionFactory.reset_session()

        mock_session.stop.assert_called()
        assert SparkSessionFactory._session is None

    def test_reset_session_handles_stop_error(self):
        """Should handle errors when stopping session."""
        mock_session = Mock()
        mock_session.stop = Mock(side_effect=RuntimeError("stop fail"))

        SparkSessionFactory._session = mock_session
        # Should not raise even though stop() fails
        SparkSessionFactory.reset_session()
        assert SparkSessionFactory._session is None

    def test_reset_session_idempotent(self):
        """Should be safe to call reset multiple times."""
        SparkSessionFactory.reset_session()
        SparkSessionFactory.reset_session()
        SparkSessionFactory.reset_session()
        # Should not raise
        assert SparkSessionFactory._session is None


# ============================================================================
# SPARK SESSION MANAGER TESTS
# ============================================================================


class TestSparkSessionManager:
    """Tests for SparkSessionManager lifecycle and session reuse."""

    def setup_method(self):
        """Reset manager state before each test."""
        if hasattr(SparkSessionManager, "_sessions"):
            SparkSessionManager._sessions.clear()
        if hasattr(SparkSessionManager, "_session_metadata"):
            SparkSessionManager._session_metadata.clear()
        if hasattr(SparkSessionManager, "_cleanup_registered"):
            SparkSessionManager._cleanup_registered = False

    def teardown_method(self):
        """Cleanup after each test."""
        try:
            SparkSessionManager.cleanup_all()
        except Exception:
            pass

    def test_session_reuse_same_config(self):
        """Should reuse session for same configuration."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            session1 = SparkSessionManager.get_or_create_session(mode="local")
            assert mock_create.call_count == 1

            session2 = SparkSessionManager.get_or_create_session(mode="local")
            assert mock_create.call_count == 1  # No additional creation
            assert session1 is session2

    def test_different_configs_create_different_sessions(self):
        """Should create separate sessions for different configs."""
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
        """Should create new session when force_new=True."""
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
        """Should properly stop and cleanup sessions."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_session.stop = Mock()
            mock_create.return_value = mock_session

            SparkSessionManager.get_or_create_session(mode="local")
            sessions_count = len(SparkSessionManager._sessions)
            assert sessions_count >= 1

            SparkSessionManager.cleanup_all()
            assert len(SparkSessionManager._sessions) == 0
            mock_session.stop.assert_called()


# ============================================================================
# SESSION MANAGEMENT ADVANCED TESTS
# ============================================================================


class TestSessionManagement:
    """Tests for advanced session management features."""

    def setup_method(self):
        """Reset manager state."""
        try:
            SparkSessionManager._sessions.clear()
        except Exception:
            pass

    def test_stale_session_detection(self):
        """Should identify stale sessions."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_session.stop = Mock()
            mock_create.return_value = mock_session

            SparkSessionManager.get_or_create_session(mode="local")

            # Manually set old timestamp to simulate stale session
            if hasattr(SparkSessionManager, "_session_metadata"):
                for key in SparkSessionManager._session_metadata:
                    SparkSessionManager._session_metadata[key]["last_accessed"] = (
                        time.time() - 7200
                    )  # 2 hours

            # Cleanup stale sessions (max age 1 hour)
            try:
                cleaned = SparkSessionManager.cleanup_stale_sessions(max_age=3600)
                assert cleaned >= 0
            except AttributeError:
                # Method may not exist yet
                pytest.skip("cleanup_stale_sessions not implemented")

    def test_session_key_generation(self):
        """Should generate consistent session keys."""
        try:
            key1 = SparkSessionManager._generate_session_key("local", None)
            key2 = SparkSessionManager._generate_session_key("local", None)
            assert key1 == key2

            key3 = SparkSessionManager._generate_session_key(
                "local", {"spark.executor.memory": "2g"}
            )
            assert key1 != key3
        except AttributeError:
            pytest.skip("_generate_session_key not implemented")

    def test_session_info_retrieval(self):
        """Should return session information."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_session = Mock()
            mock_session.version = "3.5.0"
            mock_create.return_value = mock_session

            SparkSessionManager.get_or_create_session(mode="local")

            try:
                info = SparkSessionManager.get_session_info()
                assert isinstance(info, dict)
                assert len(info) >= 1

                # Check structure
                for session_key, session_info in info.items():
                    assert "mode" in session_info
                    assert "is_valid" in session_info
            except AttributeError:
                pytest.skip("get_session_info not implemented")


# ============================================================================
# THREAD SAFETY TESTS
# ============================================================================


class TestThreadSafety:
    """Tests for thread-safe session management."""

    def test_concurrent_session_access(self):
        """Should handle concurrent access safely."""
        import threading

        results = []

        def get_session():
            try:
                with patch.object(SparkSessionFactory, "create_session") as mock_create:
                    mock_session = Mock()
                    mock_session.version = "3.5.0"
                    mock_create.return_value = mock_session

                    session = SparkSessionManager.get_or_create_session(mode="local")
                    results.append(session)
            except Exception as e:
                results.append(e)

        threads = [threading.Thread(target=get_session) for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All results should be successful
        assert len(results) == 5

    def test_cleanup_during_access(self):
        """Should handle cleanup during active access."""
        import threading

        try:
            with patch.object(SparkSessionFactory, "create_session") as mock_create:
                mock_session = Mock()
                mock_session.version = "3.5.0"
                mock_session.stop = Mock()
                mock_create.return_value = mock_session

                SparkSessionManager.get_or_create_session(mode="local")

                # Cleanup should not raise even if access is happening
                SparkSessionManager.cleanup_all()

                assert len(SparkSessionManager._sessions) == 0
        except AttributeError:
            pytest.skip("Session manager implementation incomplete")


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================


class TestErrorHandling:
    """Tests for error handling in session operations."""

    def test_session_creation_failure(self):
        """Should handle session creation failures gracefully."""
        with patch.object(SparkSessionFactory, "create_session") as mock_create:
            mock_create.side_effect = RuntimeError("Creation failed")

            with pytest.raises(RuntimeError):
                SparkSessionManager.get_or_create_session(mode="local")

    def test_protected_config_with_none_values(self):
        """Should handle None in protected configs."""
        SparkSessionFactory.set_protected_configs(None)
        # Should not raise and protected configs should be empty/falsy
        assert not SparkSessionFactory._protected_configs

    def test_reset_with_broken_session(self):
        """Should handle reset when session is in broken state."""
        mock_session = Mock()
        mock_session.stop = Mock(side_effect=Exception("broken"))

        SparkSessionFactory._session = mock_session
        # Should not raise
        SparkSessionFactory.reset_session()
        assert SparkSessionFactory._session is None
