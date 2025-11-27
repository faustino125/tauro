"""Unit tests for health module.

Tests the health check framework including health checks,
health monitoring, and diagnostic utilities.
"""

import pytest
import time
import tempfile
import os
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from engine.mlops.health import (
    HealthStatus,
    HealthCheckResult,
    HealthReport,
    HealthCheck,
    StorageHealthCheck,
    MemoryHealthCheck,
    DiskHealthCheck,
    ComponentHealthCheck,
    HealthMonitor,
    get_health_monitor,
    check_health,
    is_healthy,
    is_ready,
)


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_all_statuses_defined(self):
        """Verify all health statuses exist."""
        assert HealthStatus.HEALTHY is not None
        assert HealthStatus.DEGRADED is not None
        assert HealthStatus.UNHEALTHY is not None
        assert HealthStatus.UNKNOWN is not None

    def test_status_values(self):
        """Test status string values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.UNKNOWN.value == "unknown"

    def test_status_comparison(self):
        """Test status comparison."""
        # Can compare directly
        assert HealthStatus.HEALTHY == HealthStatus.HEALTHY
        assert HealthStatus.HEALTHY != HealthStatus.UNHEALTHY


class TestHealthCheckResult:
    """Tests for HealthCheckResult dataclass."""

    def test_result_creation(self):
        """Test creating a health check result."""
        result = HealthCheckResult(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="All systems operational",
            duration_ms=15.5,
            details={"metric": 100},
        )

        assert result.name == "test_check"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "All systems operational"
        assert result.duration_ms == 15.5
        assert result.details["metric"] == 100

    def test_result_defaults(self):
        """Test result with default values."""
        result = HealthCheckResult(name="simple", status=HealthStatus.HEALTHY)

        assert result.message == ""
        assert result.duration_ms == 0.0
        assert result.details == {}

    def test_result_is_healthy(self):
        """Test is_healthy property."""
        healthy = HealthCheckResult(name="test", status=HealthStatus.HEALTHY)
        assert healthy.is_healthy is True

        degraded = HealthCheckResult(name="test", status=HealthStatus.DEGRADED)
        assert degraded.is_healthy is False

        unhealthy = HealthCheckResult(name="test", status=HealthStatus.UNHEALTHY)
        assert unhealthy.is_healthy is False

    def test_result_to_dict(self):
        """Test conversion to dictionary."""
        result = HealthCheckResult(
            name="test",
            status=HealthStatus.HEALTHY,
            message="OK",
            duration_ms=10.0,
            details={"key": "value"},
        )

        data = result.to_dict()

        assert data["name"] == "test"
        assert data["status"] == "healthy"
        assert data["message"] == "OK"
        assert data["duration_ms"] == 10.0
        assert data["details"]["key"] == "value"


class TestHealthReport:
    """Tests for HealthReport dataclass."""

    def test_report_creation(self):
        """Test creating a health report."""
        results = [
            HealthCheckResult(name="check1", status=HealthStatus.HEALTHY),
            HealthCheckResult(name="check2", status=HealthStatus.HEALTHY),
        ]

        report = HealthReport(
            overall_status=HealthStatus.HEALTHY, checks=results, timestamp="2024-01-01T00:00:00Z"
        )

        assert report.overall_status == HealthStatus.HEALTHY
        assert len(report.checks) == 2

    def test_report_overall_healthy(self):
        """Test report with all healthy checks."""
        results = [
            HealthCheckResult(name="check1", status=HealthStatus.HEALTHY),
            HealthCheckResult(name="check2", status=HealthStatus.HEALTHY),
        ]

        report = HealthReport(overall_status=HealthStatus.HEALTHY, checks=results)

        assert report.is_healthy is True

    def test_report_overall_degraded(self):
        """Test report with degraded status."""
        results = [
            HealthCheckResult(name="check1", status=HealthStatus.HEALTHY),
            HealthCheckResult(name="check2", status=HealthStatus.DEGRADED),
        ]

        report = HealthReport(overall_status=HealthStatus.DEGRADED, checks=results)

        assert report.is_healthy is False

    def test_report_to_dict(self):
        """Test conversion to dictionary."""
        results = [
            HealthCheckResult(name="check1", status=HealthStatus.HEALTHY),
        ]

        report = HealthReport(
            overall_status=HealthStatus.HEALTHY, checks=results, timestamp="2024-01-01T00:00:00Z"
        )

        data = report.to_dict()

        assert data["overall_status"] == "healthy"
        assert "checks" in data
        assert len(data["checks"]) == 1


class SimpleHealthCheck(HealthCheck):
    """Simple health check implementation for testing."""

    def __init__(self, name: str = "simple", should_pass: bool = True):
        super().__init__(name=name)
        self.should_pass = should_pass

    def check(self) -> HealthCheckResult:
        """Perform the health check."""
        if self.should_pass:
            return HealthCheckResult(
                name=self.name, status=HealthStatus.HEALTHY, message="Check passed"
            )
        else:
            return HealthCheckResult(
                name=self.name, status=HealthStatus.UNHEALTHY, message="Check failed"
            )


class TestHealthCheck:
    """Tests for HealthCheck base class."""

    def test_check_implementation(self):
        """Test basic health check implementation."""
        check = SimpleHealthCheck(name="test", should_pass=True)

        result = check.check()

        assert result.status == HealthStatus.HEALTHY
        assert result.name == "test"

    def test_check_failure(self):
        """Test health check failure."""
        check = SimpleHealthCheck(name="failing", should_pass=False)

        result = check.check()

        assert result.status == HealthStatus.UNHEALTHY

    def test_check_with_timeout(self):
        """Test health check timeout handling."""

        class SlowCheck(HealthCheck):
            def check(self) -> HealthCheckResult:
                time.sleep(0.5)
                return HealthCheckResult(name=self.name, status=HealthStatus.HEALTHY)

        check = SlowCheck(name="slow", timeout=0.1)

        result = check.execute()

        # Should timeout and return unhealthy
        assert result.status == HealthStatus.UNHEALTHY
        assert "timeout" in result.message.lower()


class TestStorageHealthCheck:
    """Tests for StorageHealthCheck."""

    def test_storage_healthy(self):
        """Test storage health check with healthy storage."""
        mock_storage = Mock()
        mock_storage.exists.return_value = True
        mock_storage.write_json.return_value = Mock()
        mock_storage.read_json.return_value = {"test": "data"}

        check = StorageHealthCheck(name="storage", storage=mock_storage, test_path="/_health_check")

        result = check.check()

        assert result.status == HealthStatus.HEALTHY

    def test_storage_unhealthy(self):
        """Test storage health check with failing storage."""
        mock_storage = Mock()
        mock_storage.write_json.side_effect = Exception("Write failed")

        check = StorageHealthCheck(name="storage", storage=mock_storage, test_path="/_health_check")

        result = check.check()

        assert result.status == HealthStatus.UNHEALTHY
        assert "Write failed" in result.message


class TestMemoryHealthCheck:
    """Tests for MemoryHealthCheck."""

    def test_memory_check(self):
        """Test memory health check."""
        check = MemoryHealthCheck(
            name="memory", warning_threshold=0.95, critical_threshold=0.99  # High threshold to pass
        )

        result = check.check()

        # Should generally be healthy unless system is under heavy load
        assert result.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]
        assert "memory" in result.details

    def test_memory_check_details(self):
        """Test memory check provides details."""
        check = MemoryHealthCheck(name="memory")

        result = check.check()

        # Should include memory usage details
        assert "total_mb" in result.details or "memory" in str(result.details).lower()


class TestDiskHealthCheck:
    """Tests for DiskHealthCheck."""

    def test_disk_check(self):
        """Test disk health check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            check = DiskHealthCheck(
                name="disk",
                path=tmpdir,
                warning_threshold=0.99,  # High threshold to pass
                critical_threshold=0.999,
            )

            result = check.check()

            # Should be healthy for temp directory
            assert result.status == HealthStatus.HEALTHY

    def test_disk_check_invalid_path(self):
        """Test disk check with invalid path."""
        check = DiskHealthCheck(name="disk", path="/nonexistent/path/that/does/not/exist")

        result = check.check()

        # Should be unhealthy or handle gracefully
        # Some systems might still return info for root
        assert result.status in [HealthStatus.HEALTHY, HealthStatus.UNHEALTHY]


class TestComponentHealthCheck:
    """Tests for ComponentHealthCheck."""

    def test_component_healthy(self):
        """Test component health check with healthy component."""
        mock_component = Mock()
        mock_component.get_stats.return_value = Mock(operations=100, errors=0, error_rate=0.0)

        check = ComponentHealthCheck(
            name="component", component=mock_component, error_threshold=0.1
        )

        result = check.check()

        assert result.status == HealthStatus.HEALTHY

    def test_component_degraded(self):
        """Test component health check with high error rate."""
        mock_component = Mock()
        mock_component.get_stats.return_value = Mock(operations=100, errors=10, error_rate=0.1)

        check = ComponentHealthCheck(
            name="component", component=mock_component, error_threshold=0.05  # 5% threshold
        )

        result = check.check()

        # Should be degraded due to high error rate
        assert result.status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]


class TestHealthMonitor:
    """Tests for HealthMonitor."""

    def test_monitor_creation(self):
        """Test creating a health monitor."""
        monitor = HealthMonitor()

        assert monitor is not None

    def test_register_check(self):
        """Test registering a health check."""
        monitor = HealthMonitor()
        check = SimpleHealthCheck(name="test")

        monitor.register(check)

        # Check should be registered
        report = monitor.check_all()
        assert any(r.name == "test" for r in report.checks)

    def test_unregister_check(self):
        """Test unregistering a health check."""
        monitor = HealthMonitor()
        check = SimpleHealthCheck(name="test")

        monitor.register(check)
        monitor.unregister("test")

        report = monitor.check_all()
        assert not any(r.name == "test" for r in report.checks)

    def test_check_all(self):
        """Test running all health checks."""
        monitor = HealthMonitor()

        monitor.register(SimpleHealthCheck(name="check1", should_pass=True))
        monitor.register(SimpleHealthCheck(name="check2", should_pass=True))

        report = monitor.check_all()

        assert report.overall_status == HealthStatus.HEALTHY
        assert len(report.checks) == 2

    def test_check_all_with_failure(self):
        """Test health report with a failing check."""
        monitor = HealthMonitor()

        monitor.register(SimpleHealthCheck(name="healthy", should_pass=True))
        monitor.register(SimpleHealthCheck(name="failing", should_pass=False))

        report = monitor.check_all()

        assert report.overall_status == HealthStatus.UNHEALTHY

    def test_check_specific(self):
        """Test checking a specific health check."""
        monitor = HealthMonitor()

        monitor.register(SimpleHealthCheck(name="target", should_pass=True))
        monitor.register(SimpleHealthCheck(name="other", should_pass=False))

        result = monitor.check("target")

        assert result.status == HealthStatus.HEALTHY
        assert result.name == "target"

    def test_liveness_check(self):
        """Test liveness probe."""
        monitor = HealthMonitor()
        monitor.register(SimpleHealthCheck(name="live", should_pass=True))

        is_live = monitor.is_live()

        assert is_live is True

    def test_readiness_check(self):
        """Test readiness probe."""
        monitor = HealthMonitor()
        monitor.register(SimpleHealthCheck(name="ready", should_pass=True))

        is_ready_result = monitor.is_ready()

        assert is_ready_result is True


class TestGlobalFunctions:
    """Tests for global convenience functions."""

    def test_get_health_monitor(self):
        """Test getting the global health monitor."""
        monitor1 = get_health_monitor()
        monitor2 = get_health_monitor()

        # Should return same instance (singleton)
        assert monitor1 is monitor2

    def test_check_health(self):
        """Test global check_health function."""
        # Get monitor and register a check
        monitor = get_health_monitor()
        monitor.register(SimpleHealthCheck(name="global_test", should_pass=True))

        report = check_health()

        assert report is not None
        assert report.overall_status in list(HealthStatus)

    def test_is_healthy(self):
        """Test global is_healthy function."""
        result = is_healthy()

        assert isinstance(result, bool)

    def test_is_ready(self):
        """Test global is_ready function."""
        result = is_ready()

        assert isinstance(result, bool)


class TestHealthCheckIntegration:
    """Integration tests for health check system."""

    def test_full_health_monitoring_workflow(self):
        """Test complete health monitoring workflow."""
        monitor = HealthMonitor()

        # Register multiple checks
        monitor.register(SimpleHealthCheck(name="api", should_pass=True))
        monitor.register(SimpleHealthCheck(name="db", should_pass=True))
        monitor.register(MemoryHealthCheck(name="memory"))

        # Run all checks
        report = monitor.check_all()

        # Should have results for all checks
        assert len(report.checks) >= 3

        # Should have overall status
        assert report.overall_status in list(HealthStatus)

        # Should have timestamp
        assert report.timestamp is not None

    def test_health_check_timing(self):
        """Test that health checks track timing."""

        class TimedCheck(HealthCheck):
            def check(self) -> HealthCheckResult:
                time.sleep(0.01)  # 10ms
                return HealthCheckResult(name=self.name, status=HealthStatus.HEALTHY)

        check = TimedCheck(name="timed")
        result = check.execute()

        # Should have recorded duration
        assert result.duration_ms >= 10  # At least 10ms
