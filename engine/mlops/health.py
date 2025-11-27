from __future__ import annotations

import os
import platform
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from loguru import logger


if TYPE_CHECKING:
    from engine.mlops.storage import StorageBackend


class HealthStatus(str, Enum):
    """Health check status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    name: str
    status: HealthStatus
    message: str
    duration_ms: float
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp,
            "details": self.details,
        }


@dataclass
class HealthReport:
    """Aggregated health report from multiple checks."""

    overall_status: HealthStatus
    checks: List[HealthCheckResult]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    total_duration_ms: float = 0.0
    system_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "overall_status": self.overall_status.value,
            "checks": [c.to_dict() for c in self.checks],
            "timestamp": self.timestamp,
            "total_duration_ms": self.total_duration_ms,
            "system_info": self.system_info,
        }

    @property
    def is_healthy(self) -> bool:
        """Check if overall status is healthy."""
        return self.overall_status == HealthStatus.HEALTHY

    @property
    def is_ready(self) -> bool:
        """Check if system is ready (healthy or degraded)."""
        return self.overall_status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)


HealthCheckFunc = Callable[[], HealthCheckResult]


class HealthCheck:
    """
    Base class for health checks.
    """

    def __init__(
        self,
        name: str,
        timeout: float = 5.0,
        critical: bool = True,
    ):
        """
        Initialize health check.
        """
        self.name = name
        self.timeout = timeout
        self.critical = critical

    def execute(self) -> HealthCheckResult:
        """
        Execute the health check with timing.
        """
        start = time.perf_counter()

        try:
            result = self.check()
            result.duration_ms = (time.perf_counter() - start) * 1000
            return result

        except Exception as e:
            duration_ms = (time.perf_counter() - start) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Check failed: {e}",
                duration_ms=duration_ms,
                details={"error": str(e)},
            )

    def check(self) -> HealthCheckResult:
        """
        Perform the actual health check.
        """
        raise NotImplementedError


class StorageHealthCheck(HealthCheck):
    """
    Health check for storage backend connectivity.
    """

    def __init__(
        self,
        storage: "StorageBackend",
        check_write: bool = True,
        test_path: str = ".health_check",
    ):
        super().__init__("storage", timeout=10.0, critical=True)
        self.storage = storage
        self.check_write = check_write
        self.test_path = test_path

    def check(self) -> HealthCheckResult:
        """Perform storage health check."""
        details: Dict[str, Any] = {}

        # Check if storage exists
        try:
            self.storage.exists(self.test_path)
            details["exists_check"] = "passed"
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Storage exists check failed: {e}",
                duration_ms=0,
                details={"error": str(e)},
            )

        # Check write capability if requested
        if self.check_write:
            try:
                test_data = {
                    "health_check": True,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self.storage.write_json(
                    test_data,
                    f"{self.test_path}/health_check.json",
                    mode="overwrite",
                )
                details["write_check"] = "passed"

                # Cleanup
                try:
                    self.storage.delete(f"{self.test_path}/health_check.json")
                except Exception:
                    pass

            except Exception as e:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Storage write/read check failed: {e}",
                    duration_ms=0,
                    details={"error": str(e), **details},
                )

        # Get storage stats if available
        if hasattr(self.storage, "get_stats"):
            details["stats"] = self.storage.get_stats()

        return HealthCheckResult(
            name=self.name,
            status=HealthStatus.HEALTHY,
            message="Storage is healthy",
            duration_ms=0,
            details=details,
        )


class MemoryHealthCheck(HealthCheck):
    """
    Health check for memory usage.
    """

    def __init__(
        self,
        warning_threshold_mb: float = 1024.0,
        critical_threshold_mb: float = 2048.0,
    ):
        super().__init__("memory", timeout=1.0, critical=False)
        self.warning_threshold_mb = warning_threshold_mb
        self.critical_threshold_mb = critical_threshold_mb

    def check(self) -> HealthCheckResult:
        """Perform memory health check."""
        try:
            import psutil  # type: ignore

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            details = {
                "rss_mb": round(memory_mb, 2),
                "vms_mb": round(memory_info.vms / (1024 * 1024), 2),
                "warning_threshold_mb": self.warning_threshold_mb,
                "critical_threshold_mb": self.critical_threshold_mb,
            }

            if memory_mb >= self.critical_threshold_mb:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Memory usage critical: {memory_mb:.0f}MB",
                    duration_ms=0,
                    details=details,
                )
            elif memory_mb >= self.warning_threshold_mb:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.DEGRADED,
                    message=f"Memory usage high: {memory_mb:.0f}MB",
                    duration_ms=0,
                    details=details,
                )
            else:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    message=f"Memory usage normal: {memory_mb:.0f}MB",
                    duration_ms=0,
                    details=details,
                )

        except ImportError:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNKNOWN,
                message="psutil not available for memory check",
                duration_ms=0,
            )


class DiskHealthCheck(HealthCheck):
    """
    Health check for disk space.
    """

    def __init__(
        self,
        path: str = ".",
        warning_threshold_percent: float = 80.0,
        critical_threshold_percent: float = 95.0,
    ):
        super().__init__("disk", timeout=1.0, critical=False)
        self.path = path
        self.warning_threshold = warning_threshold_percent
        self.critical_threshold = critical_threshold_percent

    def check(self) -> HealthCheckResult:
        """Perform disk health check."""
        try:
            import shutil

            total, used, free = shutil.disk_usage(self.path)

            usage_percent = (used / total) * 100

            details = {
                "total_gb": round(total / (1024**3), 2),
                "used_gb": round(used / (1024**3), 2),
                "free_gb": round(free / (1024**3), 2),
                "usage_percent": round(usage_percent, 1),
                "path": self.path,
            }

            if usage_percent >= self.critical_threshold:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Disk usage critical: {usage_percent:.1f}%",
                    duration_ms=0,
                    details=details,
                )
            elif usage_percent >= self.warning_threshold:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.DEGRADED,
                    message=f"Disk usage high: {usage_percent:.1f}%",
                    duration_ms=0,
                    details=details,
                )
            else:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    message=f"Disk usage normal: {usage_percent:.1f}%",
                    duration_ms=0,
                    details=details,
                )

        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNKNOWN,
                message=f"Disk check failed: {e}",
                duration_ms=0,
            )


class ComponentHealthCheck(HealthCheck):
    """
    Health check for MLOps components.
    """

    def __init__(
        self,
        component: Any,
        component_name: str,
    ):
        super().__init__(f"component_{component_name}", timeout=5.0, critical=True)
        self.component = component
        self.component_name = component_name

    def check(self) -> HealthCheckResult:
        """Perform component health check."""
        details: Dict[str, Any] = {
            "component_name": self.component_name,
        }

        # Check if component has state
        if hasattr(self.component, "state"):
            state = self.component.state
            details["state"] = state

            if state in ("stopped", "error"):
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Component {self.component_name} is {state}",
                    duration_ms=0,
                    details=details,
                )

        # Check if component has stats
        if hasattr(self.component, "get_stats"):
            try:
                stats = self.component.get_stats()
                details["stats"] = stats
            except Exception as e:
                details["stats_error"] = str(e)

        # Check if component is ready
        if hasattr(self.component, "is_ready"):
            is_ready = self.component.is_ready
            details["is_ready"] = is_ready

            if not is_ready:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.DEGRADED,
                    message=f"Component {self.component_name} is not ready",
                    duration_ms=0,
                    details=details,
                )

        return HealthCheckResult(
            name=self.name,
            status=HealthStatus.HEALTHY,
            message=f"Component {self.component_name} is healthy",
            duration_ms=0,
            details=details,
        )


class HealthMonitor:
    """
    Central health monitoring service.
    """

    def __init__(
        self,
        check_interval: float = 30.0,
        history_size: int = 100,
    ):
        """
        Initialize health monitor.
        """
        self._checks: Dict[str, HealthCheck] = {}
        self._history: List[HealthReport] = []
        self._history_size = history_size
        self._check_interval = check_interval
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[HealthReport], None]] = []

    def add_check(self, check: HealthCheck) -> None:
        """
        Add a health check.
        """
        with self._lock:
            self._checks[check.name] = check

    def remove_check(self, name: str) -> bool:
        """
        Remove a health check.
        """
        with self._lock:
            if name in self._checks:
                del self._checks[name]
                return True
            return False

    def add_callback(self, callback: Callable[[HealthReport], None]) -> None:
        """
        Add a callback for health report notifications.
        """
        with self._lock:
            self._callbacks.append(callback)

    def check_health(self) -> HealthReport:
        """
        Run all health checks and return aggregate report.
        """
        start = time.perf_counter()

        with self._lock:
            checks = list(self._checks.values())

        # Run all checks
        results: List[HealthCheckResult] = []
        for check in checks:
            result = check.execute()
            results.append(result)

        # Determine overall status
        overall_status = self._aggregate_status(results)

        total_duration = (time.perf_counter() - start) * 1000

        report = HealthReport(
            overall_status=overall_status,
            checks=results,
            total_duration_ms=total_duration,
            system_info=self._get_system_info(),
        )

        # Store in history
        with self._lock:
            self._history.append(report)
            if len(self._history) > self._history_size:
                self._history = self._history[-self._history_size :]

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(report)
                except Exception as e:
                    logger.warning(f"Health callback failed: {e}")

        return report

    def _aggregate_status(
        self,
        results: List[HealthCheckResult],
    ) -> HealthStatus:
        """
        Aggregate individual check statuses.
        """
        has_degraded = False

        for result in results:
            check = self._checks.get(result.name)
            is_critical = check.critical if check else True

            if result.status == HealthStatus.UNHEALTHY:
                if is_critical:
                    return HealthStatus.UNHEALTHY
                has_degraded = True
            elif result.status == HealthStatus.DEGRADED:
                has_degraded = True

        if has_degraded:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def _get_system_info(self) -> Dict[str, Any]:
        """Get system information for health report."""
        return {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "hostname": platform.node(),
            "pid": os.getpid(),
        }

    def get_history(self, limit: int = 10) -> List[HealthReport]:
        """
        Get recent health report history.
        """
        with self._lock:
            return list(self._history[-limit:])

    def start_monitoring(self) -> None:
        """Start periodic health monitoring in background."""
        with self._lock:
            if self._running:
                return

            self._running = True
            self._thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True,
            )
            self._thread.start()
            logger.info("Health monitoring started")

    def stop_monitoring(self) -> None:
        """Stop periodic health monitoring."""
        with self._lock:
            self._running = False

        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

        logger.info("Health monitoring stopped")

    def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                self.check_health()
            except Exception as e:
                logger.error(f"Health check error: {e}")

            # Sleep in small intervals for faster shutdown
            for _ in range(int(self._check_interval)):
                if not self._running:
                    break
                time.sleep(1.0)

    def is_healthy(self) -> bool:
        """
        Quick check if system is healthy.

        Returns:
            True if overall status is HEALTHY
        """
        report = self.check_health()
        return report.is_healthy

    def is_ready(self) -> bool:
        """
        Quick check if system is ready.
        """
        report = self.check_health()
        return report.is_ready

    def liveness_probe(self) -> Dict[str, Any]:
        """
        Kubernetes-style liveness probe.
        """
        return {
            "alive": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def readiness_probe(self) -> Dict[str, Any]:
        """
        Kubernetes-style readiness probe.
        """
        report = self.check_health()
        return {
            "ready": report.is_ready,
            "status": report.overall_status.value,
            "timestamp": report.timestamp,
        }


_global_monitor: Optional[HealthMonitor] = None
_monitor_lock = threading.Lock()


def get_health_monitor() -> HealthMonitor:
    """Get the global health monitor instance."""
    global _global_monitor
    with _monitor_lock:
        if _global_monitor is None:
            _global_monitor = HealthMonitor()
        return _global_monitor


def check_health() -> HealthReport:
    """Run health checks using global monitor."""
    return get_health_monitor().check_health()


def is_healthy() -> bool:
    """Quick health check using global monitor."""
    return get_health_monitor().is_healthy()


def is_ready() -> bool:
    """Quick readiness check using global monitor."""
    return get_health_monitor().is_ready()
