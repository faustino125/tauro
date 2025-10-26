from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Callable, List
import asyncio
import time

from loguru import logger


class ServiceState(Enum):
    """Service lifecycle states"""

    INITIALIZING = "initializing"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class ServicePriority(Enum):
    """Request execution priorities"""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class ServiceConfig:
    """Service configuration"""

    max_concurrent_requests: int = 10
    request_timeout: float = 30.0
    health_check_interval: float = 30.0
    metrics_enabled: bool = True
    max_retries: int = 3
    backoff_factor: float = 2.0


@dataclass
class ServiceMetrics:
    """Service performance metrics"""

    total_requests: int = 0
    active_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeout_requests: int = 0
    rejected_requests: int = 0
    average_response_time: float = 0.0
    last_health_check: float = 0.0
    start_time: float = field(default_factory=time.time)


class BaseService(ABC):
    """
    Abstract base class for all services.

    Provides common functionality:
    - Async lifecycle management (start/stop)
    - Concurrency control via semaphore
    - Health monitoring
    - Metrics tracking
    - Event handling
    """

    def __init__(
        self, config: Optional[ServiceConfig] = None, name: Optional[str] = None
    ):
        self.config = config or ServiceConfig()
        self.name = name or self.__class__.__name__
        self.state = ServiceState.INITIALIZING
        self.metrics = ServiceMetrics()

        # Concurrency control
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self._lock = asyncio.Lock()

        # Health monitoring
        self._health_check_task: Optional[asyncio.Task] = None
        self._health_status = {"status": "initializing"}

        # Event handlers
        self._event_handlers: Dict[str, List[Callable]] = {}

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self) -> None:
        """Start the service"""
        if self.state != ServiceState.INITIALIZING:
            logger.warning(f"{self.name} already started")
            return

        try:
            logger.info(f"Starting {self.name}...")
            await self._initialize()
            self.state = ServiceState.RUNNING
            self.metrics.start_time = time.time()

            # Start health monitoring
            if self.config.health_check_interval > 0:
                self._health_check_task = asyncio.create_task(self._health_monitor())

            await self._emit_event("started")
            logger.info(f"{self.name} started successfully")

        except Exception as e:
            self.state = ServiceState.ERROR
            logger.exception(f"Failed to start {self.name}: {e}")
            raise

    async def stop(self) -> None:
        """Stop the service"""
        if self.state in [ServiceState.STOPPING, ServiceState.STOPPED]:
            logger.warning(f"{self.name} already stopped")
            return

        self.state = ServiceState.STOPPING
        logger.info(f"Stopping {self.name}...")

        # Ensure that if the health check task is cancelled we still run cleanup
        cancelled_exc = None
        cancelled_tb = None
        if self._health_check_task:
            self._health_check_task.cancel()
            # Await task using gather to capture CancelledError as a returned exception
            results = await asyncio.gather(
                self._health_check_task, return_exceptions=True
            )
            for res in results:
                if isinstance(res, asyncio.CancelledError):
                    logger.debug(f"Health check task for {self.name} cancelled")
                    cancelled_exc = res
                    cancelled_tb = res.__traceback__

        try:
            await self._cleanup()
            self.state = ServiceState.STOPPED

            uptime = time.time() - self.metrics.start_time
            await self._emit_event("stopped")
            logger.info(f"{self.name} stopped (uptime: {uptime:.2f}s)")

        except asyncio.CancelledError as ex:
            logger.debug(f"Stop sequence for {self.name} cancelled during cleanup")
            raise ex
        except Exception as e:
            self.state = ServiceState.ERROR
            logger.exception(f"Error during cleanup for {self.name}: {e}")
        # If the health check task was cancelled earlier, re-raise to propagate cancellation
        if cancelled_exc:
            # Re-raise the original CancelledError with its traceback to preserve context
            # This ensures cancellation semantics are preserved for callers.
            raise cancelled_exc.with_traceback(cancelled_tb)

    @abstractmethod
    async def _initialize(self) -> None:
        """Initialize service resources (implemented by subclasses)"""
        pass

    @abstractmethod
    async def _cleanup(self) -> None:
        """Cleanup service resources (implemented by subclasses)"""
        pass

    @asynccontextmanager
    async def _acquire_slot(self, priority: ServicePriority = ServicePriority.NORMAL):
        """
        Acquire an execution slot with timeout.
        Provides concurrency control and metrics tracking.
        """
        start_time = time.time()

        try:
            # Priority-based timeout
            timeout = self.config.request_timeout
            if priority == ServicePriority.CRITICAL:
                timeout *= 2

            await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout)

            async with self._lock:
                self.metrics.active_requests += 1
                self.metrics.total_requests += 1

            yield

        except asyncio.TimeoutError:
            async with self._lock:
                self.metrics.rejected_requests += 1
                self.metrics.timeout_requests += 1
            logger.warning(f"{self.name} rejected request (timeout)")
            raise asyncio.TimeoutError("Service busy, please try again later")

        finally:
            try:
                self._semaphore.release()
                async with self._lock:
                    self.metrics.active_requests -= 1

                    # Update average response time
                    elapsed = time.time() - start_time
                    if self.metrics.successful_requests > 0:
                        total_time = self.metrics.average_response_time * (
                            self.metrics.successful_requests - 1
                        )
                        self.metrics.average_response_time = (
                            total_time + elapsed
                        ) / self.metrics.successful_requests
            except asyncio.CancelledError:
                # Preserve cancellation semantics: do not swallow CancelledError
                raise
            except Exception as e:
                logger.error(f"Error releasing slot: {e}")

    async def _health_monitor(self) -> None:
        """Background health monitoring task"""
        while self.state == ServiceState.RUNNING:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                self._health_status = await self._perform_health_check()
                self.metrics.last_health_check = time.time()

            except asyncio.CancelledError:
                logger.debug(f"Health monitor for {self.name} cancelled")
                raise  # Re-raise to properly propagate cancellation
            except Exception as e:
                logger.exception(f"{self.name} health check failed: {e}")
                self._health_status = {"status": "error", "error": str(e)}
                self.state = ServiceState.DEGRADED

    async def _perform_health_check(self) -> Dict[str, Any]:
        """
        Perform service-specific health check.
        Override in subclasses for custom checks.

        Note: Kept as async to allow subclasses to perform async health checks.
        """
        # Yield control to event loop for async compatibility
        await asyncio.sleep(0)

        return {
            "status": "healthy" if self.state == ServiceState.RUNNING else "unhealthy",
            "active_requests": self.metrics.active_requests,
            "total_requests": self.metrics.total_requests,
            "success_rate": (
                self.metrics.successful_requests / self.metrics.total_requests
                if self.metrics.total_requests > 0
                else 0
            ),
            "uptime_seconds": time.time() - self.metrics.start_time,
        }

    async def _emit_event(
        self, event: str, data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Emit service event to registered handlers"""
        if event in self._event_handlers:
            tasks = []
            for handler in self._event_handlers[event]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        tasks.append(handler(data or {}))
                    else:
                        tasks.append(
                            asyncio.get_event_loop().run_in_executor(
                                None, handler, data or {}
                            )
                        )
                except Exception as e:
                    logger.error(f"Event handler error for {event}: {e}")

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    def on_event(self, event: str, handler: Callable) -> None:
        """Register event handler"""
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(handler)

    async def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status"""
        async with self._lock:
            return {
                **self._health_status,
                "service_name": self.name,
                "state": self.state.value,
                "metrics": {
                    "total_requests": self.metrics.total_requests,
                    "active_requests": self.metrics.active_requests,
                    "successful_requests": self.metrics.successful_requests,
                    "failed_requests": self.metrics.failed_requests,
                    "rejected_requests": self.metrics.rejected_requests,
                    "average_response_time": self.metrics.average_response_time,
                    "uptime_seconds": time.time() - self.metrics.start_time,
                },
            }

    async def get_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        async with self._lock:
            return {
                "service_name": self.name,
                "state": self.state.value,
                "metrics": {
                    "total_requests": self.metrics.total_requests,
                    "active_requests": self.metrics.active_requests,
                    "successful_requests": self.metrics.successful_requests,
                    "failed_requests": self.metrics.failed_requests,
                    "timeout_requests": self.metrics.timeout_requests,
                    "rejected_requests": self.metrics.rejected_requests,
                    "average_response_time": self.metrics.average_response_time,
                    "uptime_seconds": time.time() - self.metrics.start_time,
                },
                "config": {
                    "max_concurrent_requests": self.config.max_concurrent_requests,
                    "request_timeout": self.config.request_timeout,
                    "health_check_interval": self.config.health_check_interval,
                },
            }
