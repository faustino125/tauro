import functools
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from loguru import logger


class ResilienceError(Exception):
    """Base exception for resilience operations."""

    pass


class RetryExhaustedError(ResilienceError):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, operation: str, attempts: int, last_error: Exception):
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Operation '{operation}' failed after {attempts} attempts. "
            f"Last error: {last_error}"
        )


class CircuitBreakerOpenError(ResilienceError):
    """Raised when circuit breaker is open and preventing operations."""

    def __init__(self, name: str, reset_time: float):
        self.name = name
        self.reset_time = reset_time
        super().__init__(
            f"Circuit breaker '{name}' is open. Will reset in {reset_time:.1f} seconds."
        )


class ResourceLimitError(ResilienceError):
    """Raised when resource limits are exceeded."""

    def __init__(self, resource: str, current: int, limit: int):
        self.resource = resource
        self.current = current
        self.limit = limit
        super().__init__(f"Resource limit exceeded for '{resource}': {current}/{limit}")


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 0.1
    max_delay: float = 10.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (IOError, OSError, TimeoutError)
    non_retryable_exceptions: tuple = ()

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")


# Default retry configurations for different operation types
DEFAULT_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    initial_delay=0.1,
    max_delay=5.0,
)

IO_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    initial_delay=0.2,
    max_delay=10.0,
    retryable_exceptions=(IOError, OSError, TimeoutError, ConnectionError),
)

STORAGE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    initial_delay=0.5,
    max_delay=15.0,
    retryable_exceptions=(IOError, OSError, TimeoutError),
    non_retryable_exceptions=(PermissionError, ValueError, FileNotFoundError, FileExistsError),
)


def calculate_delay(
    attempt: int,
    config: RetryConfig,
) -> float:
    """
    Calculate delay for next retry attempt using exponential backoff.

    Args:
        attempt: Current attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    delay = config.initial_delay * (config.exponential_base**attempt)
    delay = min(delay, config.max_delay)

    if config.jitter:
        import random

        # Add jitter: +/- 25% of delay
        jitter_range = delay * 0.25
        delay = delay + random.uniform(-jitter_range, jitter_range)
        delay = max(0, delay)

    return delay


def should_retry(
    exception: Exception,
    config: RetryConfig,
) -> bool:
    """
    Determine if an exception should trigger a retry.

    Args:
        exception: The exception that was raised
        config: Retry configuration

    Returns:
        True if operation should be retried
    """
    # Never retry non-retryable exceptions
    if config.non_retryable_exceptions:
        if isinstance(exception, config.non_retryable_exceptions):
            return False

    # Retry if it's a retryable exception type
    return isinstance(exception, config.retryable_exceptions)


F = TypeVar("F", bound=Callable[..., Any])


def _handle_retry_exception(
    exception: Exception,
    attempt: int,
    name: str,
    config: RetryConfig,
    on_retry: Optional[Callable[[Exception, int], None]],
) -> None:
    """
    Handle a retry exception by logging and sleeping.

    Args:
        exception: The exception that occurred
        attempt: Current attempt number (0-indexed)
        name: Operation name
        config: Retry configuration
        on_retry: Optional callback on retry

    Raises:
        RetryExhaustedError: If no more attempts available
    """
    if not should_retry(exception, config):
        logger.warning(f"Non-retryable exception in '{name}': {exception}")
        raise

    if attempt + 1 >= config.max_attempts:
        logger.error(f"'{name}' failed after {attempt + 1} attempts: {exception}")
        raise RetryExhaustedError(name, attempt + 1, exception) from exception

    delay = calculate_delay(attempt, config)
    logger.warning(
        f"'{name}' attempt {attempt + 1}/{config.max_attempts} "
        f"failed: {exception}. Retrying in {delay:.2f}s..."
    )

    if on_retry:
        try:
            on_retry(exception, attempt + 1)
        except Exception:
            pass

    time.sleep(delay)


def with_retry(
    config: Optional[RetryConfig] = None,
    operation_name: Optional[str] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[F], F]:
    """
    Decorator to add retry logic to a function.
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            last_exception: Optional[Exception] = None

            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    _handle_retry_exception(e, attempt, name, config, on_retry)

            # Should not reach here, but safety net
            raise RetryExhaustedError(name, config.max_attempts, last_exception)

        return wrapper  # type: ignore

    return decorator


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    half_open_max_calls: int = 3


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        with self._lock:
            self._maybe_transition_from_open()
            return self._state

    @property
    def is_available(self) -> bool:
        """Check if circuit allows operations."""
        return self.state != CircuitState.OPEN

    def _maybe_transition_from_open(self) -> None:
        """Check if timeout has passed and transition to half-open."""
        if self._state != CircuitState.OPEN:
            return

        if self._last_failure_time is None:
            return

        elapsed = time.time() - self._last_failure_time
        if elapsed >= self.config.timeout:
            logger.info(f"Circuit breaker '{self.name}' transitioning from OPEN to HALF_OPEN")
            self._state = CircuitState.HALF_OPEN
            self._half_open_calls = 0
            self._success_count = 0

    def record_success(self) -> None:
        """Record a successful operation."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    logger.info(f"Circuit breaker '{self.name}' transitioning to CLOSED")
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def record_failure(self, error: Optional[Exception] = None) -> None:
        """Record a failed operation."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(
                    f"Circuit breaker '{self.name}' failure in HALF_OPEN, "
                    f"transitioning to OPEN" + (f": {error}" if error else "")
                )
                self._state = CircuitState.OPEN
                self._half_open_calls = 0
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    logger.warning(
                        f"Circuit breaker '{self.name}' threshold reached "
                        f"({self._failure_count}), transitioning to OPEN"
                        + (f": {error}" if error else "")
                    )
                    self._state = CircuitState.OPEN

    def check(self) -> None:
        """
        Check if operation is allowed.
        """
        with self._lock:
            self._maybe_transition_from_open()

            if self._state == CircuitState.OPEN:
                remaining = 0.0
                if self._last_failure_time:
                    remaining = max(
                        0, self.config.timeout - (time.time() - self._last_failure_time)
                    )
                raise CircuitBreakerOpenError(self.name, remaining)

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._half_open_calls > self.config.half_open_max_calls:
                    # Too many calls in half-open, go back to open
                    self._state = CircuitState.OPEN
                    raise CircuitBreakerOpenError(self.name, self.config.timeout)

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            self._last_failure_time = None
            logger.info(f"Circuit breaker '{self.name}' manually reset to CLOSED")

    def __call__(self, func: F) -> F:
        """Use circuit breaker as a decorator."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.check()
            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception:
                self.record_failure()
                raise

        return wrapper  # type: ignore


@dataclass
class ResourceLimits:
    """Resource limits configuration."""

    max_active_runs: int = 100
    max_metrics_per_run: int = 10000
    max_parameters_per_run: int = 1000
    max_artifacts_per_run: int = 100
    max_memory_mb: Optional[int] = None
    warn_threshold: float = 0.8  # Warn at 80% of limit


class ResourceTracker:
    """
    Track and enforce resource limits.
    """

    def __init__(
        self,
        name: str,
        limits: Optional[ResourceLimits] = None,
    ):
        self.name = name
        self.limits = limits or ResourceLimits()

        self._counters: Dict[str, int] = {}
        self._lock = threading.RLock()

    def increment(self, resource: str, amount: int = 1) -> int:
        """
        Increment a resource counter.
        """
        with self._lock:
            current = self._counters.get(resource, 0)
            new_value = current + amount

            # Check limits
            limit = self._get_limit(resource)
            if limit is not None and new_value > limit:
                raise ResourceLimitError(resource, new_value, limit)

            # Warn if approaching limit
            if limit is not None:
                ratio = new_value / limit
                if ratio >= self.limits.warn_threshold:
                    logger.warning(
                        f"Resource '{resource}' at {ratio:.0%} of limit ({new_value}/{limit})"
                    )

            self._counters[resource] = new_value
            return new_value

    def decrement(self, resource: str, amount: int = 1) -> int:
        """
        Decrement a resource counter.
        """
        with self._lock:
            current = self._counters.get(resource, 0)
            new_value = max(0, current - amount)
            self._counters[resource] = new_value
            return new_value

    def get(self, resource: str) -> int:
        """Get current count for a resource."""
        with self._lock:
            return self._counters.get(resource, 0)

    def reset(self, resource: Optional[str] = None) -> None:
        """Reset resource counter(s)."""
        with self._lock:
            if resource:
                self._counters.pop(resource, None)
            else:
                self._counters.clear()

    def _get_limit(self, resource: str) -> Optional[int]:
        """Get limit for a resource."""
        limit_map = {
            "active_runs": self.limits.max_active_runs,
            "metrics": self.limits.max_metrics_per_run,
            "parameters": self.limits.max_parameters_per_run,
            "artifacts": self.limits.max_artifacts_per_run,
        }
        return limit_map.get(resource)

    def get_status(self) -> Dict[str, Any]:
        """Get current resource status."""
        with self._lock:
            status = {}
            for resource, count in self._counters.items():
                limit = self._get_limit(resource)
                status[resource] = {
                    "current": count,
                    "limit": limit,
                    "usage": f"{count}/{limit}" if limit else str(count),
                }
            return status


@dataclass
class CleanupTask:
    """Represents a cleanup task."""

    name: str
    callback: Callable[[], None]
    priority: int = 0  # Higher = run first
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class CleanupManager:
    """
    Manage cleanup tasks for graceful shutdown.
    """

    def __init__(self):
        self._tasks: List[CleanupTask] = []
        self._lock = threading.RLock()
        self._executed = False

    def register(
        self,
        name: str,
        callback: Callable[[], None],
        priority: int = 0,
    ) -> None:
        """
        Register a cleanup task.
        """
        with self._lock:
            task = CleanupTask(name=name, callback=callback, priority=priority)
            self._tasks.append(task)
            logger.debug(f"Registered cleanup task: {name}")

    def unregister(self, name: str) -> bool:
        """
        Unregister a cleanup task.
        """
        with self._lock:
            original_len = len(self._tasks)
            self._tasks = [t for t in self._tasks if t.name != name]
            removed = len(self._tasks) < original_len
            if removed:
                logger.debug(f"Unregistered cleanup task: {name}")
            return removed

    def execute(self, timeout_per_task: float = 5.0, silent: bool = False) -> Dict[str, bool]:
        """
        Execute all cleanup tasks.
        """
        with self._lock:
            if self._executed:
                if not silent:
                    logger.warning("Cleanup already executed")
                return {}

            self._executed = True

            # Sort by priority (descending)
            sorted_tasks = sorted(self._tasks, key=lambda t: t.priority, reverse=True)

            results = {}
            for task in sorted_tasks:
                try:
                    start = time.time()
                    task.callback()
                    elapsed = time.time() - start

                    if not silent and elapsed > timeout_per_task:
                        logger.warning(
                            f"Cleanup task '{task.name}' took {elapsed:.2f}s "
                            f"(expected < {timeout_per_task}s)"
                        )

                    results[task.name] = True
                    if not silent:
                        logger.debug(f"Cleanup task '{task.name}' completed")

                except Exception as e:
                    results[task.name] = False
                    if not silent:
                        logger.error(f"Cleanup task '{task.name}' failed: {e}")

            if not silent:
                if results:
                    logger.info(
                        f"Cleanup completed: {sum(results.values())}/{len(results)} tasks successful"
                    )
                else:
                    # No tasks: avoid noisy logs on simple invocations like --version
                    pass
            return results

    def reset(self) -> None:
        """Reset cleanup manager for reuse."""
        with self._lock:
            self._tasks.clear()
            self._executed = False


# Global cleanup manager
_cleanup_manager = CleanupManager()


def get_cleanup_manager() -> CleanupManager:
    """Get the global cleanup manager."""
    return _cleanup_manager


def register_cleanup(
    name: str,
    callback: Callable[[], None],
    priority: int = 0,
) -> None:
    """
    Register a cleanup task with the global cleanup manager.
    """
    _cleanup_manager.register(name, callback, priority)


# Register atexit handler for automatic cleanup
import atexit


def _atexit_cleanup():
    """Run cleanup on process exit."""
    try:
        _cleanup_manager.execute(silent=True)
    except Exception as e:
        pass


atexit.register(_atexit_cleanup)
