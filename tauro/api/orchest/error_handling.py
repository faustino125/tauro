"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, Optional, Callable, TypeVar
from datetime import datetime, timezone
from functools import wraps

from loguru import logger  # type: ignore

T = TypeVar("T")


class ErrorMetrics:
    """Track error metrics for alerting and monitoring."""

    def __init__(self):
        self.circuit_breaker_rejects = 0
        self.bulkhead_rejects = 0
        self.execution_timeouts = 0
        self.database_errors = 0
        self.parsing_errors = 0
        self.thread_errors = 0
        self.total_operations = 0
        self.total_errors = 0

    def increment(self, error_type: str) -> None:
        """Increment error counter."""
        if hasattr(self, error_type):
            setattr(self, error_type, getattr(self, error_type) + 1)
        self.total_errors += 1
        self.total_operations += 1

    def record_success(self) -> None:
        """Record successful operation."""
        self.total_operations += 1

    def error_rate(self) -> float:
        """Get current error rate."""
        if self.total_operations == 0:
            return 0.0
        return self.total_errors / self.total_operations

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        return {
            "total_operations": self.total_operations,
            "total_errors": self.total_errors,
            "error_rate": self.error_rate(),
            "circuit_breaker_rejects": self.circuit_breaker_rejects,
            "bulkhead_rejects": self.bulkhead_rejects,
            "execution_timeouts": self.execution_timeouts,
            "database_errors": self.database_errors,
            "parsing_errors": self.parsing_errors,
            "thread_errors": self.thread_errors,
        }

    def check_alert_threshold(self, threshold: float = 0.1) -> Optional[str]:
        """
        Check if error rate exceeds alert threshold.

        Returns:
            Alert message if threshold exceeded, None otherwise
        """
        if self.total_operations < 10:
            # Need at least 10 operations for meaningful rate
            return None

        rate = self.error_rate()
        if rate > threshold:
            return (
                f"High error rate detected: {rate*100:.1f}% "
                f"({self.total_errors}/{self.total_operations} operations)"
            )
        return None


def log_operation_error(
    operation_name: str,
    error_type: str = "general",
    entity_id: Optional[str] = None,
    **context: Any,
) -> None:
    """
    Log operation error with structured context.
    """
    log_context = {
        "operation": operation_name,
        "error_type": error_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if entity_id:
        log_context["entity_id"] = entity_id

    log_context.update(context)

    logger.error(
        f"Operation failed: {operation_name}",
        extra=log_context,
    )


def log_fallback_used(
    operation_name: str,
    fallback_action: str,
    entity_id: Optional[str] = None,
    reason: Optional[str] = None,
    **context: Any,
) -> None:
    """
    Log when fallback action is used instead of primary operation.
    """
    log_context = {
        "operation": operation_name,
        "fallback_action": fallback_action,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if entity_id:
        log_context["entity_id"] = entity_id

    log_context.update(context)

    logger.warning(
        f"Using fallback for {operation_name}",
        extra=log_context,
    )


def with_error_handling(
    operation_name: str,
    error_type: str = "general",
    metrics: Optional[ErrorMetrics] = None,
) -> Callable:
    """
    Decorator for operations with structured error handling and logging.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                result = func(*args, **kwargs)
                if metrics:
                    metrics.record_success()
                return result
            except Exception as e:
                if metrics:
                    metrics.increment(error_type)

                log_context = {
                    "operation": operation_name,
                    "error_type": error_type,
                    "error": str(e),
                    "exception_type": type(e).__name__,
                }

                # Try to extract entity_id from kwargs
                for key in ("entity_id", "id", "run_id", "schedule_id", "pipeline_id"):
                    if key in kwargs:
                        log_context["entity_id"] = kwargs[key]
                        break

                logger.error(
                    f"Operation failed: {operation_name}",
                    extra=log_context,
                )
                raise

        return wrapper

    return decorator


def safe_fallback(
    primary_func: Callable[..., T],
    fallback_value: T,
    operation_name: str,
    entity_id: Optional[str] = None,
    metrics: Optional[ErrorMetrics] = None,
    error_type: str = "general",
    log_error: bool = True,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Execute primary function with fallback on error.
    """
    try:
        return primary_func(*args, **kwargs)
    except Exception as e:
        if metrics:
            metrics.increment(error_type)

        if log_error:
            log_context = {
                "operation": operation_name,
                "fallback_action": f"Returned default: {fallback_value}",
                "error": str(e),
                "exception_type": type(e).__name__,
            }

            if entity_id:
                log_context["entity_id"] = entity_id

            logger.warning(
                f"Using fallback for {operation_name}",
                extra=log_context,
            )

        return fallback_value


class ContextLogger:
    """
    Helper for maintaining correlation context across async operations.
    """

    def __init__(self, **context: Any):
        """
        Initialize with base context.

        Args:
            **context: Base context fields (run_id, schedule_id, etc.)
        """
        self.context = dict(context)

    def error(self, message: str, **extra: Any) -> None:
        """Log error with context."""
        log_data = dict(self.context)
        log_data.update(extra)
        logger.error(message, extra=log_data)

    def warning(self, message: str, **extra: Any) -> None:
        """Log warning with context."""
        log_data = dict(self.context)
        log_data.update(extra)
        logger.warning(message, extra=log_data)

    def info(self, message: str, **extra: Any) -> None:
        """Log info with context."""
        log_data = dict(self.context)
        log_data.update(extra)
        logger.info(message, extra=log_data)

    def debug(self, message: str, **extra: Any) -> None:
        """Log debug with context."""
        log_data = dict(self.context)
        log_data.update(extra)
        logger.debug(message, extra=log_data)

    def with_extra(self, **extra: Any) -> "ContextLogger":
        """Create new logger with additional context."""
        new_context = dict(self.context)
        new_context.update(extra)
        return ContextLogger(**new_context)
