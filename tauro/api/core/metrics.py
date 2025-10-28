"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Callable
import time
from functools import wraps

try:
    from prometheus_client import (
        Counter,
        Histogram,
        Gauge,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

    # Dummy implementations when prometheus_client is not available
    class Counter:  # noqa: F811
        def __init__(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def inc(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

    class Histogram:  # noqa: F811
        def __init__(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def observe(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def labels(self, *args, **kwargs):  # noqa: ARG002
            return self  # return self for chaining

    class Gauge:  # noqa: F811
        def __init__(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def set(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def inc(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable

        def dec(self, *args, **kwargs):  # noqa: ARG002
            pass  # noop when prometheus_client unavailable


# =============================================================================
# Request Metrics
# =============================================================================

# Counter for total requests
requests_total = Counter(
    "tauro_requests_total",
    "Total number of requests",
    ["method", "endpoint", "status"],
)

# Histogram for request duration
request_duration_seconds = Histogram(
    "tauro_request_duration_seconds",
    "Request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# Gauge for active requests
active_requests = Gauge(
    "tauro_active_requests",
    "Number of currently active requests",
    ["method", "endpoint"],
)

# =============================================================================
# Pipeline Metrics
# =============================================================================

# Counter for pipeline executions
pipeline_executions_total = Counter(
    "tauro_pipeline_executions_total",
    "Total pipeline executions",
    ["pipeline_id", "status"],
)

# Gauge for active pipeline runs
active_pipeline_runs = Gauge(
    "tauro_active_pipeline_runs",
    "Number of active pipeline runs",
    ["pipeline_id"],
)

# Histogram for pipeline execution duration
pipeline_execution_duration_seconds = Histogram(
    "tauro_pipeline_execution_duration_seconds",
    "Pipeline execution duration in seconds",
    ["pipeline_id"],
    buckets=(1, 5, 10, 30, 60, 300, 600, 3600),
)

# =============================================================================
# Streaming Metrics
# =============================================================================

# Gauge for active streams
active_streams = Gauge(
    "tauro_active_streams",
    "Number of active streams",
)

# Counter for stream messages
stream_messages_total = Counter(
    "tauro_stream_messages_total",
    "Total stream messages processed",
    ["stream_id"],
)

# Gauge for stream errors
stream_errors_total = Counter(
    "tauro_stream_errors_total",
    "Total stream errors",
    ["stream_id"],
)

# =============================================================================
# Scheduler Metrics
# =============================================================================

# Counter for scheduled jobs
scheduled_jobs_total = Counter(
    "tauro_scheduled_jobs_total",
    "Total scheduled job executions",
    ["schedule_id", "status"],
)

# Gauge for pending jobs
pending_jobs = Gauge(
    "tauro_pending_jobs",
    "Number of pending scheduled jobs",
    ["schedule_id"],
)

# =============================================================================
# Rate Limiter Metrics
# =============================================================================

# Counter for rate limit violations
rate_limit_violations_total = Counter(
    "tauro_rate_limit_violations_total",
    "Total rate limit violations",
    ["client_ip"],
)

# Gauge for tracked clients
tracked_clients = Gauge(
    "tauro_rate_limiter_tracked_clients",
    "Number of clients tracked by rate limiter",
)

# =============================================================================
# Cache Metrics
# =============================================================================

# Counter for cache hits/misses
cache_hits_total = Counter(
    "tauro_cache_hits_total",
    "Total cache hits",
    ["cache_name"],
)

cache_misses_total = Counter(
    "tauro_cache_misses_total",
    "Total cache misses",
    ["cache_name"],
)

# =============================================================================
# Database Metrics
# =============================================================================

# Histogram for database query duration
db_query_duration_seconds = Histogram(
    "tauro_db_query_duration_seconds",
    "Database query duration in seconds",
    ["query_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0),
)

# Counter for database errors
db_errors_total = Counter(
    "tauro_db_errors_total",
    "Total database errors",
    ["query_type", "error_type"],
)

# =============================================================================
# Decorator for automatic instrumentation
# =============================================================================


def _record_metrics(ep: str, method: str, duration: float, status: int) -> None:
    """Helper to record metrics (reduces cognitive complexity)"""
    labels = {"method": method, "endpoint": ep}
    request_duration_seconds.labels(**labels).observe(duration)
    requests_total.labels(**{**labels, "status": status}).inc()


def instrument_request(endpoint: str = None):
    """
    Decorator to automatically instrument endpoint with metrics.

    Usage:
        @router.get("/path")
        @instrument_request("my_endpoint")
        async def handler():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            ep = endpoint or func.__name__
            request_obj = kwargs.get("request")
            method = request_obj.method if request_obj else "GET"
            labels = {"method": method, "endpoint": ep}

            active_requests.labels(**labels).inc()
            start_time = time.time()
            status_code = 500

            try:
                result = await func(*args, **kwargs)
                status_code = getattr(result, "status_code", 200)
                return result
            finally:
                duration = time.time() - start_time
                active_requests.labels(**labels).dec()
                _record_metrics(ep, method, duration, status_code)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            ep = endpoint or func.__name__
            request_obj = kwargs.get("request")
            method = request_obj.method if request_obj else "GET"
            labels = {"method": method, "endpoint": ep}

            active_requests.labels(**labels).inc()
            start_time = time.time()
            status_code = 500

            try:
                result = func(*args, **kwargs)
                status_code = getattr(result, "status_code", 200)
                return result
            finally:
                duration = time.time() - start_time
                active_requests.labels(**labels).dec()
                _record_metrics(ep, method, duration, status_code)

        # Return async wrapper if original is async
        return async_wrapper if hasattr(func, "__await__") else sync_wrapper

    return decorator


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "PROMETHEUS_AVAILABLE",
    "requests_total",
    "request_duration_seconds",
    "active_requests",
    "pipeline_executions_total",
    "active_pipeline_runs",
    "pipeline_execution_duration_seconds",
    "active_streams",
    "stream_messages_total",
    "stream_errors_total",
    "scheduled_jobs_total",
    "pending_jobs",
    "rate_limit_violations_total",
    "tracked_clients",
    "cache_hits_total",
    "cache_misses_total",
    "db_query_duration_seconds",
    "db_errors_total",
    "instrument_request",
]
