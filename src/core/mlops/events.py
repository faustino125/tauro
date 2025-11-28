from __future__ import annotations

import threading
import time
import weakref
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from loguru import logger


class EventType(str, Enum):
    """MLOps event types."""

    # Experiment events
    EXPERIMENT_CREATED = "experiment.created"
    EXPERIMENT_DELETED = "experiment.deleted"

    # Run events
    RUN_STARTED = "run.started"
    RUN_ENDED = "run.ended"
    RUN_FAILED = "run.failed"
    RUN_CLEANED = "run.cleaned"

    # Metric events
    METRIC_LOGGED = "metric.logged"
    METRICS_FLUSHED = "metrics.flushed"
    METRIC_THRESHOLD_EXCEEDED = "metric.threshold_exceeded"

    # Parameter events
    PARAMETER_LOGGED = "parameter.logged"

    # Artifact events
    ARTIFACT_LOGGED = "artifact.logged"
    ARTIFACT_DOWNLOADED = "artifact.downloaded"

    # Model events
    MODEL_REGISTERED = "model.registered"
    MODEL_PROMOTED = "model.promoted"
    MODEL_DELETED = "model.deleted"
    MODEL_DOWNLOADED = "model.downloaded"

    # Storage events
    STORAGE_WRITE_STARTED = "storage.write.started"
    STORAGE_WRITE_COMPLETED = "storage.write.completed"
    STORAGE_READ_STARTED = "storage.read.started"
    STORAGE_READ_COMPLETED = "storage.read.completed"
    STORAGE_ERROR = "storage.error"

    # System events
    CONTEXT_INITIALIZED = "context.initialized"
    CONTEXT_CLEANUP = "context.cleanup"
    CLEANUP_STARTED = "cleanup.started"
    CLEANUP_COMPLETED = "cleanup.completed"

    # Circuit breaker events
    CIRCUIT_OPENED = "circuit.opened"
    CIRCUIT_CLOSED = "circuit.closed"
    CIRCUIT_HALF_OPEN = "circuit.half_open"

    # Lock events
    LOCK_ACQUIRED = "lock.acquired"
    LOCK_RELEASED = "lock.released"
    LOCK_TIMEOUT = "lock.timeout"
    LOCK_STALE_CLEANED = "lock.stale_cleaned"


@dataclass
class Event:
    """Represents an MLOps event."""

    event_type: EventType
    timestamp: str
    data: Dict[str, Any]
    source: str = "mlops"
    correlation_id: Optional[str] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "data": self.data,
            "source": self.source,
            "correlation_id": self.correlation_id,
        }


EventCallback = Callable[[Event], None]


class EventEmitter:
    """
    Thread-safe event emitter for MLOps components.
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self._listeners: Dict[EventType, Set[EventCallback]] = defaultdict(set)
        self._wildcard_listeners: Set[EventCallback] = set()
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history = 1000
        self._enabled = True

    def on(
        self,
        event_type: Union[EventType, str],
        callback: EventCallback,
    ) -> None:
        """
        Register an event listener.
        """
        with self._lock:
            if event_type == "*":
                self._wildcard_listeners.add(callback)
            else:
                if isinstance(event_type, str):
                    event_type = EventType(event_type)
                self._listeners[event_type].add(callback)

    def off(
        self,
        event_type: Union[EventType, str],
        callback: EventCallback,
    ) -> None:
        """
        Remove an event listener.
        """
        with self._lock:
            if event_type == "*":
                self._wildcard_listeners.discard(callback)
            else:
                if isinstance(event_type, str):
                    event_type = EventType(event_type)
                self._listeners[event_type].discard(callback)

    def emit(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        source: str = "mlops",
        correlation_id: Optional[str] = None,
    ) -> Event:
        """
        Emit an event to all registered listeners.
        """
        if not self._enabled:
            return Event(event_type=event_type, timestamp="", data=data)

        event = Event(
            event_type=event_type,
            timestamp=datetime.now(timezone.utc).isoformat(),
            data=data,
            source=source,
            correlation_id=correlation_id,
        )

        with self._lock:
            # Store in history
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history :]

            # Get listeners (copy to avoid modification during iteration)
            listeners = list(self._listeners.get(event_type, set()))
            wildcard = list(self._wildcard_listeners)

        # Call listeners outside lock
        all_listeners = listeners + wildcard
        for callback in all_listeners:
            try:
                callback(event)
            except Exception as e:
                logger.warning(f"Event listener error for {event_type.value}: {e}")

        return event

    def enable(self) -> None:
        """Enable event emission."""
        self._enabled = True

    def disable(self) -> None:
        """Disable event emission (for performance)."""
        self._enabled = False

    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 100,
    ) -> List[Event]:
        """
        Get recent event history.
        """
        with self._lock:
            if event_type is None:
                return list(self._event_history[-limit:])
            return [e for e in self._event_history[-limit * 2 :] if e.event_type == event_type][
                -limit:
            ]

    def clear_history(self) -> None:
        """Clear event history."""
        with self._lock:
            self._event_history.clear()

    def listener_count(self, event_type: Optional[EventType] = None) -> int:
        """Get count of registered listeners."""
        with self._lock:
            if event_type is None:
                total = sum(len(ls) for ls in self._listeners.values())
                return total + len(self._wildcard_listeners)
            return len(self._listeners.get(event_type, set()))


@dataclass
class MetricSample:
    """A single metric sample."""

    name: str
    value: float
    timestamp: str
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricAggregation:
    """Aggregated metric statistics."""

    name: str
    count: int
    total: float
    min_value: float
    max_value: float
    last_value: float
    last_timestamp: str

    @property
    def mean(self) -> float:
        """Calculate mean value."""
        return self.total / self.count if self.count > 0 else 0.0


class MetricsCollector:
    """
    Collects and aggregates operational metrics for MLOps.
    """

    def __init__(self):
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._aggregations: Dict[str, MetricAggregation] = {}
        self._lock = threading.RLock()
        self._max_timer_samples = 1000

    def increment(
        self,
        name: str,
        value: int = 1,
        tags: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Increment a counter metric.
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._counters[key] += value
            return self._counters[key]

    def decrement(
        self,
        name: str,
        value: int = 1,
        tags: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Decrement a counter metric.
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._counters[key] = max(0, self._counters[key] - value)
            return self._counters[key]

    def gauge(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric (point-in-time value).
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._gauges[key] = value
            self._update_aggregation(key, value)

    def timer(self, name: str, tags: Optional[Dict[str, str]] = None):
        """
        Context manager for timing operations.
        """
        return _TimerContext(self, name, tags)

    def record_time(
        self,
        name: str,
        duration: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a duration measurement.
        """
        key = self._make_key(name, tags)
        with self._lock:
            self._timers[key].append(duration)
            if len(self._timers[key]) > self._max_timer_samples:
                self._timers[key] = self._timers[key][-self._max_timer_samples :]
            self._update_aggregation(key, duration)

    def _make_key(self, name: str, tags: Optional[Dict[str, str]]) -> str:
        """Create a metric key from name and tags."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"

    def _update_aggregation(self, key: str, value: float) -> None:
        """Update running aggregation for a metric."""
        now = datetime.now(timezone.utc).isoformat()

        if key not in self._aggregations:
            self._aggregations[key] = MetricAggregation(
                name=key,
                count=1,
                total=value,
                min_value=value,
                max_value=value,
                last_value=value,
                last_timestamp=now,
            )
        else:
            agg = self._aggregations[key]
            agg.count += 1
            agg.total += value
            agg.min_value = min(agg.min_value, value)
            agg.max_value = max(agg.max_value, value)
            agg.last_value = value
            agg.last_timestamp = now

    def get_counter(self, name: str, tags: Optional[Dict[str, str]] = None) -> int:
        """Get current counter value."""
        key = self._make_key(name, tags)
        with self._lock:
            return self._counters.get(key, 0)

    def get_gauge(self, name: str, tags: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get current gauge value."""
        key = self._make_key(name, tags)
        with self._lock:
            return self._gauges.get(key)

    def get_timer_stats(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, float]:
        """
        Get statistics for a timer.
        """
        key = self._make_key(name, tags)
        with self._lock:
            samples = self._timers.get(key, [])
            if not samples:
                return {}

            sorted_samples = sorted(samples)
            count = len(sorted_samples)

            return {
                "count": count,
                "min": min(sorted_samples),
                "max": max(sorted_samples),
                "mean": sum(sorted_samples) / count,
                "p50": sorted_samples[int(count * 0.5)],
                "p95": sorted_samples[int(count * 0.95)] if count >= 20 else sorted_samples[-1],
                "p99": sorted_samples[int(count * 0.99)] if count >= 100 else sorted_samples[-1],
            }

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all collected metrics.
        """
        with self._lock:
            summary = {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "timers": {},
                "aggregations": {},
            }

            for key in self._timers:
                samples = self._timers[key]
                if samples:
                    summary["timers"][key] = {
                        "count": len(samples),
                        "mean": sum(samples) / len(samples),
                        "min": min(samples),
                        "max": max(samples),
                    }

            for key, agg in self._aggregations.items():
                summary["aggregations"][key] = {
                    "count": agg.count,
                    "mean": agg.mean,
                    "min": agg.min_value,
                    "max": agg.max_value,
                    "last": agg.last_value,
                }

            return summary

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
            self._aggregations.clear()


class _TimerContext:
    """Context manager for timing operations."""

    def __init__(
        self,
        collector: MetricsCollector,
        name: str,
        tags: Optional[Dict[str, str]],
    ):
        self._collector = collector
        self._name = name
        self._tags = tags
        self._start_time: Optional[float] = None

    def __enter__(self):
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._start_time is not None:
            duration = time.perf_counter() - self._start_time
            self._collector.record_time(self._name, duration, self._tags)
        return False


# Global event emitter
_global_emitter: Optional[EventEmitter] = None
_emitter_lock = threading.Lock()


def get_event_emitter() -> EventEmitter:
    """Get the global event emitter instance."""
    global _global_emitter
    with _emitter_lock:
        if _global_emitter is None:
            _global_emitter = EventEmitter("global")
        return _global_emitter


def emit_event(
    event_type: EventType,
    data: Dict[str, Any],
    source: str = "mlops",
) -> Event:
    """
    Emit an event using the global emitter.
    """
    return get_event_emitter().emit(event_type, data, source)


# Global metrics collector
_global_metrics: Optional[MetricsCollector] = None
_metrics_lock = threading.Lock()


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _global_metrics
    with _metrics_lock:
        if _global_metrics is None:
            _global_metrics = MetricsCollector()
        return _global_metrics


class HookType(str, Enum):
    """Types of hooks for MLOps operations."""

    # Pre-operation hooks
    PRE_RUN_START = "pre.run.start"
    PRE_RUN_END = "pre.run.end"
    PRE_METRIC_LOG = "pre.metric.log"
    PRE_MODEL_REGISTER = "pre.model.register"
    PRE_MODEL_PROMOTE = "pre.model.promote"
    PRE_STORAGE_WRITE = "pre.storage.write"
    PRE_STORAGE_READ = "pre.storage.read"

    # Post-operation hooks
    POST_RUN_START = "post.run.start"
    POST_RUN_END = "post.run.end"
    POST_METRIC_LOG = "post.metric.log"
    POST_MODEL_REGISTER = "post.model.register"
    POST_MODEL_PROMOTE = "post.model.promote"
    POST_STORAGE_WRITE = "post.storage.write"
    POST_STORAGE_READ = "post.storage.read"

    # Error hooks
    ON_ERROR = "on.error"
    ON_RETRY = "on.retry"


HookCallback = Callable[[HookType, Dict[str, Any]], Optional[Dict[str, Any]]]


class HooksManager:
    """
    Manages hooks for extending MLOps operations.
    """

    def __init__(self):
        self._hooks: Dict[HookType, List[HookCallback]] = defaultdict(list)
        self._lock = threading.RLock()

    def register(
        self,
        hook_type: HookType,
        callback: Optional[HookCallback] = None,
        priority: int = 0,
    ):
        """
        Register a hook callback.
        """

        def decorator(func: HookCallback) -> HookCallback:
            with self._lock:
                # Insert at position based on priority
                hooks = self._hooks[hook_type]
                # Find insertion point (higher priority first)
                insert_idx = len(hooks)
                for i, (_, p) in enumerate(hooks):
                    if priority > p:
                        insert_idx = i
                        break
                hooks.insert(insert_idx, (func, priority))
            return func

        if callback is not None:
            return decorator(callback)
        return decorator

    def unregister(
        self,
        hook_type: HookType,
        callback: HookCallback,
    ) -> bool:
        """
        Unregister a hook callback.
        """
        with self._lock:
            hooks = self._hooks[hook_type]
            for i, (cb, _) in enumerate(hooks):
                if cb == callback:
                    hooks.pop(i)
                    return True
            return False

    def execute(
        self,
        hook_type: HookType,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute all hooks for a given type.
        """
        with self._lock:
            hooks = list(self._hooks.get(hook_type, []))

        result = data
        for callback, _ in hooks:
            try:
                hook_result = callback(hook_type, result)
                if hook_result is not None:
                    result = hook_result
            except Exception as e:
                logger.error(f"Hook {hook_type.value} failed: {e}")
                # For ON_ERROR hooks, don't re-raise
                if hook_type != HookType.ON_ERROR:
                    raise

        return result

    def clear(self, hook_type: Optional[HookType] = None) -> None:
        """
        Clear registered hooks.
        """
        with self._lock:
            if hook_type is None:
                self._hooks.clear()
            else:
                self._hooks[hook_type].clear()


# Global hooks manager
_global_hooks: Optional[HooksManager] = None
_hooks_lock = threading.Lock()


def get_hooks_manager() -> HooksManager:
    """Get the global hooks manager instance."""
    global _global_hooks
    with _hooks_lock:
        if _global_hooks is None:
            _global_hooks = HooksManager()
        return _global_hooks


@dataclass
class AuditEntry:
    """An audit log entry."""

    timestamp: str
    operation: str
    user: Optional[str]
    resource_type: str
    resource_id: str
    action: str
    status: str
    details: Dict[str, Any]
    duration_ms: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "operation": self.operation,
            "user": self.user,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "action": self.action,
            "status": self.status,
            "details": self.details,
            "duration_ms": self.duration_ms,
        }


class AuditLogger:
    """
    Audit logger for tracking MLOps operations.
    """

    def __init__(
        self,
        max_entries: int = 10000,
        persist_callback: Optional[Callable[[AuditEntry], None]] = None,
    ):
        self._entries: List[AuditEntry] = []
        self._max_entries = max_entries
        self._persist_callback = persist_callback
        self._lock = threading.RLock()

    def log(
        self,
        operation: str,
        resource_type: str,
        resource_id: str,
        action: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        user: Optional[str] = None,
        duration_ms: Optional[float] = None,
    ) -> AuditEntry:
        """
        Log an audit entry.
        """
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            operation=operation,
            user=user,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            status=status,
            details=details or {},
            duration_ms=duration_ms,
        )

        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[-self._max_entries :]

        # Persist if callback provided
        if self._persist_callback:
            try:
                self._persist_callback(entry)
            except Exception as e:
                logger.warning(f"Failed to persist audit entry: {e}")

        return entry

    def _matches_query(
        self,
        entry: AuditEntry,
        operation: Optional[str],
        resource_type: Optional[str],
        resource_id: Optional[str],
        status: Optional[str],
        since: Optional[str],
    ) -> bool:
        """Check if an entry matches the query criteria."""
        if operation and entry.operation != operation:
            return False
        if resource_type and entry.resource_type != resource_type:
            return False
        if resource_id and entry.resource_id != resource_id:
            return False
        if status and entry.status != status:
            return False
        if since and entry.timestamp < since:
            return False
        return True

    def query(
        self,
        operation: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        """
        Query audit entries.
        """
        with self._lock:
            results = []

            for entry in reversed(self._entries):
                if len(results) >= limit:
                    break

                if self._matches_query(entry, operation, resource_type, resource_id, status, since):
                    results.append(entry)

            return results

    def get_summary(self) -> Dict[str, Any]:
        """Get audit log summary statistics."""
        with self._lock:
            operations: Dict[str, int] = defaultdict(int)
            statuses: Dict[str, int] = defaultdict(int)
            resource_types: Dict[str, int] = defaultdict(int)

            for entry in self._entries:
                operations[entry.operation] += 1
                statuses[entry.status] += 1
                resource_types[entry.resource_type] += 1

            return {
                "total_entries": len(self._entries),
                "operations": dict(operations),
                "statuses": dict(statuses),
                "resource_types": dict(resource_types),
            }

    def clear(self) -> int:
        """Clear all audit entries. Returns count of cleared entries."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            return count


# Global audit logger
_global_audit: Optional[AuditLogger] = None
_audit_lock = threading.Lock()


def get_audit_logger() -> AuditLogger:
    """Get the global audit logger instance."""
    global _global_audit
    with _audit_lock:
        if _global_audit is None:
            _global_audit = AuditLogger()
        return _global_audit
