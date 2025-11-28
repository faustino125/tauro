"""Unit tests for events module.

Tests the event-driven architecture components including EventEmitter,
MetricsCollector, HooksManager, and AuditLogger.
"""

import pytest
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from core.mlops.events import (
    EventType,
    Event,
    EventEmitter,
    MetricsCollector,
    HookType,
    HooksManager,
    AuditLogger,
    AuditEntry,
    get_event_emitter,
    get_metrics_collector,
)


class TestEventType:
    """Tests for EventType enum."""

    def test_all_event_types_defined(self):
        """Verify all expected event types exist."""
        assert EventType.MODEL_REGISTERED is not None
        assert EventType.MODEL_PROMOTED is not None
        assert EventType.MODEL_DELETED is not None
        assert EventType.EXPERIMENT_CREATED is not None
        assert EventType.RUN_STARTED is not None
        assert EventType.RUN_ENDED is not None
        assert EventType.RUN_FAILED is not None
        assert EventType.METRIC_LOGGED is not None
        assert EventType.ARTIFACT_LOGGED is not None


class TestEvent:
    """Tests for Event dataclass."""

    def test_event_creation(self):
        """Test creating an event with all fields."""
        event = Event(
            event_type=EventType.MODEL_REGISTERED,
            timestamp="2024-01-01T00:00:00Z",
            data={"model_name": "test_model"},
            source="test",
            correlation_id="corr-123",
        )

        assert event.event_type == EventType.MODEL_REGISTERED
        assert event.source == "test"
        assert event.data["model_name"] == "test_model"
        assert event.correlation_id == "corr-123"

    def test_event_to_dict(self):
        """Test event to_dict method."""
        event = Event(
            event_type=EventType.RUN_STARTED,
            timestamp="2024-01-01T00:00:00Z",
            data={"run_id": "run-123"},
        )

        event_dict = event.to_dict()
        assert "event_type" in event_dict
        assert "timestamp" in event_dict
        assert "data" in event_dict


class TestEventEmitter:
    """Tests for EventEmitter class."""

    def test_subscribe_and_emit(self):
        """Test subscribing to events and emitting them."""
        emitter = EventEmitter()
        received_events = []

        def handler(event):
            received_events.append(event)

        emitter.on(EventType.MODEL_REGISTERED, handler)

        emitter.emit(EventType.MODEL_REGISTERED, {"model_name": "test"})

        assert len(received_events) == 1
        assert received_events[0].event_type == EventType.MODEL_REGISTERED

    def test_subscribe_all_events(self):
        """Test subscribing to all events with '*'."""
        emitter = EventEmitter()
        received = []

        emitter.on("*", lambda e: received.append(e))

        emitter.emit(EventType.MODEL_REGISTERED, {"model": "test1"})
        emitter.emit(EventType.RUN_STARTED, {"run": "test2"})

        assert len(received) == 2

    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        emitter = EventEmitter()
        received = []

        def handler(event):
            received.append(event)

        emitter.on(EventType.METRIC_LOGGED, handler)
        emitter.off(EventType.METRIC_LOGGED, handler)

        emitter.emit(EventType.METRIC_LOGGED, {"metric": "accuracy"})

        assert len(received) == 0

    def test_multiple_handlers(self):
        """Test multiple handlers for same event type."""
        emitter = EventEmitter()
        handler1_received = []
        handler2_received = []

        emitter.on(EventType.RUN_STARTED, lambda e: handler1_received.append(e))
        emitter.on(EventType.RUN_STARTED, lambda e: handler2_received.append(e))

        emitter.emit(EventType.RUN_STARTED, {"run_id": "123"})

        assert len(handler1_received) == 1
        assert len(handler2_received) == 1

    def test_event_history(self):
        """Test event history tracking."""
        emitter = EventEmitter()
        emitter._max_history = 5

        for i in range(10):
            emitter.emit(EventType.METRIC_LOGGED, {"step": i})

        history = emitter.get_history()
        assert len(history) == 5

    def test_clear_history(self):
        """Test clearing event history."""
        emitter = EventEmitter()

        emitter.emit(EventType.METRIC_LOGGED, {"metric": "test"})

        assert len(emitter.get_history()) > 0
        emitter.clear_history()
        assert len(emitter.get_history()) == 0

    def test_enable_disable(self):
        """Test enabling/disabling event emission."""
        emitter = EventEmitter()
        received = []

        emitter.on(EventType.MODEL_REGISTERED, lambda e: received.append(e))

        emitter.disable()
        emitter.emit(EventType.MODEL_REGISTERED, {"model": "test"})
        assert len(received) == 0

        emitter.enable()
        emitter.emit(EventType.MODEL_REGISTERED, {"model": "test"})
        assert len(received) == 1


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_increment_counter(self):
        """Test incrementing a counter."""
        metrics = MetricsCollector()

        metrics.increment("requests_total")
        metrics.increment("requests_total")
        metrics.increment("requests_total", 5)

        counter = metrics.get_counter("requests_total")
        assert counter == 7

    def test_decrement_counter(self):
        """Test decrementing a counter."""
        metrics = MetricsCollector()

        metrics.increment("active_tasks", 10)
        metrics.decrement("active_tasks", 3)

        counter = metrics.get_counter("active_tasks")
        assert counter == 7

    def test_record_gauge(self):
        """Test recording gauge values."""
        metrics = MetricsCollector()

        metrics.gauge("active_connections", 10)
        metrics.gauge("active_connections", 15)

        gauge = metrics.get_gauge("active_connections")
        assert gauge == 15

    def test_timer_context_manager(self):
        """Test timing context manager."""
        metrics = MetricsCollector()

        with metrics.timer("db_query"):
            time.sleep(0.01)

        stats = metrics.get_timer_stats("db_query")
        assert stats["count"] == 1
        assert stats["min"] >= 0.01

    def test_record_time(self):
        """Test manual time recording."""
        metrics = MetricsCollector()

        metrics.record_time("api_call", 0.1)
        metrics.record_time("api_call", 0.2)
        metrics.record_time("api_call", 0.3)

        stats = metrics.get_timer_stats("api_call")
        assert stats["count"] == 3
        assert 0.19 < stats["mean"] < 0.21

    def test_reset_metrics(self):
        """Test resetting all metrics."""
        metrics = MetricsCollector()

        metrics.increment("counter")
        metrics.gauge("gauge", 10)

        metrics.reset()

        assert metrics.get_counter("counter") == 0
        assert metrics.get_gauge("gauge") is None

    def test_get_summary(self):
        """Test getting all metrics at once."""
        metrics = MetricsCollector()

        metrics.increment("requests")
        metrics.gauge("memory", 1024)
        metrics.record_time("latency", 0.05)

        summary = metrics.get_summary()

        assert "counters" in summary
        assert "gauges" in summary
        assert "timers" in summary

    def test_metrics_with_tags(self):
        """Test metrics with tags."""
        metrics = MetricsCollector()

        metrics.increment("requests", tags={"endpoint": "/api"})
        metrics.increment("requests", tags={"endpoint": "/health"})

        counter1 = metrics.get_counter("requests", tags={"endpoint": "/api"})
        counter2 = metrics.get_counter("requests", tags={"endpoint": "/health"})

        assert counter1 == 1
        assert counter2 == 1


class TestHookType:
    """Tests for HookType enum."""

    def test_hook_types_defined(self):
        """Verify hook types exist."""
        assert HookType.PRE_RUN_START is not None
        assert HookType.POST_RUN_END is not None
        assert HookType.PRE_MODEL_REGISTER is not None
        assert HookType.POST_MODEL_REGISTER is not None
        assert HookType.ON_ERROR is not None


class TestHooksManager:
    """Tests for HooksManager class."""

    def test_register_hook(self):
        """Test registering a hook."""
        hooks = HooksManager()
        called = []

        def my_hook(hook_type, data):
            called.append((hook_type, data))
            return data

        hooks.register(HookType.PRE_RUN_START, my_hook)
        hooks.execute(HookType.PRE_RUN_START, {"run_id": "123"})

        assert len(called) == 1
        assert called[0][0] == HookType.PRE_RUN_START

    def test_register_as_decorator(self):
        """Test registering hook as decorator."""
        hooks = HooksManager()
        called = []

        @hooks.register(HookType.POST_RUN_END)
        def my_hook(hook_type, data):
            called.append(data)
            return data

        hooks.execute(HookType.POST_RUN_END, {"status": "completed"})

        assert len(called) == 1

    def test_hook_modifies_data(self):
        """Test that hooks can modify data."""
        hooks = HooksManager()

        def add_timestamp(hook_type, data):
            data["timestamp"] = "2024-01-01"
            return data

        hooks.register(HookType.PRE_MODEL_REGISTER, add_timestamp)

        result = hooks.execute(HookType.PRE_MODEL_REGISTER, {"model": "test"})

        assert "timestamp" in result
        assert result["timestamp"] == "2024-01-01"

    def test_unregister_hook(self):
        """Test unregistering a hook."""
        hooks = HooksManager()
        called = []

        def my_hook(hook_type, data):
            called.append(data)
            return data

        hooks.register(HookType.PRE_RUN_START, my_hook)
        hooks.unregister(HookType.PRE_RUN_START, my_hook)
        hooks.execute(HookType.PRE_RUN_START, {})

        assert len(called) == 0

    def test_clear_hooks(self):
        """Test clearing hooks."""
        hooks = HooksManager()

        hooks.register(HookType.PRE_RUN_START, lambda h, d: d)
        hooks.register(HookType.POST_RUN_END, lambda h, d: d)

        hooks.clear()

        result = hooks.execute(HookType.PRE_RUN_START, {"test": True})
        assert result == {"test": True}


class TestAuditEntry:
    """Tests for AuditEntry dataclass."""

    def test_entry_creation(self):
        """Test creating an audit entry."""
        entry = AuditEntry(
            timestamp="2024-01-01T00:00:00Z",
            operation="model.register",
            user="test_user",
            resource_type="model",
            resource_id="my_model",
            action="create",
            status="success",
            details={"version": 1},
        )

        assert entry.operation == "model.register"
        assert entry.user == "test_user"
        assert entry.status == "success"

    def test_entry_to_dict(self):
        """Test converting entry to dict."""
        entry = AuditEntry(
            timestamp="2024-01-01T00:00:00Z",
            operation="run.start",
            user=None,
            resource_type="run",
            resource_id="run_123",
            action="start",
            status="success",
            details={},
        )

        entry_dict = entry.to_dict()
        assert "timestamp" in entry_dict
        assert "operation" in entry_dict
        assert "status" in entry_dict


class TestAuditLogger:
    """Tests for AuditLogger class."""

    def test_log_entry(self):
        """Test logging an audit entry."""
        audit = AuditLogger()

        entry = audit.log(
            operation="model.register",
            resource_type="model",
            resource_id="my_model",
            action="create",
            status="success",
            details={"framework": "sklearn"},
        )

        assert entry.operation == "model.register"
        assert entry.status == "success"

    def test_query_entries(self):
        """Test querying audit entries."""
        audit = AuditLogger()

        audit.log("model.register", "model", "m1", "create", "success")
        audit.log("model.delete", "model", "m2", "delete", "success")
        audit.log("run.start", "run", "r1", "start", "success")

        results = audit.query(operation="model.register")
        assert len(results) == 1

        results = audit.query(resource_type="model")
        assert len(results) == 2

    def test_query_by_status(self):
        """Test querying by status."""
        audit = AuditLogger()

        audit.log("op1", "type1", "id1", "create", "success")
        audit.log("op2", "type2", "id2", "create", "failure")
        audit.log("op3", "type3", "id3", "create", "success")

        results = audit.query(status="failure")
        assert len(results) == 1

    def test_get_summary(self):
        """Test getting audit summary."""
        audit = AuditLogger()

        audit.log("model.register", "model", "m1", "create", "success")
        audit.log("model.register", "model", "m2", "create", "success")
        audit.log("run.start", "run", "r1", "start", "failure")

        summary = audit.get_summary()

        assert summary["total_entries"] == 3
        assert "operations" in summary
        assert "statuses" in summary

    def test_clear_entries(self):
        """Test clearing audit entries."""
        audit = AuditLogger()

        audit.log("op", "type", "id", "action", "status")
        audit.log("op", "type", "id", "action", "status")

        count = audit.clear()

        assert count == 2
        assert len(audit.query()) == 0

    def test_max_entries_limit(self):
        """Test that entries are limited."""
        audit = AuditLogger(max_entries=5)

        for i in range(10):
            audit.log(f"op_{i}", "type", f"id_{i}", "action", "status")

        results = audit.query(limit=100)
        assert len(results) == 5


class TestGlobalInstances:
    """Tests for global singleton instances."""

    def test_get_event_emitter(self):
        """Test getting global event emitter."""
        emitter1 = get_event_emitter()
        emitter2 = get_event_emitter()

        assert emitter1 is emitter2

    def test_get_metrics_collector(self):
        """Test getting global metrics collector."""
        metrics1 = get_metrics_collector()
        metrics2 = get_metrics_collector()

        assert metrics1 is metrics2
