"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Schedule management service - centralized business logic for scheduling.

This service is independent of CLI/API and can be consumed by both.
"""

from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple, Callable
from threading import Event, Thread, RLock
from datetime import datetime, timedelta, timezone
import time

from loguru import logger

from tauro.orchest.models import ScheduleKind, Schedule, RunState
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    get_resilience_manager,
)
from tauro.config.contexts import Context

try:
    from croniter import croniter

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    logger.warning(
        "croniter not installed, cron schedules will use placeholder behavior"
    )


def _parse_interval_seconds(expr: str) -> int:
    """Parse interval expression to seconds."""
    try:
        return max(1, int(expr.strip()))
    except Exception:
        return 60


class ScheduleMetrics:
    """Metrics collector for schedule service."""

    def __init__(self):
        self._lock = RLock()
        self.metrics = {
            "cycles": 0,
            "schedules_processed": 0,
            "runs_created": 0,
            "errors": 0,
            "last_cycle_time": 0,
            "avg_cycle_time": 0,
            "start_time": datetime.now(timezone.utc),
        }

    def increment(self, key: str, value: int = 1):
        """Increment a metrics counter."""
        with self._lock:
            if key in self.metrics and isinstance(self.metrics[key], int):
                self.metrics[key] += value

    def set_value(self, key: str, value: Any):
        """Set a metric value."""
        with self._lock:
            self.metrics[key] = value

    def get_metrics(self) -> Dict[str, Any]:
        """Return all collected metrics."""
        with self._lock:
            return self.metrics.copy()


class ScheduleService:
    """
    Centralized service for managing pipeline schedules.

    This service encapsulates all business logic related to scheduling
    and can be consumed independently by CLI or API without coupling.
    """

    def __init__(
        self,
        context: Context,
        store: Optional[OrchestratorStore] = None,
        run_executor: Optional[Any] = None,
        stuck_run_timeout_minutes: int = 120,
        max_consecutive_failures: int = 10,
        enable_circuit_breakers: bool = True,
    ):
        """
        Initialize the schedule service.

        Args:
            context: Execution context
            store: Orchestrator store for persistence
            run_executor: Callable to execute runs (accepts run_id and kwargs)
            stuck_run_timeout_minutes: Timeout for stuck runs
            max_consecutive_failures: Maximum consecutive failures before stopping
            enable_circuit_breakers: Enable per-schedule circuit breakers
        """
        self.context = context
        self.store = store or OrchestratorStore()
        self.run_executor = run_executor
        self._stop = Event()
        self._thread: Optional[Thread] = None
        self._monitor_thread: Optional[Thread] = None
        self._metrics = ScheduleMetrics()
        self.stuck_run_timeout_minutes = stuck_run_timeout_minutes
        self.max_consecutive_failures = max_consecutive_failures
        self._consecutive_failures = 0
        self._lock = RLock()

        # Resilience
        self._enable_circuit_breakers = enable_circuit_breakers
        self._resilience = get_resilience_manager()
        self._schedule_circuit_breakers: Dict[str, CircuitBreaker] = {}

        logger.debug("ScheduleService initialized")

    def create_schedule(
        self,
        pipeline_id: str,
        kind: ScheduleKind,
        expression: str,
        max_concurrency: int = 1,
        retry_policy: Optional[Dict[str, int]] = None,
        timeout_seconds: Optional[int] = None,
        enabled: bool = True,
    ) -> Schedule:
        """
        Create a new schedule.

        Args:
            pipeline_id: Pipeline identifier
            kind: Schedule kind (INTERVAL or CRON)
            expression: Schedule expression
            max_concurrency: Maximum concurrent runs
            retry_policy: Retry policy configuration
            timeout_seconds: Execution timeout
            enabled: Whether schedule is enabled

        Returns:
            Created Schedule instance
        """
        schedule = self.store.create_schedule(
            pipeline_id=pipeline_id,
            kind=kind,
            expression=expression,
            max_concurrency=max_concurrency,
            retry_policy=retry_policy or {},
            timeout_seconds=timeout_seconds,
        )

        if not enabled:
            self.store.update_schedule(schedule.id, enabled=False)

        logger.info(f"Schedule created: {schedule.id} ({kind} '{expression}')")
        return schedule

    def update_schedule(self, schedule_id: str, **updates) -> bool:
        """
        Update a schedule.

        Args:
            schedule_id: Schedule identifier
            **updates: Fields to update

        Returns:
            True if updated successfully
        """
        try:
            self.store.update_schedule(schedule_id, **updates)
            logger.info(f"Schedule {schedule_id} updated")
            return True
        except Exception as e:
            logger.error(f"Failed to update schedule {schedule_id}: {e}")
            return False

    def delete_schedule(self, schedule_id: str) -> bool:
        """
        Delete a schedule.

        Args:
            schedule_id: Schedule identifier

        Returns:
            True if deleted successfully
        """
        try:
            self.store.delete_schedule(schedule_id)
            logger.info(f"Schedule {schedule_id} deleted")
            return True
        except Exception as e:
            logger.error(f"Failed to delete schedule {schedule_id}: {e}")
            return False

    def list_schedules(
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = False,
    ) -> List[Schedule]:
        """
        List schedules with optional filters.

        Args:
            pipeline_id: Filter by pipeline ID
            enabled_only: Only return enabled schedules

        Returns:
            List of Schedule instances
        """
        return self.store.list_schedules(
            pipeline_id=pipeline_id,
            enabled_only=enabled_only,
        )

    def start(self, poll_interval: float = 1.0):
        """
        Start the scheduler in background.

        Args:
            poll_interval: Polling interval in seconds
        """
        if self._thread and self._thread.is_alive():
            logger.warning("Scheduler is already running")
            return

        self._stop.clear()
        self._thread = Thread(
            target=self._loop, args=(poll_interval,), daemon=True, name="SchedulerMain"
        )
        self._thread.start()

        # Start monitoring thread
        self._monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="SchedulerMonitor"
        )
        self._monitor_thread.start()

        logger.info("Scheduler started")

    def stop(self, timeout: Optional[float] = 30.0):
        """
        Stop the scheduler gracefully.

        Args:
            timeout: Maximum time to wait for shutdown
        """
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        if self._monitor_thread:
            self._monitor_thread.join(timeout=timeout)
        logger.info("Scheduler stopped")

    def backfill(self, pipeline_id: str, count: int):
        """
        Create multiple backfill runs for a pipeline.

        Args:
            pipeline_id: Pipeline identifier
            count: Number of runs to create
        """
        if not self.run_executor:
            raise RuntimeError("No run executor configured for backfill")

        for _ in range(max(0, int(count))):
            pr = self.store.create_pipeline_run(pipeline_id, params={})
            Thread(target=self.run_executor, args=(pr.id,), daemon=True).start()
            time.sleep(0.1)

        logger.info(f"Backfill enqueued: {count} runs for pipeline {pipeline_id}")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get scheduler metrics.

        Returns:
            Dictionary with metrics data
        """
        metrics = self._metrics.get_metrics()

        # Add circuit breaker metrics if enabled
        if self._enable_circuit_breakers:
            cb_metrics = {}
            for schedule_id, cb in self._schedule_circuit_breakers.items():
                cb_stats = cb.get_metrics()
                cb_metrics[schedule_id] = {
                    "state": cb_stats.state.value,
                    "failure_count": cb_stats.failure_count,
                    "total_calls": cb_stats.total_calls,
                    "rejected_calls": cb_stats.rejected_calls,
                }
            metrics["schedule_circuit_breakers"] = cb_metrics

        return metrics

    def get_health_status(self) -> Dict[str, Any]:
        """
        Get scheduler health status.

        Returns:
            Dictionary with health status information
        """
        metrics = self.get_metrics()

        is_healthy = True
        issues: List[str] = []
        warnings: List[str] = []

        # Check if scheduler is running
        is_running = self._thread is not None and self._thread.is_alive()
        if not is_running:
            is_healthy = False
            issues.append("Scheduler is not running")

        # Check consecutive failures
        if self._consecutive_failures >= self.max_consecutive_failures // 2:
            warnings.append(f"High consecutive failures: {self._consecutive_failures}")

        if self._consecutive_failures >= self.max_consecutive_failures:
            is_healthy = False
            issues.append("Maximum consecutive failures reached")

        # Evaluate circuit breakers and error rate using helpers to reduce complexity
        if self._enable_circuit_breakers:
            cb_warnings, cb_issues = self._evaluate_circuit_breakers(metrics)
            warnings.extend(cb_warnings)
            issues.extend(cb_issues)

        er_issue, er_warnings = self._evaluate_error_rate(metrics)
        if er_issue:
            is_healthy = False
            issues.append(er_issue)
        warnings.extend(er_warnings)

        return {
            "healthy": is_healthy,
            "running": is_running,
            "issues": issues,
            "warnings": warnings,
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _evaluate_circuit_breakers(
        self, metrics: Dict[str, Any]
    ) -> Tuple[List[str], List[str]]:
        """Return (warnings, issues) related to circuit breaker states."""
        open_circuits: List[str] = []
        half_open_circuits: List[str] = []
        cb_data = metrics.get("schedule_circuit_breakers", {})
        for schedule_id, cb_data_item in cb_data.items():
            state = cb_data_item.get("state")
            if state == "OPEN":
                open_circuits.append(schedule_id)
            elif state == "HALF_OPEN":
                half_open_circuits.append(schedule_id)

        warnings: List[str] = []
        issues: List[str] = []
        if open_circuits:
            warnings.append(
                f"{len(open_circuits)} schedule(s) with OPEN circuit breaker"
            )
            issues.extend(
                [f"Schedule {sid}: circuit breaker OPEN" for sid in open_circuits]
            )
        if half_open_circuits:
            warnings.append(f"{len(half_open_circuits)} schedule(s) recovering")
        return warnings, issues

    def _evaluate_error_rate(
        self, metrics: Dict[str, Any]
    ) -> Tuple[Optional[str], List[str]]:
        """Return (issue_or_none, warnings) based on error rate metrics."""
        total_cycles = metrics.get("cycles", 0)
        if total_cycles <= 0:
            return None, []

        errors = metrics.get("errors", 0)
        error_rate = errors / total_cycles
        if error_rate > 0.5:
            return f"High error rate: {error_rate:.1%}", []
        if error_rate > 0.2:
            return None, [f"Elevated error rate: {error_rate:.1%}"]
        return None, []

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _get_schedule_circuit_breaker(self, schedule_id: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a specific schedule."""
        if schedule_id not in self._schedule_circuit_breakers:
            self._schedule_circuit_breakers[
                schedule_id
            ] = self._resilience.get_or_create_circuit_breaker(
                f"schedule_{schedule_id}",
                CircuitBreakerConfig(
                    failure_threshold=3,
                    success_threshold=2,
                    timeout_seconds=300.0,
                ),
            )
        return self._schedule_circuit_breakers[schedule_id]

    def _loop(self, poll_interval: float):
        """Main scheduler loop."""
        while not self._stop.is_set():
            cycle_start = time.time()
            try:
                now, schedules = self._fetch_schedules_and_update_metrics()
                for s in schedules:
                    if self._stop.is_set():
                        return
                    self._process_schedule(s, now)

                cycle_time = time.time() - cycle_start
                self._update_cycle_metrics(cycle_time)

                time_to_sleep = max(0, poll_interval - cycle_time)
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)

            except Exception as e:
                self._handle_loop_exception(e, poll_interval)

    def _monitor_loop(self):
        """Monitoring loop to detect stuck executions."""
        while not self._stop.is_set():
            try:
                time.sleep(60)  # Check every minute
                if self._stop.is_set():
                    break

                self._check_stuck_runs()
                self._check_schedule_health()

            except Exception as e:
                logger.exception(f"Monitor loop error: {e}")
                time.sleep(60)

    def _fetch_schedules_and_update_metrics(self):
        """Fetch schedules and update metrics."""
        now = datetime.now(timezone.utc)
        schedules = self.store.list_schedules(enabled_only=True)
        self._metrics.increment("cycles")
        self._metrics.set_value("schedules_processed", len(schedules))
        return now, schedules

    def _process_schedule(self, s: Schedule, now: datetime):
        """Process a single schedule."""
        if not self._is_due(s, now):
            return

        if not self._can_schedule(s):
            return

        try:
            self._create_and_start_run(s, now)
            # Reset consecutive failures on success
            with self._lock:
                self._consecutive_failures = 0
        except Exception as e:
            self._handle_schedule_error(s, e)

    def _is_due(self, s: Schedule, now: datetime) -> bool:
        """Check if schedule is due for execution."""
        if not s.enabled:
            return False
        if s.next_run_at and now < s.next_run_at:
            return False
        return True

    def _can_schedule(self, s: Schedule) -> bool:
        """Check if schedule can create a new run."""
        running = self.store.list_pipeline_runs(
            pipeline_id=s.pipeline_id, state=RunState.RUNNING
        )
        queued = self.store.list_pipeline_runs(
            pipeline_id=s.pipeline_id, state=RunState.QUEUED
        )
        if len(running) + len(queued) >= s.max_concurrency:
            logger.debug(
                f"Skipping schedule {s.id} due to concurrency "
                f"(running={len(running)}, queued={len(queued)}, max={s.max_concurrency})"
            )
            return False
        return True

    def _create_and_start_run(self, s: Schedule, now: datetime):
        """Create and start a run for a schedule."""

        def _create_run():
            """Internal function to create a run."""
            pr = self.store.create_pipeline_run(s.pipeline_id, params={})
            self.store.update_pipeline_run_state(pr.id, RunState.QUEUED)
            self._metrics.increment("runs_created")

            logger.info(
                f"[Scheduler] Created run {pr.id} for pipeline '{s.pipeline_id}'",
                extra={
                    "schedule_id": s.id,
                    "pipeline_id": s.pipeline_id,
                    "run_id": pr.id,
                },
            )

            # Start run in background if executor is configured
            if self.run_executor:
                Thread(
                    target=self.run_executor,
                    args=(pr.id,),
                    kwargs={
                        "retries": int((s.retry_policy or {}).get("retries", 0)),
                        "retry_delay_sec": int((s.retry_policy or {}).get("delay", 0)),
                        "timeout_seconds": s.timeout_seconds,
                    },
                    daemon=True,
                    name=f"SchedulerRun-{pr.id[:8]}",
                ).start()

            return pr.id

        next_run = self._compute_next_run(s, now)
        self._attempt_run_creation(s, _create_run, next_run)

    def _compute_next_run(self, s: Schedule, now: datetime) -> datetime:
        """Compute the next run datetime for a schedule (extracted to reduce complexity)."""
        if s.kind == ScheduleKind.INTERVAL:
            interval = _parse_interval_seconds(s.expression)
            return now + timedelta(seconds=interval)

        if s.kind == ScheduleKind.CRON and HAS_CRONITER:
            try:
                base_time = s.next_run_at or now
                cron_iter = croniter(s.expression, base_time)
                return cron_iter.get_next(datetime)
            except Exception:
                return now + timedelta(seconds=60)

        return now + timedelta(seconds=60)

    def _attempt_run_creation(
        self, s: Schedule, create_callable: Callable[[], str], next_run: datetime
    ):
        """Try to claim the schedule and create a run, applying circuit breaker logic if enabled."""
        claimed = self.store.claim_schedule_next_run(s.id, s.next_run_at, next_run)
        if not claimed:
            return

        if self._enable_circuit_breakers:
            cb = self._get_schedule_circuit_breaker(s.id)
            try:
                cb.call(create_callable)
            except CircuitBreakerOpenError as e:
                logger.warning(
                    f"Schedule {s.id} rejected by circuit breaker",
                    extra={"schedule_id": s.id, "error": str(e)},
                )
                self._metrics.increment("errors")
                self.store.add_schedule_failure_to_dlq(
                    s.id, s.pipeline_id, f"Circuit breaker OPEN: {str(e)}"
                )
        else:
            create_callable()

    def _handle_schedule_error(self, s: Schedule, e: Exception):
        """Handle schedule processing error."""
        logger.exception(f"Failed to process schedule {s.id}: {e}")
        self._metrics.increment("errors")
        self.store.add_schedule_failure_to_dlq(s.id, s.pipeline_id, str(e))

        with self._lock:
            self._consecutive_failures += 1

        if self._consecutive_failures >= self.max_consecutive_failures:
            logger.error(
                f"Too many consecutive failures ({self._consecutive_failures}), "
                "stopping scheduler for safety"
            )
            self.stop()

    def _handle_loop_exception(self, e: Exception, poll_interval: float):
        """Handle main loop exception."""
        logger.exception(f"Scheduler loop error: {e}")
        self._metrics.increment("errors")

        with self._lock:
            self._consecutive_failures += 1

        if self._consecutive_failures >= self.max_consecutive_failures:
            logger.error(
                f"Too many consecutive failures ({self._consecutive_failures}), "
                "stopping scheduler for safety"
            )
            self.stop()
            return

        time.sleep(poll_interval)

    def _update_cycle_metrics(self, cycle_time: float):
        """Update cycle time metrics."""
        self._metrics.set_value("last_cycle_time", cycle_time)

        avg_cycle_time = self._metrics.metrics["avg_cycle_time"]
        if avg_cycle_time == 0:
            new_avg = cycle_time
        else:
            new_avg = (avg_cycle_time * 0.9) + (cycle_time * 0.1)
        self._metrics.set_value("avg_cycle_time", new_avg)

    def _check_stuck_runs(self):
        """Detect and handle stuck executions."""
        try:
            stuck_runs = self.store.get_stuck_pipeline_runs(
                self.stuck_run_timeout_minutes
            )

            for run in stuck_runs:
                logger.warning(
                    f"Found stuck pipeline run {run.id} for pipeline '{run.pipeline_id}'"
                )

                self.store.update_pipeline_run_state(
                    run.id,
                    RunState.FAILED,
                    error=f"Run stuck for more than {self.stuck_run_timeout_minutes} minutes",
                )

                # Mark tasks as failed
                task_runs = self.store.list_task_runs(run.id)
                for task_run in task_runs:
                    if task_run.state == RunState.RUNNING:
                        self.store.update_task_run_state(
                            task_run.id,
                            RunState.FAILED,
                            error="Parent pipeline run was marked as stuck",
                        )

        except Exception as e:
            logger.exception(f"Error checking stuck runs: {e}")

    def _check_schedule_health(self):
        """Check schedule health and disable problematic schedules."""
        try:
            failures = self.store.get_schedule_failures(limit=100)

            # Group by schedule_id
            failures_by_schedule = {}
            for failure in failures:
                schedule_id = failure["schedule_id"]
                if schedule_id not in failures_by_schedule:
                    failures_by_schedule[schedule_id] = []
                failures_by_schedule[schedule_id].append(failure)

            # Disable schedules with many consecutive failures
            for schedule_id, schedule_failures in failures_by_schedule.items():
                if len(schedule_failures) >= self.max_consecutive_failures:
                    logger.warning(
                        f"Disabling schedule {schedule_id} due to "
                        f"{len(schedule_failures)} consecutive failures"
                    )
                    self.store.update_schedule(schedule_id, enabled=False)

        except Exception as e:
            logger.exception(f"Error checking schedule health: {e}")
