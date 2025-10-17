from __future__ import annotations
from typing import Optional, Dict, Any, List
from threading import Event, Thread, RLock
from datetime import datetime, timedelta, timezone
import signal
import threading
import time
import logging

from loguru import logger  # type: ignore

from tauro.orchest.models import ScheduleKind, Schedule, RunState
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.runner import OrchestratorRunner
from tauro.orchest.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    get_resilience_manager,
)
from tauro.config.contexts import Context

try:
    from croniter import croniter  # type: ignore

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    logger.warning(
        "croniter not installed, cron schedules will use placeholder behavior"
    )


def _parse_interval_seconds(expr: str) -> int:
    try:
        return max(1, int(expr.strip()))
    except Exception:
        return 60


class SchedulerMetrics:
    """Class to collect scheduler metrics."""

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


class SchedulerService:
    """
    Enhanced scheduler with:
    - Per-schedule circuit breakers to isolate problematic schedules
    - Improved health monitoring
    - Detailed metrics
    - Robust error handling
    """

    def __init__(
        self,
        context: Context,
        store: Optional[OrchestratorStore] = None,
        stuck_run_timeout_minutes: int = 120,
        max_consecutive_failures: int = 10,
        enable_circuit_breakers: bool = True,
    ):
        self.context = context
        self.store = store or OrchestratorStore()
        self.runner = OrchestratorRunner(context, self.store)
        self._stop = Event()
        self._thread: Optional[Thread] = None
        self._monitor_thread: Optional[Thread] = None
        self._metrics = SchedulerMetrics()
        self.stuck_run_timeout_minutes = stuck_run_timeout_minutes
        self.max_consecutive_failures = max_consecutive_failures
        self._consecutive_failures = 0
        self._lock = RLock()

        # Resilience
        self._enable_circuit_breakers = enable_circuit_breakers
        self._resilience = get_resilience_manager()
        self._schedule_circuit_breakers: Dict[str, CircuitBreaker] = {}

        # Configure signal handlers for graceful shutdown
        self._register_signal_handlers()

    def _get_schedule_circuit_breaker(self, schedule_id: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a specific schedule."""
        if schedule_id not in self._schedule_circuit_breakers:
            self._schedule_circuit_breakers[
                schedule_id
            ] = self._resilience.get_or_create_circuit_breaker(
                f"schedule_{schedule_id}",
                CircuitBreakerConfig(
                    failure_threshold=3,  # Más restrictivo que el global
                    success_threshold=2,
                    timeout_seconds=300.0,  # 5 minutos
                ),
            )
        return self._schedule_circuit_breakers[schedule_id]

    def _register_signal_handlers(self) -> None:
        """
        Register handlers only when running in the main thread. Some platforms/threads
        fail if signal.signal is called from a non-main thread.
        """
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "Failed to register signals: %s", exc
                )
        else:
            logging.getLogger(__name__).debug(
                "Running in non-main thread: not registering signal handlers."
            )

    def _signal_handler(self, signum, frame) -> None:
        logging.getLogger(__name__).info(
            "SchedulerService received signal %s, stopping...", signum
        )
        self.stop()

    def start(self, poll_interval: float = 1.0):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(
            target=self._loop, args=(poll_interval,), daemon=True, name="SchedulerMain"
        )
        self._thread.start()

        # Iniciar hilo de monitorización
        self._monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="SchedulerMonitor"
        )
        self._monitor_thread.start()

        logger.info("Scheduler started")

    def stop(self, timeout: Optional[float] = 30.0):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        if self._monitor_thread:
            self._monitor_thread.join(timeout=timeout)
        logger.info("Scheduler stopped")

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

    def _check_stuck_runs(self):
        """Detect and handle stuck executions."""
        try:
            stuck_runs = self.store.get_stuck_pipeline_runs(
                self.stuck_run_timeout_minutes
            )

            for run in stuck_runs:
                logger.warning(
                    f"Found stuck pipeline run {run.id} for pipeline '{run.pipeline_id}' "
                    f"that started at {run.started_at}. Marking as failed."
                )

                # Mark as failed
                self.store.update_pipeline_run_state(
                    run.id,
                    RunState.FAILED,
                    error=f"Run stuck for more than {self.stuck_run_timeout_minutes} minutes",
                )

                # Also mark all its tasks as failed
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
            # Revisar la dead letter queue para schedules con muchos fallos
            failures = self.store.get_schedule_failures(limit=100)

            # Agrupar por schedule_id
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

    def _loop(self, poll_interval: float):
        while not self._stop.is_set():
            cycle_start = time.time()
            try:
                now, schedules = self._fetch_schedules_and_update_metrics()
                for s in schedules:
                    stop_now = self._process_schedule(s, now)
                    if stop_now:
                        return

                cycle_time = time.time() - cycle_start
                self._update_cycle_metrics(cycle_time)

                time_to_sleep = max(0, poll_interval - cycle_time)
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)

            except Exception as e:
                self._handle_loop_exception(e, poll_interval)

    def _fetch_schedules_and_update_metrics(self):
        now = datetime.now(timezone.utc)
        schedules = self.store.list_schedules(enabled_only=True)
        self._metrics.increment("cycles")
        self._metrics.set_value("schedules_processed", len(schedules))
        return now, schedules

    def _process_schedule(self, s: Schedule, now: datetime) -> bool:
        if not self._is_due(s, now):
            return False

        if not self._can_schedule(s):
            return False

        try:
            stop_now = self._create_and_start_run(s, now)
            if stop_now:
                return True
            # Reset consecutive failures counter on success
            with self._lock:
                self._consecutive_failures = 0
        except Exception as e:
            stop_now = self._handle_schedule_creation_error(s, e)
            if stop_now:
                return True

        return False

    def _handle_loop_exception(self, e: Exception, poll_interval: float):
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

    def _can_schedule(self, s: Schedule) -> bool:
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

    def _create_and_start_run(self, s: Schedule, now: datetime) -> bool:
        """
        Create the pipeline run, start it in a thread and bump the next run.
        Uses per-schedule circuit breakers to isolate problematic schedules.
        Returns True if the scheduler should stop (due to reaching failure threshold), otherwise False.
        """

        def _create_run():
            """Internal function to create a run (used with circuit breaker)."""
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

            Thread(
                target=self._start_run_in_thread,
                args=(pr.id, s),
                daemon=True,
                name=f"SchedulerRun-{pr.id[:8]}",
            ).start()

            return pr.id

        try:
            # Compute the next_run value we will set (bump)
            if s.kind == ScheduleKind.INTERVAL:
                interval = _parse_interval_seconds(s.expression)
                next_run = now + timedelta(seconds=interval)
            elif s.kind == ScheduleKind.CRON and HAS_CRONITER:
                try:
                    base_time = s.next_run_at or now
                    cron_iter = croniter(s.expression, base_time)
                    next_run = cron_iter.get_next(datetime)
                except Exception:
                    next_run = now + timedelta(seconds=60)
            else:
                # fallback conservative bump
                next_run = now + timedelta(seconds=60)

            # Attempt to atomically claim the schedule for this runner. If claim
            # succeeds we proceed to create the run; otherwise another instance
            # already claimed it.
            claimed = self.store.claim_schedule_next_run(s.id, s.next_run_at, next_run)
            if not claimed:
                # someone else claimed it
                return False

            # Use per-schedule circuit breaker if enabled
            if self._enable_circuit_breakers:
                cb = self._get_schedule_circuit_breaker(s.id)
                cb.call(_create_run)
            else:
                _create_run()

            # Reset consecutive failures counter on success
            with self._lock:
                self._consecutive_failures = 0

            return False

        except CircuitBreakerOpenError as e:
            logger.warning(
                f"Schedule {s.id} rejected by circuit breaker",
                extra={
                    "schedule_id": s.id,
                    "pipeline_id": s.pipeline_id,
                    "error": str(e),
                },
            )
            self._metrics.increment("errors")
            self.store.add_schedule_failure_to_dlq(
                s.id, s.pipeline_id, f"Circuit breaker OPEN: {str(e)}"
            )
            return False

        except Exception as e:
            return self._handle_schedule_creation_error(s, e)

    def _handle_schedule_creation_error(self, s: Schedule, e: Exception) -> bool:
        logger.exception(f"Failed to create run for schedule {s.id}: {e}")
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
            return True

        return False

    def _update_cycle_metrics(self, cycle_time: float):
        self._metrics.set_value("last_cycle_time", cycle_time)

        avg_cycle_time = self._metrics.metrics["avg_cycle_time"]
        if avg_cycle_time == 0:
            new_avg = cycle_time
        else:
            new_avg = (avg_cycle_time * 0.9) + (cycle_time * 0.1)
        self._metrics.set_value("avg_cycle_time", new_avg)

    def _start_run_in_thread(self, run_id: str, s: Schedule):
        try:
            self.runner.start_run(
                run_id,
                retries=int((s.retry_policy or {}).get("retries", 0)),
                retry_delay_sec=int((s.retry_policy or {}).get("delay", 0)),
                concurrency=None,
                timeout_seconds=s.timeout_seconds,
            )
        except Exception as e:
            logger.exception(f"Error executing scheduled run {run_id}")
            self.store.add_schedule_failure_to_dlq(
                s.id, s.pipeline_id, f"Run execution failed: {e}"
            )

    def _is_due(self, s: Schedule, now: datetime) -> bool:
        if not s.enabled:
            return False
        # If next_run_at exists and is in future -> not due
        if s.next_run_at and now < s.next_run_at:
            return False

        # For interval schedules, if next_run_at is missing consider due and compute on bump
        if s.kind == ScheduleKind.INTERVAL:
            return True

        # For cron schedules:
        if s.kind == ScheduleKind.CRON:
            # If we have croniter, we trust next_run_at or croniter to compute; if missing, treat as due
            return True

        return False

    def _bump_next_run(self, s: Schedule, now: datetime):
        if s.kind == ScheduleKind.INTERVAL:
            interval = _parse_interval_seconds(s.expression)
            next_run = now + timedelta(seconds=interval)
            try:
                self.store.update_schedule(s.id, next_run_at=next_run)
            except Exception:
                logger.exception("Failed updating schedule next_run_at for interval")
        elif s.kind == ScheduleKind.CRON:
            if HAS_CRONITER:
                try:
                    base_time = s.next_run_at or now
                    cron_iter = croniter(s.expression, base_time)
                    next_run = cron_iter.get_next(datetime)
                    self.store.update_schedule(s.id, next_run_at=next_run)
                except Exception:
                    logger.exception(f"Error parsing cron expression {s.expression}")
                    # fallback to conservative 60s
                    self.store.update_schedule(
                        s.id, next_run_at=now + timedelta(seconds=60)
                    )
            else:
                # fallback placeholder: schedule in 60s
                self.store.update_schedule(
                    s.id, next_run_at=now + timedelta(seconds=60)
                )

    def backfill(self, pipeline_id: str, count: int):
        for _ in range(max(0, int(count))):
            pr = self.store.create_pipeline_run(pipeline_id, params={})
            Thread(
                target=self._start_run_in_thread, args=(pr.id, Schedule()), daemon=True
            ).start()
            time.sleep(0.1)

    def get_metrics(self) -> Dict[str, Any]:
        """Return scheduler metrics including circuit breakers."""
        metrics = self._metrics.get_metrics()

        # Añadir métricas de circuit breakers si están habilitados
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
        """Return scheduler health status."""
        metrics = self.get_metrics()

        is_healthy = True
        issues: List[str] = []
        warnings: List[str] = []

        # Verificar si el scheduler está corriendo
        is_running = self._thread is not None and self._thread.is_alive()
        if not is_running:
            is_healthy = False
            issues.append("Scheduler is not running")

        # Verificar fallos consecutivos
        if self._consecutive_failures >= self.max_consecutive_failures // 2:
            warnings.append(f"High consecutive failures: {self._consecutive_failures}")

        if self._consecutive_failures >= self.max_consecutive_failures:
            is_healthy = False
            issues.append("Maximum consecutive failures reached")

        # Verificar circuit breakers de forma delegada
        if self._enable_circuit_breakers:
            open_circuits, half_open_circuits = self._get_circuit_breaker_summary(
                metrics
            )
            if open_circuits:
                warnings.append(
                    f"{len(open_circuits)} schedule(s) with OPEN circuit breaker"
                )
                issues.extend(
                    [f"Schedule {sid}: circuit breaker OPEN" for sid in open_circuits]
                )
            if half_open_circuits:
                warnings.append(f"{len(half_open_circuits)} schedule(s) recovering")

        # Verificar tasa de error de forma delegada
        error_rate = self._compute_error_rate(metrics)
        if error_rate is not None:
            if error_rate > 0.5:
                is_healthy = False
                issues.append(f"High error rate: {error_rate:.1%}")
            elif error_rate > 0.2:
                warnings.append(f"Elevated error rate: {error_rate:.1%}")

        return {
            "healthy": is_healthy,
            "running": is_running,
            "issues": issues,
            "warnings": warnings,
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _get_circuit_breaker_summary(
        self, metrics: Dict[str, Any]
    ) -> tuple[List[str], List[str]]:
        """Return lists of open and half-open circuit breaker schedule ids."""
        open_circuits: List[str] = []
        half_open_circuits: List[str] = []
        cb_data_map = metrics.get("schedule_circuit_breakers", {})
        for schedule_id, cb_data in cb_data_map.items():
            state = cb_data.get("state")
            if state == "OPEN":
                open_circuits.append(schedule_id)
            elif state == "HALF_OPEN":
                half_open_circuits.append(schedule_id)
        return open_circuits, half_open_circuits

    def _compute_error_rate(self, metrics: Dict[str, Any]) -> Optional[float]:
        """Compute error rate as errors / cycles or None if cycles == 0."""
        total_cycles = metrics.get("cycles", 0)
        if not total_cycles:
            return None
        errors = metrics.get("errors", 0)
        try:
            return float(errors) / float(total_cycles)
        except Exception:
            return None
