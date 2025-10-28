"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations
from typing import Dict, Optional, List
from datetime import datetime, timezone
import threading
import time
import warnings

from loguru import logger  # type: ignore
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Dict
import threading
import time
from datetime import datetime, timezone
from collections import deque

from tauro.core.config.contexts import Context
from tauro.api.orchest.models import PipelineRun, TaskRun, RunState
from tauro.api.orchest.store import OrchestratorStore
from tauro.api.orchest.executor.local import LocalDagExecutor
from tauro.api.orchest.resilience import (
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    get_resilience_manager,
)

# Deprecation warning
warnings.warn(
    "OrchestratorRunner is deprecated and will be removed in v3.0. "
    "Please use tauro.orchest.services.RunService instead.",
    DeprecationWarning,
    stacklevel=2,
)


class OrchestratorRunner:
    """
    Enhanced pipeline runner with:
    - Circuit breaker to prevent failure cascades
    - Bulkhead to isolate resources
    - Dynamic worker pool
    - Detailed metrics
    - Configurable retry policy
    """

    def __init__(
        self,
        context: Context,
        store: Optional[OrchestratorStore] = None,
        max_workers: Optional[int] = None,
        enable_circuit_breaker: bool = True,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    ):
        self.context = context
        self.store = store or OrchestratorStore(context=context)

        # Worker pool with dynamic scaling
        self._max_workers = max_workers or 4
        self._min_workers = max(1, self._max_workers // 2)
        self._pool = ThreadPoolExecutor(
            max_workers=self._max_workers, thread_name_prefix="OrchestratorRunner"
        )

        # Active executors management
        self._active_executors: Dict[str, LocalDagExecutor] = {}
        self._futures: Dict[str, Future] = {}
        self._lock = threading.RLock()

        # Resilience patterns
        self._resilience = get_resilience_manager()
        self._enable_circuit_breaker = enable_circuit_breaker

        if enable_circuit_breaker:
            self._circuit_breaker = self._resilience.get_or_create_circuit_breaker(
                "orchestrator_runner",
                circuit_breaker_config
                or CircuitBreakerConfig(
                    failure_threshold=5, success_threshold=2, timeout_seconds=60.0
                ),
            )

        # Bulkhead to limit concurrent executions
        self._bulkhead = self._resilience.get_or_create_bulkhead(
            "orchestrator_runner_executions",
            max_concurrent_calls=max_workers or 4,
            max_wait_duration=30.0,
        )

        # Metrics
        self._metrics = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "cancelled_runs": 0,
            "avg_execution_time": 0.0,
            "max_execution_time": 0.0,
            "min_execution_time": float("inf"),
        }
        self._metrics_lock = threading.RLock()

        # Recent execution history for monitoring
        self._recent_executions = deque(maxlen=100)

    def _update_metrics(self, execution_time: float, success: bool):
        """Update execution metrics."""
        with self._metrics_lock:
            self._metrics["total_runs"] += 1
            if success:
                self._metrics["successful_runs"] += 1
            else:
                self._metrics["failed_runs"] += 1

            # Update execution times
            self._metrics["max_execution_time"] = max(
                self._metrics["max_execution_time"], execution_time
            )
            self._metrics["min_execution_time"] = min(
                self._metrics["min_execution_time"], execution_time
            )

            # Moving average of execution time
            current_avg = self._metrics["avg_execution_time"]
            total = self._metrics["total_runs"]
            self._metrics["avg_execution_time"] = (
                current_avg * (total - 1) + execution_time
            ) / total

            # Append to history
            self._recent_executions.append(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "execution_time": execution_time,
                    "success": success,
                }
            )

    def create_run(self, pipeline_id: str, params: Optional[Dict] = None) -> PipelineRun:
        pr = self.store.create_pipeline_run(pipeline_id, params or {})
        logger.info(f"Created PipelineRun {pr.id} for pipeline '{pipeline_id}'")
        return pr

    def _check_circuit_breaker(self, run_id: str) -> bool:
        """
        Check if circuit breaker allows execution.
        Returns True if OK, False if OPEN (reject).
        """
        if not self._enable_circuit_breaker:
            return True

        cb_metrics = self._circuit_breaker.get_metrics()
        if cb_metrics.state.value == "OPEN":
            logger.error(
                f"Circuit breaker is OPEN, cannot start run {run_id}",
                extra={"run_id": run_id, "circuit_state": cb_metrics.state.value},
            )
            self.store.update_pipeline_run_state(
                run_id,
                RunState.FAILED,
                error="Circuit breaker is OPEN - system is experiencing issues",
            )
            return False
        return True

    def _validate_run_state(self, run_id: str) -> Optional[PipelineRun]:
        """
        Validate that run exists and is in startable state.
        Returns PipelineRun if valid, or None with error already persisted.
        """
        pr = self.store.get_pipeline_run(run_id)
        if not pr:
            raise ValueError(f"PipelineRun '{run_id}' not found")

        if pr.state not in (RunState.PENDING, RunState.QUEUED, RunState.FAILED):
            logger.warning(f"Run {run_id} in state {pr.state}, cannot start")
            return None

        return pr

    def _create_task_state_callback(self, pr: PipelineRun) -> Any:
        """
        Create a callback function for persisting task state changes.
        """
        persisted_tasks: Dict[str, str] = {}
        persisted_tasks_lock = threading.Lock()

        def on_task_state(tr: TaskRun):
            """Persist task state to database."""
            try:
                with persisted_tasks_lock:
                    if tr.task_id not in persisted_tasks:
                        stored_tr = self.store.create_task_run(pr.id, tr.task_id)
                        tr.id = stored_tr.id
                        persisted_tasks[tr.task_id] = tr.id

                    self.store.update_task_run_state(
                        persisted_tasks[tr.task_id],
                        tr.state,
                        try_number=tr.try_number,
                        started_at=tr.started_at,
                        finished_at=tr.finished_at,
                        error=tr.error,
                        log_uri=tr.log_uri,
                    )
            except Exception:
                logger.exception(
                    f"Failed to persist task state for run {pr.id}",
                    extra={"run_id": pr.id, "task_id": tr.task_id},
                )

        return on_task_state

    def _execute_pipeline_with_protections(
        self,
        pr: PipelineRun,
        on_task_state: Any,
        retries: int,
        retry_delay_sec: int,
        concurrency: Optional[int],
        timeout_seconds: Optional[int],
    ) -> None:
        """
        Execute pipeline DAG with resilience protections and tracking.
        """
        ex = LocalDagExecutor(self.context)

        with self._lock:
            self._active_executors[pr.id] = ex

        try:
            start_date = (pr.params or {}).get("start_date")
            end_date = (pr.params or {}).get("end_date")

            ex.execute(
                pr.pipeline_id,
                start_date=start_date,
                end_date=end_date,
                on_task_state=on_task_state,
                retries=retries,
                retry_delay_sec=retry_delay_sec,
                concurrency=concurrency,
                timeout_seconds=timeout_seconds,
            )
        finally:
            with self._lock:
                self._active_executors.pop(pr.id, None)

    def _handle_run_success(self, run_id: str, pr: PipelineRun, execution_time: float) -> RunState:
        """Handle successful pipeline execution."""
        logger.info(
            f"Run {run_id} completed successfully in {execution_time:.2f} seconds",
            extra={
                "run_id": run_id,
                "pipeline_id": pr.pipeline_id,
                "execution_time": execution_time,
            },
        )

        self.store.update_pipeline_run_state(
            pr.id, RunState.SUCCESS, finished_at=datetime.now(timezone.utc)
        )

        self._update_metrics(execution_time, success=True)
        return RunState.SUCCESS

    def _handle_run_failure(
        self,
        run_id: str,
        pr: PipelineRun,
        execution_time: float,
        error: Exception,
        is_circuit_breaker: bool = False,
    ) -> RunState:
        """Handle failed pipeline execution."""
        if is_circuit_breaker:
            error_msg = "Rejected by circuit breaker - system is experiencing issues"
            logger.error(
                f"Run {run_id} rejected by circuit breaker",
                extra={"run_id": run_id, "error": str(error)},
            )
        else:
            error_msg = str(error)
            logger.exception(
                f"Run {run_id} failed after {execution_time:.2f} seconds",
                extra={
                    "run_id": run_id,
                    "pipeline_id": pr.pipeline_id,
                    "execution_time": execution_time,
                },
            )

        self.store.update_pipeline_run_state(
            pr.id,
            RunState.FAILED,
            finished_at=datetime.now(timezone.utc),
            error=error_msg,
        )

        self._update_metrics(execution_time, success=False)
        return RunState.FAILED

    def start_run(
        self,
        run_id: str,
        retries: int = 0,
        retry_delay_sec: int = 0,
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> RunState:
        """
        Start a pipeline run with resilience protections.

        This method orchestrates the execution by delegating to specialized methods
        for better testability and maintainability.
        """
        # Step 1: Check circuit breaker
        if not self._check_circuit_breaker(run_id):
            return RunState.FAILED

        # Step 2: Validate run exists and is in startable state
        pr = self._validate_run_state(run_id)
        if pr is None:
            return RunState.QUEUED  # Already logged, return current state

        # Step 3: Mark as running
        self.store.update_pipeline_run_state(
            pr.id, RunState.RUNNING, started_at=datetime.now(timezone.utc)
        )

        # Step 4: Create task state callback
        on_task_state = self._create_task_state_callback(pr)

        # Step 5: Execute with timing
        start_time = time.time()

        try:
            # Execute with circuit breaker if enabled
            if self._enable_circuit_breaker:
                self._circuit_breaker.call(
                    self._execute_pipeline_with_protections,
                    pr,
                    on_task_state,
                    retries,
                    retry_delay_sec,
                    concurrency,
                    timeout_seconds,
                )
            else:
                self._execute_pipeline_with_protections(
                    pr,
                    on_task_state,
                    retries,
                    retry_delay_sec,
                    concurrency,
                    timeout_seconds,
                )

            execution_time = time.time() - start_time
            return self._handle_run_success(run_id, pr, execution_time)

        except CircuitBreakerOpenError as e:
            execution_time = time.time() - start_time
            return self._handle_run_failure(run_id, pr, execution_time, e, is_circuit_breaker=True)

        except Exception as e:
            execution_time = time.time() - start_time
            return self._handle_run_failure(run_id, pr, execution_time, e)

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self.store.get_pipeline_run(run_id)

    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        return self.store.list_pipeline_runs(
            pipeline_id=pipeline_id,
            state=state,
            limit=limit,
            created_after=created_after,
            created_before=created_before,
        )

    def list_task_runs(self, run_id: str, state: Optional[RunState] = None) -> List[TaskRun]:
        return self.store.list_task_runs(run_id, state=state)

    def cancel_run(self, run_id: str) -> bool:
        """Cancel an in-progress run with improved error handling."""
        pr = self.store.get_pipeline_run(run_id)

        if not pr:
            logger.warning(f"PipelineRun '{run_id}' not found")
            return False

        if pr.state.is_terminal():
            logger.warning(f"PipelineRun '{run_id}' is already in terminal state {pr.state}")
            return False

        with self._lock:
            ex = self._active_executors.get(run_id)
            fut = self._futures.get(run_id)

        # Try cancelling the executor
        if ex is not None:
            try:
                ex._stop_event.set()
                logger.info(f"Stop event set for executor of run {run_id}")
            except Exception as e:
                logger.exception(f"Failed to signal executor to stop: {e}")

        # Try cancelling the future
        if fut is not None:
            try:
                cancelled = fut.cancel()
                logger.info(
                    f"Future cancel {'succeeded' if cancelled else 'failed'} for run {run_id}"
                )
            except Exception as e:
                logger.debug(f"Future cancel failed: {e}")

        # Update state in the database
        try:
            self.store.update_pipeline_run_state(
                pr.id,
                RunState.CANCELLED,
                finished_at=datetime.now(timezone.utc),
                error="Run was manually cancelled",
            )

            # Cancel all associated tasks
            task_runs = self.store.list_task_runs(run_id)
            for task_run in task_runs:
                if not task_run.state.is_terminal():
                    self.store.update_task_run_state(
                        task_run.id,
                        RunState.CANCELLED,
                        finished_at=datetime.now(timezone.utc),
                        error="Parent pipeline run was cancelled",
                    )

            logger.info(f"Cancelled run {run_id}")

            # Update metrics
            with self._metrics_lock:
                self._metrics["cancelled_runs"] += 1

            return True

        except Exception as e:
            logger.exception(f"Error updating cancellation status: {e}")
            return False

    def run_pipeline(self, pipeline_id: str, params: Optional[Dict] = None, **kwargs) -> str:
        """
        Convenience: create and kick off a background run protected by the bulkhead.
        Returns run_id immediately.
        """

        def _execute():
            """Wrapper to execute under bulkhead protection.

            Uses the `pr` created in the outer scope to avoid creating two
            pipeline runs (one returned to the caller and another executed).
            """
            try:
                # Use bulkhead to limit concurrent executions
                result = self._bulkhead.execute(
                    self.start_run,
                    pr.id,
                    kwargs.get("retries", 0),
                    kwargs.get("retry_delay_sec", 0),
                    kwargs.get("concurrency", None),
                    kwargs.get("timeout_seconds", None),
                )
                return result

            except RuntimeError as e:
                # Bulkhead rejected the execution
                logger.warning(
                    f"Run {pr.id} rejected by bulkhead",
                    extra={"run_id": pr.id, "error": str(e)},
                )
                self.store.update_pipeline_run_state(
                    pr.id,
                    RunState.FAILED,
                    finished_at=datetime.now(timezone.utc),
                    error="System at max capacity, try again later",
                )
                raise

        pr = self.create_run(pipeline_id, params or {})
        fut = self._pool.submit(_execute)

        with self._lock:
            self._futures[pr.id] = fut

        # Add callback to cleanup completed futures
        def cleanup_future(f):
            with self._lock:
                self._futures.pop(pr.id, None)

        fut.add_done_callback(cleanup_future)

        return pr.id

    def get_metrics(self) -> Dict[str, Any]:
        """Obtener métricas del runner"""
        with self._metrics_lock:
            metrics = self._metrics.copy()

        with self._lock:
            metrics["active_runs"] = len(self._active_executors)
            metrics["queued_runs"] = len(self._futures)

        # Añadir métricas de resiliencia
        if self._enable_circuit_breaker:
            cb_metrics = self._circuit_breaker.get_metrics()
            metrics["circuit_breaker"] = {
                "state": cb_metrics.state.value,
                "failure_count": cb_metrics.failure_count,
                "total_calls": cb_metrics.total_calls,
                "failed_calls": cb_metrics.failed_calls,
                "rejected_calls": cb_metrics.rejected_calls,
            }

        bulkhead_metrics = self._bulkhead.get_metrics()
        metrics["bulkhead"] = bulkhead_metrics

        # Métricas de ejecuciones recientes
        if self._recent_executions:
            recent_list = list(self._recent_executions)
            recent_success_rate = sum(1 for e in recent_list if e["success"]) / len(recent_list)
            metrics["recent_success_rate"] = recent_success_rate

        return metrics

    def get_health_status(self) -> Dict[str, Any]:
        """Obtener estado de salud del runner"""
        metrics = self.get_metrics()

        # Determinar salud basada en métricas
        is_healthy = True
        issues = []

        if self._enable_circuit_breaker:
            cb_state = metrics.get("circuit_breaker", {}).get("state")
            if cb_state == "OPEN":
                is_healthy = False
                issues.append("Circuit breaker is OPEN")
            elif cb_state == "HALF_OPEN":
                issues.append("Circuit breaker is HALF_OPEN (recovering)")

        # Verificar tasa de éxito reciente
        recent_success_rate = metrics.get("recent_success_rate", 1.0)
        if recent_success_rate < 0.5:
            is_healthy = False
            issues.append(f"Low success rate: {recent_success_rate:.1%}")

        # Verificar capacidad
        bulkhead_metrics = metrics.get("bulkhead", {})
        concurrent = bulkhead_metrics.get("concurrent_calls", 0)
        max_concurrent = bulkhead_metrics.get("max_concurrent_calls", 1)
        if concurrent >= max_concurrent:
            issues.append("At max capacity")

        return {
            "healthy": is_healthy,
            "issues": issues,
            "metrics": metrics,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def shutdown(self, wait: bool = True, timeout: Optional[float] = 30.0) -> None:
        """
        Shutdown mejorado con timeout y cleanup completo.
        """
        logger.info("Shutting down OrchestratorRunner...")

        # 1. Señalar a todos los ejecutores activos que deben detenerse
        with self._lock:
            active_count = len(self._active_executors)
            for run_id, ex in self._active_executors.items():
                try:
                    ex._stop_event.set()
                    logger.debug(f"Stop signal sent to executor for run {run_id}")
                except Exception as e:
                    logger.warning(f"Failed to stop executor {run_id}: {e}")

            futures = list(self._futures.values())
            future_count = len(futures)

        logger.info(f"Stopping {active_count} active executors and {future_count} futures")

        # 2. Cancelar todos los futures pendientes
        for f in futures:
            try:
                f.cancel()
            except Exception:
                pass

        # 3. Shutdown del pool con timeout
        try:
            self._pool.shutdown(wait=wait, cancel_futures=True)
            logger.info("ThreadPoolExecutor shutdown completed")
        except Exception as e:
            logger.exception(f"Error during pool shutdown: {e}")

        # 4. Limpiar estructuras de datos
        with self._lock:
            self._active_executors.clear()
            self._futures.clear()

        logger.info("OrchestratorRunner shutdown complete")
