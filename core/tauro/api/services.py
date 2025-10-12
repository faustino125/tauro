import asyncio
import logging
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from tauro.streaming.pipeline_manager import PipelineManager
from tauro.exec.executor import PipelineExecutor
from tauro.orchest.resilience import (
    Bulkhead,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    RetryPolicy,
    get_resilience_manager,
)

from fastapi import status

from .utils import handle_exceptions, log_execution_time, log, APIError, NotFoundError

logger = logging.getLogger("tauro.api.services")
logger.setLevel(logging.INFO)


class PipelineService:
    """
    Service mejorado con:
    - Bulkhead pattern para aislar recursos
    - Circuit breaker para prevenir cascadas de fallos
    - Retry policy configurable
    - Manejo de errores robusto
    - Timeouts y cancelación mejorados
    """

    def __init__(
        self,
        store,
        runner=None,
        max_workers: int = 10,
        enable_circuit_breaker: bool = True,
        enable_retry: bool = True,
    ):
        self.store = store
        self.runner = runner
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)

        # Resilience patterns
        self._resilience = get_resilience_manager()
        self._enable_circuit_breaker = enable_circuit_breaker
        self._enable_retry = enable_retry

        # Circuit breaker
        if enable_circuit_breaker:
            self._circuit_breaker = self._resilience.get_or_create_circuit_breaker(
                "pipeline_service",
                CircuitBreakerConfig(
                    failure_threshold=5, success_threshold=2, timeout_seconds=120.0
                ),
            )

        # Bulkhead - usar la versión sincrónica del módulo resilience
        # Para async, usaremos el semaphore existente más métricas
        self._bulkhead_metrics = {
            "total_calls": 0,
            "concurrent_calls": 0,
            "rejected_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
        }
        self._metrics_lock = asyncio.Lock()

        # Retry policy
        if enable_retry:
            self._retry_policy = RetryPolicy(
                max_attempts=3,
                initial_delay=1.0,
                max_delay=60.0,
                exponential_base=2.0,
                jitter=True,
            )

    @asynccontextmanager
    async def _acquire_slot(self, run_id: str):
        """Context manager para adquirir un slot de ejecución con timeout y métricas"""
        async with self._metrics_lock:
            self._bulkhead_metrics["total_calls"] += 1

        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=30.0)

            async with self._metrics_lock:
                self._bulkhead_metrics["concurrent_calls"] += 1

            yield

        except asyncio.TimeoutError:
            async with self._metrics_lock:
                self._bulkhead_metrics["rejected_calls"] += 1

            raise APIError(
                message="System too busy, please try again later",
                code="too_busy",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        finally:
            try:
                async with self._metrics_lock:
                    self._bulkhead_metrics["concurrent_calls"] -= 1
                self._semaphore.release()
            except Exception:
                pass

    @handle_exceptions
    @log_execution_time
    async def run_pipeline(self, pipeline_id: str, params: Dict[str, Any]) -> str:
        """
        Ejecutar pipeline con circuit breaker, bulkhead y retry policy
        """
        if not hasattr(self.store, "create_run"):
            raise APIError("Store does not implement create_run")

        # Verificar circuit breaker antes de intentar ejecutar
        if self._enable_circuit_breaker:
            cb_metrics = self._circuit_breaker.get_metrics()
            if cb_metrics.state.value == "OPEN":
                raise APIError(
                    message="Service temporarily unavailable due to high error rate",
                    code="circuit_breaker_open",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    details={
                        "circuit_state": cb_metrics.state.value,
                        "failure_count": cb_metrics.failure_count,
                    },
                )

        async with self._acquire_slot(pipeline_id):
            if self.runner and hasattr(self.runner, "run_pipeline"):
                res = self.runner.run_pipeline(pipeline_id, params)
                if asyncio.iscoroutine(res):
                    run_id = await res
                else:
                    run_id = res

                async with self._lock:
                    self._tasks[run_id] = None  # placeholder

                async with self._metrics_lock:
                    self._bulkhead_metrics["successful_calls"] += 1

                return run_id

            run_id = self.store.create_run(pipeline_id, params)

            async def _execute():
                try:
                    log.info(
                        "Pipeline run started", run_id=run_id, pipeline=pipeline_id
                    )

                    try:
                        result = await asyncio.wait_for(
                            self._execute_pipeline_with_protection(pipeline_id, params),
                            timeout=3600,  # 1 hora timeout
                        )

                        self._update_run_result(run_id, result, "success")

                        async with self._metrics_lock:
                            self._bulkhead_metrics["successful_calls"] += 1

                        log.info(
                            "Pipeline run finished", run_id=run_id, status="success"
                        )

                    except asyncio.TimeoutError:
                        self._update_run_status(run_id, "timeout")
                        async with self._metrics_lock:
                            self._bulkhead_metrics["failed_calls"] += 1
                        log.warning("Pipeline run timed out", run_id=run_id)
                        raise APIError(
                            message="Pipeline execution timed out",
                            code="timeout",
                            status_code=status.HTTP_408_REQUEST_TIMEOUT,
                        )

                    return result

                except asyncio.CancelledError:
                    self._update_run_status(run_id, "cancelled")
                    async with self._metrics_lock:
                        self._bulkhead_metrics["failed_calls"] += 1
                    log.info("Pipeline run cancelled", run_id=run_id)
                    raise

                except Exception as exc:
                    self._update_run_error(run_id, str(exc))
                    async with self._metrics_lock:
                        self._bulkhead_metrics["failed_calls"] += 1
                    log.exception("Pipeline run failed", run_id=run_id, error=str(exc))
                    raise

            task = asyncio.create_task(_execute())

            async with self._lock:
                self._tasks[run_id] = task

            task.add_done_callback(
                lambda _: asyncio.create_task(self._cleanup_task(run_id))
            )

            return run_id

    async def _execute_pipeline_with_protection(
        self, pipeline_id: str, params: Dict[str, Any]
    ):
        """
        Ejecutar pipeline con circuit breaker y retry policy
        """

        async def _execute_once():
            if self.runner and hasattr(self.runner, "run_pipeline"):
                res = self.runner.run_pipeline(pipeline_id, params)
                if asyncio.iscoroutine(res):
                    return await res
                return res
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    lambda: PipelineExecutor(
                        {"pipeline_id": pipeline_id, "params": params}
                    ).run(),
                )

        # Ejecutar con circuit breaker si está habilitado
        if self._enable_circuit_breaker:
            try:
                # Para async, necesitamos wrapper sincrónico
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: self._circuit_breaker.call(
                        lambda: asyncio.run(_execute_once())
                    ),
                )
                return result
            except CircuitBreakerOpenError:
                raise APIError(
                    message="Service temporarily unavailable",
                    code="circuit_breaker_open",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
        else:
            return await _execute_once()

    async def _cleanup_task(self, run_id: str):
        """Limpiar tarea completada"""
        async with self._lock:
            if run_id in self._tasks:
                del self._tasks[run_id]

    def _update_run_result(self, run_id: str, result: Any, status: str):
        """Actualizar resultado de ejecución"""
        try:
            if hasattr(self.store, "update_run_result"):
                self.store.update_run_result(run_id, result, status)
            else:
                self.store.update_run_status(run_id, status)
        except Exception:
            log.exception("Failed updating run result", run_id=run_id)

    def _update_run_status(self, run_id: str, status: str):
        """Actualizar estado de ejecución"""
        try:
            self.store.update_run_status(run_id, status)
        except Exception:
            log.exception("Failed updating run status", run_id=run_id)

    def _update_run_error(self, run_id: str, error: str):
        """Actualizar error de ejecución"""
        try:
            if hasattr(self.store, "update_run_error"):
                self.store.update_run_error(run_id, error)
            else:
                self.store.update_run_status(run_id, "failed")
        except Exception:
            log.exception("Failed updating run error", run_id=run_id)

    @handle_exceptions
    async def cancel_run(self, run_id: str) -> bool:
        """Cancelar ejecución con verificación de existencia"""
        run = self.get_run(run_id)
        if not run:
            raise NotFoundError("pipeline run", run_id)

        if run.get("status") in ["success", "failed", "cancelled", "timeout"]:
            raise APIError(
                message=f"Run is already in terminal state: {run.get('status')}",
                code="already_terminated",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if self.runner and hasattr(self.runner, "cancel_run"):
            res = self.runner.cancel_run(run_id)
            if asyncio.iscoroutine(res):
                return await res
            return bool(res)

        async with self._lock:
            task = self._tasks.get(run_id)

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Exception while cancelling task", run_id=run_id)

            return True

        self._update_run_status(run_id, "cancelled")
        return True

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Obtener información de ejecución"""
        getter = getattr(self.store, "get_run", None)
        if getter is None:
            return None
        return getter(run_id)

    def list_runs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Listar ejecuciones con paginación"""
        lister = getattr(self.store, "list_runs", None)
        if lister is None:
            return []
        return lister(limit=limit, offset=offset)

    async def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio incluyendo resilience patterns"""
        async with self._lock:
            active = len(self._tasks)
            available = max(self._max_workers - active, 0)

        async with self._metrics_lock:
            bulkhead_metrics = self._bulkhead_metrics.copy()

        stats = {
            "active_tasks": active,
            "max_workers": self._max_workers,
            "available_slots": available,
            "bulkhead": bulkhead_metrics,
        }

        # Añadir métricas de circuit breaker si está habilitado
        if self._enable_circuit_breaker:
            cb_metrics = self._circuit_breaker.get_metrics()
            stats["circuit_breaker"] = {
                "state": cb_metrics.state.value,
                "failure_count": cb_metrics.failure_count,
                "total_calls": cb_metrics.total_calls,
                "failed_calls": cb_metrics.failed_calls,
                "rejected_calls": cb_metrics.rejected_calls,
            }

        return stats

    async def get_health_status(self) -> Dict[str, Any]:
        """Obtener estado de salud del servicio"""
        stats = await self.get_stats()

        is_healthy = True
        issues = []
        warnings = []

        # Verificar circuit breaker
        if self._enable_circuit_breaker:
            cb_state = stats.get("circuit_breaker", {}).get("state")
            if cb_state == "OPEN":
                is_healthy = False
                issues.append("Circuit breaker is OPEN")
            elif cb_state == "HALF_OPEN":
                warnings.append("Circuit breaker is HALF_OPEN (recovering)")

        # Verificar capacidad
        available = stats.get("available_slots", 0)
        if available == 0:
            warnings.append("No available slots - system at max capacity")

        # Verificar tasa de rechazo
        bulkhead = stats.get("bulkhead", {})
        total_calls = bulkhead.get("total_calls", 0)
        rejected = bulkhead.get("rejected_calls", 0)

        if total_calls > 0:
            rejection_rate = rejected / total_calls
            if rejection_rate > 0.3:
                is_healthy = False
                issues.append(f"High rejection rate: {rejection_rate:.1%}")
            elif rejection_rate > 0.1:
                warnings.append(f"Elevated rejection rate: {rejection_rate:.1%}")

        # Verificar tasa de fallos
        failed = bulkhead.get("failed_calls", 0)
        successful = bulkhead.get("successful_calls", 0)

        if (failed + successful) > 0:
            failure_rate = failed / (failed + successful)
            if failure_rate > 0.5:
                is_healthy = False
                issues.append(f"High failure rate: {failure_rate:.1%}")
            elif failure_rate > 0.2:
                warnings.append(f"Elevated failure rate: {failure_rate:.1%}")

        return {
            "healthy": is_healthy,
            "issues": issues,
            "warnings": warnings,
            "stats": stats,
        }


class StreamingService:
    """
    Servicio para manejar pipelines de streaming con resilience patterns
    """

    def __init__(self, store, runner=None, enable_circuit_breaker: bool = True):
        self.store = store
        self.runner = runner
        self._resilience = get_resilience_manager()
        self._enable_circuit_breaker = enable_circuit_breaker

        if enable_circuit_breaker:
            self._circuit_breaker = self._resilience.get_or_create_circuit_breaker(
                "streaming_service",
                CircuitBreakerConfig(
                    failure_threshold=5, success_threshold=2, timeout_seconds=90.0
                ),
            )

        self._metrics = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
        }
        self._metrics_lock = asyncio.Lock()

    async def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio"""
        async with self._metrics_lock:
            metrics = self._metrics.copy()

        if self._enable_circuit_breaker:
            cb_metrics = self._circuit_breaker.get_metrics()
            metrics["circuit_breaker"] = {
                "state": cb_metrics.state.value,
                "failure_count": cb_metrics.failure_count,
                "total_calls": cb_metrics.total_calls,
            }

        return metrics

    async def get_health_status(self) -> Dict[str, Any]:
        """Obtener estado de salud del servicio"""
        stats = await self.get_stats()

        is_healthy = True
        issues = []

        if self._enable_circuit_breaker:
            cb_state = stats.get("circuit_breaker", {}).get("state")
            if cb_state == "OPEN":
                is_healthy = False
                issues.append("Circuit breaker is OPEN")

        return {"healthy": is_healthy, "issues": issues, "stats": stats}
