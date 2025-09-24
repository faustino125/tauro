import asyncio
import logging
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from tauro.streaming.pipeline_manager import PipelineManager
from tauro.exec.executor import PipelineExecutor

from .utils import handle_exceptions, log_execution_time, log, APIError, NotFoundError

logger = logging.getLogger("tauro.api.services")
logger.setLevel(logging.INFO)


class PipelineService:
    """
    Service mejorado con manejo de errores, timeouts y cancelación
    """

    def __init__(self, store, runner=None, max_workers: int = 10):
        self.store = store
        self.runner = runner
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)

    @asynccontextmanager
    async def _acquire_slot(self, run_id: str):
        """Context manager para adquirir un slot de ejecución con timeout"""
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=30.0)
            yield
        except asyncio.TimeoutError:
            raise APIError(
                message="System too busy, please try again later",
                code="too_busy",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        finally:
            self._semaphore.release()

    @handle_exceptions
    @log_execution_time
    async def run_pipeline(self, pipeline_id: str, params: Dict[str, Any]) -> str:
        """Ejecutar pipeline con control de concurrencia y timeout"""
        if not hasattr(self.store, "create_run"):
            raise APIError("Store does not implement create_run")

        async with self._acquire_slot(pipeline_id):
            # Crear metadata de ejecución
            run_id = self.store.create_run(pipeline_id, params)

            async def _execute():
                try:
                    log.info(
                        "Pipeline run started", run_id=run_id, pipeline=pipeline_id
                    )

                    # Ejecutar con timeout
                    try:
                        result = await asyncio.wait_for(
                            self._execute_pipeline(pipeline_id, params),
                            timeout=3600,  # 1 hora timeout
                        )

                        # Guardar resultado
                        self._update_run_result(run_id, result, "success")
                        log.info(
                            "Pipeline run finished", run_id=run_id, status="success"
                        )

                    except asyncio.TimeoutError:
                        self._update_run_status(run_id, "timeout")
                        log.warning("Pipeline run timed out", run_id=run_id)
                        raise APIError(
                            message="Pipeline execution timed out",
                            code="timeout",
                            status_code=status.HTTP_408_REQUEST_TIMEOUT,
                        )

                    return result

                except asyncio.CancelledError:
                    self._update_run_status(run_id, "cancelled")
                    log.info("Pipeline run cancelled", run_id=run_id)
                    raise

                except Exception as exc:
                    self._update_run_error(run_id, str(exc))
                    log.exception("Pipeline run failed", run_id=run_id, error=str(exc))
                    raise

            # Crear y registrar tarea
            task = asyncio.create_task(_execute())

            async with self._lock:
                self._tasks[run_id] = task

            # Configurar callback para limpieza
            task.add_done_callback(
                lambda _: asyncio.create_task(self._cleanup_task(run_id))
            )

            return run_id

    async def _execute_pipeline(self, pipeline_id: str, params: Dict[str, Any]):
        """Ejecutar pipeline (implementación específica)"""
        if self.runner and hasattr(self.runner, "run_pipeline"):
            res = self.runner.run_pipeline(pipeline_id, params)
            if asyncio.iscoroutine(res):
                return await res
            return res
        else:
            # Fallback a ejecución en thread
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                lambda: PipelineExecutor(
                    {"pipeline_id": pipeline_id, "params": params}
                ).run(),
            )

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
        # Verificar que la ejecución existe
        run = self.get_run(run_id)
        if not run:
            raise NotFoundError("pipeline run", run_id)

        # Verificar que no esté en estado terminal
        if run.get("status") in ["success", "failed", "cancelled", "timeout"]:
            raise APIError(
                message=f"Run is already in terminal state: {run.get('status')}",
                code="already_terminated",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        # Intentar cancelar a través del runner
        if self.runner and hasattr(self.runner, "cancel_run"):
            res = self.runner.cancel_run(run_id)
            if asyncio.iscoroutine(res):
                return await res
            return bool(res)

        # Fallback: cancelar tarea local
        async with self._lock:
            task = self._tasks.get(run_id)

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Exception while cancelling task", run_id=run_id)

            return True

        # Marcar como cancelado en store
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

    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio"""
        async with self._lock:
            return {
                "active_tasks": len(self._tasks),
                "max_workers": self._max_workers,
                "available_slots": self._semaphore._value,
            }
