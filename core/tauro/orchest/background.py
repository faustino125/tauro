import asyncio
import logging
from typing import Dict, Any, Optional

from loguru import logger  # type: ignore


class BackgroundTaskManager:
    """
    Administra tareas asyncio por run_id, actualiza estado en el Store y permite cancelación.
    """

    def __init__(self, store, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.store = store
        self.loop = loop or asyncio.get_event_loop()
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def create_task(self, run_id: str, coro):
        """
        Crea y registra una tarea que envuelve 'coro' y gestiona status/result en el Store.
        """
        async with self._lock:
            task = self.loop.create_task(self._wrap(run_id, coro))
            self._tasks[run_id] = task
            return task

    async def register_task(self, run_id: str, task: asyncio.Task):
        async with self._lock:
            self._tasks[run_id] = task

    async def cancel_task(self, run_id: str) -> bool:
        async with self._lock:
            task = self._tasks.get(run_id)
            if not task:
                return False
            task.cancel()
            # marcar cancelado en store inmediatamente
            try:
                self.store.update_run_status(run_id, "cancel_requested")
            except Exception:
                logger.exception("Error updating run status on cancel")
            return True

    async def _wrap(self, run_id: str, coro):
        """
        Envuelve la ejecución: actualiza estado, guarda resultado/errores y limpia task map.
        """
        try:
            # marcar como running antes de ejecutar
            try:
                self.store.update_run_status(run_id, "running")
            except Exception:
                logger.exception(
                    "Could not update run status to running for %s", run_id
                )

            result = await coro

            try:
                self.store.update_run_result(run_id, result, status="success")
            except Exception:
                logger.exception("Error saving run result for %s", run_id)
        except asyncio.CancelledError:
            logger.info("Run %s cancelled", run_id)
            try:
                self.store.update_run_status(run_id, "cancelled")
            except Exception:
                logger.exception("Error updating cancelled status for %s", run_id)
            raise
        except Exception as exc:
            logger.exception("Run %s failed: %s", run_id, exc)
            try:
                self.store.update_run_error(run_id, str(exc))
                self.store.update_run_status(run_id, "failed")
            except Exception:
                logger.exception("Error saving run error for %s", run_id)
        finally:
            async with self._lock:
                self._tasks.pop(run_id, None)

    async def shutdown(self):
        """
        Cancela todas las tareas y espera finalización.
        """
        async with self._lock:
            tasks = list(self._tasks.values())
            for t in tasks:
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with self._lock:
            self._tasks.clear()
