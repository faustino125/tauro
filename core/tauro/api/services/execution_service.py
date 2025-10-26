import logging
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncDatabase  # type: ignore

from tauro.api.schemas.models import RunCreate, RunState


logger = logging.getLogger(__name__)


class ExecutionNotFoundError(Exception):
    """Ejecución no encontrada"""

    pass


class ExecutionAlreadyRunningError(Exception):
    """Ya hay una ejecución en progreso"""

    pass


class InvalidExecutionError(Exception):
    """Parámetros de ejecución inválidos"""

    pass


class ExecutionService:
    """
    Servicio de orquestación centralizada.

    Responsabilidades:
    - Recibir solicitudes de ejecución
    - Validar precondiciones
    - Delegar a OrchestratorRunner
    - Rastrar estado de ejecuciones
    - Manejar cancelaciones
    - Limpieza de recursos
    - Ejecutar en background de forma asíncrona
    """

    def __init__(self, db: AsyncDatabase):
        """
        Inicializa el servicio con una instancia de MongoDB.

        Args:
            db: AsyncDatabase instance de Motor
        """
        self.db = db
        self.runs_collection = db["pipeline_runs"]
        self.tasks_collection = db["task_runs"]
        self.projects_collection = db["projects"]
        self.schedules_collection = db["schedules"]

        # Nota: OrchestratorRunner se inyectará en tiempo de deployment
        self.orchestrator_runner = None

        # Diccionario para rastrar tasks asincronas activas
        self._active_tasks: Dict[str, asyncio.Task] = {}

    def set_orchestrator_runner(self, orchestrator_runner):
        """
        Inyecta la instancia de OrchestratorRunner.

        Args:
            orchestrator_runner: Instancia del executor centralizado
        """
        self.orchestrator_runner = orchestrator_runner
        logger.info("OrchestratorRunner inyectado en ExecutionService")

    async def submit_execution(
        self,
        run_id: str,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Somete un run para ejecución centralizada.

        IMPORTANTE: Este método retorna 202 ACCEPTED inmediatamente.
        La ejecución ocurre en background en una task asíncrona separada.

        Cambios de estado:
        1. PENDING → RUNNING (inmediatamente en este método)
        2. RUNNING → SUCCESS/FAILED (en background en _execute_run_async)

        Args:
            run_id: ID del run a ejecutar
            timeout_seconds: Timeout para la ejecución

        Returns:
            Diccionario con status de aceptación (202):
            {
                "status": "accepted",
                "run_id": run_id,
                "status_url": "/api/v1/runs/{run_id}",
                "timestamp": datetime.now().isoformat(),
                "message": "Pipeline execution started in background"
            }

        Raises:
            ExecutionNotFoundError: Si el run no existe
            ExecutionAlreadyRunningError: Si ya está en ejecución
            InvalidExecutionError: Si hay errores de validación
        """
        logger.info(f"Somete ejecución para run {run_id}")

        try:
            # 1. VALIDAR: Run existe
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise ExecutionNotFoundError(f"Run {run_id} no encontrado")

            # 2. VALIDAR: No está ya ejecutándose
            if run.get("state") == RunState.RUNNING.value:
                raise ExecutionAlreadyRunningError(f"Run {run_id} ya está en ejecución")

            # 3. VALIDAR: Está en PENDING
            if run.get("state") != RunState.PENDING.value:
                raise InvalidExecutionError(
                    f"No se puede ejecutar un run en estado {run.get('state')}. "
                    f"Solo se pueden ejecutar runs en PENDING."
                )

            # 4. CAMBIAR ESTADO: PENDING → RUNNING
            now = datetime.now(timezone.utc)
            await self.runs_collection.update_one(
                {"id": run_id},
                {
                    "$set": {
                        "state": RunState.RUNNING.value,
                        "started_at": now,
                        "progress": {
                            "total_tasks": 0,
                            "completed_tasks": 0,
                            "failed_tasks": 0,
                            "tasks_by_state": {},
                        },
                    }
                },
            )
            logger.info(f"Run {run_id} cambiado a RUNNING")

            # 5. INICIAR TASK ASÍNCRONA (no bloquea respuesta HTTP)
            task = asyncio.create_task(self._execute_run_async(run_id, timeout_seconds))
            self._active_tasks[run_id] = task
            logger.info(f"Task asíncrona creada para run {run_id}")

            # 6. RETORNAR 202 ACCEPTED inmediatamente
            return {
                "status": "accepted",
                "run_id": run_id,
                "status_url": f"/api/v1/runs/{run_id}",
                "timestamp": now.isoformat(),
                "message": "Pipeline execution started in background",
            }

        except (
            ExecutionNotFoundError,
            ExecutionAlreadyRunningError,
            InvalidExecutionError,
        ):
            raise
        except Exception as e:
            logger.error(
                f"Error al someter ejecución para run {run_id}: {e}", exc_info=True
            )
            raise InvalidExecutionError(f"Error al someter ejecución: {str(e)}")

    # =========================================================================
    # ASYNC EXECUTION (BACKGROUND)
    # =========================================================================

    async def _execute_run_async(
        self, run_id: str, timeout_seconds: Optional[int] = None
    ) -> None:
        """
        Ejecuta el run de forma asíncrona en background.

        IMPORTANTE: Este método se ejecuta en una task asíncrona separada
        NO bloquea la respuesta HTTP del endpoint.
        """
        logger.info(f"Iniciando ejecución asíncrona de run {run_id}")

        try:
            # Cargar y validar
            run, project = await self._load_run_and_project(run_id)

            # Construir contexto
            context = self._build_execution_context(project)
            pipeline_id = run.get("pipeline_id")
            params = run.get("params", {})

            logger.info(
                f"Contexto construido para pipeline {pipeline_id}, "
                f"params={params}, timeout={timeout_seconds}s"
            )

            # Ejecutar pipeline (interno maneja timeout y simulación)
            result = await self._invoke_orchestrator(
                context, pipeline_id, params, run_id, timeout_seconds
            )

            # Marcar éxito
            logger.info(f"Pipeline {pipeline_id} completado exitosamente")
            completion_time = datetime.now(timezone.utc)
            await self.runs_collection.update_one(
                {"id": run_id},
                {
                    "$set": {
                        "state": RunState.SUCCESS.value,
                        "completed_at": completion_time,
                        "progress": result.get("progress", {}),
                        "error": None,
                    }
                },
            )
            logger.info(f"Run {run_id} finalizado con SUCCESS")

        except asyncio.TimeoutError:
            logger.error(f"Run {run_id} timeout después de {timeout_seconds}s")
            await self._handle_timeout(run_id, timeout_seconds)

        except ExecutionNotFoundError as e:
            logger.error(f"Error de validación en run {run_id}: {e}")
            await self._mark_run_failed(run_id, str(e))

        except Exception as e:
            logger.error(f"Error ejecutando run {run_id}: {e}", exc_info=True)
            await self._mark_run_failed(run_id, str(e))

        finally:
            # Limpiar task de lista activa
            if run_id in self._active_tasks:
                del self._active_tasks[run_id]
            logger.info(f"Task limpiada para run {run_id}")

    async def _load_run_and_project(self, rid: str):
        run_doc = await self.runs_collection.find_one({"id": rid})
        if not run_doc:
            raise ExecutionNotFoundError(f"Run {rid} desapareció")

        project_doc = await self.projects_collection.find_one(
            {"id": run_doc.get("project_id")}
        )
        if not project_doc:
            raise InvalidExecutionError(
                f"Proyecto {run_doc.get('project_id')} no encontrado"
            )
        return run_doc, project_doc

    async def _invoke_orchestrator(
        self, ctx, pid, prms, rid: str, timeout_seconds: Optional[int] = None
    ):
        if not self.orchestrator_runner:
            logger.warning(
                f"OrchestratorRunner no disponible, simulando ejecución para {rid}"
            )
            await asyncio.sleep(1)
            return {
                "progress": {
                    "total_tasks": 1,
                    "completed_tasks": 1,
                    "failed_tasks": 0,
                }
            }

        # Ejecutar en OrchestratorRunner con/ sin timeout
        if timeout_seconds:
            return await asyncio.wait_for(
                self.orchestrator_runner.execute_async(
                    context=ctx, pipeline_id=pid, params=prms, run_id=rid
                ),
                timeout=timeout_seconds,
            )
        return await self.orchestrator_runner.execute_async(
            context=ctx, pipeline_id=pid, params=prms, run_id=rid
        )

    async def _handle_timeout(self, run_id: str, timeout_seconds: Optional[int]):
        # Intentar cancelar operaciones en OrchestratorRunner
        if self.orchestrator_runner and hasattr(
            self.orchestrator_runner, "cancel_execution"
        ):
            try:
                await self.orchestrator_runner.cancel_execution(run_id)
                logger.info(f"Cancelado en OrchestratorRunner por timeout: {run_id}")
            except Exception as e:
                logger.warning(
                    f"Error cancelando en OrchestratorRunner por timeout: {e}"
                )

        await self._mark_run_failed(
            run_id, f"Execution timeout after {timeout_seconds} seconds"
        )

    # =========================================================================
    # GET EXECUTION STATUS
    # =========================================================================

    async def get_execution_status(
        self,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Obtiene el status actual de una ejecución.

        Usado para polling de la API:
        GET /api/v1/runs/{run_id}

        Retorna:
        - state: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED
        - progress: total, completed, failed tasks
        - timestamps: started_at, completed_at
        - error: mensaje de error si falló

        Args:
            run_id: ID del run

        Returns:
            Dict con status actual

        Raises:
            ExecutionNotFoundError: Si run no existe
        """
        run = await self.runs_collection.find_one({"id": run_id})

        if not run:
            raise ExecutionNotFoundError(f"Run {run_id} no encontrado")

        # Remover _id de MongoDB
        run.pop("_id", None)

        return {
            "run_id": run_id,
            "state": run.get("state", RunState.PENDING.value),
            "progress": run.get("progress", {}),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "error": run.get("error"),
            "timeout_seconds": run.get("timeout_seconds"),
        }

    # =========================================================================
    # CANCEL EXECUTION
    # =========================================================================

    async def cancel_execution(
        self, run_id: str, reason: str = "User cancelled"
    ) -> bool:
        """
        Cancela una ejecución en progreso.

        Flujo:
        1. Validar que run existe
        2. Si está en estado RUNNING/PENDING, cambiar a CANCELLED
        3. Signal a OrchestratorRunner si es posible
        4. Cancelar la task asíncrona asociada (si existe)

        Args:
            run_id: ID del run a cancelar
            reason: Razón de la cancelación

        Returns:
            True si fue cancelado, False si no estaba ejecutándose

        Raises:
            ExecutionNotFoundError: Si run no existe
        """
        logger.info(f"Cancelando ejecución run {run_id}: {reason}")

        run = await self.runs_collection.find_one({"id": run_id})
        if not run:
            raise ExecutionNotFoundError(f"Run {run_id} no encontrado")

        current_state = run.get("state")

        # Solo cancelar si está en RUNNING o PENDING
        if current_state not in [RunState.RUNNING.value, RunState.PENDING.value]:
            logger.warning(
                f"No se puede cancelar run {run_id}: "
                f"estado es {current_state}, no RUNNING/PENDING"
            )
            return False

        # Cambiar estado a CANCELLED
        now = datetime.now(timezone.utc)
        await self.runs_collection.update_one(
            {"id": run_id},
            {
                "$set": {
                    "state": RunState.CANCELLED.value,
                    "completed_at": now,
                    "error": reason,
                }
            },
        )

        # Delegar señales y cancelación de task a helpers simplificados
        await self._signal_orchestrator_cancel(run_id)
        await self._cancel_active_task(run_id)

        logger.info(f"Run {run_id} marcado como CANCELLED")
        return True

    async def _signal_orchestrator_cancel(self, run_id: str) -> None:
        """
        Intenta notificar al OrchestratorRunner que cancele la ejecución.
        Silencia errores y registra warnings si falla.
        """
        if not self.orchestrator_runner or not hasattr(
            self.orchestrator_runner, "cancel_execution"
        ):
            return

        try:
            await self.orchestrator_runner.cancel_execution(run_id)
            logger.info(f"Cancel request forwarded to OrchestratorRunner: {run_id}")
        except Exception as e:
            logger.warning(f"Error cancelling in OrchestratorRunner: {e}")

    async def _cancel_active_task(self, run_id: str) -> None:
        """
        Cancela la asyncio.Task asociada al run (si existe) y espera un breve timeout.
        """
        task = self._active_tasks.get(run_id)
        if not task:
            return

        if task.done():
            logger.debug(f"Task ya finalizada para run {run_id}")
            return

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5.0)
            logger.info(f"Task asíncrona cancelada exitosamente: {run_id}")
        except asyncio.TimeoutError:
            logger.warning(f"Task cancelada pero no terminó en timeout: {run_id}")
        except asyncio.CancelledError:
            logger.info(f"Task ya estaba cancelada: {run_id}")
            raise
        except Exception as e:
            logger.warning(f"Error esperando cancelación de task: {e}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _build_execution_context(self, project: Dict[str, Any]) -> Any:
        """
        Construye un contexto Tauro para ejecución.

        El contexto necesita:
        - Input paths
        - Output paths
        - Global settings
        - Environment variables

        Args:
            project: Documento de proyecto de MongoDB

        Returns:
            Context object de Tauro (puede ser simulado o real)
        """
        logger.debug(f"Construyendo contexto para proyecto {project.get('id')}")

        try:
            # Si hay tauro.config.contexts disponible
            from tauro.config.contexts import Context

            # Crear context desde settings del proyecto
            settings = project.get("global_settings", {})

            context = Context(
                input_path=settings.get("input_path", "/tmp/input"),
                output_path=settings.get("output_path", "/tmp/output"),
                mode=settings.get("mode", "batch"),
            )

            return context

        except ImportError as e:
            logger.warning(f"Tauro Context no disponible: {e}, usando simulado")
            # Retornar un dict simulado si Tauro no está disponible
            return {
                "input_path": project.get("global_settings", {}).get("input_path"),
                "output_path": project.get("global_settings", {}).get("output_path"),
            }

    async def _mark_run_failed(self, run_id: str, error_message: str) -> None:
        """
        Marca un run como FAILED con mensaje de error.

        Args:
            run_id: ID del run
            error_message: Mensaje de error
        """
        await self.runs_collection.update_one(
            {"id": run_id},
            {
                "$set": {
                    "state": RunState.FAILED.value,
                    "completed_at": datetime.now(timezone.utc),
                    "error": error_message,
                }
            },
        )
        logger.info(f"Run {run_id} marcado como FAILED: {error_message}")

    async def check_execution_health(self) -> Dict[str, Any]:
        """
        Verifica la salud del servicio de ejecución.

        Returns:
            Diccionario con información de salud
        """
        try:
            # Contar runs por estado
            running = await self.runs_collection.count_documents(
                {"state": RunState.RUNNING.value}
            )
            pending = await self.runs_collection.count_documents(
                {"state": RunState.PENDING.value}
            )
            failed = await self.runs_collection.count_documents(
                {"state": RunState.FAILED.value}
            )

            orchestrator_status = (
                "available" if self.orchestrator_runner else "unavailable"
            )

            return {
                "status": "healthy",
                "orchestrator": orchestrator_status,
                "active_runs": {
                    "running": running,
                    "pending": pending,
                },
                "recent_failures": failed,
                "active_tasks": len(self._active_tasks),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Error al verificar salud: {e}", exc_info=True)
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
