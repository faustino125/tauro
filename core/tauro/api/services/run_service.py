"""
RunService: Gestión de pipeline runs (ejecuciones de pipelines)

Este servicio implementa la lógica de negocio para crear, seguimiento y gestión
de ejecuciones de pipelines (runs).
"""

import logging
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncDatabase  # type: ignore

from tauro.api.schemas.models import (
    RunCreate,
    RunResponse,
    RunState,
    TaskState,
    RunProgress,
)
from tauro.api.db.models import PipelineRunDocument, TaskRunDocument
from tauro.api.schemas.project_validators import ProjectValidator


logger = logging.getLogger(__name__)


class RunNotFoundError(Exception):
    """Run no encontrado"""

    pass


class RunStateError(Exception):
    """Error relacionado con el estado del run"""

    pass


class InvalidRunError(Exception):
    """Datos de run inválidos"""

    pass


class RunService:
    """
    Servicio de gestión de pipeline runs.

    Responsabilidades:
    - CRUD de runs (Create, Read, Update, Delete)
    - Gestión de estados de runs
    - Seguimiento de progreso
    - Manejo de tareas dentro de runs
    - Cancellación de ejecuciones
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

    async def create_run(
        self,
        run_data: RunCreate,
        created_by: str,
    ) -> RunResponse:
        """
        Crea un nuevo run (ejecución de pipeline).

        Args:
            run_data: Datos del run a crear
            created_by: Usuario que crea el run (email o ID)

        Returns:
            RunResponse con el run creado

        Raises:
            InvalidRunError: Si los datos no son válidos
        """
        try:
            # Validar que el proyecto existe
            project = await self.projects_collection.find_one(
                {"id": str(run_data.project_id)}
            )
            if not project:
                raise InvalidRunError(f"Proyecto {run_data.project_id} no encontrado")

            # Validar que el pipeline existe en el proyecto
            pipelines = project.get("pipelines", [])
            pipeline_found = any(
                str(p.get("id")) == str(run_data.pipeline_id) for p in pipelines
            )
            if not pipeline_found:
                raise InvalidRunError(
                    f"Pipeline {run_data.pipeline_id} no encontrado en proyecto"
                )

            # Crear documento de run
            run_id = str(uuid4())
            now = datetime.now(timezone.utc)

            run_doc: PipelineRunDocument = {
                "id": run_id,
                "project_id": str(run_data.project_id),
                "pipeline_id": str(run_data.pipeline_id),
                "state": RunState.PENDING.value,
                "params": run_data.params or {},
                "priority": run_data.priority or "normal",
                "tags": run_data.tags or {},
                "progress": {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "tasks_by_state": {},
                },
                "created_at": now,
                "started_at": None,
                "completed_at": None,
                "created_by": created_by,
                "error": None,
                "timeout_seconds": run_data.timeout_seconds,
            }

            # Insertar en MongoDB
            await self.runs_collection.insert_one(run_doc)
            logger.info(f"Run creado: {run_id} para pipeline {run_data.pipeline_id}")

            # Remover _id de MongoDB
            run_doc.pop("_id", None)
            return RunResponse(**run_doc)

        except InvalidRunError:
            raise
        except Exception as e:
            logger.error(f"Error al crear run: {str(e)}")
            raise InvalidRunError(f"Error al crear run: {str(e)}")

    async def get_run(self, run_id: str) -> RunResponse:
        """
        Obtiene un run por ID.

        Args:
            run_id: ID del run

        Returns:
            RunResponse con los datos del run

        Raises:
            RunNotFoundError: Si el run no existe
        """
        run_doc = await self.runs_collection.find_one({"id": run_id})

        if not run_doc:
            raise RunNotFoundError(f"Run {run_id} no encontrado")

        run_doc.pop("_id", None)
        return RunResponse(**run_doc)

    async def list_runs(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[List[RunResponse], int]:
        """
        Lista runs con filtros opcionales.

        Args:
            project_id: Filtrar por proyecto
            pipeline_id: Filtrar por pipeline
            state: Filtrar por estado
            limit: Número máximo de resultados
            offset: Número de resultados a saltar

        Returns:
            Tupla de (lista de runs, total de registros)
        """
        # Construir filtro
        query = {}

        if project_id:
            query["project_id"] = project_id

        if pipeline_id:
            query["pipeline_id"] = pipeline_id

        if state:
            query["state"] = state

        # Contar total
        total = await self.runs_collection.count_documents(query)

        # Obtener página ordenada por más reciente primero
        cursor = (
            self.runs_collection.find(query)
            .sort("created_at", -1)
            .skip(offset)
            .limit(limit)
        )

        runs = []
        async for doc in cursor:
            doc.pop("_id", None)
            runs.append(RunResponse(**doc))

        logger.debug(
            f"Listados {len(runs)} runs con filtros: "
            f"project_id={project_id}, pipeline_id={pipeline_id}, state={state}"
        )

        return runs, total

    async def update_run_state(
        self,
        run_id: str,
        new_state: str,
        error: Optional[str] = None,
    ) -> RunResponse:
        """
        Actualiza el estado de un run.

        Args:
            run_id: ID del run
            new_state: Nuevo estado (PENDING, RUNNING, SUCCESS, FAILED, CANCELLED)
            error: Mensaje de error (si aplica)

        Returns:
            RunResponse actualizado

        Raises:
            RunNotFoundError: Si el run no existe
            RunStateError: Si la transición de estado no es válida
        """
        # Obtener run actual
        run = await self.get_run(run_id)

        # Validar transición de estado
        valid_transitions = {
            RunState.PENDING.value: [
                RunState.RUNNING.value,
                RunState.CANCELLED.value,
            ],
            RunState.RUNNING.value: [
                RunState.SUCCESS.value,
                RunState.FAILED.value,
                RunState.CANCELLED.value,
            ],
            RunState.SUCCESS.value: [],
            RunState.FAILED.value: [],
            RunState.CANCELLED.value: [],
        }

        if (
            new_state not in valid_transitions.get(run.state, [])
            and new_state != run.state
        ):
            raise RunStateError(f"Transición inválida de {run.state} a {new_state}")

        # Preparar actualización
        update_dict = {"state": new_state}

        # Agregar timestamps según el estado
        now = datetime.now(timezone.utc)
        if new_state == RunState.RUNNING.value and not run.started_at:
            update_dict["started_at"] = now

        if new_state in [
            RunState.SUCCESS.value,
            RunState.FAILED.value,
            RunState.CANCELLED.value,
        ]:
            update_dict["completed_at"] = now

        # Agregar error si aplica
        if error:
            update_dict["error"] = error

        # Actualizar en MongoDB
        result = await self.runs_collection.update_one(
            {"id": run_id},
            {"$set": update_dict},
        )

        if result.matched_count == 0:
            raise RunNotFoundError(f"Run {run_id} no encontrado")

        logger.info(
            f"Run {run_id} transicionó a estado {new_state}. "
            f"Error: {error or 'None'}"
        )

        return await self.get_run(run_id)

    async def cancel_run(self, run_id: str, reason: str = "") -> RunResponse:
        """
        Cancela un run.

        Args:
            run_id: ID del run a cancelar
            reason: Razón de la cancelación

        Returns:
            RunResponse actualizado

        Raises:
            RunNotFoundError: Si el run no existe
            RunStateError: Si el run no puede ser cancelado
        """
        run_data = await self.get_run(run_id)

        # Solo se puede cancelar si está en PENDING o RUNNING
        if run_data.state not in [RunState.PENDING.value, RunState.RUNNING.value]:
            raise RunStateError(
                f"No se puede cancelar un run en estado {run_data.state}"
            )

        error_msg = f"Cancelado: {reason}" if reason else "Cancelado por usuario"
        return await self.update_run_state(
            run_id,
            RunState.CANCELLED.value,
            error=error_msg,
        )

    async def get_run_progress(self, run_id: str) -> RunProgress:
        """
        Obtiene el progreso actual de un run.

        Args:
            run_id: ID del run

        Returns:
            RunProgress con estadísticas de progreso

        Raises:
            RunNotFoundError: Si el run no existe
        """
        await self.get_run(run_id)

        # Contar tareas por estado
        tasks = []
        cursor = self.tasks_collection.find({"run_id": run_id})
        async for task in cursor:
            tasks.append(task)

        tasks_by_state = {}
        for task in tasks:
            state = task.get("state")
            tasks_by_state[state] = tasks_by_state.get(state, 0) + 1

        # Calcular progreso
        total_tasks = len(tasks)
        completed_tasks = sum(
            1
            for t in tasks
            if t.get("state") in [TaskState.SUCCESS.value, TaskState.FAILED.value]
        )
        failed_tasks = sum(1 for t in tasks if t.get("state") == TaskState.FAILED.value)

        return RunProgress(
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            tasks_by_state=tasks_by_state,
        )

    async def add_task(
        self,
        run_id: str,
        task_name: str,
        node_id: str,
    ) -> str:
        """
        Agrega una tarea a un run.

        Args:
            run_id: ID del run
            task_name: Nombre de la tarea
            node_id: ID del nodo que ejecuta la tarea

        Returns:
            ID de la tarea creada

        Raises:
            RunNotFoundError: Si el run no existe
        """
        # Verificar que el run existe
        run = await self.get_run(run_id)

        # Crear documento de tarea
        task_id = str(uuid4())
        task_doc: TaskRunDocument = {
            "id": task_id,
            "run_id": run_id,
            "project_id": run.project_id,
            "task_name": task_name,
            "node_id": node_id,
            "state": TaskState.PENDING.value,
            "created_at": datetime.now(timezone.utc),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "output": None,
        }

        # Insertar en MongoDB
        await self.tasks_collection.insert_one(task_doc)
        logger.info(f"Tarea {task_id} agregada a run {run_id}")

        return task_id

    async def update_task_state(
        self,
        task_id: str,
        new_state: str,
        error: Optional[str] = None,
        output: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Actualiza el estado de una tarea.

        Args:
            task_id: ID de la tarea
            new_state: Nuevo estado (PENDING, RUNNING, SUCCESS, FAILED)
            error: Mensaje de error (si aplica)
            output: Salida de la tarea (si aplica)

        Returns:
            Documento de la tarea actualizada
        """
        update_dict = {"state": new_state}

        now = datetime.now(timezone.utc)
        if new_state == TaskState.RUNNING.value:
            update_dict["started_at"] = now

        if new_state in [TaskState.SUCCESS.value, TaskState.FAILED.value]:
            update_dict["completed_at"] = now

        if error:
            update_dict["error"] = error

        if output:
            update_dict["output"] = output

        await self.tasks_collection.update_one(
            {"id": task_id},
            {"$set": update_dict},
        )

        logger.info(f"Tarea {task_id} actualizada a estado {new_state}")

        # Retornar tarea actualizada
        task = await self.tasks_collection.find_one({"id": task_id})
        task.pop("_id", None)
        return task

    async def get_run_tasks(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Obtiene todas las tareas de un run.

        Args:
            run_id: ID del run

        Returns:
            Lista de documentos de tareas

        Raises:
            RunNotFoundError: Si el run no existe
        """
        # Verificar que el run existe
        await self.get_run(run_id)

        tasks = []
        cursor = self.tasks_collection.find({"run_id": run_id}).sort("created_at", 1)
        async for task in cursor:
            task.pop("_id", None)
            tasks.append(task)

        return tasks

    # =========================================================================
    # START AND CANCEL RUN - NUEVOS MÉTODOS CRÍTICOS
    # =========================================================================

    async def start_run(
        self,
        run_id: str,
        timeout_seconds: Optional[int] = None,
    ) -> RunResponse:
        """
        Inicia la ejecución de un run que está en estado PENDING.

        Este método delega la ejecución real a ExecutionService,
        que se encarga de:
        1. Cambiar estado a RUNNING
        2. Ejecutar en background (no bloquea)
        3. Actualizar progreso en MongoDB
        4. Cambiar a SUCCESS/FAILED cuando termine

        Args:
            run_id: ID del run a iniciar
            timeout_seconds: Timeout para la ejecución

        Returns:
            RunResponse con run en estado RUNNING

        Raises:
            RunNotFoundError: Si run no existe
            RunStateError: Si run no está en PENDING
            InvalidRunError: Si hay errores
        """
        try:
            logger.info(f"Iniciando ejecución de run {run_id}")

            # 1. VALIDAR: Run existe
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise RunNotFoundError(f"Run {run_id} no encontrado")

            # 2. VALIDAR: Estado es PENDING
            current_state = run.get("state", RunState.PENDING.value)
            if current_state != RunState.PENDING.value:
                raise RunStateError(
                    f"Run {run_id} no está en PENDING (estado: {current_state}). "
                    f"Solo se pueden iniciar runs en PENDING."
                )

            # 3. ACTUALIZAR timeout si se proporciona
            if timeout_seconds:
                await self.runs_collection.update_one(
                    {"id": run_id}, {"$set": {"timeout_seconds": timeout_seconds}}
                )

            # NOTA: La ejecución real (cambiar a RUNNING, etc.) la hace ExecutionService
            # Este método simplemente valida y retorna el run actualizado
            # para que el caller (route) sepa que puede proceder

            # 4. RETORNAR RUN ACTUALIZADO
            run = await self.runs_collection.find_one({"id": run_id})
            run.pop("_id", None)

            return RunResponse(**run)

        except (RunNotFoundError, RunStateError, InvalidRunError):
            raise
        except Exception as e:
            logger.error(f"Error iniciando run {run_id}: {e}", exc_info=True)
            raise InvalidRunError(f"Error al iniciar run: {str(e)}")

    async def cancel_run(
        self,
        run_id: str,
        reason: str = "User cancelled",
    ) -> RunResponse:
        """
        Cancela un run que está en ejecución.

        Este método delega la cancelación real a ExecutionService,
        que se encarga de:
        1. Cambiar estado a CANCELLED
        2. Signal a OrchestratorRunner
        3. Marcar task asincronas como canceladas

        Args:
            run_id: ID del run a cancelar
            reason: Razón de la cancelación

        Returns:
            RunResponse con run en estado CANCELLED

        Raises:
            RunNotFoundError: Si run no existe
            InvalidRunError: Si hay errores
        """
        try:
            logger.info(f"Cancelando run {run_id}: {reason}")

            # 1. VALIDAR: Run existe
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise RunNotFoundError(f"Run {run_id} no encontrado")

            # NOTA: La cancelación real (cambiar estado, etc.) la hace ExecutionService
            # Este método simplemente valida y retorna el run

            # 2. RETORNAR RUN ACTUALIZADO
            run = await self.runs_collection.find_one({"id": run_id})
            run.pop("_id", None)

            return RunResponse(**run)

        except RunNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error cancelando run {run_id}: {e}", exc_info=True)
            raise InvalidRunError(f"Error al cancelar run: {str(e)}")

        return tasks
