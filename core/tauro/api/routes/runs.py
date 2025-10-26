"""
Runs Router - Pipeline Run Management Endpoints

Endpoints para gestionar la ejecución de pipelines:
- Crear, listar, obtener estado de runs
- Cancelar ejecuciones
- Obtener tareas y logs de un run
"""

from fastapi import APIRouter, Depends, status, Query
from typing import Optional
from datetime import datetime, timezone
from loguru import logger

from tauro.api.core.deps import (
    get_run_service,
)
from tauro.api.core.responses import (
    APIResponse,
    ListResponse,
    success_response,
    error_response,
    list_response,
)
from tauro.api.schemas.models import (
    RunCreate,
    RunResponse,
    RunState,
)
from tauro.api.services.run_service import (
    RunNotFoundError,
    RunStateError,
    InvalidRunError,
)


# =============================================================================
# Router Setup
# =============================================================================

router = APIRouter(prefix="/runs", tags=["runs"])


# =============================================================================
# Error Message Constants
# =============================================================================

ERROR_RUN_NOT_FOUND = "Run not found"
ERROR_INVALID_RUN = "Invalid run configuration"
ERROR_RUN_STATE = "Invalid state transition"


# =============================================================================
# CREATE - Crear nuevo run
# =============================================================================


@router.post(
    "",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Crear un nuevo run",
    description="Crea un nuevo pipeline run en estado PENDING",
)
async def create_run(
    request: RunCreate,
    run_service=Depends(get_run_service),
):
    """
    Crear un nuevo pipeline run.

    Este endpoint crea un nuevo run en estado PENDING.
    Posteriormente, use POST /runs/{run_id}/start para iniciar la ejecución.
    """
    try:
        logger.info(
            f"Creating run for project={request.project_id}, "
            f"pipeline={request.pipeline_id}"
        )

        run = await run_service.create_run(
            project_id=request.project_id,
            pipeline_id=request.pipeline_id,
            schedule_id=request.schedule_id,
            priority=request.priority,
            parameters=request.parameters,
        )

        logger.info(f"Run created: {run.get('id')}")
        return success_response(run)

    except InvalidRunError as e:
        logger.error(f"Invalid run: {e}")
        return error_response(code="INVALID_RUN", message=str(e))
    except Exception as e:
        logger.error(f"Error creating run: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Error creating run",
            details={"error": str(e)},
        )


# =============================================================================
# READ - Obtener un run específico
# =============================================================================


@router.get(
    "/{run_id}",
    response_model=APIResponse,
    summary="Obtener estado de un run",
    description="Obtiene el estado actual y detalles de un pipeline run",
)
async def get_run(
    run_id: str,
    run_service=Depends(get_run_service),
):
    """
    Obtener estado actual de un run.

    Retorna información completa del run incluyendo estado actual,
    tareas ejecutadas y progreso.
    """
    try:
        logger.debug(f"Getting run {run_id}")

        run = await run_service.get_run(run_id)

        if not run:
            return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)

        return success_response(run)

    except Exception as e:
        logger.error(f"Error getting run {run_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error getting run",
            details={"error": str(e)},
        )


# =============================================================================
# LIST - Listar runs con filtros
# =============================================================================


@router.get(
    "",
    response_model=ListResponse,
    summary="Listar runs",
    description="Lista los pipeline runs con filtros opcionales",
)
async def list_runs(
    project_id: Optional[str] = Query(None, description="Filtrar por proyecto"),
    pipeline_id: Optional[str] = Query(None, description="Filtrar por pipeline"),
    state: Optional[str] = Query(None, description="Filtrar por estado"),
    skip: int = Query(0, ge=0, description="Número de runs a saltar"),
    limit: int = Query(50, ge=1, le=100, description="Máximo de runs a retornar"),
    run_service=Depends(get_run_service),
):
    """
    Listar pipeline runs con filtros.

    Retorna una lista paginada de runs con filtros opcionales por
    proyecto, pipeline y estado.
    """
    try:
        logger.debug(
            f"Listing runs: project_id={project_id}, pipeline_id={pipeline_id}, "
            f"state={state}, skip={skip}, limit={limit}"
        )

        runs, total = await run_service.list_runs(
            project_id=project_id,
            pipeline_id=pipeline_id,
            state=state,
            skip=skip,
            limit=limit,
        )

        return list_response(runs, total, limit, skip)

    except Exception as e:
        logger.error(f"Error listing runs: {e}")
        raise error_response(
            code="INTERNAL_ERROR",
            message="Error listing runs",
            details={"error": str(e)},
        )


# =============================================================================
# START - Iniciar/resumir un run
# =============================================================================


@router.post(
    "/{run_id}/start",
    response_model=APIResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Iniciar ejecución de un run",
    description="Inicia la ejecución asíncrona de un pipeline run",
)
async def start_run(
    run_id: str,
    timeout_seconds: Optional[int] = Query(
        None, ge=1, description="Timeout en segundos"
    ),
    run_service=Depends(get_run_service),
):
    """
    Iniciar ejecución de un run.

    Inicia la ejecución del run de forma asíncrona. El endpoint retorna
    inmediatamente con 202 Accepted. Use GET /runs/{run_id} para
    monitorear el progreso.
    """
    try:
        logger.info(f"Starting run {run_id} (timeout: {timeout_seconds}s)")

        await run_service.start_run(run_id, timeout_seconds=timeout_seconds)

        logger.info(f"Run {run_id} accepted for execution")

        return success_response(
            {
                "run_id": run_id,
                "status": "accepted",
                "message": "Pipeline execution started in background",
                "status_url": f"/api/v1/runs/{run_id}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    except RunNotFoundError as e:
        logger.warning(f"Run not found: {e}")
        return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)
    except RunStateError as e:
        logger.warning(f"Invalid run state: {e}")
        return error_response(code="INVALID_RUN_STATE", message=str(e))
    except InvalidRunError as e:
        logger.warning(f"Invalid run: {e}")
        return error_response(code="INVALID_RUN", message=str(e))
    except Exception as e:
        logger.error(f"Error starting run {run_id}: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Error starting run execution",
            details={"error": str(e)},
        )


# =============================================================================
# CANCEL - Cancelar un run
# =============================================================================


@router.post(
    "/{run_id}/cancel",
    response_model=APIResponse,
    summary="Cancelar ejecución de un run",
    description="Cancela la ejecución de un pipeline run",
)
async def cancel_run(
    run_id: str,
    reason: Optional[str] = Query(None, description="Razón de cancelación"),
    run_service=Depends(get_run_service),
):
    """
    Cancelar ejecución de un run.

    Cancela la ejecución del run. Solo funciona si el run está en estado
    RUNNING o PENDING.
    """
    try:
        logger.info(f"Cancelling run {run_id}, reason={reason}")

        run = await run_service.get_run(run_id)
        if not run:
            return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)

        await run_service.cancel_run(run_id, reason=reason)

        return success_response(
            {
                "run_id": run_id,
                "state": "CANCELLED",
                "message": "Run cancelled successfully",
            }
        )

    except RunNotFoundError:
        return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)
    except RunStateError as e:
        return error_response(code="INVALID_RUN_STATE", message=str(e))
    except Exception as e:
        logger.error(f"Error cancelling run {run_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error cancelling run",
            details={"error": str(e)},
        )


# =============================================================================
# TASKS - Obtener tareas de un run
# =============================================================================


@router.get(
    "/{run_id}/tasks",
    response_model=ListResponse,
    summary="Obtener tareas de un run",
    description="Obtiene la lista de tareas ejecutadas en un run",
)
async def get_run_tasks(
    run_id: str,
    skip: int = Query(0, ge=0, description="Número de tareas a saltar"),
    limit: int = Query(50, ge=1, le=100, description="Máximo de tareas a retornar"),
    run_service=Depends(get_run_service),
):
    """
    Obtener tareas de un run.

    Retorna la lista paginada de tareas (nodos del pipeline) ejecutadas
    o siendo ejecutadas en el run.
    """
    try:
        logger.debug(f"Getting tasks for run {run_id}")

        run = await run_service.get_run(run_id)
        if not run:
            return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)

        tasks, total = await run_service.get_run_tasks(run_id, skip=skip, limit=limit)

        return list_response(tasks, total, limit, skip)

    except RunNotFoundError:
        return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting tasks for run {run_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error getting tasks",
            details={"error": str(e)},
        )


# =============================================================================
# LOGS - Obtener logs de un run
# =============================================================================


@router.get(
    "/{run_id}/logs",
    response_model=ListResponse,
    summary="Obtener logs de un run",
    description="Obtiene los logs de ejecución de un run",
)
async def get_run_logs(
    run_id: str,
    skip: int = Query(0, ge=0, description="Número de logs a saltar"),
    limit: int = Query(100, ge=1, le=1000, description="Máximo de logs a retornar"),
    level: Optional[str] = Query(
        None, description="Filtrar por nivel (DEBUG, INFO, WARNING, ERROR)"
    ),
    run_service=Depends(get_run_service),
):
    """
    Obtener logs de un run.

    Retorna los logs de ejecución del run. Puede filtrar por nivel de log
    y paginar el resultado.
    """
    try:
        logger.debug(f"Getting logs for run {run_id}, level={level}")

        run = await run_service.get_run(run_id)
        if not run:
            return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)

        logs, total = await run_service.get_run_logs(
            run_id,
            skip=skip,
            limit=limit,
            level=level,
        )

        return list_response(logs, total, limit, skip)

    except RunNotFoundError:
        return error_response(code="RUN_NOT_FOUND", message=ERROR_RUN_NOT_FOUND)
    except Exception as e:
        logger.error(f"Error getting logs for run {run_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error getting logs",
            details={"error": str(e)},
        )


__all__ = ["router"]
