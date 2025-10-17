from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import Optional
from loguru import logger

from tauro.api.core import (
    get_scheduler_service,
    get_orchestrator_store,
)
from tauro.api.schemas import (
    ScheduleCreateRequest,
    ScheduleUpdateRequest,
    ScheduleResponse,
    ScheduleListResponse,
    MessageResponse,
    ScheduleKind,
)

router = APIRouter(prefix="/schedules", tags=["scheduling"])


# =============================================================================
# Error Message Constants
# =============================================================================

ERROR_SCHEDULER_UNAVAILABLE = "SchedulerService not available"
ERROR_SCHEDULE_NOT_FOUND = "Schedule not found"
ERROR_INVALID_SCHEDULE_ID = "Invalid schedule ID"


# =============================================================================
# Schedule Management
# =============================================================================


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    request: ScheduleCreateRequest,
    scheduler=Depends(get_scheduler_service),
):
    """
    Crear un nuevo schedule.

    Crea una programación para ejecutar un pipeline de forma automática.
    """
    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_SCHEDULER_UNAVAILABLE,
        )

    try:
        schedule = scheduler.create_schedule(
            pipeline_id=request.pipeline_id,
            kind=request.kind.value,
            expression=request.expression,
            params=request.params,
            enabled=request.enabled,
            max_concurrency=request.max_concurrency,
            timeout_seconds=request.timeout_seconds,
        )

        return ScheduleResponse(
            id=schedule.schedule_id,
            pipeline_id=schedule.pipeline_id,
            kind=ScheduleKind(schedule.kind.value),
            expression=schedule.expression,
            params=schedule.params,
            enabled=schedule.enabled,
            max_concurrency=schedule.max_concurrency or 1,
            timeout_seconds=schedule.timeout_seconds,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
            last_run_at=schedule.last_run_at,
            next_run_at=schedule.next_run_at,
        )

    except Exception as e:
        logger.error(f"Error creating schedule: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("", response_model=ScheduleListResponse)
async def list_schedules(
    pipeline_id: Optional[str] = None,
    enabled: Optional[bool] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    store=Depends(get_orchestrator_store),
):
    """
    Listar schedules.

    Retorna la lista de schedules configurados, con filtros opcionales.
    """
    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorStore not available",
        )

    try:
        filters = {}
        if pipeline_id:
            filters["pipeline_id"] = pipeline_id
        if enabled is not None:
            filters["enabled"] = enabled

        all_schedules = store.list_schedules(**filters)

        # Pagination
        total = len(all_schedules)
        schedules = all_schedules[offset : offset + limit]

        return ScheduleListResponse(
            schedules=[
                ScheduleResponse(
                    id=schedule.schedule_id,
                    pipeline_id=schedule.pipeline_id,
                    kind=ScheduleKind(schedule.kind.value),
                    expression=schedule.expression,
                    params=schedule.params,
                    enabled=schedule.enabled,
                    max_concurrency=schedule.max_concurrency or 1,
                    timeout_seconds=schedule.timeout_seconds,
                    created_at=schedule.created_at,
                    updated_at=schedule.updated_at,
                    last_run_at=schedule.last_run_at,
                    next_run_at=schedule.next_run_at,
                )
                for schedule in schedules
            ],
            total=total,
        )

    except Exception as e:
        logger.error(f"Error listing schedules: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: str,
    store=Depends(get_orchestrator_store),
):
    """Obtener información de un schedule específico"""
    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorStore not available",
        )

    try:
        schedule = store.get_schedule(schedule_id)

        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule '{schedule_id}' not found",
            )

        return ScheduleResponse(
            id=schedule.schedule_id,
            pipeline_id=schedule.pipeline_id,
            kind=ScheduleKind(schedule.kind.value),
            expression=schedule.expression,
            params=schedule.params,
            enabled=schedule.enabled,
            max_concurrency=schedule.max_concurrency or 1,
            timeout_seconds=schedule.timeout_seconds,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
            last_run_at=schedule.last_run_at,
            next_run_at=schedule.next_run_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: str,
    request: ScheduleUpdateRequest,
    scheduler=Depends(get_scheduler_service),
):
    """
    Actualizar un schedule.

    Permite modificar la configuración de un schedule existente.
    """
    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_SCHEDULER_UNAVAILABLE,
        )

    try:
        # Get current schedule
        schedule = scheduler.store.get_schedule(schedule_id)

        if not schedule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule '{schedule_id}' not found",
            )

        # Update fields
        update_data = request.dict(exclude_unset=True)
        scheduler.update_schedule(schedule_id, **update_data)

        # Get updated schedule
        updated_schedule = scheduler.store.get_schedule(schedule_id)

        return ScheduleResponse(
            id=updated_schedule.schedule_id,
            pipeline_id=updated_schedule.pipeline_id,
            kind=ScheduleKind(updated_schedule.kind.value),
            expression=updated_schedule.expression,
            params=updated_schedule.params,
            enabled=updated_schedule.enabled,
            max_concurrency=updated_schedule.max_concurrency or 1,
            timeout_seconds=updated_schedule.timeout_seconds,
            created_at=updated_schedule.created_at,
            updated_at=updated_schedule.updated_at,
            last_run_at=updated_schedule.last_run_at,
            next_run_at=updated_schedule.next_run_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.delete("/{schedule_id}", response_model=MessageResponse)
async def delete_schedule(
    schedule_id: str,
    scheduler=Depends(get_scheduler_service),
):
    """
    Eliminar un schedule.

    Elimina permanentemente un schedule del sistema.
    """
    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_SCHEDULER_UNAVAILABLE,
        )

    try:
        scheduler.delete_schedule(schedule_id)

        return MessageResponse(message=f"Schedule {schedule_id} deleted successfully")

    except Exception as e:
        logger.error(f"Error deleting schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# =============================================================================
# Schedule Control
# =============================================================================


@router.post("/{schedule_id}/pause", response_model=MessageResponse)
async def pause_schedule(
    schedule_id: str,
    scheduler=Depends(get_scheduler_service),
):
    """
    Pausar un schedule.

    Deshabilita temporalmente un schedule sin eliminarlo.
    """
    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_SCHEDULER_UNAVAILABLE,
        )

    try:
        scheduler.update_schedule(schedule_id, enabled=False)

        return MessageResponse(message=f"Schedule {schedule_id} paused successfully")

    except Exception as e:
        logger.error(f"Error pausing schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/{schedule_id}/resume", response_model=MessageResponse)
async def resume_schedule(
    schedule_id: str,
    scheduler=Depends(get_scheduler_service),
):
    """
    Reanudar un schedule.

    Habilita un schedule previamente pausado.
    """
    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_SCHEDULER_UNAVAILABLE,
        )

    try:
        scheduler.update_schedule(schedule_id, enabled=True)

        return MessageResponse(message=f"Schedule {schedule_id} resumed successfully")

    except Exception as e:
        logger.error(f"Error resuming schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
