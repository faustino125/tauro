"""
Scheduling Router - Pipeline Schedule Management

Endpoints para gestionar programaciones de pipelines:
- Crear, listar, obtener schedules
- Pausar/reanudar schedules
- Backfill de runs
"""

from fastapi import APIRouter, Depends, status, Query
from typing import Optional
from loguru import logger

from core.api.core import get_orchestrator_store, get_config_manager
from core.api.core.responses import (
    APIResponse,
    ListResponse,
    success_response,
    error_response,
    list_response,
)
from core.api.schemas import (
    ScheduleCreateRequest,
    ScheduleUpdateRequest,
    ScheduleResponse,
)

router = APIRouter(prefix="/schedules", tags=["scheduling"])


def _schedule_to_response(schedule) -> dict:
    """Convert orchestrator Schedule model into API response."""
    return {
        "id": schedule.id,
        "pipeline_id": schedule.pipeline_id,
        "kind": schedule.kind.value
        if hasattr(schedule.kind, "value")
        else schedule.kind,
        "expression": schedule.expression,
        "enabled": schedule.enabled,
        "max_concurrency": schedule.max_concurrency,
        "retry_policy": schedule.retry_policy or {"retries": 0, "delay": 0},
        "timeout_seconds": schedule.timeout_seconds,
        "created_at": schedule.created_at,
        "updated_at": schedule.updated_at,
        "next_run_at": schedule.next_run_at,
    }


# =============================================================================
# Error Message Constants
# =============================================================================

ERROR_STORE_UNAVAILABLE = "OrchestratorStore not available"
ERROR_SCHEDULE_NOT_FOUND = "Schedule not found"


# =============================================================================
# CREATE - Create new schedule
# =============================================================================


@router.post("", response_model=APIResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    request: ScheduleCreateRequest,
    store=Depends(get_orchestrator_store),
    config_manager=Depends(get_config_manager),
):
    """
    Create a new schedule.

    Create a schedule to automatically execute a pipeline.
    Validates that the pipeline exists before creating the schedule.
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        # Validate that the pipeline exists
        if config_manager:
            try:
                pipelines_config = config_manager.load_config("pipeline")
                if request.pipeline_id not in pipelines_config:
                    return error_response(
                        code="PIPELINE_NOT_FOUND",
                        message=f"Pipeline '{request.pipeline_id}' not found",
                    )
            except Exception as e:
                logger.warning(f"Could not validate pipeline: {e}")
                # Continue anyway in case ConfigManager is unavailable

        # Create the schedule
        schedule = store.create_schedule(
            pipeline_id=request.pipeline_id,
            kind=request.kind,
            expression=request.expression,
            enabled=request.enabled,
            max_concurrency=request.max_concurrency or 1,
            retry_policy=request.retry_policy,
            timeout_seconds=request.timeout_seconds,
            next_run_at=request.next_run_at,
        )

        logger.info(f"Schedule created: {schedule.id}")
        return success_response(_schedule_to_response(schedule))

    except Exception as e:
        logger.error(f"Error creating schedule: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Error creating schedule",
            details={"error": str(e)},
        )


# =============================================================================
# LIST - List schedules with filters
# =============================================================================


@router.get("", response_model=ListResponse)
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
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        schedules = store.list_schedules(
            pipeline_id=pipeline_id,
            enabled_only=enabled is True,
        )

        if enabled is False:
            schedules = [s for s in schedules if not s.enabled]

        # Pagination
        total = len(schedules)
        schedules_page = schedules[offset : offset + limit]
        converted = [_schedule_to_response(schedule) for schedule in schedules_page]

        logger.debug(f"Listed {len(converted)} schedules")
        return list_response(converted, total, limit, offset)

    except Exception as e:
        logger.error(f"Error listing schedules: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error listing schedules",
            details={"error": str(e)},
        )


# =============================================================================
# READ - Get a specific schedule
# =============================================================================


@router.get("/{schedule_id}", response_model=APIResponse)
async def get_schedule(
    schedule_id: str,
    store=Depends(get_orchestrator_store),
):
    """Get information for a specific schedule"""
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        schedule = store.get_schedule(schedule_id)

        if not schedule:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        return success_response(_schedule_to_response(schedule))

    except Exception as e:
        logger.error(f"Error getting schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error getting schedule",
            details={"error": str(e)},
        )


# =============================================================================
# UPDATE - Update a schedule
# =============================================================================


@router.patch("/{schedule_id}", response_model=APIResponse)
async def update_schedule(
    schedule_id: str,
    request: ScheduleUpdateRequest,
    store=Depends(get_orchestrator_store),
):
    """
    Update a schedule.

    Allow modifying the configuration of an existing schedule.
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        current = store.get_schedule(schedule_id)

        if not current:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        update_data = request.dict(exclude_unset=True)
        allowed = {
            "expression",
            "enabled",
            "max_concurrency",
            "retry_policy",
            "timeout_seconds",
            "next_run_at",
        }
        payload = {k: v for k, v in update_data.items() if k in allowed}

        if payload:
            store.update_schedule(schedule_id, **payload)

        updated_schedule = store.get_schedule(schedule_id)

        if not updated_schedule:
            return error_response(
                code="SCHEDULE_NOT_FOUND",
                message=f"Schedule '{schedule_id}' not found after update",
            )

        logger.info(f"Schedule updated: {schedule_id}")
        return success_response(_schedule_to_response(updated_schedule))

    except Exception as e:
        logger.error(f"Error updating schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error updating schedule",
            details={"error": str(e)},
        )


# =============================================================================
# DELETE - Delete a schedule
# =============================================================================


@router.delete("/{schedule_id}", response_model=APIResponse)
async def delete_schedule(
    schedule_id: str,
    store=Depends(get_orchestrator_store),
):
    """
    Eliminar un schedule.

    Elimina permanentemente un schedule del sistema.
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        deleted = store.delete_schedule(schedule_id)

        if not deleted:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        logger.info(f"Schedule deleted: {schedule_id}")
        return success_response(
            {"message": f"Schedule {schedule_id} deleted successfully"}
        )

    except Exception as e:
        logger.error(f"Error deleting schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error deleting schedule",
            details={"error": str(e)},
        )


# =============================================================================
# PAUSE - Pause a schedule
# =============================================================================


@router.post("/{schedule_id}/pause", response_model=APIResponse)
async def pause_schedule(
    schedule_id: str,
    store=Depends(get_orchestrator_store),
):
    """
    Pausar un schedule.

    Deshabilita temporalmente un schedule sin eliminarlo.
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        store.update_schedule(schedule_id, enabled=False)

        schedule = store.get_schedule(schedule_id)
        if schedule is None:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        logger.info(f"Schedule paused: {schedule_id}")
        return success_response(
            {"message": f"Schedule {schedule_id} paused successfully"}
        )

    except Exception as e:
        logger.error(f"Error pausing schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error pausing schedule",
            details={"error": str(e)},
        )


# =============================================================================
# RESUME - Resume a schedule
# =============================================================================


@router.post("/{schedule_id}/resume", response_model=APIResponse)
async def resume_schedule(
    schedule_id: str,
    store=Depends(get_orchestrator_store),
):
    """
    Reanudar un schedule.

    Habilita un schedule previamente pausado.
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        store.update_schedule(schedule_id, enabled=True)

        schedule = store.get_schedule(schedule_id)
        if schedule is None:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        logger.info(f"Schedule resumed: {schedule_id}")
        return success_response(
            {"message": f"Schedule {schedule_id} resumed successfully"}
        )

    except Exception as e:
        logger.error(f"Error resuming schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error resuming schedule",
            details={"error": str(e)},
        )


# =============================================================================
# BACKFILL - Create historical runs for a schedule
# =============================================================================


@router.post("/{schedule_id}/backfill", response_model=APIResponse)
async def backfill_schedule(
    schedule_id: str,
    count: int = Query(
        10, ge=1, le=100, description="Number of historical runs to create"
    ),
    store=Depends(get_orchestrator_store),
):
    """
    Create N historical runs for a schedule (backfill).

    Useful for recovering missed runs due to downtime or for testing.

    Query parameters:
    - count: Number of historical runs to create (default: 10, max: 100)

    Example: POST /api/v1/schedules/{schedule_id}/backfill?count=20

    Response:
    ```json
    {
        "status": "success",
        "data": {
            "schedule_id": "uuid",
            "runs_created": 20,
            "created_run_ids": ["run-1", "run-2", ...]
        }
    }
    ```
    """
    if not store:
        return error_response(code="STORE_UNAVAILABLE", message=ERROR_STORE_UNAVAILABLE)

    try:
        logger.info(f"Backfilling schedule {schedule_id} with {count} runs")

        # Get schedule
        schedule = store.get_schedule(schedule_id)
        if not schedule:
            return error_response(
                code="SCHEDULE_NOT_FOUND", message=f"Schedule '{schedule_id}' not found"
            )

        # Create backfill runs
        run_ids = []
        for i in range(count):
            try:
                # Create a run from the schedule
                run = store.create_run(
                    pipeline_id=schedule.pipeline_id,
                    params={},
                    priority="normal",
                )
                run_ids.append(str(run.id) if hasattr(run, "id") else run)
            except Exception as e:
                logger.warning(f"Error creating backfill run {i}: {e}")
                continue

        logger.info(
            f"Backfill completed: {len(run_ids)} runs created for schedule {schedule_id}"
        )

        return success_response(
            {
                "schedule_id": schedule_id,
                "runs_created": len(run_ids),
                "created_run_ids": run_ids,
            }
        )

    except Exception as e:
        logger.error(f"Error backfilling schedule {schedule_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error backfilling schedule",
            details={"error": str(e)},
        )


__all__ = ["router"]
