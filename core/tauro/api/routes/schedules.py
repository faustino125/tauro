from __future__ import annotations
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, status, Request
from loguru import logger
from pydantic import BaseModel
from typing import Any, Dict
import asyncio

from tauro.api.auth import get_current_user, get_admin_user
from tauro.api.deps import get_store, track_db_operation
from tauro.api.models import ScheduleCreateRequest, ScheduleResponse, ErrorResponse
from tauro.orchest.models import ScheduleKind

router = APIRouter(
    prefix="/schedules",
    tags=["schedules"],
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)


class ScheduleCreate(BaseModel):
    pipeline_id: str
    cron: str
    params: Dict[str, Any] = {}


@router.post(
    "",
    response_model=ScheduleResponse,
    status_code=201,
    summary="Create a new schedule",
    response_description="Schedule created",
)
def create_schedule(
    payload: ScheduleCreateRequest,
    user: dict = Depends(get_admin_user),
    store=Depends(get_store),
) -> ScheduleResponse:
    """
    Create a new schedule for a pipeline.

    Requires admin privileges.
    """
    try:
        # Validar el tipo de schedule
        try:
            kind = ScheduleKind(payload.kind)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid schedule kind: {payload.kind}. Must be one of {[e.value for e in ScheduleKind]}",
            )

        # Validar expresión cron si es necesario
        if kind == ScheduleKind.CRON:
            try:
                from croniter import croniter  # type: ignore

                croniter(payload.expression)  # Validar expresión
            except ImportError:
                logger.warning("croniter not installed, skipping cron validation")
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid cron expression: {str(e)}",
                )

        with track_db_operation("create", "schedule"):
            sched = store.create_schedule(
                pipeline_id=payload.pipeline_id,
                kind=kind,
                expression=payload.expression,
                max_concurrency=payload.max_concurrency,
                retry_policy={
                    "retries": payload.retries,
                    "delay": payload.retry_delay_sec,
                },
                timeout_seconds=payload.timeout_seconds,
                enabled=payload.enabled,
            )

        logger.info(
            f"User {user['id']} created schedule {sched.id} for pipeline '{sched.pipeline_id}'"
        )

        return ScheduleResponse(
            id=sched.id,
            pipeline_id=sched.pipeline_id,
            kind=sched.kind.value,
            expression=sched.expression,
            enabled=sched.enabled,
            max_concurrency=sched.max_concurrency,
            retry_policy=sched.retry_policy,
            timeout_seconds=sched.timeout_seconds,
            next_run_at=sched.next_run_at,
            created_at=sched.created_at,
            updated_at=sched.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to create schedule for pipeline {payload.pipeline_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create schedule: {str(e)}",
        )


@router.get(
    "",
    response_model=List[ScheduleResponse],
    summary="List schedules",
    response_description="List of schedules",
)
def list_schedules(
    pipeline_id: Optional[str] = Query(
        default=None, description="Filter by pipeline ID"
    ),
    enabled: Optional[bool] = Query(
        default=None, description="Filter by enabled status"
    ),
    kind: Optional[str] = Query(default=None, description="Filter by schedule kind"),
    user: dict = Depends(get_current_user),
    store=Depends(get_store),
):
    """
    Get a list of schedules with optional filtering.
    """
    try:
        with track_db_operation("read", "schedule"):
            scheds = store.list_schedules(
                pipeline_id=pipeline_id,
                enabled_only=enabled if enabled is not None else False,
                kind=ScheduleKind(kind) if kind else None,
            )

        return [
            ScheduleResponse(
                id=s.id,
                pipeline_id=s.pipeline_id,
                kind=s.kind.value,
                expression=s.expression,
                enabled=s.enabled,
                max_concurrency=s.max_concurrency,
                retry_policy=s.retry_policy,
                timeout_seconds=s.timeout_seconds,
                next_run_at=s.next_run_at,
                created_at=s.created_at,
                updated_at=s.updated_at,
            )
            for s in scheds
        ]
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid filter value: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Failed to list schedules: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list schedules: {str(e)}",
        )


@router.get(
    "/{schedule_id}",
    response_model=ScheduleResponse,
    summary="Get schedule details",
    response_description="Schedule details",
)
def get_schedule(
    schedule_id: str,
    user: dict = Depends(get_current_user),
    store=Depends(get_store),
):
    """
    Get detailed information about a specific schedule.
    """
    try:
        # Necesitamos una función en el store para obtener un schedule por ID
        if hasattr(store, "get_schedule"):
            with track_db_operation("read", "schedule"):
                sched = store.get_schedule(schedule_id)
        else:
            # Implementación alternativa: buscar en la lista
            with track_db_operation("read", "schedule"):
                all_scheds = store.list_schedules()
                sched = next((s for s in all_scheds if s.id == schedule_id), None)

        if not sched:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule {schedule_id} not found",
            )

        return ScheduleResponse(
            id=sched.id,
            pipeline_id=sched.pipeline_id,
            kind=sched.kind.value,
            expression=sched.expression,
            enabled=sched.enabled,
            max_concurrency=sched.max_concurrency,
            retry_policy=sched.retry_policy,
            timeout_seconds=sched.timeout_seconds,
            next_run_at=sched.next_run_at,
            created_at=sched.created_at,
            updated_at=sched.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get schedule: {str(e)}",
        )


@router.patch(
    "/{schedule_id}",
    response_model=ScheduleResponse,
    summary="Update a schedule",
    response_description="Schedule updated",
)
def update_schedule(
    schedule_id: str,
    payload: ScheduleCreateRequest,  # Reutilizamos el modelo de creación para updates
    user: dict = Depends(get_admin_user),
    store=Depends(get_store),
):
    """
    Update an existing schedule.

    Requires admin privileges.
    """
    try:
        # Primero verificar que el schedule existe
        if hasattr(store, "get_schedule"):
            with track_db_operation("read", "schedule"):
                existing = store.get_schedule(schedule_id)
        else:
            with track_db_operation("read", "schedule"):
                all_scheds = store.list_schedules()
                existing = next((s for s in all_scheds if s.id == schedule_id), None)

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule {schedule_id} not found",
            )

        # Validar el tipo de schedule
        try:
            kind = ScheduleKind(payload.kind)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid schedule kind: {payload.kind}",
            )

        # Actualizar el schedule
        update_fields = {
            "pipeline_id": payload.pipeline_id,
            "kind": kind,
            "expression": payload.expression,
            "max_concurrency": payload.max_concurrency,
            "retry_policy": {
                "retries": payload.retries,
                "delay": payload.retry_delay_sec,
            },
            "timeout_seconds": payload.timeout_seconds,
            "enabled": payload.enabled,
        }

        with track_db_operation("update", "schedule"):
            store.update_schedule(schedule_id, **update_fields)

        # Obtener el schedule actualizado
        if hasattr(store, "get_schedule"):
            with track_db_operation("read", "schedule"):
                updated = store.get_schedule(schedule_id)
        else:
            with track_db_operation("read", "schedule"):
                all_scheds = store.list_schedules()
                updated = next((s for s in all_scheds if s.id == schedule_id), None)

        logger.info(f"User {user['id']} updated schedule {schedule_id}")

        return ScheduleResponse(
            id=updated.id,
            pipeline_id=updated.pipeline_id,
            kind=updated.kind.value,
            expression=updated.expression,
            enabled=updated.enabled,
            max_concurrency=updated.max_concurrency,
            retry_policy=updated.retry_policy,
            timeout_seconds=updated.timeout_seconds,
            next_run_at=updated.next_run_at,
            created_at=updated.created_at,
            updated_at=updated.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update schedule: {str(e)}",
        )


@router.delete(
    "/{schedule_id}",
    status_code=204,
    summary="Delete a schedule",
    response_description="Schedule deleted",
)
def delete_schedule(
    schedule_id: str,
    user: dict = Depends(get_admin_user),
    store=Depends(get_store),
):
    """
    Delete a schedule.

    Requires admin privileges.
    """
    try:
        # Necesitamos una función en el store para eliminar schedules
        if hasattr(store, "delete_schedule"):
            with track_db_operation("delete", "schedule"):
                success = store.delete_schedule(schedule_id)
        else:
            # Implementación alternativa: deshabilitar el schedule
            with track_db_operation("update", "schedule"):
                store.update_schedule(schedule_id, enabled=False)
            success = True

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schedule {schedule_id} not found",
            )

        logger.info(f"User {user['id']} deleted schedule {schedule_id}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete schedule {schedule_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete schedule: {str(e)}",
        )


@router.post("/")
async def create_schedule(req: Request, body: ScheduleCreate):
    """
    Crear schedule. Intentar usar pipeline_service si expone scheduling,
    sino caer en store/scheduler.
    """
    ps = getattr(req.app.state, "pipeline_service", None)
    store = getattr(req.app.state, "store", None)
    scheduler = getattr(req.app.state, "scheduler", None)

    if ps and hasattr(ps, "create_schedule"):
        res = ps.create_schedule(body.pipeline_id, body.cron, body.params)
        if asyncio.iscoroutine(res):
            res = await res
        return res

    if scheduler and hasattr(scheduler, "create_schedule"):
        return scheduler.create_schedule(body.pipeline_id, body.cron, body.params)

    if store and hasattr(store, "create_schedule"):
        return store.create_schedule(body.pipeline_id, body.cron, body.params)

    raise HTTPException(status_code=500, detail="No scheduling backend available")


@router.get("/")
async def list_schedules(req: Request):
    ps = getattr(req.app.state, "pipeline_service", None)
    store = getattr(req.app.state, "store", None)
    scheduler = getattr(req.app.state, "scheduler", None)

    if ps and hasattr(ps, "list_schedules"):
        res = ps.list_schedules()
        if asyncio.iscoroutine(res):
            res = await res
        return res

    if scheduler and hasattr(scheduler, "list_schedules"):
        return scheduler.list_schedules()
    if store and hasattr(store, "list_schedules"):
        return store.list_schedules()

    return []
