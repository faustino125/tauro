from __future__ import annotations
from typing import List, Optional
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    status,
    BackgroundTasks,
    Request,
)
from loguru import logger
import uuid
import asyncio

from tauro.api.auth import get_current_user
from tauro.api.deps import get_runner, get_context, get_store, track_db_operation
from tauro.api.models import (
    RunCreateRequest,
    RunStartRequest,
    RunResponse,
    TaskRunResponse,
    ErrorResponse,
)
from tauro.orchest.models import RunState
from pydantic import BaseModel
from typing import Any, Dict

router = APIRouter(
    prefix="/runs",
    tags=["runs"],
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)


class RunCreate(BaseModel):
    pipeline_id: str
    params: Dict[str, Any] = {}


@router.post(
    "",
    response_model=RunResponse,
    status_code=201,
    summary="Create a new pipeline run",
    response_description="Pipeline run created",
)
def create_run(
    payload: RunCreateRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
    runner=Depends(get_runner),
) -> RunResponse:
    """
    Create a new pipeline run.

    The run will be created in PENDING state and needs to be started separately.
    """
    try:
        with track_db_operation("create", "pipeline_run"):
            pr = runner.create_run(payload.pipeline_id, params=payload.params or {})

        logger.info(
            f"User {user['id']} created run {pr.id} for pipeline '{pr.pipeline_id}'"
        )

        return RunResponse(
            id=pr.id,
            pipeline_id=pr.pipeline_id,
            state=pr.state.value,
            created_at=pr.created_at,
            started_at=pr.started_at,
            finished_at=pr.finished_at,
            params=pr.params,
        )
    except Exception as e:
        logger.error(f"Failed to create run for pipeline {payload.pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create run: {str(e)}",
        )


@router.post(
    "/{run_id}/start",
    response_model=RunResponse,
    summary="Start a pipeline run",
    response_description="Pipeline run started",
)
def start_run(
    run_id: str,
    payload: RunStartRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
    runner=Depends(get_runner),
):
    """
    Start a pipeline run execution.

    This will begin executing the pipeline tasks based on the provided configuration.
    """
    try:
        # Verificar que el run existe
        with track_db_operation("read", "pipeline_run"):
            pr = runner.get_run(run_id)
        if not pr:
            raise HTTPException(status_code=404, detail="Run not found")

        # Iniciar ejecución en background para no bloquear la request
        background_tasks.add_task(
            _execute_run_in_background, run_id, payload, user, runner
        )

        return RunResponse(
            id=pr.id,
            pipeline_id=pr.pipeline_id,
            state=RunState.RUNNING.value,  # Asumimos que va a empezar a ejecutar
            created_at=pr.created_at,
            started_at=pr.started_at,
            finished_at=pr.finished_at,
            params=pr.params,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start run: {str(e)}",
        )


def _execute_run_in_background(
    run_id: str, payload: RunStartRequest, user: dict, runner
):
    """Ejecutar el run en segundo plano"""
    try:
        state = runner.start_run(
            run_id,
            retries=payload.retries,
            retry_delay_sec=payload.retry_delay_sec,
            concurrency=payload.concurrency,
        )
        logger.info(f"Run {run_id} completed with state {state} by user {user['id']}")
    except Exception as e:
        logger.error(f"Background execution of run {run_id} failed: {e}")


@router.get(
    "",
    response_model=List[RunResponse],
    summary="List pipeline runs",
    response_description="List of pipeline runs",
)
def list_runs(
    pipeline_id: Optional[str] = Query(
        default=None, description="Filter by pipeline ID"
    ),
    state: Optional[str] = Query(default=None, description="Filter by run state"),
    limit: int = Query(
        default=50, ge=1, le=500, description="Maximum number of runs to return"
    ),
    offset: int = Query(default=0, ge=0, description="Number of runs to skip"),
    user: dict = Depends(get_current_user),
    runner=Depends(get_runner),
    store=Depends(get_store),
):
    """
    Get a list of pipeline runs with optional filtering.
    """
    try:
        enum_state = RunState(state) if state else None

        with track_db_operation("read", "pipeline_run"):
            runs = runner.list_runs(
                pipeline_id=pipeline_id, state=enum_state, limit=limit, offset=offset
            )

        # Obtener el total de runs para paginación (omitted; not used)

        out: List[RunResponse] = []
        for pr in runs:
            out.append(
                RunResponse(
                    id=pr.id,
                    pipeline_id=pr.pipeline_id,
                    state=pr.state.value,
                    created_at=pr.created_at,
                    started_at=pr.started_at,
                    finished_at=pr.finished_at,
                    params=pr.params,
                )
            )

        return out
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid state value: {state}",
        )
    except Exception as e:
        logger.error(f"Failed to list runs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list runs: {str(e)}",
        )


@router.get(
    "/{run_id}",
    response_model=RunResponse,
    summary="Get pipeline run details",
    response_description="Pipeline run details",
)
def get_run(
    run_id: str,
    user: dict = Depends(get_current_user),
    runner=Depends(get_runner),
):
    """
    Get detailed information about a specific pipeline run.
    """
    try:
        with track_db_operation("read", "pipeline_run"):
            pr = runner.get_run(run_id)
        if not pr:
            raise HTTPException(status_code=404, detail="Run not found")

        with track_db_operation("read", "task_run"):
            tasks = runner.list_task_runs(run_id)

        return RunResponse(
            id=pr.id,
            pipeline_id=pr.pipeline_id,
            state=pr.state.value,
            created_at=pr.created_at,
            started_at=pr.started_at,
            finished_at=pr.finished_at,
            params=pr.params,
            tasks=[
                TaskRunResponse(
                    id=t.id,
                    task_id=t.task_id,
                    state=t.state.value,
                    try_number=t.try_number,
                    started_at=t.started_at,
                    finished_at=t.finished_at,
                    error=t.error,
                )
                for t in tasks
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get run: {str(e)}",
        )


@router.get(
    "/{run_id}/tasks",
    response_model=List[TaskRunResponse],
    summary="Get task runs for a pipeline run",
    response_description="List of task runs",
)
def get_run_tasks(
    run_id: str,
    user: dict = Depends(get_current_user),
    runner=Depends(get_runner),
):
    """
    Get the task runs for a specific pipeline run.
    """
    try:
        with track_db_operation("read", "task_run"):
            tasks = runner.list_task_runs(run_id)

        return [
            TaskRunResponse(
                id=t.id,
                task_id=t.task_id,
                state=t.state.value,
                try_number=t.try_number,
                started_at=t.started_at,
                finished_at=t.finished_at,
                error=t.error,
            )
            for t in tasks
        ]
    except Exception as e:
        logger.error(f"Failed to get tasks for run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get tasks: {str(e)}",
        )


@router.post("/")
async def create_run(req: Request, body: RunCreate):
    """
    Crear y lanzar una ejecución usando pipeline_service.
    """
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")

    run_id = await pipeline_service.run_pipeline(body.pipeline_id, body.params)
    return {"run_id": run_id}


@router.post("/{run_id}/cancel")
async def cancel_run(req: Request, run_id: str):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")

    cancelled = await pipeline_service.cancel_run(run_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Run not found or cannot cancel")
    return {"cancelled": True}
