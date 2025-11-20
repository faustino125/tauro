"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
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
from tauro.api.schemas.models import RunCreate
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
# CREATE - Create new run
# =============================================================================


@router.post(
    "",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new run",
    description="Create a new pipeline run in PENDING state",
)
async def create_run(
    request: RunCreate,
    run_service=Depends(get_run_service),
):
    """
    Create a new pipeline run.
    """
    try:
        logger.info(
            f"Creating run for project={request.project_id}, pipeline={request.pipeline_id}"
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
# READ - Get a specific run
# =============================================================================


@router.get(
    "/{run_id}",
    response_model=APIResponse,
    summary="Get run status",
    description="Get current status and details of a pipeline run",
)
async def get_run(
    run_id: str,
    run_service=Depends(get_run_service),
):
    """
    Get current status of a run.
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
# LIST - List runs with filters
# =============================================================================


@router.get(
    "",
    response_model=ListResponse,
    summary="List runs",
    description="List pipeline runs with optional filters",
)
async def list_runs(
    project_id: Optional[str] = Query(None, description="Filter by project"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline"),
    state: Optional[str] = Query(None, description="Filter by state"),
    skip: int = Query(0, ge=0, description="Number of runs to skip"),
    limit: int = Query(50, ge=1, le=100, description="Maximum runs to return"),
    run_service=Depends(get_run_service),
):
    """
    List pipeline runs with filters.
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
        return error_response(
            code="INTERNAL_ERROR",
            message="Error listing runs",
            details={"error": str(e)},
        )


# =============================================================================
# START - Start/resume a run
# =============================================================================


@router.post(
    "/{run_id}/start",
    response_model=APIResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start execution of a run",
    description="Start async execution of a pipeline run",
)
async def start_run(
    run_id: str,
    timeout_seconds: Optional[int] = Query(None, ge=1, description="Timeout in seconds"),
    run_service=Depends(get_run_service),
):
    """
    Start execution of a run.
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
# CANCEL - Cancel a run
# =============================================================================


@router.post(
    "/{run_id}/cancel",
    response_model=APIResponse,
    summary="Cancel execution of a run",
    description="Cancel the execution of a pipeline run",
)
async def cancel_run(
    run_id: str,
    reason: Optional[str] = Query(None, description="Reason for cancellation"),
    run_service=Depends(get_run_service),
):
    """
    Cancel execution of a run.
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
# TASKS - Get tasks for a run
# =============================================================================


@router.get(
    "/{run_id}/tasks",
    response_model=ListResponse,
    summary="Get tasks for a run",
    description="Get list of tasks executed in a run",
)
async def get_run_tasks(
    run_id: str,
    skip: int = Query(0, ge=0, description="Number of tasks to skip"),
    limit: int = Query(50, ge=1, le=100, description="Maximum tasks to return"),
    run_service=Depends(get_run_service),
):
    """
    Get tasks for a run.
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
# LOGS - Get logs for a run
# =============================================================================


@router.get(
    "/{run_id}/logs",
    response_model=ListResponse,
    summary="Get logs for a run",
    description="Get execution logs for a run",
)
async def get_run_logs(
    run_id: str,
    skip: int = Query(0, ge=0, description="Number of logs to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum logs to return"),
    level: Optional[str] = Query(None, description="Filter by level (DEBUG, INFO, WARNING, ERROR)"),
    run_service=Depends(get_run_service),
):
    """
    Get logs for a run.

    Returns run execution logs. Can filter by log level
    and paginate the result.
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
