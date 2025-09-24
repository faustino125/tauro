from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger

from tauro.api.auth import get_admin_user
from tauro.api.deps import get_scheduler, get_store
from tauro.api.models import BackfillRequest, ErrorResponse
from tauro.orchest.models import RunState

router = APIRouter(
    prefix="/control",
    tags=["control"],
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)


@router.post(
    "/backfill",
    summary="Create multiple runs for backfill",
    response_description="Backfill operation enqueued",
)
def backfill(
    payload: BackfillRequest,
    user: dict = Depends(get_admin_user),
    scheduler=Depends(get_scheduler),
):
    """
    Create multiple runs for a pipeline (backfill).

    Requires admin privileges.
    """
    try:
        scheduler.backfill(payload.pipeline_id, payload.count)
        return {
            "status": "enqueued",
            "pipeline_id": payload.pipeline_id,
            "count": payload.count,
        }
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backfill operation failed: {str(e)}",
        )


@router.post(
    "/scheduler/start",
    summary="Start the scheduler",
    response_description="Scheduler started",
)
def scheduler_start(
    user: dict = Depends(get_admin_user),
    scheduler=Depends(get_scheduler),
):
    """
    Start the scheduler manually.

    Requires admin privileges.
    """
    try:
        scheduler.start()
        return {"status": "started"}
    except Exception as e:
        logger.error(f"Scheduler start failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Scheduler start failed: {str(e)}",
        )


@router.post(
    "/scheduler/stop",
    summary="Stop the scheduler",
    response_description="Scheduler stopped",
)
def scheduler_stop(
    user: dict = Depends(get_admin_user),
    scheduler=Depends(get_scheduler),
):
    """
    Stop the scheduler manually.

    Requires admin privileges.
    """
    try:
        scheduler.stop()
        return {"status": "stopped"}
    except Exception as e:
        logger.error(f"Scheduler stop failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Scheduler stop failed: {str(e)}",
        )


@router.get(
    "/scheduler/status",
    summary="Get scheduler status",
    response_description="Scheduler status information",
)
def scheduler_status(
    user: dict = Depends(get_admin_user),
    scheduler=Depends(get_scheduler),
):
    """
    Get the current status of the scheduler.

    Requires admin privileges.
    """
    try:
        # Asumiendo que el scheduler tiene un método para obtener el estado
        if hasattr(scheduler, "get_status"):
            status_info = scheduler.get_status()
        else:
            # Implementación por defecto
            status_info = {
                "running": scheduler._thread.is_alive()
                if hasattr(scheduler, "_thread")
                else False,
                "thread_name": getattr(scheduler._thread, "name", "Unknown")
                if hasattr(scheduler, "_thread")
                else "Unknown",
            }
        return status_info
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get scheduler status: {str(e)}",
        )


@router.post(
    "/runs/{run_id}/cancel",
    summary="Cancel a running pipeline",
    response_description="Pipeline cancellation requested",
)
def cancel_run(
    run_id: str,
    user: dict = Depends(get_admin_user),
    store=Depends(get_store),
):
    """
    Cancel a running pipeline execution.

    Requires admin privileges.
    """
    try:
        # Obtener la ejecución
        run = store.get_pipeline_run(run_id)
        if not run:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Run {run_id} not found"
            )

        # Verificar que no esté en estado terminal
        if run.state in [RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Run {run_id} is already in terminal state {run.state}",
            )

        # Actualizar estado a CANCELLED
        store.update_pipeline_run_state(run_id, RunState.CANCELLED)

        logger.info(f"Run {run_id} cancelled by user {user['id']}")
        return {"status": "cancelled", "run_id": run_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel run: {str(e)}",
        )
