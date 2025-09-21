from __future__ import annotations
from fastapi import APIRouter, Depends

from tauro.api.auth import get_current_user
from tauro.api.deps import get_scheduler
from tauro.api.models import BackfillRequest

router = APIRouter(prefix="/control", tags=["control"])


@router.post("/backfill")
def backfill(
    payload: BackfillRequest,
    user: str = Depends(get_current_user),
    scheduler=Depends(get_scheduler),
):
    scheduler.backfill(payload.pipeline_id, payload.count)
    return {
        "status": "enqueued",
        "pipeline_id": payload.pipeline_id,
        "count": payload.count,
    }


@router.post("/scheduler/start")
def scheduler_start(
    user: str = Depends(get_current_user),
    scheduler=Depends(get_scheduler),
):
    scheduler.start()
    return {"status": "started"}


@router.post("/scheduler/stop")
def scheduler_stop(
    user: str = Depends(get_current_user),
    scheduler=Depends(get_scheduler),
):
    scheduler.stop()
    return {"status": "stopped"}
