from __future__ import annotations
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query

from tauro.api.auth import get_current_user
from tauro.api.deps import get_store
from tauro.api.models import ScheduleCreateRequest, ScheduleResponse
from tauro.orchest.models import ScheduleKind

router = APIRouter(prefix="/schedules", tags=["schedules"])


@router.post("", response_model=ScheduleResponse, status_code=201)
def create_schedule(
    payload: ScheduleCreateRequest,
    user: str = Depends(get_current_user),
    store=Depends(get_store),
) -> ScheduleResponse:
    try:
        kind = ScheduleKind(payload.kind)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid schedule kind")
    sched = store.create_schedule(
        pipeline_id=payload.pipeline_id,
        kind=kind,
        expression=payload.expression,
        max_concurrency=payload.max_concurrency,
        retry_policy={"retries": payload.retries, "delay": payload.retry_delay_sec},
        timeout_seconds=payload.timeout_seconds,
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


@router.get("", response_model=List[ScheduleResponse])
def list_schedules(
    pipeline_id: Optional[str] = Query(default=None),
    user: str = Depends(get_current_user),
    store=Depends(get_store),
):
    scheds = store.list_schedules(pipeline_id=pipeline_id)
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
