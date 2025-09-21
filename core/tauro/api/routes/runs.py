from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from tauro.api.auth import get_current_user
from tauro.api.deps import get_runner, get_context
from tauro.api.models import (
    RunCreateRequest,
    RunStartRequest,
    RunResponse,
    TaskRunResponse,
)
from tauro.orchest.models import RunState

router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", response_model=RunResponse, status_code=201)
def create_run(
    payload: RunCreateRequest,
    user: str = Depends(get_current_user),
    runner=Depends(get_runner),
) -> RunResponse:
    pr = runner.create_run(payload.pipeline_id, params=payload.params or {})
    return RunResponse(
        id=pr.id,
        pipeline_id=pr.pipeline_id,
        state=pr.state.value,
        created_at=pr.created_at,
        started_at=pr.started_at,
        finished_at=pr.finished_at,
        params=pr.params,
    )


@router.post("/{run_id}/start", response_model=RunResponse)
def start_run(
    run_id: str,
    payload: RunStartRequest,
    user: str = Depends(get_current_user),
    runner=Depends(get_runner),
):
    # Permite cambiar env por request (opcional)
    if payload.env:
        # Si deseas soportar context-switch por request, puedes crear runner con ese env aquí
        pass

    state = runner.start_run(
        run_id,
        retries=payload.retries,
        retry_delay_sec=payload.retry_delay_sec,
        concurrency=payload.concurrency,
    )
    pr = runner.get_run(run_id)
    if not pr:
        raise HTTPException(status_code=404, detail="Run not found")
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


@router.get("", response_model=List[RunResponse])
def list_runs(
    pipeline_id: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    user: str = Depends(get_current_user),
    runner=Depends(get_runner),
):
    enum_state = RunState(state) if state else None
    runs = runner.list_runs(pipeline_id=pipeline_id, state=enum_state, limit=limit)
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


@router.get("/{run_id}", response_model=RunResponse)
def get_run(
    run_id: str,
    user: str = Depends(get_current_user),
    runner=Depends(get_runner),
):
    pr = runner.get_run(run_id)
    if not pr:
        raise HTTPException(status_code=404, detail="Run not found")
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
