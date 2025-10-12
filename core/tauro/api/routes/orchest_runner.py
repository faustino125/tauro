from __future__ import annotations
from typing import Any, Dict
import asyncio

from fastapi import APIRouter, Request, HTTPException
from starlette.concurrency import run_in_threadpool

router = APIRouter(prefix="/orchest/runs", tags=["orchest"])


SERVICE_NOT_INITIALIZED = "Service not initialized"


class RunCreate(Dict):
    pipeline_id: str
    params: Dict[str, Any] = {}


@router.post("/")
async def create_run(req: Request, body: RunCreate):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    maybe = pipeline_service.run_pipeline(
        body.get("pipeline_id"), body.get("params", {})
    )
    if asyncio.iscoroutine(maybe):
        run_id = await maybe
    else:
        run_id = await run_in_threadpool(
            pipeline_service.run_pipeline,
            body.get("pipeline_id"),
            body.get("params", {}),
        )
    return {"run_id": run_id}


@router.get("/{run_id}")
async def get_run(req: Request, run_id: str):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    res = pipeline_service.get_run(run_id)
    if asyncio.iscoroutine(res):
        res = await res
    if not res:
        raise HTTPException(status_code=404, detail="Run not found")
    return res


@router.get("/")
async def list_runs(req: Request):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    res = pipeline_service.list_runs()
    if asyncio.iscoroutine(res):
        res = await res
    return res


@router.post("/{run_id}/cancel")
async def cancel_run(req: Request, run_id: str):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    maybe = pipeline_service.cancel_run(run_id)
    if asyncio.iscoroutine(maybe):
        cancelled = await maybe
    else:
        cancelled = await run_in_threadpool(pipeline_service.cancel_run, run_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Run not found or cannot cancel")
    return {"cancelled": True}
