from __future__ import annotations
from typing import List, Dict, Optional
from __future__ import annotations

from typing import List, Dict, Optional, Any
import asyncio

from fastapi import APIRouter, Request, HTTPException, Body, Depends, Query, status
from loguru import logger

from tauro.api.auth import get_current_user
from tauro.api.deps import get_context
from tauro.config.contexts import Context
from tauro.exec.executor import PipelineExecutor

router = APIRouter()


class PipelineCreate(Dict):
    """Lightweight placeholder for pipeline creation payload.

    We keep this simple to avoid increasing runtime dependencies in this
    refactor. Callers already validate payloads at higher levels.
    """


@router.post("/", status_code=201)
async def create_pipeline(req: Request, body: PipelineCreate):
    """Create pipeline. Prefer pipeline_service.create_pipeline if available, else store."""
    ps = getattr(req.app.state, "pipeline_service", None)
    store = getattr(req.app.state, "store", None)
    if ps and hasattr(ps, "create_pipeline"):
        res = ps.create_pipeline(body.get("id"), body.get("spec"))
        if asyncio.iscoroutine(res):
            res = await res
        return res
    if store and hasattr(store, "create_pipeline"):
        return store.create_pipeline(body.get("id"), body.get("spec"))
    raise HTTPException(status_code=500, detail="No backend to create pipeline")


@router.get("", response_model=List[str])
def list_pipelines(
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
    search: Optional[str] = Query(None, description="Filter pipelines by name"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of pipelines to return"
    ),
):
    """Get a list of all available pipelines."""
    try:
        executor = PipelineExecutor(context, None)
        pipelines = sorted(executor.list_pipelines())

        if search:
            pipelines = [p for p in pipelines if search.lower() in p.lower()]

        if limit and len(pipelines) > limit:
            pipelines = pipelines[:limit]

        return pipelines
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list pipelines: {str(e)}",
        )


@router.get("/{pipeline_id}")
def get_pipeline_info(
    pipeline_id: str,
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    """Get detailed information about a specific pipeline."""
    try:
        executor = PipelineExecutor(context, None)
        info = executor.get_pipeline_info(pipeline_id)

        if not info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_id}' not found",
            )

        return info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline info for {pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pipeline info: {str(e)}",
        )


@router.get("/{pipeline_id}/nodes", response_model=List[str])
def get_pipeline_nodes(
    pipeline_id: str,
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    """Get the list of nodes in a pipeline."""
    try:
        executor = PipelineExecutor(context, None)
        info = executor.get_pipeline_info(pipeline_id)

        if not info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_id}' not found",
            )

        nodes = info.get("nodes", [])
        if not nodes and "pipeline" in info:
            nodes = info["pipeline"].get("nodes", [])

        return nodes
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline nodes for {pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pipeline nodes: {str(e)}",
        )


@router.post("/{pipeline_id}/run")
async def run_pipeline(
    req: Request, pipeline_id: str, params: Dict[str, Any] = Body(default_factory=dict)
):
    """Launch pipeline execution using pipeline_service.run_pipeline."""
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")

    run_id = await pipeline_service.run_pipeline(pipeline_id, params or {})
    return {"run_id": run_id}
