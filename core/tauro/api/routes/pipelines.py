from __future__ import annotations
from typing import List, Dict, Optional
from fastapi import APIRouter, Request, HTTPException, Body
from pydantic import BaseModel
from typing import Any, Dict, Optional
import asyncio

router = APIRouter()


class PipelineCreate(BaseModel):
    id: str
    spec: Dict[str, Any]


@router.post("/")
async def create_pipeline(req: Request, body: PipelineCreate):
    """
    Crear pipeline. Preferir pipeline_service.create_pipeline si existe,
    si no usar store.
    """
    ps = getattr(req.app.state, "pipeline_service", None)
    store = getattr(req.app.state, "store", None)
    if ps and hasattr(ps, "create_pipeline"):
        res = ps.create_pipeline(body.id, body.spec)
        if asyncio.iscoroutine(res):
            res = await res
        return res
    if store and hasattr(store, "create_pipeline"):
        return store.create_pipeline(body.id, body.spec)
    raise HTTPException(status_code=500, detail="No backend to create pipeline")


@router.get(
    "",
    response_model=List[str],
    summary="List available pipelines",
    response_description="List of pipeline IDs",
)
def list_pipelines(
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
    search: Optional[str] = Query(None, description="Filter pipelines by name"),
    limit: Optional[int] = Query(
        100, ge=1, le=1000, description="Maximum number of pipelines to return"
    ),
):
    """
    Get a list of all available pipelines.
    """
    try:
        executor = PipelineExecutor(context, None)
        pipelines = sorted(executor.list_pipelines())

        # Aplicar filtro de búsqueda si se proporciona
        if search:
            pipelines = [p for p in pipelines if search.lower() in p.lower()]

        # Aplicar límite
        if limit and len(pipelines) > limit:
            pipelines = pipelines[:limit]

        return pipelines
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list pipelines: {str(e)}",
        )


@router.get(
    "/{pipeline_id}",
    response_model=Dict,
    summary="Get pipeline details",
    response_description="Pipeline information",
)
def get_pipeline_info(
    pipeline_id: str,
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    """
    Get detailed information about a specific pipeline.
    """
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


@router.get(
    "/{pipeline_id}/nodes",
    response_model=List[str],
    summary="Get pipeline nodes",
    response_description="List of node IDs in the pipeline",
)
def get_pipeline_nodes(
    pipeline_id: str,
    user: dict = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    """
    Get the list of nodes in a pipeline.
    """
    try:
        executor = PipelineExecutor(context, None)
        info = executor.get_pipeline_info(pipeline_id)

        if not info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_id}' not found",
            )

        # Extraer nodos de la información de la pipeline
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
    req: Request, pipeline_id: str, params: Dict[str, Any] = Body(default={})
):
    """
    Lanzar ejecución de pipeline usando pipeline_service.run_pipeline.
    """
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")

    run_id = await pipeline_service.run_pipeline(pipeline_id, params or {})
    return {"run_id": run_id}
