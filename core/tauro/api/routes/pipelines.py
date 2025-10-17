from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import List, Optional
from loguru import logger

from tauro.api.core import (
    get_config_manager,
    get_orchestrator_runner,
    get_orchestrator_store,
)
from tauro.api.schemas import (
    PipelineInfo,
    PipelineListResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    RunListResponse,
    RunCancelRequest,
    MessageResponse,
    RunState,
)

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


# =============================================================================
# Pipeline Management
# =============================================================================


@router.get("", response_model=PipelineListResponse)
async def list_pipelines(
    config_manager=Depends(get_config_manager),
):
    """
    Listar todos los pipelines disponibles.

    Retorna la lista de pipelines configurados en el sistema.
    """
    if not config_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ConfigManager not available",
        )

    try:
        # Obtener configuración de pipelines
        pipelines_config = config_manager.load_config("pipeline")

        pipelines = []
        for pipeline_id, pipeline_config in pipelines_config.items():
            pipelines.append(
                PipelineInfo(
                    id=pipeline_id,
                    name=pipeline_config.get("name", pipeline_id),
                    description=pipeline_config.get("description"),
                    type=pipeline_config.get("type", "batch"),
                    nodes=pipeline_config.get("nodes", []),
                )
            )

        return PipelineListResponse(pipelines=pipelines, total=len(pipelines))

    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{pipeline_id}", response_model=PipelineInfo)
async def get_pipeline(
    pipeline_id: str,
    config_manager=Depends(get_config_manager),
):
    """Obtener información de un pipeline específico"""
    if not config_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ConfigManager not available",
        )

    try:
        pipelines_config = config_manager.load_config("pipeline")

        if pipeline_id not in pipelines_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_id}' not found",
            )

        pipeline_config = pipelines_config[pipeline_id]

        return PipelineInfo(
            id=pipeline_id,
            name=pipeline_config.get("name", pipeline_id),
            description=pipeline_config.get("description"),
            type=pipeline_config.get("type", "batch"),
            nodes=pipeline_config.get("nodes", []),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline {pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# =============================================================================
# Pipeline Execution
# =============================================================================


@router.post(
    "/{pipeline_id}/runs",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def run_pipeline(
    pipeline_id: str,
    request: PipelineRunRequest,
    runner=Depends(get_orchestrator_runner),
):
    """
    Ejecutar un pipeline.

    Crea una nueva ejecución del pipeline especificado con los parámetros dados.
    """
    if not runner:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorRunner not available",
        )

    try:
        # Crear y ejecutar run
        run = runner.create_run(
            pipeline_id=pipeline_id,
            params=request.params,
        )

        # Ejecutar en background
        runner.run_async(run.run_id)

        return PipelineRunResponse(
            run_id=run.run_id,
            pipeline_id=run.pipeline_id,
            state=RunState(run.state.value),
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            params=run.params,
            error=run.error,
            tags=request.tags,
        )

    except Exception as e:
        logger.error(f"Error running pipeline {pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# =============================================================================
# Run Management
# =============================================================================


@router.get("/{pipeline_id}/runs", response_model=RunListResponse)
async def list_runs(
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    state: Optional[RunState] = None,
    store=Depends(get_orchestrator_store),
):
    """
    Listar ejecuciones de un pipeline.

    Retorna el historial de ejecuciones del pipeline especificado.
    """
    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorStore not available",
        )

    try:
        # Obtener runs del store
        filters = {"pipeline_id": pipeline_id}
        if state:
            filters["state"] = state.value

        all_runs = store.list_pipeline_runs(**filters)

        # Pagination
        total = len(all_runs)
        runs = all_runs[offset : offset + limit]

        return RunListResponse(
            runs=[
                PipelineRunResponse(
                    run_id=run.run_id,
                    pipeline_id=run.pipeline_id,
                    state=RunState(run.state.value),
                    created_at=run.created_at,
                    started_at=run.started_at,
                    finished_at=run.finished_at,
                    params=run.params,
                    error=run.error,
                )
                for run in runs
            ],
            total=total,
        )

    except Exception as e:
        logger.error(f"Error listing runs for pipeline {pipeline_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/runs/{run_id}", response_model=PipelineRunResponse)
async def get_run(
    run_id: str,
    store=Depends(get_orchestrator_store),
):
    """Obtener estado de una ejecución específica"""
    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorStore not available",
        )

    try:
        run = store.get_pipeline_run(run_id)

        if not run:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run '{run_id}' not found",
            )

        return PipelineRunResponse(
            run_id=run.run_id,
            pipeline_id=run.pipeline_id,
            state=RunState(run.state.value),
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            params=run.params,
            error=run.error,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/runs/{run_id}/cancel", response_model=MessageResponse)
async def cancel_run(
    run_id: str,
    request: RunCancelRequest = RunCancelRequest(),
    runner=Depends(get_orchestrator_runner),
):
    """
    Cancelar una ejecución en progreso.

    Intenta cancelar la ejecución especificada si está en estado PENDING o RUNNING.
    """
    if not runner:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OrchestratorRunner not available",
        )

    try:
        success = await runner.cancel_run(run_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Run cannot be cancelled (may be already finished)",
            )

        return MessageResponse(
            message=f"Run {run_id} cancelled successfully", detail=request.reason
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
