"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from fastapi import APIRouter, HTTPException, Depends, status, Query
from typing import Any, Dict, List, Optional
from loguru import logger

from tauro.api.core import (
    get_config_manager,
    get_orchestrator_store,
    get_current_settings,
    get_config_service,
    get_db_context_initializer,
    validate_identifier,
)
from tauro.api.core.pagination import paginate_list
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
from tauro.core.config.exceptions import ActiveConfigNotFound, ConfigRepositoryError

try:  # pragma: no cover - optional in certain deployments
    from tauro.cli.execution import ContextInitializer
except ImportError:
    ContextInitializer = None  # type: ignore

try:  # pragma: no cover - optional en ciertas instalaciones
    from tauro.api.orchest import OrchestratorRunner
except ImportError:
    OrchestratorRunner = None  # type: ignore

ERROR_CONFIG_MANAGER_UNAVAILABLE = "ConfigManager not available"
ERROR_CONFIG_SERVICE_UNAVAILABLE = "ConfigService not available"
ERROR_RUNNER_UNAVAILABLE = "OrchestratorRunner not available"
ERROR_STORE_UNAVAILABLE = "OrchestratorStore not available"
ENVIRONMENT_QUERY_DESCRIPTION = "Environment name"

router = APIRouter(prefix="/projects/{project_id}/pipelines", tags=["pipelines"])


def _pipeline_run_to_response(
    run,
    *,
    tags: Optional[Dict[str, str]] = None,
) -> PipelineRunResponse:
    """Build API response from orchestrator PipelineRun instance."""
    return PipelineRunResponse(
        run_id=run.id,
        pipeline_id=run.pipeline_id,
        state=RunState(run.state.value),
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        params=run.params,
        error=run.error,
        tags=tags,
    )


def _resolve_environment(settings, environment: Optional[str]) -> str:
    """Return provided environment or fallback to settings."""
    return environment or settings.environment


def _load_pipelines_from_service(
    project_id: str,
    environment: str,
    *,
    config_service,
):
    try:
        bundle = config_service.get_active_context(project_id, environment)
        return bundle.pipelines_config
    except ActiveConfigNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        )
    except ConfigRepositoryError as exc:
        logger.error(f"Config repository error loading pipelines: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to load pipelines from configuration repository",
        )


def _load_pipelines_from_files(config_manager) -> Dict[str, Dict[str, Any]]:
    if not config_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_CONFIG_MANAGER_UNAVAILABLE,
        )

    try:
        pipelines_config = config_manager.load_config("pipeline")
        if not isinstance(pipelines_config, dict):
            raise ValueError("Pipeline configuration must be a dictionary")
        return pipelines_config
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error loading pipelines from files: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to load pipeline configuration",
        )


def _build_execution_context(
    project_id: str,
    environment: str,
    *,
    settings,
    db_initializer,
    config_manager,
):
    if settings.config_source == "mongo":
        if db_initializer is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database-backed context initializer not available",
            )
        try:
            return db_initializer.initialize(project_id, environment)
        except ActiveConfigNotFound as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
        except ConfigRepositoryError as exc:
            logger.error(f"Config repository error creating context: {exc}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to build execution context from database",
            )
    else:
        if ContextInitializer is None or config_manager is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ContextInitializer not available",
            )
        try:
            initializer = ContextInitializer(config_manager)
            return initializer.initialize(environment)
        except Exception as exc:
            logger.error(f"Error building context from file configuration: {exc}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to build execution context",
            )


# =============================================================================
# Pipeline Management
# =============================================================================


@router.get("", response_model=PipelineListResponse)
async def list_pipelines(
    project_id: str,
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    settings=Depends(get_current_settings),
    config_service=Depends(get_config_service),
    config_manager=Depends(get_config_manager),
):
    """List available pipelines for the given project/environment."""

    project_id = validate_identifier(project_id, "project_id")
    env = _resolve_environment(settings, environment)

    if settings.config_source == "mongo":
        if not config_service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ERROR_CONFIG_SERVICE_UNAVAILABLE,
            )
        pipelines_config = _load_pipelines_from_service(
            project_id,
            env,
            config_service=config_service,
        )
    else:
        pipelines_config = _load_pipelines_from_files(config_manager)

    pipelines: List[PipelineInfo] = []
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


@router.get("/{pipeline_id}", response_model=PipelineInfo)
async def get_pipeline(
    project_id: str,
    pipeline_id: str,
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    settings=Depends(get_current_settings),
    config_service=Depends(get_config_service),
    config_manager=Depends(get_config_manager),
):
    """Return details for a specific pipeline."""

    project_id = validate_identifier(project_id, "project_id")
    pipeline_id = validate_identifier(pipeline_id, "pipeline_id")
    env = _resolve_environment(settings, environment)

    if settings.config_source == "mongo":
        if not config_service:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ERROR_CONFIG_SERVICE_UNAVAILABLE,
            )
        pipelines_config = _load_pipelines_from_service(
            project_id,
            env,
            config_service=config_service,
        )
    else:
        pipelines_config = _load_pipelines_from_files(config_manager)

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


# =============================================================================
# Pipeline Execution
# =============================================================================


@router.post(
    "/{pipeline_id}/runs",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def run_pipeline(
    project_id: str,
    pipeline_id: str,
    request: PipelineRunRequest,
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    settings=Depends(get_current_settings),
    db_initializer=Depends(get_db_context_initializer),
    config_manager=Depends(get_config_manager),
    store=Depends(get_orchestrator_store),
):
    """
    Execute a pipeline.

    Create a new execution of the specified pipeline with the given parameters.
    """
    project_id = validate_identifier(project_id, "project_id")
    pipeline_id = validate_identifier(pipeline_id, "pipeline_id")
    env = _resolve_environment(settings, environment)

    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_STORE_UNAVAILABLE,
        )

    if OrchestratorRunner is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_RUNNER_UNAVAILABLE,
        )

    context = _build_execution_context(
        project_id,
        env,
        settings=settings,
        db_initializer=db_initializer,
        config_manager=config_manager,
    )

    try:
        runner = OrchestratorRunner(
            context=context,
            store=store,
            max_workers=settings.scheduler_max_workers,
        )

        run_id = runner.run_pipeline(
            pipeline_id=pipeline_id,
            params=request.params or {},
            timeout_seconds=request.timeout,
        )

        run = store.get_pipeline_run(run_id) or runner.get_run(run_id)

        if not run:
            raise RuntimeError("Run created but not found in store")

        return _pipeline_run_to_response(run, tags=request.tags)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error running pipeline {pipeline_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )


# =============================================================================
# Run Management
# =============================================================================


@router.get("/{pipeline_id}/runs", response_model=RunListResponse)
async def list_runs(
    project_id: str,
    pipeline_id: str,
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    state: Optional[RunState] = None,
    settings=Depends(get_current_settings),
    store=Depends(get_orchestrator_store),
):
    """
    Listar ejecuciones de un pipeline.

    Retorna el historial de ejecuciones del pipeline especificado.
    """
    # Validate path parameters
    project_id = validate_identifier(project_id, "project_id")
    pipeline_id = validate_identifier(pipeline_id, "pipeline_id")
    env = _resolve_environment(settings, environment)

    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_STORE_UNAVAILABLE,
        )

    try:
        # Get all runs for this pipeline with optional state filter
        all_runs = store.list_pipeline_runs(pipeline_id=pipeline_id)

        # Filter by state if provided
        if state:
            all_runs = [run for run in all_runs if run.state == state]

        # Log environment for traceability
        logger.debug(
            "Listing runs",
            extra={"project_id": project_id, "pipeline_id": pipeline_id, "env": env},
        )

        # Apply pagination using centralized utility
        paginated = paginate_list(all_runs, limit=limit, offset=offset)

        return RunListResponse(
            runs=[_pipeline_run_to_response(run) for run in paginated.items],
            total=paginated.total,
        )

    except Exception as e:
        logger.error(f"Error listing runs for pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/runs/{run_id}", response_model=PipelineRunResponse)
async def get_run(
    project_id: str,
    run_id: str,
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    settings=Depends(get_current_settings),
    store=Depends(get_orchestrator_store),
):
    """Obtain the state of a specific pipeline run."""

    project_id = validate_identifier(project_id, "project_id")
    run_id = validate_identifier(run_id, "run_id")
    env = _resolve_environment(settings, environment)

    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_STORE_UNAVAILABLE,
        )

    try:
        run = store.get_pipeline_run(run_id)

        if not run:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run '{run_id}' not found",
            )

        logger.debug(
            "Run lookup",
            extra={"project_id": project_id, "run_id": run_id, "env": env},
        )

        return _pipeline_run_to_response(run)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error getting run {run_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )


@router.post("/runs/{run_id}/cancel", response_model=MessageResponse)
async def cancel_run(
    project_id: str,
    run_id: str,
    request: RunCancelRequest = RunCancelRequest(),
    environment: Optional[str] = Query(default=None, description=ENVIRONMENT_QUERY_DESCRIPTION),
    settings=Depends(get_current_settings),
    db_initializer=Depends(get_db_context_initializer),
    config_manager=Depends(get_config_manager),
    store=Depends(get_orchestrator_store),
):
    """Cancel an in-progress run for the given project/environment."""

    project_id = validate_identifier(project_id, "project_id")
    run_id = validate_identifier(run_id, "run_id")
    env = _resolve_environment(settings, environment)

    if not store:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_STORE_UNAVAILABLE,
        )

    if OrchestratorRunner is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ERROR_RUNNER_UNAVAILABLE,
        )

    context = _build_execution_context(
        project_id,
        env,
        settings=settings,
        db_initializer=db_initializer,
        config_manager=config_manager,
    )

    try:
        runner = OrchestratorRunner(
            context=context,
            store=store,
            max_workers=settings.scheduler_max_workers,
        )

        success = runner.cancel_run(run_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Run cannot be cancelled (may be already finished)",
            )

        logger.info(
            f"Run {run_id} cancelled",
            extra={"project_id": project_id, "run_id": run_id, "env": env},
        )

        return MessageResponse(
            message=f"Run {run_id} cancelled successfully", detail=request.reason
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error cancelling run {run_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
