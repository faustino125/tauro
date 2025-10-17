from fastapi import APIRouter, Depends
from datetime import datetime
from loguru import logger

from tauro.api.core import get_current_settings, get_orchestrator_store
from tauro.api.schemas import HealthCheck, APIInfo, StatsResponse, RunState
from tauro.api.core.config import Settings

router = APIRouter(tags=["monitoring"])


# =============================================================================
# Health Check
# =============================================================================


@router.get("/health", response_model=HealthCheck)
async def health_check(
    settings: Settings = Depends(get_current_settings),
    store=Depends(get_orchestrator_store),
):
    """
    Health check endpoint.

    Verifica el estado de salud de la API y sus componentes.
    """
    components = {
        "api": "healthy",
    }

    # Check database/store
    try:
        if store:
            # Try a simple query
            store.list_pipeline_runs(limit=1)
            components["database"] = "healthy"
        else:
            components["database"] = "unavailable"
    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        components["database"] = "unhealthy"

    # Check scheduler
    components["scheduler"] = "healthy" if settings.scheduler_enabled else "disabled"

    # Overall status
    status = (
        "healthy"
        if all(c in ["healthy", "disabled"] for c in components.values())
        else "degraded"
    )

    return HealthCheck(
        status=status,
        version=settings.api_version,
        timestamp=datetime.now(),
        components=components,
    )


# =============================================================================
# API Information
# =============================================================================


@router.get("/api/v1/info", response_model=APIInfo)
async def api_info(
    settings: Settings = Depends(get_current_settings),
):
    """
    Información de la API.

    Retorna información sobre la API y su configuración.
    """
    return APIInfo(
        name=settings.api_title,
        version=settings.api_version,
        description=settings.api_description,
        environment=settings.environment,
        tauro_config=settings.get_tauro_config(),
    )


# =============================================================================
# Statistics
# =============================================================================


@router.get("/api/v1/stats", response_model=StatsResponse)
async def get_stats(
    store=Depends(get_orchestrator_store),
):
    """
    Estadísticas generales.

    Retorna estadísticas sobre pipelines, ejecuciones y schedules.
    """
    if not store:
        return StatsResponse(
            total_pipelines=0,
            total_runs=0,
            total_schedules=0,
            active_runs=0,
            failed_runs=0,
        )

    try:
        # Get all runs
        all_runs = store.list_pipeline_runs()
        # Count by state
        # RunState already imported at module level from tauro.api.schemas
        active_runs = sum(
            1
            for run in all_runs
            if run.state.value in [RunState.PENDING.value, RunState.RUNNING.value]
        )
        failed_runs = sum(
            1 for run in all_runs if run.state.value == RunState.FAILED.value
        )

        # Get schedules
        all_schedules = store.list_schedules()

        # Get unique pipelines
        pipeline_ids = {run.pipeline_id for run in all_runs}

        return StatsResponse(
            total_pipelines=len(pipeline_ids),
            total_runs=len(all_runs),
            total_schedules=len(all_schedules),
            active_runs=active_runs,
            failed_runs=failed_runs,
        )

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return StatsResponse(
            total_pipelines=0,
            total_runs=0,
            total_schedules=0,
            active_runs=0,
            failed_runs=0,
        )


# =============================================================================
# Prometheus Metrics (Optional)
# =============================================================================

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from starlette.responses import Response

    @router.get("/metrics")
    async def metrics():
        """
        Endpoint de métricas Prometheus.

        Expone métricas en formato Prometheus para monitoring.
        """
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    METRICS_AVAILABLE = True

except ImportError:
    logger.warning("prometheus_client not available, /metrics endpoint disabled")
    METRICS_AVAILABLE = False
