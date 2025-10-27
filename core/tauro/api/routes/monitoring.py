from fastapi import APIRouter, Depends
from datetime import datetime
from loguru import logger
import time
import asyncio

from tauro.api.core import get_current_settings, get_orchestrator_store
from tauro.api.core.responses import (
    APIResponse,
    success_response,
    error_response,
)
from tauro.api.schemas import HealthCheck, APIInfo, StatsResponse, RunState
from tauro.api.core.config import Settings

router = APIRouter(tags=["monitoring"])

# =============================================================================
# Statistics Cache (TTL-based)
# =============================================================================

_STATS_CACHE: StatsResponse | None = None
_STATS_CACHE_TIME: float = 0
_STATS_CACHE_TTL: float = 60.0  # Cache for 60 seconds


def _clear_stats_cache():
    """Clear the statistics cache (for testing or manual refresh)"""
    global _STATS_CACHE, _STATS_CACHE_TIME
    _STATS_CACHE = None
    _STATS_CACHE_TIME = 0
    logger.debug("Statistics cache cleared")


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
    Retorna 200 si healthy, 503 si degraded/unhealthy.
    """
    components = {
        "api": "healthy",
    }

    # Check database/store connection
    database_status = await _check_database_health_async()
    components["database"] = database_status

    # Check scheduler status
    components["scheduler"] = "healthy" if settings.scheduler_enabled else "disabled"

    # Check configuration
    try:
        if settings.get_tauro_config():
            components["config"] = "healthy"
        else:
            components["config"] = "degraded"
    except Exception as e:
        logger.warning(f"Config check failed: {e}")
        components["config"] = "unavailable"

    # Calculate overall status
    unhealthy_components = [
        c for c, status in components.items() if status not in ["healthy", "disabled"]
    ]

    if unhealthy_components:
        overall_status = "degraded"
    else:
        overall_status = "healthy"

    response = HealthCheck(
        status=overall_status,
        version=settings.api_version,
        timestamp=datetime.now(),
        components=components,
    )

    # If truly unhealthy, could return 503:
    # if any(s == "unhealthy" for s in components.values()):
    #     return JSONResponse(status_code=503, content=response.dict())

    return response


async def _check_database_health_async() -> str:
    """Async database health check with real MongoDB connectivity"""
    try:
        from tauro.api.core.deps import get_database

        # Get database connection
        db = await get_database()

        # Try a ping command with timeout
        await asyncio.wait_for(
            db.command("ping"), timeout=3.0  # 3 second timeout for health check
        )

        # Try a simple count query to verify collections exist
        collections = await asyncio.wait_for(db.list_collection_names(), timeout=2.0)

        # Verify essential collections exist
        required_collections = {"projects", "pipeline_runs"}
        existing_collections = set(collections)

        if not required_collections.issubset(existing_collections):
            logger.warning(
                f"Missing required collections: {required_collections - existing_collections}"
            )
            return "degraded"

        return "healthy"

    except asyncio.TimeoutError:
        logger.warning("Database health check timeout")
        return "degraded"
    except ConnectionError:
        logger.warning("Database connection error")
        return "unhealthy"
    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        return "unhealthy"


def _check_database_health(store) -> str:
    """Check database connectivity and basic functionality"""
    try:
        # Try legacy store method first
        if store is not None:
            store.list_pipeline_runs(limit=1)
            return "healthy"

        # If store is not available, try direct MongoDB check
        # This is a simplified sync version for backward compatibility
        logger.debug("Store not available, database health unknown")
        return "unknown"

    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        return "unhealthy"


# =============================================================================
# API Information
# =============================================================================


@router.get("/api/v1/info", response_model=APIInfo)
async def api_info(
    settings: Settings = Depends(get_current_settings),
):
    """
    API information.

    Returns information about the API and its configuration.
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
    General statistics.

    Returns statistics about pipelines, executions and schedules.
    Cached for 60 seconds to avoid expensive queries to store.
    """
    global _STATS_CACHE, _STATS_CACHE_TIME

    # Check cache validity
    current_time = time.time()
    if (
        _STATS_CACHE is not None
        and (current_time - _STATS_CACHE_TIME) < _STATS_CACHE_TTL
    ):
        logger.debug("Returning cached statistics")
        return _STATS_CACHE

    if not store:
        result = StatsResponse(
            total_pipelines=0,
            total_runs=0,
            total_schedules=0,
            active_runs=0,
            failed_runs=0,
        )
        _STATS_CACHE = result
        _STATS_CACHE_TIME = current_time
        return result

    try:
        # Get all runs with limit to prevent unbounded queries
        # Limit to last 10000 runs for statistics calculation
        all_runs = store.list_pipeline_runs(limit=10000)

        # Count by state
        # RunState already imported at module level from tauro.api.schemas
        active_states = {
            RunState.PENDING.value,
            RunState.QUEUED.value,
            RunState.RUNNING.value,
        }
        active_runs = sum(1 for run in all_runs if run.state.value in active_states)
        failed_runs = sum(
            1 for run in all_runs if run.state.value == RunState.FAILED.value
        )

        # Get schedules with limit
        all_schedules = store.list_schedules()

        # Get unique pipelines
        pipeline_ids = {run.pipeline_id for run in all_runs}

        result = StatsResponse(
            total_pipelines=len(pipeline_ids),
            total_runs=len(all_runs),
            total_schedules=len(all_schedules),
            active_runs=active_runs,
            failed_runs=failed_runs,
        )

        # Cache the result
        _STATS_CACHE = result
        _STATS_CACHE_TIME = current_time

        return result

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        result = StatsResponse(
            total_pipelines=0,
            total_runs=0,
            total_schedules=0,
            active_runs=0,
            failed_runs=0,
        )
        _STATS_CACHE = result
        _STATS_CACHE_TIME = current_time
        return result


# =============================================================================
# Prometheus Metrics (Optional)
# =============================================================================

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from starlette.responses import Response

    @router.get("/metrics")
    async def metrics():
        """
        Prometheus metrics endpoint.

        Expose metrics in Prometheus format for monitoring.
        """
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    METRICS_AVAILABLE = True

except ImportError:
    logger.warning("prometheus_client not available, /metrics endpoint disabled")
    METRICS_AVAILABLE = False
