"""
Endpoints de observabilidad para monitoreo del sistema.
Incluye métricas, health checks y estado de resilience patterns.
"""
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from ..deps import get_store, get_runner
from ..utils import log
from tauro.orchest.resilience import get_resilience_manager
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.runner import OrchestratorRunner

router = APIRouter(tags=["observability"])


@router.get("/metrics")
async def get_metrics(
    request: Request,
    store: OrchestratorStore = Depends(get_store),
    runner: OrchestratorRunner = Depends(get_runner),
) -> Dict[str, Any]:
    """
    Obtener métricas completas del sistema.

    Incluye:
    - Métricas de la base de datos
    - Métricas del runner
    - Métricas del scheduler (si está activo)
    - Métricas de los servicios
    - Métricas de resilience patterns
    """
    metrics = {}

    # Métricas de la base de datos
    try:
        metrics["database"] = store.get_database_stats()
    except Exception as e:
        log.exception("Error getting database metrics", error=str(e))
        metrics["database"] = {"error": str(e)}

    # Métricas del runner
    try:
        metrics["runner"] = runner.get_metrics()
    except Exception as e:
        log.exception("Error getting runner metrics", error=str(e))
        metrics["runner"] = {"error": str(e)}

    # Métricas del scheduler si existe
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        try:
            metrics["scheduler"] = scheduler.get_metrics()
        except Exception as e:
            log.exception("Error getting scheduler metrics", error=str(e))
            metrics["scheduler"] = {"error": str(e)}

    # Métricas de los servicios
    pipeline_service = getattr(request.app.state, "pipeline_service", None)
    if pipeline_service:
        try:
            metrics["pipeline_service"] = await pipeline_service.get_stats()
        except Exception as e:
            log.exception("Error getting pipeline service metrics", error=str(e))
            metrics["pipeline_service"] = {"error": str(e)}

    streaming_service = getattr(request.app.state, "streaming_service", None)
    if streaming_service:
        try:
            metrics["streaming_service"] = await streaming_service.get_stats()
        except Exception as e:
            log.exception("Error getting streaming service metrics", error=str(e))
            metrics["streaming_service"] = {"error": str(e)}

    # Métricas de resilience patterns
    try:
        resilience = get_resilience_manager()
        metrics["resilience"] = resilience.get_all_metrics()
    except Exception as e:
        log.exception("Error getting resilience metrics", error=str(e))
        metrics["resilience"] = {"error": str(e)}

    return metrics


def _record_component(
    health_status: Dict[str, Any],
    name: str,
    comp_health: Dict[str, Any],
    collect_warnings: bool = True,
) -> None:
    """Registra el estado de un componente en health_status y actualiza issues/warnings."""
    health_status["components"][name] = comp_health
    if not comp_health.get("healthy", False):
        health_status["overall_healthy"] = False
        health_status["issues"].extend(comp_health.get("issues", []))
    if collect_warnings:
        health_status["warnings"].extend(comp_health.get("warnings", []))


def _handle_sync_component(
    name: str,
    func,
    health_status: Dict[str, Any],
    error_label: str,
    collect_warnings: bool = True,
) -> None:
    """Maneja componentes que exponen un método sync get_health_status()."""
    try:
        comp_health = func()
        _record_component(
            health_status, name, comp_health, collect_warnings=collect_warnings
        )
    except Exception as e:
        health_status["overall_healthy"] = False
        health_status["components"][name] = {"error": str(e)}
        health_status["issues"].append(f"{error_label} health check failed: {str(e)}")


async def _handle_async_component(
    name: str,
    coro,
    health_status: Dict[str, Any],
    error_label: str,
    collect_warnings: bool = True,
) -> None:
    """Maneja componentes que exponen un método async get_health_status()."""
    try:
        comp_health = await coro()
        _record_component(
            health_status, name, comp_health, collect_warnings=collect_warnings
        )
    except Exception as e:
        health_status["overall_healthy"] = False
        health_status["components"][name] = {"error": str(e)}
        health_status["issues"].append(f"{error_label} health check failed: {str(e)}")


@router.get("/health")
async def health_check(
    request: Request,
    store: OrchestratorStore = Depends(get_store),
    runner: OrchestratorRunner = Depends(get_runner),
) -> JSONResponse:
    """
    Health check completo del sistema.

    Retorna HTTP 200 si el sistema está saludable.
    Retorna HTTP 503 si hay problemas graves.
    Retorna HTTP 207 si hay warnings pero el sistema funciona.
    """
    health_status = {
        "overall_healthy": True,
        "components": {},
        "issues": [],
        "warnings": [],
    }

    # Verificar runner (sync)
    _handle_sync_component(
        "runner",
        runner.get_health_status,
        health_status,
        "Runner",
        collect_warnings=False,
    )

    # Verificar scheduler si existe (sync)
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        _handle_sync_component(
            "scheduler",
            scheduler.get_health_status,
            health_status,
            "Scheduler",
            collect_warnings=True,
        )

    # Verificar pipeline service (async)
    pipeline_service = getattr(request.app.state, "pipeline_service", None)
    if pipeline_service:
        await _handle_async_component(
            "pipeline_service",
            pipeline_service.get_health_status,
            health_status,
            "Pipeline service",
            collect_warnings=True,
        )

    # Verificar streaming service (async)
    streaming_service = getattr(request.app.state, "streaming_service", None)
    if streaming_service:
        await _handle_async_component(
            "streaming_service",
            streaming_service.get_health_status,
            health_status,
            "Streaming service",
            collect_warnings=False,
        )

    # Verificar conectividad con la base de datos
    try:
        with store._conn() as conn:
            conn.execute("SELECT 1")
        health_status["components"]["database"] = {"healthy": True}
    except Exception as e:
        health_status["overall_healthy"] = False
        health_status["components"]["database"] = {"healthy": False, "error": str(e)}
        health_status["issues"].append(f"Database connection failed: {str(e)}")

    # Determinar código de estado HTTP
    if health_status["overall_healthy"]:
        if health_status["warnings"]:
            # Sistema funcional pero con warnings
            status_code = status.HTTP_207_MULTI_STATUS
        else:
            # Sistema completamente saludable
            status_code = status.HTTP_200_OK
    else:
        # Sistema con problemas graves
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(status_code=status_code, content=health_status)


@router.get("/health/deep")
async def deep_health_check(
    request: Request,
    store: OrchestratorStore = Depends(get_store),
    runner: OrchestratorRunner = Depends(get_runner),
) -> Dict[str, Any]:
    """
    Deep health check con verificación exhaustiva.

    Verifica:
    - Conexiones a BD
    - Pool de conexiones
    - Estado de circuit breakers
    - Capacidad del sistema
    - Ejecuciones atascadas
    - Salud de schedules
    """
    deep_health = {"timestamp": None, "checks": {}}

    from datetime import datetime, timezone

    deep_health["timestamp"] = datetime.now(timezone.utc).isoformat()

    # Verificar pool de conexiones
    try:
        pool_stats = store.connection_pool.get_stats()
        deep_health["checks"]["connection_pool"] = {
            "healthy": pool_stats["available"] > 0,
            "stats": pool_stats,
            "message": "OK" if pool_stats["available"] > 0 else "Pool exhausted",
        }
    except Exception as e:
        deep_health["checks"]["connection_pool"] = {"healthy": False, "error": str(e)}

    # Verificar ejecuciones atascadas
    try:
        stuck_runs = store.get_stuck_pipeline_runs(timeout_minutes=120)
        deep_health["checks"]["stuck_runs"] = {
            "healthy": len(stuck_runs) == 0,
            "count": len(stuck_runs),
            "message": f"Found {len(stuck_runs)} stuck runs"
            if stuck_runs
            else "No stuck runs",
        }
    except Exception as e:
        deep_health["checks"]["stuck_runs"] = {"healthy": False, "error": str(e)}

    # Verificar métricas de pipelines
    try:
        pipeline_metrics = store.get_pipeline_metrics()
        total_pipelines = len(pipeline_metrics)

        failing_pipelines = []
        for pm in pipeline_metrics:
            total = pm.get("total_runs", 0)
            failed = pm.get("failed_runs", 0)
            if total > 0 and (failed / total) > 0.5:
                failing_pipelines.append(pm["pipeline_id"])

        deep_health["checks"]["pipeline_health"] = {
            "healthy": len(failing_pipelines) == 0,
            "total_pipelines": total_pipelines,
            "failing_pipelines": failing_pipelines,
            "message": f"{len(failing_pipelines)} pipelines with >50% failure rate"
            if failing_pipelines
            else "All pipelines healthy",
        }
    except Exception as e:
        deep_health["checks"]["pipeline_health"] = {"healthy": False, "error": str(e)}

    # Verificar resilience patterns
    try:
        resilience = get_resilience_manager()
        resilience_metrics = resilience.get_all_metrics()

        open_circuits = []
        for name, metrics in resilience_metrics.get("circuit_breakers", {}).items():
            if metrics.state.value == "OPEN":
                open_circuits.append(name)

        deep_health["checks"]["resilience"] = {
            "healthy": len(open_circuits) == 0,
            "open_circuits": open_circuits,
            "message": f"{len(open_circuits)} circuit breakers OPEN"
            if open_circuits
            else "All circuits healthy",
        }
    except Exception as e:
        deep_health["checks"]["resilience"] = {"healthy": False, "error": str(e)}

    # Determinar salud general
    all_healthy = all(
        check.get("healthy", False) for check in deep_health["checks"].values()
    )

    deep_health["overall_healthy"] = all_healthy

    return deep_health


@router.post("/resilience/circuit-breakers/reset")
async def reset_circuit_breakers() -> Dict[str, Any]:
    """
    Resetear manualmente todos los circuit breakers.

    Útil para recuperación manual después de resolver problemas.
    """
    try:
        resilience = get_resilience_manager()
        resilience.reset_all()

        log.info("All circuit breakers manually reset")

        return {"success": True, "message": "All circuit breakers have been reset"}
    except Exception as e:
        log.exception("Error resetting circuit breakers", error=str(e))
        return {"success": False, "error": str(e)}


@router.get("/pipeline-metrics")
async def get_pipeline_metrics(
    pipeline_id: Optional[str] = None, store: OrchestratorStore = Depends(get_store)
) -> Dict[str, Any]:
    """
    Obtener métricas agregadas de pipelines.

    Args:
        pipeline_id: ID del pipeline (opcional). Si se omite, retorna todos.

    Returns:
        Métricas de ejecución agregadas por pipeline.
    """
    try:
        metrics = store.get_pipeline_metrics(pipeline_id)

        return {"success": True, "metrics": metrics}
    except Exception as e:
        log.exception("Error getting pipeline metrics", error=str(e))
        return {"success": False, "error": str(e)}
