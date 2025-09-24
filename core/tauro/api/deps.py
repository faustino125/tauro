from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Optional, Dict, Any, Generator
from contextlib import contextmanager

from fastapi import Depends, HTTPException, status
from loguru import logger  # type: ignore
import time
import prometheus_client as prom
from sqlalchemy.orm import Session

from tauro.cli.config import ConfigManager
from tauro.cli.execution import ContextInitializer
from tauro.config.contexts import Context
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.scheduler import SchedulerService

from tauro.api.config import ApiSettings, resolve_db_path

# Métricas Prometheus
REQUEST_DURATION = prom.Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint", "status"],
)
REQUEST_COUNT = prom.Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)
DB_QUERY_DURATION = prom.Histogram(
    "db_query_duration_seconds",
    "Database query duration in seconds",
    ["operation", "table"],
)


@contextmanager
def track_db_operation(operation: str, table: str):
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        DB_QUERY_DURATION.labels(operation=operation, table=table).observe(duration)


@lru_cache(maxsize=1)
def get_settings() -> ApiSettings:
    return ApiSettings()


@lru_cache(maxsize=1)
def get_config_manager(settings: ApiSettings = Depends(get_settings)) -> ConfigManager:
    cm = ConfigManager(
        base_path=settings.base_path,
        layer_name=settings.layer_name,
        use_case=settings.use_case,
        config_type=settings.config_type,
        interactive=False,
    )
    try:
        cm.change_to_config_directory()
    except Exception as e:
        logger.warning(f"Could not change to config directory: {e}")
    return cm


def get_context(
    env: Optional[str] = None,
    cm: ConfigManager = Depends(get_config_manager),
    settings: ApiSettings = Depends(get_settings),
) -> Context:
    initializer = ContextInitializer(cm)
    chosen_env = env or settings.default_env
    # Cache por environment para mejorar rendimiento
    cache_key = f"context_{chosen_env}"
    if not hasattr(get_context, "cache"):
        get_context.cache = {}

    if cache_key not in get_context.cache:
        get_context.cache[cache_key] = initializer.initialize(chosen_env)

    return get_context.cache[cache_key]


@lru_cache(maxsize=1)
def get_store(settings: ApiSettings = Depends(get_settings)) -> OrchestratorStore:
    """
    Construir OrchestratorStore respetando su firma actual.
    """
    db_path = settings.orchestrator_db_path
    if isinstance(db_path, str):
        db_path = Path(db_path)
    return OrchestratorStore(db_path, max_connections=settings.database_pool_size)


def get_runner(
    context: Context = Depends(get_context),
    store: OrchestratorStore = Depends(get_store),
) -> OrchestratorRunner:
    return OrchestratorRunner(context, store)


@lru_cache(maxsize=1)
def get_scheduler(
    context: Context = Depends(get_context),
    store: OrchestratorStore = Depends(get_store),
    settings: ApiSettings = Depends(get_settings),
) -> SchedulerService:
    """
    Construir SchedulerService con la firma actual (sin max_concurrent_runs).
    """
    return SchedulerService(context, store)


# Dependencia para métricas
def get_metrics():
    return {
        "request_duration": REQUEST_DURATION,
        "request_count": REQUEST_COUNT,
        "db_query_duration": DB_QUERY_DURATION,
    }


# Middleware para tracking de requests
async def track_requests(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time

    # Registrar métricas
    endpoint = request.url.path
    method = request.method
    status_code = response.status_code

    REQUEST_DURATION.labels(
        method=method, endpoint=endpoint, status=status_code
    ).observe(duration)
    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status_code).inc()

    # Añadir header de timing
    response.headers["X-Response-Time"] = str(duration)

    return response


from .config import settings

# suponiendo que existe SessionLocal en este módulo o en otro módulo de db
try:
    from tauro.api.db.conexion import (
        SessionLocal,
    )  # si hay un módulo db que expone SessionLocal
except Exception:
    SessionLocal = None  # fallback; mantener compatibilidad con proyecto


def get_db() -> Generator[Session, None, None]:
    """
    Dependency para obtener sesión DB por petición.
    Garantiza rollback en excepción y close al finalizar.
    Uso en FastAPI: Depends(get_db)
    """
    if SessionLocal is None:
        raise RuntimeError(
            "SessionLocal no está configurado. Define SessionLocal en core/tauro/api/db.py"
        )
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
