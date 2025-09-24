from __future__ import annotations
from typing import Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.middleware.gzip import GZipMiddleware
from loguru import logger
import time
import sentry_sdk
from prometheus_fastapi_instrumentator import Instrumentator

from tauro.api.config import ApiSettings
from tauro.api.deps import get_settings
from tauro.api.routes import pipelines, runs, schedules, control
from tauro.api.utils import handle_exceptions, APIError, StructuredLogger
from tauro.api.security import (
    SecurityHeadersMiddleware,
    RateLimitMiddleware,
    RequestLoggingMiddleware,
)

from tauro.cli.config import ConfigManager
from tauro.cli.execution import ContextInitializer
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.scheduler import SchedulerService
from tauro.orchest.runner import OrchestratorRunner
from tauro.api.services import PipelineService, StreamingService

# Configurar logging estructurado
log = StructuredLogger("tauro.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Context manager para startup/shutdown con manejo de errores"""
    # Startup
    try:
        log.info("Starting Tauro API")

        # Configuración
        settings = get_settings()

        # Inicializar Sentry si está configurado
        if settings.sentry_dsn:
            sentry_sdk.init(
                dsn=str(settings.sentry_dsn),
                environment=settings.environment,
                traces_sample_rate=1.0,
            )
            log.info("Sentry initialized")

        # Contexto
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
            log.warning("Could not change to config directory", error=str(e))

        context = ContextInitializer(cm).initialize(settings.default_env)
        app.state.context = context

        # Store con reconexión automática
        app.state.store = OrchestratorStore(
            settings.orchestrator_db_path,
            max_connections=settings.database_pool_size,
            timeout=settings.database_timeout,
        )

        # Runner
        app.state.runner = OrchestratorRunner(app.state.context, app.state.store)

        # Services
        app.state.pipeline_service = PipelineService(
            store=app.state.store,
            runner=app.state.runner,
            max_workers=settings.pipeline_service_max_workers,
        )

        app.state.streaming_service = StreamingService(
            store=app.state.store, runner=app.state.runner
        )

        # Scheduler opcional
        if settings.scheduler_enabled:
            app.state.scheduler = SchedulerService(
                app.state.context,
                app.state.store,
                stuck_run_timeout_minutes=settings.scheduler_stuck_timeout_minutes,
            )
            app.state.scheduler.start(poll_interval=settings.scheduler_poll_seconds)
            log.info("Scheduler started")

        log.info("Tauro API started successfully")
        yield

    except Exception as e:
        log.exception("Failed to start Tauro API", error=str(e))
        raise

    finally:
        # Shutdown
        log.info("Shutting down Tauro API")

        # Parar scheduler si existe
        scheduler = getattr(app.state, "scheduler", None)
        if scheduler is not None:
            try:
                scheduler.stop()
                log.info("Scheduler stopped")
            except Exception as e:
                log.exception("Error stopping scheduler", error=str(e))

        # Cerrar store
        store = getattr(app.state, "store", None)
        if store is not None:
            try:
                store.close()
                log.info("Store closed")
            except Exception as e:
                log.exception("Error closing store", error=str(e))

        log.info("Shutdown complete")


def _configure_logging(settings: ApiSettings) -> None:
    """Configurar logging estructurado"""
    logger.remove()

    # Console logging
    logger.add(
        lambda msg: print(msg, end=""),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} {extra}",
        level=settings.log_level.upper(),
        enqueue=True,
    )

    # File logging
    logger.add(
        "logs/tauro_api_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="7 days",
        level=settings.log_level.upper(),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message} {extra}",
        enqueue=True,
    )


def _configure_middlewares(app: FastAPI, settings: ApiSettings) -> None:
    """Configurar middlewares de seguridad y monitoreo"""

    # HTTPS redirect en producción
    if settings.environment == "prod":
        app.add_middleware(HTTPSRedirectMiddleware)

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
    )

    # GZip compression
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Security headers
    app.add_middleware(SecurityHeadersMiddleware)

    # Rate limiting
    app.add_middleware(RateLimitMiddleware)

    # Request logging
    app.add_middleware(RequestLoggingMiddleware)


def _register_exception_handlers(app: FastAPI) -> None:
    """Registrar manejadores de excepciones globales"""

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        log.debug("Validation error", path=request.url.path, errors=exc.errors())
        return JSONResponse(
            status_code=422,
            content={
                "message": "Validation error",
                "code": "validation_error",
                "details": exc.errors(),
            },
        )

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError):
        log.warning(
            "API error", path=request.url.path, code=exc.code, status=exc.status_code
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={"message": exc.message, "code": exc.code, "details": exc.details},
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        log.exception("Unhandled exception", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error", "code": "internal_error"},
        )


def _register_routes(app: FastAPI, settings: ApiSettings) -> None:
    """Registrar rutas de la API"""
    app.include_router(pipelines.router, prefix=settings.api_prefix)
    app.include_router(runs.router, prefix=settings.api_prefix)
    app.include_router(schedules.router, prefix=settings.api_prefix)
    app.include_router(control.router, prefix=settings.api_prefix)


def _add_health_checks(app: FastAPI, settings: ApiSettings) -> None:
    """Añadir endpoints de health check"""

    @app.get("/health/liveness")
    async def liveness():
        return {"status": "ok", "timestamp": time.time(), "service": settings.app_name}

    @app.get("/health/readiness")
    async def readiness():
        try:
            store = getattr(app.state, "store", None)
            if store is None:
                return {
                    "status": "degraded",
                    "database": "not-initialized",
                    "service": settings.app_name,
                }

            # Verificar conexión a base de datos
            with store._conn() as conn:
                conn.execute("SELECT 1")

            # Verificar servicios
            services_status = {}
            pipeline_service = getattr(app.state, "pipeline_service", None)
            if pipeline_service:
                services_status["pipeline_service"] = pipeline_service.get_stats()

            return {
                "status": "ok",
                "timestamp": time.time(),
                "database": "ok",
                "services": services_status,
                "service": settings.app_name,
            }
        except Exception as e:
            log.warning("Readiness check failed", error=str(e))
            return {
                "status": "degraded",
                "timestamp": time.time(),
                "database": f"error: {str(e)}",
                "service": settings.app_name,
            }

    @app.get("/health/startup")
    async def startup():
        """Endpoint para verificar que la aplicación inició correctamente"""
        # Verificar que todos los componentes críticos estén inicializados
        critical_services = ["store", "runner", "pipeline_service"]
        missing_services = []

        for service in critical_services:
            if not hasattr(app.state, service):
                missing_services.append(service)

        if missing_services:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "missing_services": missing_services,
                    "service": settings.app_name,
                },
            )

        return {
            "status": "ok",
            "timestamp": time.time(),
            "service": settings.app_name,
        }


def create_app(settings: Optional[ApiSettings] = None) -> FastAPI:
    """Factory function para crear la aplicación FastAPI"""
    settings = settings or get_settings()

    _configure_logging(settings)

    app = FastAPI(
        title=settings.app_name,
        description="Tauro Orchestrator API for managing data pipelines",
        version="1.0.0",
        docs_url=settings.docs_url if settings.environment != "prod" else None,
        openapi_url=settings.openapi_url if settings.environment != "prod" else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    _configure_middlewares(app, settings)
    _register_exception_handlers(app)
    _register_routes(app, settings)
    _add_health_checks(app, settings)

    # Configurar metrics si está habilitado
    if settings.metrics_enabled:
        Instrumentator().instrument(app).expose(app)

    return app


# Crear aplicación
app = create_app()

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()

    uvicorn.run(
        "tauro.api.main:app",
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        log_level=settings.log_level,
        reload=settings.environment == EnvironmentEnum.DEVELOPMENT,
    )
