from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger  # type: ignore
import threading

from tauro.api.config import ApiSettings
from tauro.api.deps import get_settings, get_scheduler
from tauro.api.routes import pipelines, runs, schedules, control


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(
        title=settings.app_name,
        docs_url=settings.docs_url,
        openapi_url=settings.openapi_url,
    )
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Rutas
    app.include_router(pipelines.router, prefix=settings.api_prefix)
    app.include_router(runs.router, prefix=settings.api_prefix)
    app.include_router(schedules.router, prefix=settings.api_prefix)
    app.include_router(control.router, prefix=settings.api_prefix)

    # Health
    @app.get("/health/liveness")
    def liveness():
        return {"status": "ok"}

    @app.get("/health/readiness")
    def readiness():
        return {"status": "ok"}

    @app.on_event("startup")
    def on_startup():
        if settings.scheduler_enabled:
            try:
                logger.info("Starting in-process scheduler (background thread)")
                scheduler = get_scheduler()
                # Start scheduler in background thread to avoid blocking startup
                t = threading.Thread(
                    target=scheduler.start,
                    kwargs={"poll_interval": settings.scheduler_poll_seconds},
                    daemon=True,
                )
                t.start()
            except Exception as e:
                logger.error(f"Scheduler failed to start: {e}")

    @app.on_event("shutdown")
    def on_shutdown():
        if settings.scheduler_enabled:
            try:
                get_scheduler().stop()
            except Exception as e:
                logger.warning(f"Scheduler stop raised: {e}")

    return app


app = create_app()
