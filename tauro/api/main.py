"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import sys
import logging
from contextlib import asynccontextmanager
from pathlib import Path

# Third-party
from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

# Local
from tauro.api.core.config import settings
from tauro.api.core.middleware import setup_middleware
from tauro.api.routes import (
    pipelines_router,
    scheduling_router,
    monitoring_router,
    projects_router,
    runs_router,
    configs_router,
    config_versions_router,
    logs_router,
)


# =============================================================================
# Logging Configuration
# =============================================================================


def configure_logging():
    """Configure loguru logging"""
    # Remove default handler
    logger.remove()

    # Add console handler
    logger.add(
        sys.stderr,
        format=settings.log_format,
        level=settings.log_level,
        colorize=True,
    )

    # Add file handler if specified
    if settings.log_file:
        logger.add(
            settings.log_file,
            rotation="500 MB",
            retention="10 days",
            level=settings.log_level,
        )

    logger.info(f"Logging configured: level={settings.log_level}")

    # ---------------------------------------------------------------------
    # Intercept standard logging and route through loguru for consistency
    # ---------------------------------------------------------------------
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            logger.log(level, record.getMessage())

    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(settings.log_level)


# =============================================================================
# Lifecycle Management
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    # Startup
    logger.info(f"Starting {settings.api_title} v{settings.api_version}")
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Tauro config: {settings.get_tauro_config()}")

    # Initialize MongoDB
    try:
        from tauro.api.core.deps import initialize_mongodb

        db = await initialize_mongodb(settings)
        app.state.db = db
        logger.info("✓ MongoDB initialized and connected")
    except Exception as e:
        logger.error(f"Failed to initialize MongoDB: {e}")
        raise

    # Run migrations
    try:
        from tauro.api.db.migrations import MigrationRunner

        logger.info("Running database migrations...")
        runner = MigrationRunner(db)
        await runner.run_migrations()
        logger.info("✓ Database migrations completed")
    except Exception as e:
        logger.error(f"Failed to run migrations: {e}")
        raise

    # Store settings in app state
    app.state.settings = settings

    # Attempt to initialize OrchestratorRunner (if available) during startup
    try:
        from tauro.api.core.deps import resolve_orchestrator_runner

        orchestrator = resolve_orchestrator_runner()
        if orchestrator:
            app.state.orchestrator_runner = orchestrator
            logger.info("OrchestratorRunner initialized for centralized execution")
        else:
            logger.warning("OrchestratorRunner not available; local execution only")
    except Exception as e:
        logger.debug(f"OrchestratorRunner not available: {e}")

    # Attempt to start scheduler service (if available) during startup
    try:
        from tauro.api.core.deps import resolve_scheduler_service

        scheduler = resolve_scheduler_service()
        if scheduler and settings.scheduler_enabled:
            try:
                scheduler.start()
                logger.info("Scheduler started")
                # store scheduler in app state for later shutdown
                app.state.scheduler = scheduler
            except Exception as e:
                logger.error(f"Error starting scheduler: {e}")
    except Exception:
        # If deps are not available, just continue; endpoints will return 503 as needed
        logger.debug("Scheduler service not available at startup")

    yield

    # Shutdown
    logger.info("Shutting down API")

    # Stop scheduler if running (use instance stored in app.state)
    try:
        scheduler = getattr(app.state, "scheduler", None)
        if scheduler:
            try:
                scheduler.stop()
                logger.info("Scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")
    except Exception as e:
        logger.error(f"Error during scheduler shutdown: {e}")

    # Close MongoDB connection
    try:
        from tauro.api.core.deps import close_mongodb

        close_mongodb()
    except Exception as e:
        logger.error(f"Error closing MongoDB: {e}")


# =============================================================================
# Application Factory
# =============================================================================


def create_app() -> FastAPI:
    """Create and configure FastAPI application"""

    # Configure logging
    configure_logging()

    # Create app
    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description=settings.api_description,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Setup middleware
    setup_middleware(app, settings)

    # Register routers
    app.include_router(pipelines_router, prefix=settings.api_prefix)
    app.include_router(config_versions_router, prefix=settings.api_prefix)
    app.include_router(configs_router, prefix=settings.api_prefix)
    app.include_router(projects_router, prefix=settings.api_prefix)
    app.include_router(runs_router, prefix=settings.api_prefix)
    app.include_router(scheduling_router, prefix=settings.api_prefix)
    app.include_router(logs_router, prefix=settings.api_prefix)
    app.include_router(monitoring_router)  # No prefix for health/metrics

    # Serve UI static files (production)
    ui_dist_path = Path(__file__).parent.parent / "ui" / "dist"
    if ui_dist_path.exists():
        app.mount("/ui", StaticFiles(directory=str(ui_dist_path), html=True), name="ui")
        logger.info(f"✓ UI mounted at /ui from {ui_dist_path}")
    else:
        logger.warning(f"UI dist not found at {ui_dist_path}. Run 'npm run build' in tauro/ui/")

    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint - redirect to UI if available, otherwise API info"""
        if ui_dist_path.exists():
            return RedirectResponse(url="/ui")
        return {
            "name": settings.api_title,
            "version": settings.api_version,
            "docs": "/docs",
            "health": "/health",
            "ui": "Run 'npm run build' in tauro/ui/ to enable web interface",
        }

    # Custom error handlers
    @app.exception_handler(404)
    async def not_found_handler(request, exc):
        return JSONResponse(
            status_code=404,
            content={
                "error": "Not Found",
                "detail": f"Path '{request.url.path}' not found",
            },
        )

    logger.info("Application created successfully")

    return app


# =============================================================================
# Application Instance
# =============================================================================

app = create_app()


# =============================================================================
# Main Entry Point (for debugging)
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    # Run the FastAPI application by passing the app instance directly.
    # Using the module path string can be brittle depending on how the package
    # is installed or how PYTHONPATH is configured.
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )
