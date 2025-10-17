from fastapi import FastAPI
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
import sys

from tauro.api.core.config import settings
from tauro.api.core.middleware import setup_middleware
from tauro.api.routes import pipelines_router, scheduling_router, monitoring_router


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

    # Store settings in app state
    app.state.settings = settings
    # Attempt to start scheduler service (if available) during startup
    try:
        from tauro.api.core.deps import get_scheduler_service

        scheduler = get_scheduler_service()
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

    # Stop scheduler if running (use instance stored in app.state if present)
    try:
        scheduler = getattr(app.state, "scheduler", None)
        if not scheduler:
            from tauro.api.core.deps import get_scheduler_service

            scheduler = get_scheduler_service()

        if scheduler:
            try:
                scheduler.stop()
                logger.info("Scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")
    except Exception as e:
        logger.error(f"Error during scheduler shutdown: {e}")


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
    app.include_router(scheduling_router, prefix=settings.api_prefix)
    app.include_router(monitoring_router)  # No prefix for health/metrics

    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint with API info"""
        return {
            "name": settings.api_title,
            "version": settings.api_version,
            "docs": "/docs",
            "health": "/health",
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
