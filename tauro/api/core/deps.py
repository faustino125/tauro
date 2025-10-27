from __future__ import annotations
from typing import Optional, TYPE_CHECKING, Any
from functools import lru_cache
import threading

from fastapi import Depends, HTTPException, status
from loguru import logger
from motor.motor_asyncio import AsyncDatabase  # type: ignore

from core.api.core.config import Settings, get_settings
from core.api.services.config_service import ConfigService
from core.api.db.connection import (
    init_mongodb,
    get_mongodb_client,
    get_database as get_database_dependency,
)
from core.api.services.project_service import ProjectService
from core.api.services.run_service import RunService as APIRunService
from core.api.services.schedule_service import ScheduleService as APIScheduleService
from core.api.services.execution_service import ExecutionService
from core.api.services.config_version_service import ConfigVersionService

if TYPE_CHECKING:
    from tauro.cli.config import ConfigManager
    from tauro.cli.execution import ContextInitializer
    from tauro.config.contexts import Context
    from tauro.orchest import (
        OrchestratorStore,
        OrchestratorRunner,
        ScheduleService,
        RunService as OrchestRunService,
    )

# Import Tauro core components
# Pre-declare runtime names as Any to satisfy type checkers when the package is absent
ConfigManager: Any = None
ContextInitializer: Any = None
Context: Any = None
OrchestratorStore: Any = None
ScheduleService: Any = None
OrchestratorRunner: Any = None
OrchestRunService: Any = None

try:
    from tauro.cli.config import ConfigManager
    from tauro.cli.execution import ContextInitializer
    from tauro.config.contexts import Context
    from tauro.orchest import (
        OrchestratorStore,
        OrchestratorRunner,
        ScheduleService,
        RunService as OrchestRunService,
    )

    TAURO_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Tauro core not available: {e}")
    TAURO_AVAILABLE = False
    ConfigManager = None
    ContextInitializer = None
    Context = None
    OrchestratorStore = None
    ScheduleService = None
    OrchestratorRunner = None
    OrchestRunService = None


_SCHEDULER_LOCK = threading.Lock()
_SCHEDULER_INSTANCE: Optional[Any] = None

_CONFIG_MANAGER_LOCK = threading.Lock()
_CONFIG_MANAGER_INSTANCE: Optional[Any] = None

_CONFIG_SERVICE_LOCK = threading.Lock()
_CONFIG_SERVICE_INSTANCE: Optional[ConfigService] = None

_DB_CONTEXT_INITIALIZER_LOCK = threading.Lock()
_DB_CONTEXT_INITIALIZER: Optional["DBContextInitializer"] = None

_ORCHESTRATOR_RUNNER_LOCK = threading.Lock()
_ORCHESTRATOR_RUNNER_INSTANCE: Optional[Any] = None

_EXECUTION_SERVICE_LOCK = threading.Lock()
_EXECUTION_SERVICE_INSTANCE: Optional[ExecutionService] = None


class DBContextInitializer:
    """Builds Tauro Context instances from Mongo-backed configuration."""

    def __init__(self, service: ConfigService, default_environment: str) -> None:
        self._service = service
        self._default_environment = default_environment

    def initialize(self, project_id: str, environment: Optional[str] = None) -> Any:
        if not TAURO_AVAILABLE or Context is None:
            raise RuntimeError("Tauro Context class is not available")

        env = environment or self._default_environment
        try:
            sections = self._service.load_context_dicts(project_id, env)
            context = Context.from_json_config(*sections)
            try:
                setattr(context, "env", env)
                setattr(context, "project_id", project_id)
            except Exception:
                pass
            return context
        except Exception as exc:
            logger.error(f"Error building context from database configuration: {exc}")
            raise


# =============================================================================
# Settings Dependency
# =============================================================================


def get_current_settings() -> Settings:
    """Get current settings (cached)"""
    return get_settings()


def get_config_service(
    settings: Settings = Depends(get_current_settings),
) -> Optional[ConfigService]:
    """Return singleton ConfigService when Mongo configuration is enabled.

    NOTE: MongoDB-backed configuration repository is not yet implemented.
    This function currently returns None and CONFIG_SOURCE='mongo' is not supported.

    Returns:
        None (MongoDB configuration not implemented)
    """
    if settings.config_source == "mongo":
        logger.warning(
            "CONFIG_SOURCE='mongo' is not yet supported. "
            "MongoConfigRepository implementation is pending."
        )

    # MongoDB configuration not implemented yet
    return None


def get_db_context_initializer(
    settings: Settings = Depends(get_current_settings),
    service: Optional[ConfigService] = Depends(get_config_service),
) -> Optional[DBContextInitializer]:
    """Provide cached DBContextInitializer when Mongo config is active."""

    if settings.config_source != "mongo" or service is None:
        return None

    global _DB_CONTEXT_INITIALIZER
    if _DB_CONTEXT_INITIALIZER is not None:
        return _DB_CONTEXT_INITIALIZER

    with _DB_CONTEXT_INITIALIZER_LOCK:
        if _DB_CONTEXT_INITIALIZER is None:
            _DB_CONTEXT_INITIALIZER = DBContextInitializer(
                service=service,
                default_environment=settings.environment,
            )
    return _DB_CONTEXT_INITIALIZER


# =============================================================================
# Tauro ConfigManager
# =============================================================================


_CONFIG_MANAGER_LOCK = threading.Lock()
_CONFIG_MANAGER_INSTANCE: Optional[Any] = None


def get_config_manager(
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get Tauro ConfigManager instance.

    Cached singleton to avoid recreating on each request.
    Thread-safe with explicit lock (not @lru_cache because Settings is not hashable).
    """
    if settings.config_source != "file":
        logger.debug("ConfigManager disabled because CONFIG_SOURCE != 'file'")
        return None

    if not TAURO_AVAILABLE or ConfigManager is None:
        logger.warning("ConfigManager not available")
        return None

    global _CONFIG_MANAGER_INSTANCE

    # Fast path: return existing instance
    if _CONFIG_MANAGER_INSTANCE is not None:
        return _CONFIG_MANAGER_INSTANCE

    # Slow path: create under lock
    with _CONFIG_MANAGER_LOCK:
        # Double-check: verify instance wasn't created by another thread
        if _CONFIG_MANAGER_INSTANCE is not None:
            return _CONFIG_MANAGER_INSTANCE

        try:
            cm = ConfigManager(
                base_path=settings.base_path,
                layer_name=settings.layer_name,
                use_case=settings.use_case,
                config_type=settings.config_type,
                interactive=False,
            )
            cm.change_to_config_directory()
            _CONFIG_MANAGER_INSTANCE = cm
            logger.debug("ConfigManager instance created and cached")
            return cm
        except Exception as e:
            logger.error(f"Error creating ConfigManager: {e}")
            return None


# =============================================================================
# Tauro Context
# =============================================================================


def get_context(
    config_manager: Optional[Any] = Depends(get_config_manager),
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get Tauro Context for current configuration.

    Returns None if ConfigManager is not available.
    """
    if settings.config_source == "mongo":
        # Context must be resolved per request with project/environment information.
        return None

    if not TAURO_AVAILABLE or config_manager is None:
        return None

    try:
        initializer = ContextInitializer(config_manager)
        context = initializer.initialize_context()
        return context
    except Exception as e:
        logger.error(f"Error creating Context: {e}")
        return None


# =============================================================================
# OrchestratorStore
# =============================================================================


def get_orchestrator_store(
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """Get cached OrchestratorStore configured for the current scope."""

    scope = settings.environment.upper() if settings.environment else None
    return _get_orchestrator_store_cached(scope)


@lru_cache(maxsize=1)
def _get_orchestrator_store_cached(scope: Optional[str]) -> Optional[Any]:
    if not TAURO_AVAILABLE or OrchestratorStore is None:
        logger.warning("OrchestratorStore not available")
        return None

    try:
        return OrchestratorStore(scope=scope)
    except Exception as e:
        logger.error(f"Error creating OrchestratorStore: {e}")
        return None


# =============================================================================
# OrchestratorRunner
# =============================================================================


def get_orchestrator_runner(
    context: Optional[Any] = Depends(get_context),
    store: Optional[Any] = Depends(get_orchestrator_store),
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get OrchestratorRunner instance.

    Creates a new runner for each request (not cached).
    """
    if not TAURO_AVAILABLE or context is None or store is None:
        logger.warning("OrchestratorRunner not available")
        return None

    try:
        runner = OrchestratorRunner(
            context=context,
            store=store,
            max_workers=settings.scheduler_max_workers,
        )
        return runner
    except Exception as e:
        logger.error(f"Error creating OrchestratorRunner: {e}")
        return None


# =============================================================================
# ScheduleService (New Centralized Service)
# =============================================================================


def get_scheduler_service(
    context: Optional[Any] = Depends(get_context),
    store: Optional[Any] = Depends(get_orchestrator_store),
) -> Optional[Any]:
    """
    Get ScheduleService instance (centralized v2.0 service).

    Cached singleton that runs in background.
    """
    if not TAURO_AVAILABLE or context is None or store is None:
        logger.warning("ScheduleService not available")
        return None

    global _SCHEDULER_INSTANCE

    with _SCHEDULER_LOCK:
        if _SCHEDULER_INSTANCE is not None:
            return _SCHEDULER_INSTANCE

        try:
            scheduler = ScheduleService(
                context=context,
                store=store,
            )
            _SCHEDULER_INSTANCE = scheduler
            # Do not start the scheduler here; lifecycle management should be
            # performed by the application startup/shutdown handlers.
            return scheduler
        except Exception as e:
            logger.error(f"Error creating ScheduleService: {e}")
            return None


def resolve_scheduler_service() -> Optional[Any]:
    """
    Utility to obtain scheduler service outside of dependency injection.

    Thread-safe: Uses global lock to prevent race conditions when called
    from multiple threads concurrently.
    """
    global _SCHEDULER_INSTANCE

    # Fast path: if scheduler already exists, return it
    if _SCHEDULER_INSTANCE is not None:
        return _SCHEDULER_INSTANCE

    # Slow path: create scheduler under lock to prevent race conditions
    with _SCHEDULER_LOCK:
        # Double-check: verify instance wasn't created by another thread
        if _SCHEDULER_INSTANCE is not None:
            return _SCHEDULER_INSTANCE

        try:
            settings = get_current_settings()
            if settings.config_source == "mongo":
                logger.warning(
                    "ScheduleService resolution is not supported with CONFIG_SOURCE=mongo"
                )
                return None
            config_manager = get_config_manager(settings=settings)
            context = get_context(config_manager=config_manager)
            store = get_orchestrator_store(settings=settings)

            if context is None or store is None:
                logger.warning("Cannot resolve scheduler: missing context or store")
                return None

            scheduler = ScheduleService(
                context=context,
                store=store,
            )
            _SCHEDULER_INSTANCE = scheduler
            logger.info("ScheduleService instance created and cached")
            return scheduler
        except Exception as e:
            logger.error(f"Error resolving ScheduleService: {e}")
            return None


# =============================================================================
# Cleanup
# =============================================================================


def cleanup_scheduler(scheduler: Optional[Any] = Depends(get_scheduler_service)):
    """Stop scheduler on shutdown"""
    if scheduler:
        try:
            scheduler.stop()
            logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
        finally:
            global _SCHEDULER_INSTANCE
            with _SCHEDULER_LOCK:
                _SCHEDULER_INSTANCE = None


# =============================================================================
# MongoDB Dependencies
# =============================================================================


async def initialize_mongodb(settings: Settings) -> AsyncDatabase:
    """
    Initialize MongoDB connection

    Args:
        settings: Application settings

    Returns:
        AsyncDatabase instance
    """
    try:
        # Initialize client (doesn't connect yet)
        client = init_mongodb(
            uri=settings.mongodb_uri,
            db_name=settings.mongodb_db_name,
            timeout_seconds=settings.mongodb_timeout_seconds,
        )
        logger.info("MongoDB client initialized")

        # Actually connect to database
        db = await client.connect()
        logger.info("MongoDB connection established")

        # Run migrations
        from core.api.db.migrations import init_database

        await init_database(db)
        logger.info("Database migrations completed")

        return db

    except Exception as e:
        logger.error(f"Failed to initialize MongoDB: {e}")
        raise


def close_mongodb() -> None:
    """Close MongoDB connection"""
    try:
        client = get_mongodb_client()
        client.disconnect()
        logger.info("MongoDB connection closed")
    except Exception as e:
        logger.error(f"Error closing MongoDB: {e}")


async def get_database() -> AsyncDatabase:
    """
    FastAPI dependency for getting MongoDB database

    Returns:
        AsyncDatabase instance
    """
    async for db in get_database_dependency():
        yield db


# =============================================================================
# Phase 2 Services (ProjectService, RunService, etc.)
# =============================================================================

_PROJECT_SERVICE_LOCK = threading.Lock()
_PROJECT_SERVICE_INSTANCE: Optional[ProjectService] = None

_RUN_SERVICE_LOCK = threading.Lock()
_RUN_SERVICE_INSTANCE: Optional[APIRunService] = None

_SCHEDULE_SERVICE_LOCK = threading.Lock()
_SCHEDULE_SERVICE_INSTANCE: Optional[APIScheduleService] = None

_EXECUTION_SERVICE_LOCK = threading.Lock()
_EXECUTION_SERVICE_INSTANCE: Optional[ExecutionService] = None


def get_project_service(
    db: AsyncDatabase = Depends(get_database),
) -> ProjectService:
    """
    Get ProjectService instance (cached singleton).

    Args:
        db: MongoDB database instance from dependency

    Returns:
        ProjectService instance
    """
    global _PROJECT_SERVICE_INSTANCE

    if _PROJECT_SERVICE_INSTANCE is not None:
        return _PROJECT_SERVICE_INSTANCE

    with _PROJECT_SERVICE_LOCK:
        if _PROJECT_SERVICE_INSTANCE is None:
            _PROJECT_SERVICE_INSTANCE = ProjectService(db)
            logger.debug("ProjectService instance created and cached")

    return _PROJECT_SERVICE_INSTANCE


def get_run_service(
    db: AsyncDatabase = Depends(get_database),
) -> APIRunService:
    """
    Get RunService instance (cached singleton).

    Args:
        db: MongoDB database instance from dependency

    Returns:
        RunService instance
    """
    global _RUN_SERVICE_INSTANCE

    if _RUN_SERVICE_INSTANCE is not None:
        return _RUN_SERVICE_INSTANCE

    with _RUN_SERVICE_LOCK:
        if _RUN_SERVICE_INSTANCE is None:
            _RUN_SERVICE_INSTANCE = APIRunService(db)
            logger.debug("RunService instance created and cached")

    return _RUN_SERVICE_INSTANCE


def get_schedule_service(
    db: AsyncDatabase = Depends(get_database),
) -> APIScheduleService:
    """
    Get ScheduleService instance (cached singleton).

    Args:
        db: MongoDB database instance from dependency

    Returns:
        ScheduleService instance
    """
    global _SCHEDULE_SERVICE_INSTANCE

    if _SCHEDULE_SERVICE_INSTANCE is not None:
        return _SCHEDULE_SERVICE_INSTANCE

    with _SCHEDULE_SERVICE_LOCK:
        if _SCHEDULE_SERVICE_INSTANCE is None:
            _SCHEDULE_SERVICE_INSTANCE = APIScheduleService(db)
            logger.debug("ScheduleService instance created and cached")

    return _SCHEDULE_SERVICE_INSTANCE


def resolve_orchestrator_runner() -> Optional[Any]:
    """
    Get OrchestratorRunner singleton instance.

    Initializes the runner on first call and caches the instance. Returns None if
    tauro.orchest package is not available.

    Returns:
        OrchestratorRunner instance or None if not available
    """
    global _ORCHESTRATOR_RUNNER_INSTANCE

    # Return cached instance if available
    if _ORCHESTRATOR_RUNNER_INSTANCE is not None:
        return _ORCHESTRATOR_RUNNER_INSTANCE

    # Check if OrchestratorRunner is available
    if OrchestratorRunner is None:
        logger.warning(
            "OrchestratorRunner not available. "
            "tauro.orchest package may not be installed. "
            "Centralized execution will not work."
        )
        return None

    # Create instance with thread safety
    with _ORCHESTRATOR_RUNNER_LOCK:
        if _ORCHESTRATOR_RUNNER_INSTANCE is None:
            try:
                # Create runner with empty context (will be populated per execution)
                from tauro.orchest import OrchestratorStore

                store = OrchestratorStore()
                _ORCHESTRATOR_RUNNER_INSTANCE = OrchestratorRunner(
                    context=None, store=store
                )
                logger.info("OrchestratorRunner singleton initialized")
            except Exception as e:
                logger.error(
                    f"Failed to initialize OrchestratorRunner: {type(e).__name__}: {e}"
                )
                return None

    return _ORCHESTRATOR_RUNNER_INSTANCE


def get_execution_service(
    db: AsyncDatabase = Depends(get_database),
    orchestrator_runner: Optional[Any] = Depends(resolve_orchestrator_runner),
) -> ExecutionService:
    """
    Get ExecutionService instance (cached singleton) with OrchestratorRunner injected.

    Args:
        db: MongoDB database instance from dependency
        orchestrator_runner: OrchestratorRunner instance from dependency (optional)

    Returns:
        ExecutionService instance configured for centralized execution
    """
    global _EXECUTION_SERVICE_INSTANCE

    if _EXECUTION_SERVICE_INSTANCE is not None:
        # Update runner reference if not set
        if orchestrator_runner and not _EXECUTION_SERVICE_INSTANCE.orchestrator_runner:
            _EXECUTION_SERVICE_INSTANCE.orchestrator_runner = orchestrator_runner
        return _EXECUTION_SERVICE_INSTANCE

    with _EXECUTION_SERVICE_LOCK:
        if _EXECUTION_SERVICE_INSTANCE is None:
            service = ExecutionService(db)
            # Inject orchestrator runner if available
            if orchestrator_runner:
                service.orchestrator_runner = orchestrator_runner
                logger.debug("ExecutionService initialized with OrchestratorRunner")
            else:
                logger.warning(
                    "ExecutionService initialized without OrchestratorRunner"
                )
            _EXECUTION_SERVICE_INSTANCE = service
            logger.debug("ExecutionService instance created and cached")

    return _EXECUTION_SERVICE_INSTANCE


_CONFIG_VERSION_SERVICE_LOCK = threading.Lock()
_CONFIG_VERSION_SERVICE_INSTANCE: Optional[ConfigVersionService] = None


def get_config_version_service(
    db: AsyncDatabase = Depends(get_database),
) -> ConfigVersionService:
    """
    Get ConfigVersionService instance (cached singleton).

    Args:
        db: MongoDB database instance from dependency

    Returns:
        ConfigVersionService instance for version management
    """
    global _CONFIG_VERSION_SERVICE_INSTANCE

    if _CONFIG_VERSION_SERVICE_INSTANCE is not None:
        return _CONFIG_VERSION_SERVICE_INSTANCE

    with _CONFIG_VERSION_SERVICE_LOCK:
        if _CONFIG_VERSION_SERVICE_INSTANCE is None:
            _CONFIG_VERSION_SERVICE_INSTANCE = ConfigVersionService(db)
            logger.debug("ConfigVersionService instance created and cached")

    return _CONFIG_VERSION_SERVICE_INSTANCE


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "get_current_settings",
    "get_config_service",
    "get_config_manager",
    "get_db_context_initializer",
    "get_context",
    "get_orchestrator_store",
    "get_orchestrator_runner",
    "get_scheduler_service",
    "resolve_scheduler_service",
    "cleanup_scheduler",
    "initialize_mongodb",
    "close_mongodb",
    "get_database",
    "get_project_service",
    "get_run_service",
    "get_schedule_service",
    "get_execution_service",
    "get_config_version_service",
]
