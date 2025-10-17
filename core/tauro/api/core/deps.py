from __future__ import annotations
from typing import Optional, TYPE_CHECKING, Any
from functools import lru_cache

from fastapi import Depends
from loguru import logger

from tauro.api.core.config import Settings, get_settings

if TYPE_CHECKING:
    from tauro.cli.config import ConfigManager
    from tauro.cli.execution import ContextInitializer
    from tauro.config.contexts import Context
    from tauro.orchest.store import OrchestratorStore
    from tauro.orchest.scheduler import SchedulerService
    from tauro.orchest.runner import OrchestratorRunner

# Import Tauro core components
# Pre-declare runtime names as Any to satisfy type checkers when the package is absent
ConfigManager: Any = None
ContextInitializer: Any = None
Context: Any = None
OrchestratorStore: Any = None
SchedulerService: Any = None
OrchestratorRunner: Any = None

try:
    from tauro.cli.config import ConfigManager
    from tauro.cli.execution import ContextInitializer
    from tauro.config.contexts import Context
    from tauro.orchest.store import OrchestratorStore
    from tauro.orchest.scheduler import SchedulerService
    from tauro.orchest.runner import OrchestratorRunner

    TAURO_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Tauro core not available: {e}")
    TAURO_AVAILABLE = False
    ConfigManager = None
    ContextInitializer = None
    Context = None
    OrchestratorStore = None
    SchedulerService = None
    OrchestratorRunner = None


# =============================================================================
# Settings Dependency
# =============================================================================


def get_current_settings() -> Settings:
    """Get current settings (cached)"""
    return get_settings()


# =============================================================================
# Tauro ConfigManager
# =============================================================================


@lru_cache(maxsize=1)
def get_config_manager(
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get Tauro ConfigManager instance.

    Cached to avoid recreating on each request.
    """
    if not TAURO_AVAILABLE or ConfigManager is None:
        logger.warning("ConfigManager not available")
        return None

    try:
        cm = ConfigManager(
            base_path=settings.base_path,
            layer_name=settings.layer_name,
            use_case=settings.use_case,
            config_type=settings.config_type,
            interactive=False,
        )
        cm.change_to_config_directory()
        return cm
    except Exception as e:
        logger.error(f"Error creating ConfigManager: {e}")
        return None


# =============================================================================
# Tauro Context
# =============================================================================


def get_context(
    config_manager: Optional[Any] = Depends(get_config_manager),
) -> Optional[Any]:
    """
    Get Tauro Context for current configuration.

    Returns None if ConfigManager is not available.
    """
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


@lru_cache(maxsize=1)
def get_orchestrator_store(
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get OrchestratorStore instance.

    Cached singleton for the application lifecycle.
    """
    if not TAURO_AVAILABLE or OrchestratorStore is None:
        logger.warning("OrchestratorStore not available")
        return None

    try:
        store = OrchestratorStore(
            db_path=settings.database_url.replace("sqlite:///", "")
        )
        return store
    except Exception as e:
        logger.error(f"Error creating OrchestratorStore: {e}")
        return None


# =============================================================================
# OrchestratorRunner
# =============================================================================


def get_orchestrator_runner(
    config_manager: Optional[Any] = Depends(get_config_manager),
    store: Optional[Any] = Depends(get_orchestrator_store),
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get OrchestratorRunner instance.

    Creates a new runner for each request (not cached).
    """
    if not TAURO_AVAILABLE or config_manager is None or store is None:
        return None

    try:
        runner = OrchestratorRunner(
            config_manager=config_manager,
            store=store,
            max_workers=settings.scheduler_max_workers,
        )
        return runner
    except Exception as e:
        logger.error(f"Error creating OrchestratorRunner: {e}")
        return None


# =============================================================================
# SchedulerService
# =============================================================================


@lru_cache(maxsize=1)
def get_scheduler_service(
    runner: Optional[Any] = Depends(get_orchestrator_runner),
    store: Optional[Any] = Depends(get_orchestrator_store),
    settings: Settings = Depends(get_current_settings),
) -> Optional[Any]:
    """
    Get SchedulerService instance.

    Cached singleton that runs in background.
    """
    if not TAURO_AVAILABLE or runner is None or store is None:
        logger.warning("SchedulerService not available")
        return None

    try:
        scheduler = SchedulerService(
            runner=runner,
            store=store,
            poll_interval=settings.scheduler_poll_interval,
        )
        # Do not start the scheduler here; lifecycle management should be
        # performed by the application startup/shutdown handlers.
        return scheduler
    except Exception as e:
        logger.error(f"Error creating SchedulerService: {e}")
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


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "get_current_settings",
    "get_config_manager",
    "get_context",
    "get_orchestrator_store",
    "get_orchestrator_runner",
    "get_scheduler_service",
    "cleanup_scheduler",
]
