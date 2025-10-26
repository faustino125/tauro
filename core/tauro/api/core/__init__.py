"""Configuración del módulo core"""

from .config import settings, get_settings, Settings
from .deps import (
    get_current_settings,
    get_config_service,
    get_config_manager,
    get_db_context_initializer,
    get_context,
    get_orchestrator_store,
    get_orchestrator_runner,
    get_scheduler_service,
    resolve_scheduler_service,
)
from .middleware import setup_middleware
from .validators import (
    validate_identifier,
    validate_project_id,
    validate_pipeline_id,
    validate_schedule_id,
    validate_run_id,
)

__all__ = [
    "settings",
    "get_settings",
    "Settings",
    "get_current_settings",
    "get_config_service",
    "get_config_manager",
    "get_db_context_initializer",
    "get_context",
    "get_orchestrator_store",
    "get_orchestrator_runner",
    "get_scheduler_service",
    "resolve_scheduler_service",
    "setup_middleware",
    "validate_identifier",
    "validate_project_id",
    "validate_pipeline_id",
    "validate_schedule_id",
    "validate_run_id",
]
