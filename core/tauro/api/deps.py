from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Optional

from fastapi import Depends
from loguru import logger  # type: ignore

from tauro.cli.config import ConfigManager
from tauro.cli.execution import ContextInitializer
from tauro.config.contexts import Context
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.runner import OrchestratorRunner
from tauro.orchest.scheduler import SchedulerService

from tauro.api.config import ApiSettings, resolve_db_path


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
    # Intentar cambiar al directorio de config detectado, pero no fallar si no es posible
    try:
        cm.change_to_config_directory()
    except Exception as e:
        # No detener la app: documentar la situación y continuar con rutas que no requieran cambio de CWD
        logger.warning(f"Could not change to config directory: {e}")
    return cm


def get_context(
    env: Optional[str] = None,
    cm: ConfigManager = Depends(get_config_manager),
    settings: ApiSettings = Depends(get_settings),
) -> Context:
    initializer = ContextInitializer(cm)
    # Usar env explícito o default
    chosen_env = env or settings.default_env
    # Nota: para mejorar rendimiento se puede cachear por chosen_env (lru_cache) si Context es thread-safe.
    return initializer.initialize(chosen_env)


@lru_cache(maxsize=1)
def get_store(settings: ApiSettings = Depends(get_settings)) -> OrchestratorStore:
    db_path: Path = resolve_db_path(settings)
    return OrchestratorStore(db_path)


def get_runner(
    context: Context = Depends(get_context),
    store: OrchestratorStore = Depends(get_store),
) -> OrchestratorRunner:
    return OrchestratorRunner(context, store)


@lru_cache(maxsize=1)
def get_scheduler(
    context: Context = Depends(get_context),
    store: OrchestratorStore = Depends(get_store),
) -> SchedulerService:
    return SchedulerService(context, store)
