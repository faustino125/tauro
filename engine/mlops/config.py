import atexit
import os
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional

from loguru import logger

from engine.mlops.storage import (
    LocalStorageBackend,
    DatabricksStorageBackend,
    StorageBackend,
)
from engine.mlops.model_registry import ModelRegistry
from engine.mlops.experiment_tracking import ExperimentTracker

if TYPE_CHECKING:
    from engine.config.contexts import Context


DEFAULT_STORAGE_PATH = "./mlops_data"
DEFAULT_REGISTRY_PATH = "model_registry"
DEFAULT_TRACKING_PATH = "experiment_tracking"
DEFAULT_METRIC_BUFFER_SIZE = 100
DEFAULT_MAX_ACTIVE_RUNS = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_STALE_RUN_AGE = 3600.0


@dataclass
class MLOpsConfig:
    """
    Configuration for MLOps initialization.
    """

    # Backend configuration
    backend_type: Literal["local", "databricks", "distributed"] = "local"
    storage_path: str = DEFAULT_STORAGE_PATH
    catalog: Optional[str] = None
    schema: Optional[str] = None

    # Registry configuration
    registry_path: str = DEFAULT_REGISTRY_PATH
    model_retention_days: int = 90
    max_versions_per_model: int = 100

    # Tracking configuration
    tracking_path: str = DEFAULT_TRACKING_PATH
    metric_buffer_size: int = DEFAULT_METRIC_BUFFER_SIZE
    auto_flush_metrics: bool = True
    max_active_runs: int = DEFAULT_MAX_ACTIVE_RUNS
    auto_cleanup_stale: bool = True
    stale_run_age_seconds: float = DEFAULT_STALE_RUN_AGE

    # Resilience configuration
    enable_retry: bool = True
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: float = DEFAULT_RETRY_DELAY
    enable_circuit_breaker: bool = False

    # Additional options
    extra_options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "MLOpsConfig":
        """
        Create configuration from environment variables.
        """
        return cls(
            backend_type=os.getenv("TAURO_MLOPS_BACKEND", "local"),  # type: ignore
            storage_path=os.getenv("TAURO_MLOPS_PATH", DEFAULT_STORAGE_PATH),
            catalog=os.getenv("TAURO_MLOPS_CATALOG"),
            schema=os.getenv("TAURO_MLOPS_SCHEMA"),
            max_retries=int(os.getenv("TAURO_MLOPS_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
            max_active_runs=int(
                os.getenv("TAURO_MLOPS_MAX_ACTIVE_RUNS", str(DEFAULT_MAX_ACTIVE_RUNS))
            ),
        )

    def validate(self) -> None:
        """Validate configuration."""
        if self.backend_type not in ("local", "databricks", "distributed"):
            raise ValueError(f"Invalid backend_type: {self.backend_type}")

        if self.backend_type in ("databricks", "distributed"):
            if not self.catalog and not os.getenv("TAURO_MLOPS_CATALOG"):
                raise ValueError("catalog is required for Databricks backend")

        if self.max_active_runs < 1:
            raise ValueError("max_active_runs must be at least 1")

        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")


class StorageBackendFactory:
    """Factory for creating storage backends based on execution mode."""

    @staticmethod
    def create_from_context(
        context: "Context",
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> StorageBackend:
        """Create appropriate storage backend from execution context."""
        mode = getattr(context, "execution_mode", "local")
        if not mode:
            logger.warning("No execution_mode found in context, defaulting to 'local'")
            mode = "local"

        mode = str(mode).lower()
        gs = getattr(context, "global_settings", {}) or {}

        if mode == "local":
            path = base_path or gs.get("mlops_path", "./mlruns")
            logger.info(f"Creating LocalStorageBackend with path: {path}")
            return LocalStorageBackend(base_path=path)

        elif mode in ("databricks", "distributed"):
            databricks_config = gs.get("databricks", {})
            catalog = kwargs.get("catalog") or databricks_config.get("catalog", "main")
            schema = kwargs.get("schema") or databricks_config.get("schema", "ml_tracking")
            workspace_url = kwargs.get("workspace_url") or databricks_config.get("host")
            token = kwargs.get("token") or databricks_config.get("token")

            logger.info(f"Creating DatabricksStorageBackend with catalog: {catalog}")
            return DatabricksStorageBackend(
                catalog=catalog,
                schema=schema,
                workspace_url=workspace_url,
                token=token,
            )

        else:
            raise ValueError(f"Invalid execution mode: {mode}")


class ExperimentTrackerFactory:
    """Factory for creating ExperimentTracker with automatic storage backend selection."""

    @staticmethod
    def from_context(
        context: "Context",
        tracking_path: str = "experiment_tracking",
        metric_buffer_size: int = 100,
        auto_flush_metrics: bool = True,
        **storage_kwargs: Any,
    ) -> ExperimentTracker:
        """Create ExperimentTracker with appropriate storage backend from context."""
        storage = StorageBackendFactory.create_from_context(context, **storage_kwargs)

        logger.info(f"Creating ExperimentTracker with {storage.__class__.__name__}")
        return ExperimentTracker(
            storage=storage,
            tracking_path=tracking_path,
            metric_buffer_size=metric_buffer_size,
            auto_flush_metrics=auto_flush_metrics,
        )


class ModelRegistryFactory:
    """Factory for creating ModelRegistry with automatic storage backend selection."""

    @staticmethod
    def from_context(
        context: "Context",
        registry_path: str = "model_registry",
        **storage_kwargs: Any,
    ) -> ModelRegistry:
        """Create ModelRegistry with appropriate storage backend from context."""
        storage = StorageBackendFactory.create_from_context(context, **storage_kwargs)

        logger.info(f"Creating ModelRegistry with {storage.__class__.__name__}")
        return ModelRegistry(
            storage=storage,
            registry_path=registry_path,
        )


# Convenience aliases
create_storage_backend = StorageBackendFactory.create_from_context
create_experiment_tracker = ExperimentTrackerFactory.from_context
create_model_registry = ModelRegistryFactory.from_context


class MLOpsContext:
    """
    Centralized MLOps context for managing Model Registry and Experiment Tracking.
    """

    _lock = threading.Lock()

    def __new__(
        cls,
        model_registry: Optional[ModelRegistry] = None,
        experiment_tracker: Optional[ExperimentTracker] = None,
        config: Optional[MLOpsConfig] = None,
        # Legacy kwargs support
        backend_type: Optional[str] = None,
        storage_path: Optional[str] = None,
        **kwargs,
    ):
        """
        Create MLOpsContext with flexible initialization.
        """
        instance = super().__new__(cls)

        # Check if using legacy API
        if backend_type is not None or storage_path is not None:
            # Legacy initialization - create from config
            logger.debug("Using legacy MLOpsContext initialization")
            legacy_config = MLOpsConfig(
                backend_type=backend_type or "local",  # type: ignore
                storage_path=storage_path or DEFAULT_STORAGE_PATH,
                **{k: v for k, v in kwargs.items() if k in MLOpsConfig.__dataclass_fields__},
            )

            # Create components from config
            resolved_backend = _resolve_backend_type(legacy_config.backend_type)

            if resolved_backend == "local":
                storage = LocalStorageBackend(base_path=legacy_config.storage_path)
            else:
                storage = DatabricksStorageBackend(
                    catalog=legacy_config.catalog or os.getenv("TAURO_MLOPS_CATALOG", "main"),
                    schema=legacy_config.schema or os.getenv("TAURO_MLOPS_SCHEMA", "ml_tracking"),
                )

            model_registry = ModelRegistry(
                storage=storage,
                registry_path=legacy_config.registry_path,
            )

            experiment_tracker = ExperimentTracker(
                storage=storage,
                tracking_path=legacy_config.tracking_path,
                metric_buffer_size=legacy_config.metric_buffer_size,
                auto_flush_metrics=legacy_config.auto_flush_metrics,
                max_active_runs=legacy_config.max_active_runs,
            )

            config = legacy_config

        # Store components on instance
        instance._model_registry = model_registry
        instance._experiment_tracker = experiment_tracker
        instance._config = config

        return instance

    def __init__(
        self,
        model_registry: Optional[ModelRegistry] = None,
        experiment_tracker: Optional[ExperimentTracker] = None,
        config: Optional[MLOpsConfig] = None,
        backend_type: Optional[str] = None,
        storage_path: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize MLOps context.
        """
        # Components set in __new__
        self.model_registry = self._model_registry
        self.experiment_tracker = self._experiment_tracker
        self.config = self._config

        # Expose storage for backward compatibility
        if self.model_registry:
            self.storage = self.model_registry.storage
        else:
            self.storage = None

        logger.info("MLOpsContext initialized")

    @classmethod
    def from_config(cls, config: MLOpsConfig) -> "MLOpsContext":
        """
        Create MLOpsContext from configuration object.
        """
        config.validate()

        # Resolve backend type
        resolved_backend = _resolve_backend_type(config.backend_type)

        # Create storage backend
        if resolved_backend == "local":
            storage = LocalStorageBackend(base_path=config.storage_path)
        else:
            storage = DatabricksStorageBackend(
                catalog=config.catalog or os.getenv("TAURO_MLOPS_CATALOG", "main"),
                schema=config.schema or os.getenv("TAURO_MLOPS_SCHEMA", "ml_tracking"),
            )

        # Create MLOps components with full configuration
        model_registry = ModelRegistry(
            storage=storage,
            registry_path=config.registry_path,
        )

        experiment_tracker = ExperimentTracker(
            storage=storage,
            tracking_path=config.tracking_path,
            metric_buffer_size=config.metric_buffer_size,
            auto_flush_metrics=config.auto_flush_metrics,
            max_active_runs=config.max_active_runs,
            auto_cleanup_stale=config.auto_cleanup_stale,
            stale_run_age_seconds=config.stale_run_age_seconds,
        )

        logger.info(
            f"MLOpsContext created from config "
            f"(backend: {resolved_backend}, max_runs: {config.max_active_runs})"
        )

        return cls(
            model_registry=model_registry,
            experiment_tracker=experiment_tracker,
            config=config,
        )

    @classmethod
    def from_context(
        cls,
        context: "Context",
        registry_path: str = DEFAULT_REGISTRY_PATH,
        tracking_path: str = DEFAULT_TRACKING_PATH,
        metric_buffer_size: int = DEFAULT_METRIC_BUFFER_SIZE,
        auto_flush_metrics: bool = True,
        max_active_runs: int = DEFAULT_MAX_ACTIVE_RUNS,
    ) -> "MLOpsContext":
        """
        Create MLOpsContext from Tauro execution context.
        """
        # Use factories for automatic mode detection
        model_registry = ModelRegistryFactory.from_context(
            context,
            registry_path=registry_path,
        )

        experiment_tracker = ExperimentTrackerFactory.from_context(
            context,
            tracking_path=tracking_path,
            metric_buffer_size=metric_buffer_size,
            auto_flush_metrics=auto_flush_metrics,
        )

        mode = getattr(context, "execution_mode", "local")
        logger.info(f"MLOpsContext created from context (mode: {mode})")

        return cls(
            model_registry=model_registry,
            experiment_tracker=experiment_tracker,
        )

    @classmethod
    def from_env(cls) -> "MLOpsContext":
        """
        Create MLOpsContext from environment variables.
        """
        config = MLOpsConfig.from_env()
        return cls.from_config(config)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get combined statistics from all components.
        """
        return {
            "model_registry": self.model_registry.get_stats()
            if hasattr(self.model_registry, "get_stats")
            else {},
            "experiment_tracker": self.experiment_tracker.get_stats(),
            "storage": self.storage.get_stats() if hasattr(self.storage, "get_stats") else {},
            "config": {
                "backend_type": self.config.backend_type if self.config else "unknown",
                "storage_path": self.config.storage_path if self.config else "unknown",
            },
        }

    def cleanup(self) -> None:
        """
        Clean up resources and close any open connections.
        """
        # Cleanup stale runs
        try:
            cleaned = self.experiment_tracker.cleanup_stale_runs()
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} stale runs during shutdown")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


_global_context: Optional[MLOpsContext] = None
_context_lock = threading.Lock()


def init_mlops(
    backend_type: Literal["local", "databricks", "distributed"] = "local",
    storage_path: str = DEFAULT_STORAGE_PATH,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    registry_path: str = DEFAULT_REGISTRY_PATH,
    tracking_path: str = DEFAULT_TRACKING_PATH,
    metric_buffer_size: int = DEFAULT_METRIC_BUFFER_SIZE,
    auto_flush_metrics: bool = True,
    max_active_runs: int = DEFAULT_MAX_ACTIVE_RUNS,
    auto_cleanup_stale: bool = True,
    stale_run_age_seconds: float = DEFAULT_STALE_RUN_AGE,
    config: Optional[MLOpsConfig] = None,
    **kwargs,
) -> MLOpsContext:
    """
    Initialize global MLOps context.
    """
    global _global_context

    with _context_lock:
        # Use provided config or create from parameters
        if config is not None:
            mlops_config = config
        else:
            mlops_config = MLOpsConfig(
                backend_type=backend_type,
                storage_path=storage_path,
                catalog=catalog,
                schema=schema,
                registry_path=registry_path,
                tracking_path=tracking_path,
                metric_buffer_size=metric_buffer_size,
                auto_flush_metrics=auto_flush_metrics,
                max_active_runs=max_active_runs,
                auto_cleanup_stale=auto_cleanup_stale,
                stale_run_age_seconds=stale_run_age_seconds,
                extra_options=kwargs,
            )

        # Create context from config
        _global_context = MLOpsContext.from_config(mlops_config)

        # Register cleanup on exit
        atexit.register(_cleanup_on_exit)

        logger.info(
            f"MLOps initialized successfully "
            f"(backend: {mlops_config.backend_type}, "
            f"max_runs: {mlops_config.max_active_runs})"
        )

        return _global_context


def _cleanup_on_exit() -> None:
    """Clean up MLOps context on process exit."""
    global _global_context
    if _global_context is not None:
        try:
            _global_context.cleanup()
        except Exception as e:
            logger.warning(f"Error during MLOps cleanup: {e}")


def _resolve_backend_type(
    backend_type: Optional[Literal["local", "databricks", "distributed"]]
) -> Literal["local", "databricks"]:
    """
    Resolve the backend type, handling aliases.
    """
    if backend_type is None:
        backend_type = "local"

    # Handle 'distributed' as alias for 'databricks'
    if backend_type == "distributed":
        return "databricks"

    if backend_type not in ("local", "databricks"):
        raise ValueError(
            f"Unknown backend_type: {backend_type}. "
            f"Supported: 'local', 'databricks', 'distributed'"
        )

    return backend_type


def get_mlops_context() -> MLOpsContext:
    """
    Get global MLOps context.
    """
    with _context_lock:
        if _global_context is None:
            raise RuntimeError("MLOps context not initialized. Call init_mlops() first.")
        return _global_context


def reset_mlops_context() -> None:
    """
    Reset the global MLOps context.
    """
    global _global_context

    with _context_lock:
        if _global_context is not None:
            try:
                _global_context.cleanup()
            except Exception as e:
                logger.warning(f"Error during context cleanup: {e}")

        _global_context = None
        logger.info("MLOps context reset")


def is_mlops_initialized() -> bool:
    """
    Check if MLOps context has been initialized.
    """
    with _context_lock:
        return _global_context is not None


def get_current_backend_type() -> Optional[Literal["local", "databricks"]]:
    """
    Get the current backend type if MLOps is initialized.
    """
    with _context_lock:
        if _global_context is None:
            return None

        storage = _global_context.storage
        if isinstance(storage, LocalStorageBackend):
            return "local"
        elif isinstance(storage, DatabricksStorageBackend):
            return "databricks"

        return None


def get_current_config() -> Optional[MLOpsConfig]:
    """
    Get the current MLOps configuration if initialized.
    """
    with _context_lock:
        if _global_context is None:
            return None
        return _global_context.config
