import atexit
import os
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional

from loguru import logger

from core.mlops.storage import (
    LocalStorageBackend,
    DatabricksStorageBackend,
    StorageBackend,
)
from core.mlops.model_registry import ModelRegistry
from core.mlops.experiment_tracking import ExperimentTracker

if TYPE_CHECKING:
    from core.config.contexts import Context


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

    catalog: Optional[str] = None  # For Databricks: Unity Catalog name

    schema: Optional[str] = None  # For Databricks: Schema name

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
            catalog=os.getenv("DATABRICKS_CATALOG"),
            schema=os.getenv("DATABRICKS_SCHEMA"),
            max_retries=int(os.getenv("TAURO_MLOPS_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
            max_active_runs=int(
                os.getenv("TAURO_MLOPS_MAX_ACTIVE_RUNS", str(DEFAULT_MAX_ACTIVE_RUNS))
            ),
        )

    def validate(self) -> None:
        """

        Validate configuration.

        """

        if self.backend_type not in ("local", "databricks", "distributed"):
            raise ValueError(f"Invalid backend_type: {self.backend_type}")

        if self.backend_type in ("databricks", "distributed"):
            if not self.catalog and not os.getenv("DATABRICKS_CATALOG"):
                logger.warning(
                    "No catalog specified for Databricks backend. "
                    "Set 'catalog' parameter or DATABRICKS_CATALOG environment variable. "
                    "Defaulting to 'main'."
                )

        if self.max_active_runs < 1:
            raise ValueError("max_active_runs must be at least 1")

        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")


class StorageBackendFactory:

    """

    Factory for creating storage backends based on execution mode.

    """

    @staticmethod
    def _log_active_env(ctx: "Context") -> Optional[str]:
        """Log the active environment from context."""

        active = getattr(ctx, "env", None) or getattr(ctx, "environment", None)

        if active:
            logger.debug(f"MLOps using active environment from exec: '{active}'")

        else:
            logger.debug("No active environment found in context, using defaults")

        return active

    @staticmethod
    def _get_execution_mode(context: "Context") -> str:
        """Get execution mode from context with fallback."""

        mode = getattr(context, "execution_mode", "local")

        if not mode:
            logger.warning("No execution_mode found in context, defaulting to 'local'")

            mode = "local"

        return str(mode).lower()

    @staticmethod
    def _resolve_pipeline_mlops_path(context: "Context", pipeline_name: str) -> Optional[str]:
        """

        Resolve MLOps path from pipeline-specific output directory.

        """

        try:
            from pathlib import Path

            # Strategy 1: Get the output path directly from context if available

            # This is set during pipeline execution and includes environment

            context_output = getattr(context, "output_path", None)

            if context_output:
                # Get the parent directory that would be used for this pipeline's outputs

                # The output path structure is: {output_path}/{environment}/{schema}/{pipeline_name}/...

                active_env = getattr(context, "env", None) or getattr(context, "environment", None)

                if active_env:
                    # Extract schema and sub_folder from pipeline_name

                    # e.g., "uc.clustering_costo_vida" -> schema="uc", sub_folder="clustering_costo_vida"

                    name_parts = pipeline_name.split(".")

                    if len(name_parts) >= 2:
                        schema = name_parts[0]

                        sub_folder = name_parts[1]

                        # Build path: {output_path}/{env}/{schema}/{sub_folder}

                        path = str(Path(context_output) / active_env / schema / sub_folder)

                        logger.debug(
                            f"[Pipeline MLOps] Using context output_path + env for '{pipeline_name}': {path}"
                        )

                        return path

            # Strategy 2: Use output_path without environment if available

            if context_output:
                name_parts = pipeline_name.split(".")

                if len(name_parts) >= 2:
                    schema = name_parts[0]

                    sub_folder = name_parts[1]

                    path = str(Path(context_output) / schema / sub_folder)

                    logger.debug(
                        f"[Pipeline MLOps] Using context output_path for '{pipeline_name}': {path}"
                    )

                    return path

            logger.debug(
                f"[Pipeline MLOps] Could not resolve pipeline-specific path for '{pipeline_name}'"
            )

            return None

        except Exception as e:
            logger.debug(f"[Pipeline MLOps] Error resolving path for '{pipeline_name}': {e}")

            return None

    @staticmethod
    def _resolve_local_mlops_path(
        context: "Context",
        base_path: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ) -> Optional[str]:
        """

        Resolve the local MLOps path considering priority and pipeline-specific output.

        """

        # Log context state for debugging

        output_path = getattr(context, "output_path", None)

        active_env = getattr(context, "env", None) or getattr(context, "environment", None)

        active_pipeline = pipeline_name or getattr(context, "active_pipeline_name", None)

        logger.debug(
            f"[MLOps Path Resolution] Context info: "
            f"output_path={output_path}, env={active_env}, pipeline={active_pipeline}"
        )

        # Priority 1: Explicit path

        if base_path:
            logger.debug(f"[MLOps Path Priority 1] Using explicit base_path: {base_path}")

            return base_path

        # Priority 2: mlops_path in global settings

        gs = getattr(context, "global_settings", {}) or {}

        if "mlops_path" in gs:
            path = gs["mlops_path"]

            logger.debug(f"[MLOps Path Priority 2] Using mlops_path from global_settings: {path}")

            return path

        # Priority 3: Pipeline-specific output path

        actual_pipeline_name = pipeline_name or getattr(context, "active_pipeline_name", None)

        if actual_pipeline_name:
            resolved = StorageBackendFactory._resolve_pipeline_mlops_path(
                context, actual_pipeline_name
            )

            if resolved:
                logger.debug(
                    f"[MLOps Path Priority 3] Using pipeline-specific path for '{actual_pipeline_name}': {resolved}"
                )

                return resolved

            else:
                logger.debug(
                    f"[MLOps Path Priority 3] Failed to resolve pipeline-specific path for '{actual_pipeline_name}', trying next priority"
                )

        # Priority 4: Auto-resolve from output_path and environment

        output_path = getattr(context, "output_path", None)

        if output_path:
            from pathlib import Path

            active_env = getattr(context, "env", None) or getattr(context, "environment", None)

            if active_env:
                path = str(Path(output_path) / active_env)

                logger.debug(
                    f"[MLOps Path Priority 4] Using output_path with environment '{active_env}' for mlops: {path}"
                )

            else:
                path = str(Path(output_path))

                logger.debug(
                    f"[MLOps Path Priority 4] Using output_path for mlops (no environment detected): {path}"
                )

            return path

        import os

        cwd = os.getcwd()

        logger.critical(
            f"[MLOps Path Priority 5 - FAIL SAFE] Cannot resolve proper MLOps path! "
            f"Missing required context information (output_path and/or environment). "
            f"Current directory: {cwd}. "
            f"MLOps will NOT be initialized to prevent creating directories in wrong location."
        )

        return None

    @staticmethod
    def create_from_context(
        context: "Context",
        base_path: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[StorageBackend]:
        """

        Create appropriate storage backend from execution context.



        Returns None if MLOps path cannot be safely resolved (e.g., missing context).

        """

        mode = StorageBackendFactory._get_execution_mode(context)

        _ = StorageBackendFactory._log_active_env(context)

        if mode == "local":
            path = StorageBackendFactory._resolve_local_mlops_path(
                context, base_path, pipeline_name
            )

            # Fail-safe: If path resolution failed, don't create storage

            if path is None:
                logger.error(
                    "MLOps storage backend initialization failed: "
                    "Could not resolve safe MLOps directory path. "
                    "MLOps will not be available."
                )

                return None

            logger.info(f"Creating LocalStorageBackend with path: {path}")

            return LocalStorageBackend(base_path=path)

        elif mode in ("databricks", "distributed"):
            # For Databricks, user must provide catalog/schema or configure via environment

            # This delegates responsibility to user/src/core/io

            catalog_to_use = catalog or os.getenv("DATABRICKS_CATALOG", "main")

            schema_to_use = schema or os.getenv("DATABRICKS_SCHEMA", "ml_tracking")

            logger.info(
                f"Creating DatabricksStorageBackend with catalog: {catalog_to_use}, "
                f"schema: {schema_to_use}"
            )

            logger.info(
                "Note: Databricks credentials should be configured via environment "
                "(DATABRICKS_HOST, DATABRICKS_TOKEN) or Databricks CLI"
            )

            return DatabricksStorageBackend(
                catalog=catalog_to_use,
                schema=schema_to_use,
                workspace_url=kwargs.get("workspace_url"),
                token=kwargs.get("token"),
            )

        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. "
                f"Supported modes: 'local', 'databricks', 'distributed'"
            )


class TrackingURIResolver:

    """

    Resolver for tracking URI paths that may be relative to output_path/environment.

    """

    @staticmethod
    def resolve_tracking_uri(
        tracking_uri: Optional[str],
        context: Optional["Context"] = None,
    ) -> Optional[str]:
        """

        Resolve tracking_uri considering environment structure.



        If tracking_uri is relative and context is available, it will be resolved as:

        output_path/{environment}/tracking_uri



        Args:

            tracking_uri: The tracking URI (can be relative or absolute)

            context: The execution context (optional)



        Returns:

            Resolved tracking URI or original if not applicable

        """

        if not tracking_uri:
            return tracking_uri

        # Skip if already absolute or remote

        if (
            tracking_uri.startswith("/")
            or tracking_uri.startswith("\\")
            or "://" in tracking_uri  # Remote URI (s3://, http://, etc.)
        ):
            return tracking_uri

        # Try to resolve relative path using context

        if context is None:
            return tracking_uri

        output_path = getattr(context, "output_path", None)

        if not output_path:
            return tracking_uri

        # Get active environment

        active_env = getattr(context, "env", None) or getattr(context, "environment", None)

        if not active_env:
            # No environment, just use output_path

            from pathlib import Path

            resolved = str(Path(output_path) / tracking_uri)

            logger.debug(f"Resolved tracking_uri (no env): {resolved}")

            return resolved

        # Resolve with environment

        from pathlib import Path

        resolved = str(Path(output_path) / active_env / tracking_uri)

        logger.debug(f"Resolved tracking_uri for env '{active_env}': {resolved}")

        return resolved


class ExperimentTrackerFactory:

    """

    Factory for creating ExperimentTracker with automatic storage backend selection.

    """

    @staticmethod
    def from_context(
        context: "Context",
        tracking_path: str = "experiment_tracking",
        metric_buffer_size: int = 100,
        auto_flush_metrics: bool = True,
        pipeline_name: Optional[str] = None,
        **storage_kwargs: Any,
    ) -> Optional[ExperimentTracker]:
        """

        Create ExperimentTracker with appropriate storage backend from context.



        Returns None if storage backend cannot be safely initialized.

        """

        storage = StorageBackendFactory.create_from_context(
            context, pipeline_name=pipeline_name, **storage_kwargs
        )

        # Fail-safe: If storage creation failed, return None

        if storage is None:
            logger.warning(
                "ExperimentTracker creation failed: Could not initialize storage backend. "
                "MLOps tracking will not be available."
            )

            return None

        # ✅ CAPTURE ENVIRONMENT for tracker initialization

        active_env = getattr(context, "env", None) or getattr(context, "environment", None)

        logger.info(
            f"Creating ExperimentTracker with {storage.__class__.__name__}"
            f"{f' (env: {active_env})' if active_env else ''}"
        )

        return ExperimentTracker(
            storage=storage,
            tracking_path=tracking_path,
            metric_buffer_size=metric_buffer_size,
            auto_flush_metrics=auto_flush_metrics,
        )


class ModelRegistryFactory:

    """

    Factory for creating ModelRegistry with automatic storage backend selection.

    """

    @staticmethod
    def from_context(
        context: "Context",
        registry_path: str = "model_registry",
        pipeline_name: Optional[str] = None,
        **storage_kwargs: Any,
    ) -> Optional[ModelRegistry]:
        """

        Create ModelRegistry with appropriate storage backend from context.



        Returns None if storage backend cannot be safely initialized.

        """

        storage = StorageBackendFactory.create_from_context(
            context, pipeline_name=pipeline_name, **storage_kwargs
        )

        # Fail-safe: If storage creation failed, return None

        if storage is None:
            logger.warning(
                "ModelRegistry creation failed: Could not initialize storage backend. "
                "MLOps model registry will not be available."
            )

            return None

        # ✅ CAPTURE ENVIRONMENT for registry initialization

        active_env = getattr(context, "env", None) or getattr(context, "environment", None)

        logger.info(
            f"Creating ModelRegistry with {storage.__class__.__name__}"
            f"{f' (env: {active_env})' if active_env else ''}"
        )

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
                    catalog=legacy_config.catalog or os.getenv("DATABRICKS_CATALOG", "main"),
                    schema=legacy_config.schema or os.getenv("DATABRICKS_SCHEMA", "ml_tracking"),
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
            # For Databricks, rely on standard environment variables and user configuration

            storage = DatabricksStorageBackend(
                catalog=config.catalog or os.getenv("DATABRICKS_CATALOG", "main"),
                schema=config.schema or os.getenv("DATABRICKS_SCHEMA", "ml_tracking"),
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
        pipeline_name: Optional[str] = None,
    ) -> "MLOpsContext":
        """

        Create MLOpsContext from Tauro execution context.



        Args:

            context: Execution context

            registry_path: Path for model registry

            tracking_path: Path for experiment tracking

            metric_buffer_size: Metric buffer size

            auto_flush_metrics: Whether to auto-flush metrics

            max_active_runs: Maximum active runs

            pipeline_name: Pipeline name for pipeline-specific path resolution

        """

        # ✅ CAPTURE ACTIVE ENVIRONMENT from context (set by exec)

        active_env = getattr(context, "env", None) or getattr(context, "environment", None)

        mode = getattr(context, "execution_mode", "local")

        logger.debug(
            f"MLOpsContext.from_context: active_env='{active_env}', mode='{mode}', pipeline='{pipeline_name}'"
        )

        # Use factories for automatic mode detection

        # Both factories will capture the same active_env from context

        model_registry = ModelRegistryFactory.from_context(
            context,
            registry_path=registry_path,
            pipeline_name=pipeline_name,
        )

        experiment_tracker = ExperimentTrackerFactory.from_context(
            context,
            tracking_path=tracking_path,
            metric_buffer_size=metric_buffer_size,
            auto_flush_metrics=auto_flush_metrics,
            pipeline_name=pipeline_name,
        )

        logger.info(
            f"MLOpsContext created from context "
            f"(mode: {mode}"
            f"{f', env: {active_env}' if active_env else ''}"
            f"{f', pipeline: {pipeline_name}' if pipeline_name else ''})"
        )

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
