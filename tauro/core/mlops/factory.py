"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import TYPE_CHECKING, Optional, Any

from loguru import logger

from tauro.core.mlops.storage import (
    StorageBackend,
    LocalStorageBackend,
    DatabricksStorageBackend,
)

if TYPE_CHECKING:
    from tauro.core.config.contexts import Context
    from tauro.core.mlops.experiment_tracking import ExperimentTracker
    from tauro.core.mlops.model_registry import ModelRegistry


class StorageBackendFactory:
    """
    Factory for creating storage backends based on execution mode.

    Automatically selects the appropriate storage backend (Local or Databricks)
    based on the execution context's mode configuration.

    Supports:
    - "local" mode → LocalStorageBackend (Parquet + JSON files)
    - "databricks"/"distributed" mode → DatabricksStorageBackend (Unity Catalog)
    """

    @staticmethod
    def create_from_context(
        context: "Context",
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> StorageBackend:
        """
        Create appropriate storage backend from execution context.

        Args:
            context: Execution context with mode and global settings
            base_path: Override for local storage base path (local mode only)
            **kwargs: Additional backend-specific parameters

        Returns:
            StorageBackend: LocalStorageBackend or DatabricksStorageBackend

        Raises:
            ValueError: If execution mode is invalid or required config is missing

        Example:
            >>> from tauro.core.config import Context
            >>> from tauro.core.mlops.factory import StorageBackendFactory
            >>>
            >>> # Local mode
            >>> context = Context(global_settings={"mode": "local", ...}, ...)
            >>> storage = StorageBackendFactory.create_from_context(context)
            >>> # → Returns LocalStorageBackend
            >>>
            >>> # Databricks mode
            >>> context = Context(global_settings={"mode": "databricks", ...}, ...)
            >>> storage = StorageBackendFactory.create_from_context(context)
            >>> # → Returns DatabricksStorageBackend
        """
        # Get execution mode from context
        mode = getattr(context, "execution_mode", "local")
        if not mode:
            logger.warning("No execution_mode found in context, defaulting to 'local'")
            mode = "local"

        mode = str(mode).lower()

        # Get global settings
        gs = getattr(context, "global_settings", {}) or {}

        if mode == "local":
            # Local mode: Use filesystem storage
            path = base_path or gs.get("mlops_path", "./mlruns")
            logger.info(f"Creating LocalStorageBackend with path: {path}")
            return LocalStorageBackend(base_path=path)

        elif mode in ("databricks", "distributed"):
            # Databricks mode: Use Unity Catalog storage
            databricks_config = gs.get("databricks", {})

            # Extract Databricks configuration
            catalog = kwargs.get("catalog") or databricks_config.get("catalog", "main")
            schema = kwargs.get("schema") or databricks_config.get("schema", "ml_tracking")
            workspace_url = kwargs.get("workspace_url") or databricks_config.get("host")
            token = kwargs.get("token") or databricks_config.get("token")

            logger.info(
                f"Creating DatabricksStorageBackend with catalog: {catalog}, schema: {schema}"
            )

            return DatabricksStorageBackend(
                catalog=catalog,
                schema=schema,
                workspace_url=workspace_url,
                token=token,
            )

        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. "
                f"Supported modes: 'local', 'databricks', 'distributed'"
            )


class ExperimentTrackerFactory:
    """
    Factory for creating ExperimentTracker with automatic storage backend selection.

    Simplifies ExperimentTracker creation by automatically detecting the execution
    mode and instantiating the appropriate storage backend.
    """

    @staticmethod
    def from_context(
        context: "Context",
        tracking_path: str = "experiment_tracking",
        metric_buffer_size: int = 100,
        auto_flush_metrics: bool = True,
        **storage_kwargs: Any,
    ) -> "ExperimentTracker":
        """
        Create ExperimentTracker with appropriate storage backend from context.

        Args:
            context: Execution context with mode configuration
            tracking_path: Subdirectory for tracking data (default: "experiment_tracking")
            metric_buffer_size: Metrics buffer size for incremental flushing (P1 feature)
            auto_flush_metrics: Enable automatic metric flushing (P1 feature)
            **storage_kwargs: Additional arguments for storage backend creation

        Returns:
            ExperimentTracker: Configured instance with auto-selected storage

        Example:
            >>> from tauro.core.config import Context
            >>> from tauro.core.mlops.factory import ExperimentTrackerFactory
            >>>
            >>> context = Context(global_settings=..., ...)
            >>>
            >>> # Auto-detects mode and creates tracker
            >>> tracker = ExperimentTrackerFactory.from_context(context)
            >>>
            >>> # Use transparent API (same for local and Databricks)
            >>> exp = tracker.create_experiment("my_exp", "Description")
            >>> run = tracker.start_run(exp.experiment_id, "run_1")
            >>> tracker.log_metric(run.run_id, "accuracy", 0.95)
            >>> tracker.end_run(run.run_id)
        """
        # Create storage backend automatically based on context
        storage = StorageBackendFactory.create_from_context(context, **storage_kwargs)

        # Import here to avoid circular dependency
        from tauro.core.mlops.experiment_tracking import ExperimentTracker

        logger.info(
            f"Creating ExperimentTracker with {storage.__class__.__name__} "
            f"(tracking_path: {tracking_path})"
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

    Simplifies ModelRegistry creation by automatically detecting the execution
    mode and instantiating the appropriate storage backend.
    """

    @staticmethod
    def from_context(
        context: "Context",
        registry_path: str = "model_registry",
        **storage_kwargs: Any,
    ) -> "ModelRegistry":
        """
        Create ModelRegistry with appropriate storage backend from context.

        Args:
            context: Execution context with mode configuration
            registry_path: Subdirectory for registry data (default: "model_registry")
            **storage_kwargs: Additional arguments for storage backend creation

        Returns:
            ModelRegistry: Configured instance with auto-selected storage

        Example:
            >>> from tauro.core.config import Context
            >>> from tauro.core.mlops.factory import ModelRegistryFactory
            >>>
            >>> context = Context(global_settings=..., ...)
            >>>
            >>> # Auto-detects mode and creates registry
            >>> registry = ModelRegistryFactory.from_context(context)
            >>>
            >>> # Use transparent API (same for local and Databricks)
            >>> version = registry.register_model(
            ...     name="iris_classifier",
            ...     version="v1.0",
            ...     artifact_path="/path/to/model.pkl",
            ...     metadata={"accuracy": 0.95}
            ... )
            >>>
            >>> # Get model back
            >>> model = registry.get_model("iris_classifier", "v1.0")
        """
        # Create storage backend automatically based on context
        storage = StorageBackendFactory.create_from_context(context, **storage_kwargs)

        # Import here to avoid circular dependency
        from tauro.core.mlops.model_registry import ModelRegistry

        logger.info(
            f"Creating ModelRegistry with {storage.__class__.__name__} "
            f"(registry_path: {registry_path})"
        )

        return ModelRegistry(
            storage=storage,
            registry_path=registry_path,
        )


# Convenience aliases for backward compatibility and discoverability
create_storage_backend = StorageBackendFactory.create_from_context
create_experiment_tracker = ExperimentTrackerFactory.from_context
create_model_registry = ModelRegistryFactory.from_context
