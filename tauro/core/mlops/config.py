"""
MLOps initialization and configuration utilities.
"""

import os
from typing import TYPE_CHECKING, Optional, Literal

from loguru import logger

from tauro.core.mlops.storage import (
    StorageBackend,
    LocalStorageBackend,
    DatabricksStorageBackend,
)
from tauro.core.mlops.model_registry import ModelRegistry
from tauro.core.mlops.experiment_tracking import ExperimentTracker
from tauro.core.mlops.factory import (
    ModelRegistryFactory,
    ExperimentTrackerFactory,
)

if TYPE_CHECKING:
    from tauro.core.config.contexts import Context


class MLOpsContext:
    """
    Centralized MLOps context for managing Model Registry and Experiment Tracking.

    PREFERRED: Use from_context() to auto-detect mode and configuration.

    Supports both local and Databricks backends with automatic mode detection.
    """

    def __init__(
        self,
        model_registry: ModelRegistry,
        experiment_tracker: ExperimentTracker,
    ):
        """
        Initialize MLOps context with pre-built components.

        Args:
            model_registry: ModelRegistry instance
            experiment_tracker: ExperimentTracker instance

        Note:
            Prefer using MLOpsContext.from_context() for automatic setup
            from Tauro execution context.
        """
        self.model_registry = model_registry
        self.experiment_tracker = experiment_tracker

        # Expose storage for backward compatibility
        self.storage = model_registry.storage

        logger.info("MLOpsContext initialized")

    @classmethod
    def from_context(
        cls,
        context: "Context",
        registry_path: str = "model_registry",
        tracking_path: str = "experiment_tracking",
        metric_buffer_size: int = 100,
        auto_flush_metrics: bool = True,
    ) -> "MLOpsContext":
        """
        Create MLOpsContext from Tauro execution context (RECOMMENDED).

        Automatically detects execution mode (local/databricks) and creates
        appropriate storage backend from context.global_settings.

        Args:
            context: Tauro execution context
            registry_path: Model registry subdirectory
            tracking_path: Experiment tracking subdirectory
            metric_buffer_size: Metrics buffer size (P1 feature)
            auto_flush_metrics: Auto-flush metrics (P1 feature)

        Returns:
            MLOpsContext with auto-configured components

        Example:
            >>> from tauro.core.config import Context
            >>> from tauro.core.mlops.config import MLOpsContext
            >>>
            >>> context = Context(global_settings=..., ...)
            >>> mlops = MLOpsContext.from_context(context)
            >>>
            >>> # Use transparent API
            >>> exp = mlops.experiment_tracker.create_experiment("exp_1")
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
        Create MLOpsContext from environment variables (LEGACY).

        ⚠️ DEPRECATED: Prefer using from_context() for better integration
        with Tauro's configuration system.

        Environment variables:
        - TAURO_MLOPS_BACKEND: "local" (default) or "databricks"
        - TAURO_MLOPS_PATH: For local backend, base path
        - TAURO_MLOPS_CATALOG: For Databricks backend
        - TAURO_MLOPS_SCHEMA: For Databricks backend
        - DATABRICKS_HOST: Databricks workspace URL
        - DATABRICKS_TOKEN: Databricks API token
        """
        logger.warning(
            "MLOpsContext.from_env() is deprecated. "
            "Use MLOpsContext.from_context() instead for better integration."
        )

        backend = os.getenv("TAURO_MLOPS_BACKEND", "local")

        if backend == "local":
            storage_path = os.getenv("TAURO_MLOPS_PATH", "./mlops_data")
            storage = LocalStorageBackend(storage_path)
            logger.info(f"MLOpsContext: Using LocalStorageBackend at {storage_path}")
        elif backend == "databricks":
            catalog = os.getenv("TAURO_MLOPS_CATALOG", "main")
            schema = os.getenv("TAURO_MLOPS_SCHEMA", "ml_tracking")

            if not catalog or not schema:
                raise ValueError(
                    "TAURO_MLOPS_CATALOG and TAURO_MLOPS_SCHEMA required for Databricks backend"
                )

            storage = DatabricksStorageBackend(
                catalog=catalog,
                schema=schema,
                workspace_url=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN"),
            )
            logger.info(f"MLOpsContext: Using DatabricksStorageBackend at {catalog}.{schema}")
        else:
            raise ValueError(f"Unknown TAURO_MLOPS_BACKEND: {backend}")

        model_registry = ModelRegistry(storage)
        experiment_tracker = ExperimentTracker(storage)

        return cls(
            model_registry=model_registry,
            experiment_tracker=experiment_tracker,
        )


# Global context (optional, for convenience)
_global_context: Optional[MLOpsContext] = None


def init_mlops(backend_type: Literal["local", "databricks"] = "local", **kwargs) -> MLOpsContext:
    """
    Initialize global MLOps context.

    Args:
        backend_type: "local" or "databricks"
        **kwargs: Additional arguments for MLOpsContext.__init__

    Returns:
        MLOpsContext instance
    """
    global _global_context
    _global_context = MLOpsContext(backend_type=backend_type, **kwargs)
    return _global_context


def get_mlops_context() -> MLOpsContext:
    """
    Get global MLOps context (must be initialized first).

    Returns:
        MLOpsContext instance

    Raises:
        RuntimeError: If context not initialized
    """
    if _global_context is None:
        raise RuntimeError("MLOps context not initialized. Call init_mlops() first.")
    return _global_context
