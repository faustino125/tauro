"""
MLOps Layer for Tauro.

Provides Model Registry and Experiment Tracking capabilities
with support for both local (Parquet) and Databricks (Unity Catalog) backends.

Factory pattern enables automatic storage backend selection based on execution mode.

NEW: MLflow integration for pipeline tracking with nested runs.
"""

from tauro.core.mlops.model_registry import (
    ModelRegistry,
    ModelMetadata,
    ModelVersion,
)
from tauro.core.mlops.experiment_tracking import (
    ExperimentTracker,
    Experiment,
    Run,
    Metric,
)
from tauro.core.mlops.storage import (
    StorageBackend,
    LocalStorageBackend,
    DatabricksStorageBackend,
)
from tauro.core.mlops.locking import (
    FileLock,
    file_lock,
    OptimisticLock,
)
from tauro.core.mlops.exceptions import (
    MLOpsException,
    ModelNotFoundError,
    ModelVersionConflictError,
    ExperimentNotFoundError,
    RunNotFoundError,
    RunNotActiveError,
    ArtifactNotFoundError,
    InvalidMetricError,
    InvalidParameterError,
    StorageBackendError,
    ModelRegistrationError,
    SchemaValidationError,
    ConcurrencyError,
)
from tauro.core.mlops.factory import (
    StorageBackendFactory,
    ExperimentTrackerFactory,
    ModelRegistryFactory,
    create_storage_backend,
    create_experiment_tracker,
    create_model_registry,
)

# NEW: Validators
from tauro.core.mlops.validators import (
    PathValidator,
    NameValidator,
    MetricValidator,
    ParameterValidator,
    MetadataValidator,
    FrameworkValidator,
    ArtifactValidator,
    validate_model_name,
    validate_experiment_name,
    validate_run_name,
    validate_metric_value,
    validate_parameters,
    validate_tags,
)

# NEW: Transactions
from tauro.core.mlops.transaction import (
    Transaction,
    SafeTransaction,
    TransactionError,
    RollbackError,
)

# MLflow integration (optional)
try:
    from tauro.core.mlops.mlflow_adapter import (
        MLflowPipelineTracker,
        is_mlflow_available,
    )
    from tauro.core.mlops.mlflow_utils import (
        MLflowConfig,
        MLflowHelper,
        setup_mlflow_for_tauro,
    )
    from tauro.core.mlops.mlflow_decorators import (
        mlflow_track,
        log_dataframe_stats,
        log_model_metrics,
        log_confusion_matrix,
        log_feature_importance,
        log_training_curve,
        MLflowNodeContext,
    )

    MLFLOW_INTEGRATION_AVAILABLE = True
except ImportError:
    MLFLOW_INTEGRATION_AVAILABLE = False
    MLflowPipelineTracker = None
    MLflowConfig = None
    MLflowHelper = None

__all__ = [
    # Core classes
    "ModelRegistry",
    "ModelMetadata",
    "ModelVersion",
    "ExperimentTracker",
    "Experiment",
    "Run",
    "Metric",
    # Storage backends
    "StorageBackend",
    "LocalStorageBackend",
    "DatabricksStorageBackend",
    # Factories (NEW - P0)
    "StorageBackendFactory",
    "ExperimentTrackerFactory",
    "ModelRegistryFactory",
    "create_storage_backend",
    "create_experiment_tracker",
    "create_model_registry",
    # Concurrency (P1)
    "FileLock",
    "file_lock",
    "OptimisticLock",
    # Exceptions (P0)
    "MLOpsException",
    "ModelNotFoundError",
    "ModelVersionConflictError",
    "ExperimentNotFoundError",
    "RunNotFoundError",
    "RunNotActiveError",
    "ArtifactNotFoundError",
    "InvalidMetricError",
    "InvalidParameterError",
    "StorageBackendError",
    "ModelRegistrationError",
    "SchemaValidationError",
    "ConcurrencyError",
    # Validators (NEW - CRITICAL)
    "PathValidator",
    "NameValidator",
    "MetricValidator",
    "ParameterValidator",
    "MetadataValidator",
    "FrameworkValidator",
    "ArtifactValidator",
    "validate_model_name",
    "validate_experiment_name",
    "validate_run_name",
    "validate_metric_value",
    "validate_parameters",
    "validate_tags",
    # Transactions (NEW - CRITICAL)
    "Transaction",
    "SafeTransaction",
    "TransactionError",
    "RollbackError",
    # MLflow integration (NEW)
    "MLflowPipelineTracker",
    "MLflowConfig",
    "MLflowHelper",
    "setup_mlflow_for_tauro",
    "mlflow_track",
    "log_dataframe_stats",
    "log_model_metrics",
    "log_confusion_matrix",
    "log_feature_importance",
    "log_training_curve",
    "MLflowNodeContext",
    "is_mlflow_available",
    "MLFLOW_INTEGRATION_AVAILABLE",
]
