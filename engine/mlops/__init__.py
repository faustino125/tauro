"""Tauro MLOps public API.
This module re-exports the most commonly used MLOps components for convenience.
"""

# Config and Context
from engine.mlops.config import MLOpsContext

# Model Registry
from engine.mlops.model_registry import (
    ModelRegistry,
    ModelMetadata,
    ModelVersion,
    ModelStage,
)

# Experiment Tracking
from engine.mlops.experiment_tracking import (
    ExperimentTracker,
    Experiment,
    Run,
    Metric,
    RunStatus,
)

# Storage Backends
from engine.mlops.storage import (
    StorageBackend,
    LocalStorageBackend,
    DatabricksStorageBackend,
)

# Locking Mechanisms
from engine.mlops.locking import (
    FileLock,
    file_lock,
    OptimisticLock,
)

# Exceptions
from engine.mlops.exceptions import (
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

# Factories
from engine.mlops.factory import (
    StorageBackendFactory,
    ExperimentTrackerFactory,
    ModelRegistryFactory,
)

# Validators
from engine.mlops.validators import (
    PathValidator,
    NameValidator,
    MetricValidator,
    ParameterValidator,
    MetadataValidator,
    FrameworkValidator,
    ArtifactValidator,
    ValidationError,
    validate_model_name,
    validate_experiment_name,
    validate_run_name,
    validate_framework,
    validate_artifact_type,
    validate_metric_value,
    validate_parameters,
    validate_tags,
    validate_description,
)

# Transactions
from engine.mlops.transaction import (
    Transaction,
    SafeTransaction,
    TransactionError,
    RollbackError,
    Operation,
)

# MLflow integration (optional)
try:
    from engine.mlops.mlflow_adapter import (
        MLflowPipelineTracker,
        is_mlflow_available,
    )
    from engine.mlops.mlflow_utils import (
        MLflowConfig,
        MLflowHelper,
        setup_mlflow_for_tauro,
    )
    from engine.mlops.mlflow_decorators import (
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
    # Config and Context
    "MLOpsContext",
    # Model Registry - Core
    "ModelRegistry",
    "ModelMetadata",
    "ModelVersion",
    "ModelStage",
    # Experiment Tracking - Core
    "ExperimentTracker",
    "Experiment",
    "Run",
    "Metric",
    "RunStatus",
    # Storage Backends
    "StorageBackend",
    "LocalStorageBackend",
    "DatabricksStorageBackend",
    # Factories
    "StorageBackendFactory",
    "ExperimentTrackerFactory",
    "ModelRegistryFactory",
    # Locking Mechanisms
    "FileLock",
    "file_lock",
    "OptimisticLock",
    # Exceptions
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
    # Validators - Classes
    "PathValidator",
    "NameValidator",
    "MetricValidator",
    "ParameterValidator",
    "MetadataValidator",
    "FrameworkValidator",
    "ArtifactValidator",
    "ValidationError",
    # Validators - Functions
    "validate_model_name",
    "validate_experiment_name",
    "validate_run_name",
    "validate_framework",
    "validate_artifact_type",
    "validate_metric_value",
    "validate_parameters",
    "validate_tags",
    "validate_description",
    # Transactions
    "Transaction",
    "SafeTransaction",
    "TransactionError",
    "RollbackError",
    "Operation",
    # MLflow Integration
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
