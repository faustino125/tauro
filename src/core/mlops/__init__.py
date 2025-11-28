"""Tauro MLOps public API.

This module re-exports the most commonly used MLOps components for convenience.
Provides unified access to Model Registry, Experiment Tracking, and supporting
infrastructure for machine learning operations.

Architecture Components:
- protocols: Abstract interfaces (Protocol classes) for type safety
- events: Event-driven observability and metrics collection
- cache: Caching layer with LRU, TTL, and batch writing
- base: Common base classes and mixins for components
- health: Health checks and system diagnostics
- concurrency: File locking, transactions, and synchronization primitives
- mlflow: MLflow integration (consolidated module)
"""

# Config and Context (includes factories)
from core.mlops.config import (
    MLOpsContext,
    MLOpsConfig,
    init_mlops,
    get_mlops_context,
    reset_mlops_context,
    is_mlops_initialized,
    get_current_backend_type,
    get_current_config,
    # Factories (consolidated from factory.py)
    StorageBackendFactory,
    ExperimentTrackerFactory,
    ModelRegistryFactory,
    create_storage_backend,
    create_experiment_tracker,
    create_model_registry,
)

# Protocols (Abstract Interfaces)
from core.mlops.protocols import (
    StorageBackendProtocol,
    ExperimentTrackerProtocol,
    ModelRegistryProtocol,
    ModelMetadataProtocol,
    ModelVersionProtocol,
    RunProtocol,
    ExperimentProtocol,
    LockProtocol,
    EventEmitterProtocol,
    EventCallback,
    SerializableProtocol,
    ValidatorProtocol,
    MLOpsContextProtocol,
)

# Events and Observability
from core.mlops.events import (
    EventType,
    Event,
    EventEmitter,
    MetricsCollector,
    HookType,
    HooksManager,
    AuditLogger,
    AuditEntry,
    get_event_emitter,
    get_metrics_collector,
    get_hooks_manager,
    get_audit_logger,
    emit_event,
)

# Cache Layer
from core.mlops.cache import (
    CacheEntry,
    CacheStats,
    LRUCache,
    TwoLevelCache,
    BatchProcessor,
    BatchOperation,
    BatchResult,
    CacheKeyBuilder,
    CachedStorage,
)

# Base Classes and Mixins
from core.mlops.base import (
    ComponentState,
    ComponentStats,
    BaseMLOpsComponent,
    IndexManagerMixin,
    ValidationMixin,
    PathManager,
    now_iso,
    parse_iso,
    age_seconds,
)

# Health Checks
from core.mlops.health import (
    HealthStatus,
    HealthCheckResult,
    HealthReport,
    HealthCheck,
    StorageHealthCheck,
    MemoryHealthCheck,
    DiskHealthCheck,
    ComponentHealthCheck,
    HealthMonitor,
    get_health_monitor,
    check_health,
    is_healthy,
    is_ready,
)

# Model Registry
from core.mlops.model_registry import (
    ModelRegistry,
    ModelMetadata,
    ModelVersion,
    ModelStage,
)

# Experiment Tracking
from core.mlops.experiment_tracking import (
    ExperimentTracker,
    Experiment,
    Run,
    Metric,
    RunStatus,
)

# Storage Backends
from core.mlops.storage import (
    StorageBackend,
    LocalStorageBackend,
    DatabricksStorageBackend,
)

# Locking Mechanisms and Concurrency (consolidated from locking.py + transaction.py)
from core.mlops.concurrency import (
    FileLock,
    file_lock,
    OptimisticLock,
    ReadWriteLock,
    LockManager,
    LockStats,
    get_lock_manager,
    # Transactions
    Transaction,
    SafeTransaction,
    TransactionState,
    TransactionError,
    RollbackError,
    Operation,
    transactional_operation,
)

# Resilience and Retry
from core.mlops.resilience import (
    RetryConfig,
    with_retry,
    CircuitBreaker,
    CircuitState,
    ResourceTracker,
    ResourceLimits,
    CleanupManager,
    register_cleanup,
    get_cleanup_manager,
)

# Exceptions (Enhanced with error codes)
from core.mlops.exceptions import (
    ErrorCode,
    ErrorContext,
    MLOpsException,
    ModelNotFoundError,
    ModelVersionConflictError,
    ExperimentNotFoundError,
    RunNotFoundError,
    RunNotActiveError,
    RunLimitExceededError,
    ArtifactNotFoundError,
    InvalidMetricError,
    InvalidParameterError,
    StorageBackendError,
    StorageCircuitOpenError,
    ModelRegistrationError,
    SchemaValidationError,
    ConcurrencyError,
    LockTimeoutError,
    ConfigurationError,
    BackendNotConfiguredError,
    ResourceLimitError,
    create_error_response,
    wrap_exception,
)

# Validators
from core.mlops.validators import (
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

# MLflow integration (consolidated module)
try:
    from core.mlops.mlflow import (
        MLflowPipelineTracker,
        MLflowConfig,
        MLflowHelper,
        MLflowNodeContext,
        setup_mlflow_for_tauro,
        mlflow_track,
        log_dataframe_stats,
        log_model_metrics,
        log_confusion_matrix,
        log_feature_importance,
        log_training_curve,
        is_mlflow_available,
        MLFLOW_AVAILABLE,
    )

    MLFLOW_INTEGRATION_AVAILABLE = MLFLOW_AVAILABLE
except ImportError:
    MLFLOW_INTEGRATION_AVAILABLE = False
    MLflowPipelineTracker = None  # type: ignore
    MLflowConfig = None  # type: ignore
    MLflowHelper = None  # type: ignore
    is_mlflow_available = lambda: False  # type: ignore

__all__ = [
    # Config and Context
    "MLOpsContext",
    "MLOpsConfig",
    "init_mlops",
    "get_mlops_context",
    "reset_mlops_context",
    "is_mlops_initialized",
    "get_current_backend_type",
    "get_current_config",
    # Factories
    "StorageBackendFactory",
    "ExperimentTrackerFactory",
    "ModelRegistryFactory",
    "create_storage_backend",
    "create_experiment_tracker",
    "create_model_registry",
    # Protocols (Abstract Interfaces)
    "StorageBackendProtocol",
    "ExperimentTrackerProtocol",
    "ModelRegistryProtocol",
    "ModelMetadataProtocol",
    "ModelVersionProtocol",
    "RunProtocol",
    "ExperimentProtocol",
    "LockProtocol",
    "EventEmitterProtocol",
    "EventCallback",
    "SerializableProtocol",
    "ValidatorProtocol",
    "MLOpsContextProtocol",
    # Events and Observability
    "EventType",
    "Event",
    "EventEmitter",
    "MetricsCollector",
    "HookType",
    "HooksManager",
    "AuditLogger",
    "AuditEntry",
    "get_event_emitter",
    "get_metrics_collector",
    "get_hooks_manager",
    "get_audit_logger",
    "emit_event",
    # Cache Layer
    "CacheEntry",
    "CacheStats",
    "LRUCache",
    "TwoLevelCache",
    "BatchProcessor",
    "BatchOperation",
    "BatchResult",
    "CacheKeyBuilder",
    "CachedStorage",
    # Base Classes and Mixins
    "ComponentState",
    "ComponentStats",
    "BaseMLOpsComponent",
    "IndexManagerMixin",
    "ValidationMixin",
    "PathManager",
    "now_iso",
    "parse_iso",
    "age_seconds",
    # Health Checks
    "HealthStatus",
    "HealthCheckResult",
    "HealthReport",
    "HealthCheck",
    "StorageHealthCheck",
    "MemoryHealthCheck",
    "DiskHealthCheck",
    "ComponentHealthCheck",
    "HealthMonitor",
    "get_health_monitor",
    "check_health",
    "is_healthy",
    "is_ready",
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
    # Concurrency (Locking + Transactions)
    "FileLock",
    "file_lock",
    "OptimisticLock",
    "ReadWriteLock",
    "LockManager",
    "LockStats",
    "get_lock_manager",
    "Transaction",
    "SafeTransaction",
    "TransactionState",
    "TransactionError",
    "RollbackError",
    "Operation",
    "transactional_operation",
    # Resilience and Retry
    "RetryConfig",
    "with_retry",
    "CircuitBreaker",
    "CircuitState",
    "ResourceTracker",
    "ResourceLimits",
    "CleanupManager",
    "register_cleanup",
    "get_cleanup_manager",
    # Exceptions
    "ErrorCode",
    "ErrorContext",
    "MLOpsException",
    "ModelNotFoundError",
    "ModelVersionConflictError",
    "ExperimentNotFoundError",
    "RunNotFoundError",
    "RunNotActiveError",
    "RunLimitExceededError",
    "ArtifactNotFoundError",
    "InvalidMetricError",
    "InvalidParameterError",
    "StorageBackendError",
    "StorageCircuitOpenError",
    "ModelRegistrationError",
    "SchemaValidationError",
    "ConcurrencyError",
    "LockTimeoutError",
    "ConfigurationError",
    "BackendNotConfiguredError",
    "ResourceLimitError",
    "create_error_response",
    "wrap_exception",
    # Validators
    "PathValidator",
    "NameValidator",
    "MetricValidator",
    "ParameterValidator",
    "MetadataValidator",
    "FrameworkValidator",
    "ArtifactValidator",
    "ValidationError",
    "validate_model_name",
    "validate_experiment_name",
    "validate_run_name",
    "validate_framework",
    "validate_artifact_type",
    "validate_metric_value",
    "validate_parameters",
    "validate_tags",
    "validate_description",
    # MLflow Integration
    "MLflowPipelineTracker",
    "MLflowConfig",
    "MLflowHelper",
    "MLflowNodeContext",
    "setup_mlflow_for_tauro",
    "mlflow_track",
    "log_dataframe_stats",
    "log_model_metrics",
    "log_confusion_matrix",
    "log_feature_importance",
    "log_training_curve",
    "is_mlflow_available",
    "MLFLOW_INTEGRATION_AVAILABLE",
]
