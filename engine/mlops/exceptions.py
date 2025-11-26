"""
Custom exceptions for MLOps layer.

Provides clear, specific error types for better error handling and debugging.
"""


class MLOpsException(Exception):
    """Base exception for all MLOps errors."""

    pass


class ModelNotFoundError(MLOpsException):
    """Raised when a requested model is not found in the registry."""

    def __init__(self, model_name: str, version: int = None):
        self.model_name = model_name
        self.version = version
        if version:
            msg = f"Model '{model_name}' version {version} not found in registry"
        else:
            msg = f"Model '{model_name}' not found in registry"
        super().__init__(msg)


class ModelVersionConflictError(MLOpsException):
    """Raised when attempting to create a model version that already exists."""

    def __init__(self, model_name: str, version: int):
        self.model_name = model_name
        self.version = version
        super().__init__(
            f"Model '{model_name}' version {version} already exists. "
            f"Use a different version or update the existing one."
        )


class ExperimentNotFoundError(MLOpsException):
    """Raised when a requested experiment is not found."""

    def __init__(self, experiment_id: str):
        self.experiment_id = experiment_id
        super().__init__(f"Experiment with ID '{experiment_id}' not found")


class RunNotFoundError(MLOpsException):
    """Raised when a requested run is not found."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        super().__init__(f"Run with ID '{run_id}' not found")


class RunNotActiveError(MLOpsException):
    """Raised when attempting to operate on an inactive run."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        super().__init__(f"Run '{run_id}' is not active. Use get_run() to access completed runs.")


class ArtifactNotFoundError(MLOpsException):
    """Raised when a requested artifact is not found."""

    def __init__(self, artifact_path: str):
        self.artifact_path = artifact_path
        super().__init__(f"Artifact not found at path: {artifact_path}")


class InvalidMetricError(MLOpsException):
    """Raised when attempting to log an invalid metric value."""

    def __init__(self, key: str, value, reason: str):
        self.key = key
        self.value = value
        self.reason = reason
        super().__init__(
            f"Invalid metric '{key}' with value {value!r}: {reason}. "
            f"Metrics must be numeric (int or float)."
        )


class InvalidParameterError(MLOpsException):
    """Raised when attempting to log an invalid parameter."""

    def __init__(self, key: str, reason: str):
        self.key = key
        self.reason = reason
        super().__init__(f"Invalid parameter '{key}': {reason}")


class StorageBackendError(MLOpsException):
    """Raised when storage backend operations fail."""

    def __init__(self, operation: str, path: str, cause: Exception):
        self.operation = operation
        self.path = path
        self.cause = cause
        super().__init__(f"Storage backend error during {operation} at '{path}': {str(cause)}")


class ModelRegistrationError(MLOpsException):
    """Raised when model registration fails."""

    def __init__(self, model_name: str, reason: str):
        self.model_name = model_name
        self.reason = reason
        super().__init__(f"Failed to register model '{model_name}': {reason}")


class SchemaValidationError(MLOpsException):
    """Raised when data schema validation fails."""

    def __init__(self, schema_type: str, reason: str):
        self.schema_type = schema_type
        self.reason = reason
        super().__init__(f"Schema validation failed for {schema_type}: {reason}")


class ConcurrencyError(MLOpsException):
    """Raised when concurrent operations conflict."""

    def __init__(self, resource: str, operation: str):
        self.resource = resource
        self.operation = operation
        super().__init__(
            f"Concurrency conflict: {operation} on '{resource}' failed. "
            f"Another process may be modifying the same resource."
        )
