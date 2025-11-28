from __future__ import annotations

from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

if TYPE_CHECKING:
    import pandas as pd


T = TypeVar("T")
ConfigT = TypeVar("ConfigT", bound="BaseConfig")


@dataclass(frozen=True)
class BaseConfig:
    """Base configuration class with validation support."""

    def validate(self) -> None:
        """
        Validate configuration values.
        """
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls: type[ConfigT], data: Dict[str, Any]) -> ConfigT:
        """Create configuration from dictionary."""
        return cls(**data)


@dataclass
class StorageMetadataProtocol:
    """Protocol for storage operation metadata."""

    path: str
    created_at: str
    updated_at: str
    size_bytes: Optional[int] = None
    format: str = "unknown"
    checksum: Optional[str] = None
    tags: Optional[Dict[str, str]] = None


@runtime_checkable
class StorageBackendProtocol(Protocol):
    """
    Protocol defining the interface for storage backends.
    """

    @abstractmethod
    def write_dataframe(
        self,
        df: "pd.DataFrame",
        path: str,
        mode: str = "overwrite",
    ) -> StorageMetadataProtocol:
        """
        Write DataFrame to storage.
        """
        ...

    @abstractmethod
    def read_dataframe(self, path: str) -> "pd.DataFrame":
        """
        Read DataFrame from storage.
        """
        ...

    @abstractmethod
    def write_json(
        self,
        data: Dict[str, Any],
        path: str,
        mode: str = "overwrite",
    ) -> StorageMetadataProtocol:
        """
        Write JSON object to storage.
        """
        ...

    @abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Read JSON object from storage.
        """
        ...

    @abstractmethod
    def write_artifact(
        self,
        artifact_path: str,
        destination: str,
        mode: str = "overwrite",
    ) -> StorageMetadataProtocol:
        """
        Write artifact (file or directory) to storage.
        """
        ...

    @abstractmethod
    def read_artifact(self, path: str, local_destination: str) -> None:
        """
        Download artifact from storage to local path.
        """
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if path exists in storage.
        """
        ...

    @abstractmethod
    def list_paths(self, prefix: str) -> List[str]:
        """
        List all paths with given prefix.
        """
        ...

    @abstractmethod
    def delete(self, path: str) -> None:
        """
        Delete path (file or directory).
        """
        ...

    def get_stats(self) -> Dict[str, Any]:
        """
        Get storage backend statistics.

        Returns:
            Dictionary with statistics
        """
        return {}


class RunStatusProtocol(str, Enum):
    """Protocol for run status values."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SCHEDULED = "SCHEDULED"


@runtime_checkable
class RunProtocol(Protocol):
    """Protocol for experiment run objects."""

    run_id: str
    experiment_id: str
    name: str
    status: RunStatusProtocol
    created_at: str
    updated_at: str
    start_time: Optional[str]
    end_time: Optional[str]
    duration_seconds: Optional[float]
    parameters: Dict[str, Any]
    metrics: Dict[str, List[Any]]
    artifacts: List[str]
    tags: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert run to dictionary."""
        ...


@runtime_checkable
class ExperimentProtocol(Protocol):
    """Protocol for experiment objects."""

    experiment_id: str
    name: str
    description: str
    created_at: str
    updated_at: str
    tags: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert experiment to dictionary."""
        ...


@runtime_checkable
class ExperimentTrackerProtocol(Protocol):
    """
    Protocol for experiment tracking implementations.
    """

    @abstractmethod
    def create_experiment(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> ExperimentProtocol:
        """
        Create a new experiment.
        """
        ...

    @abstractmethod
    def start_run(
        self,
        experiment_id: str,
        name: str = "",
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        parent_run_id: Optional[str] = None,
    ) -> RunProtocol:
        """
        Start a new experiment run.
        """
        ...

    @abstractmethod
    def end_run(
        self,
        run_id: str,
        status: RunStatusProtocol = RunStatusProtocol.COMPLETED,
    ) -> RunProtocol:
        """
        End a run with specified status.
        """
        ...

    @abstractmethod
    def log_metric(
        self,
        run_id: str,
        key: str,
        value: float,
        step: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a metric value.
        """
        ...

    @abstractmethod
    def log_parameter(
        self,
        run_id: str,
        key: str,
        value: Any,
    ) -> None:
        """
        Log a parameter.
        """
        ...

    @abstractmethod
    def log_artifact(
        self,
        run_id: str,
        artifact_path: str,
        destination: str = "",
    ) -> str:
        """
        Log an artifact.
        """
        ...

    @abstractmethod
    def get_run(self, run_id: str) -> RunProtocol:
        """
        Get run by ID.
        """
        ...

    @abstractmethod
    def list_runs(
        self,
        experiment_id: str,
        status_filter: Optional[RunStatusProtocol] = None,
        tag_filter: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List runs in an experiment.
        """
        ...

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get tracker statistics."""
        ...

    @contextmanager
    def run_context(
        self,
        experiment_id: str,
        name: str = "",
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Generator[RunProtocol, None, None]:
        """
        Context manager for experiment runs.
        """
        ...


class ModelStageProtocol(str, Enum):
    """Protocol for model lifecycle stages."""

    STAGING = "Staging"
    PRODUCTION = "Production"
    ARCHIVED = "Archived"


@runtime_checkable
class ModelMetadataProtocol(Protocol):
    """Protocol for model metadata."""

    name: str
    framework: str
    version: int
    created_at: str
    description: str
    hyperparameters: Dict[str, Any]
    metrics: Dict[str, float]
    tags: Dict[str, str]
    stage: ModelStageProtocol

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        ...


@runtime_checkable
class ModelVersionProtocol(Protocol):
    """Protocol for model version objects."""

    model_id: str
    version: int
    metadata: ModelMetadataProtocol
    artifact_uri: str
    artifact_type: str
    created_at: str
    updated_at: str
    experiment_run_id: Optional[str]
    size_bytes: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        ...


@runtime_checkable
class ModelRegistryProtocol(Protocol):
    """
    Protocol for model registry implementations.
    """

    @abstractmethod
    def register_model(
        self,
        name: str,
        artifact_path: str,
        artifact_type: str,
        framework: str,
        description: str = "",
        hyperparameters: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, float]] = None,
        tags: Optional[Dict[str, str]] = None,
        input_schema: Optional[Dict[str, str]] = None,
        output_schema: Optional[Dict[str, str]] = None,
        dependencies: Optional[List[str]] = None,
        experiment_run_id: Optional[str] = None,
    ) -> ModelVersionProtocol:
        """
        Register a new model or version.
        """
        ...

    @abstractmethod
    def get_model_version(
        self,
        name: str,
        version: Optional[int] = None,
    ) -> ModelVersionProtocol:
        """
        Get a specific model version.
        """
        ...

    @abstractmethod
    def get_model_by_stage(
        self,
        name: str,
        stage: ModelStageProtocol,
    ) -> ModelVersionProtocol:
        """
        Get the latest model version for a given stage.
        """
        ...

    @abstractmethod
    def list_models(self) -> List[Dict[str, Any]]:
        """
        List all registered models.
        """
        ...

    @abstractmethod
    def list_model_versions(self, name: str) -> List[Dict[str, Any]]:
        """
        List all versions of a model.
        """
        ...

    @abstractmethod
    def promote_model(
        self,
        name: str,
        version: int,
        stage: ModelStageProtocol,
    ) -> ModelVersionProtocol:
        """
        Promote a model version to a new stage.
        """
        ...

    @abstractmethod
    def download_artifact(
        self,
        name: str,
        version: Optional[int],
        local_destination: str,
    ) -> None:
        """
        Download model artifact.
        """
        ...

    @abstractmethod
    def delete_model_version(self, name: str, version: int) -> None:
        """
        Delete a specific model version.
        """
        ...


@runtime_checkable
class LockProtocol(Protocol):
    """Protocol for lock implementations."""

    @abstractmethod
    def acquire(self) -> bool:
        """
        Acquire the lock.
        """
        ...

    @abstractmethod
    def release(self) -> None:
        """Release the lock."""
        ...

    @property
    @abstractmethod
    def is_acquired(self) -> bool:
        """Check if lock is currently held."""
        ...

    def __enter__(self) -> "LockProtocol":
        """Context manager entry."""
        ...

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        ...


@runtime_checkable
class EventCallback(Protocol):
    """Protocol for event callbacks."""

    def __call__(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Handle an event.
        """
        ...


@runtime_checkable
class EventEmitterProtocol(Protocol):
    """Protocol for event emitter implementations."""

    @abstractmethod
    def on(self, event_type: Union[str, Any], callback: EventCallback) -> None:
        """
        Register an event callback.
        """
        ...

    @abstractmethod
    def off(self, event_type: Union[str, Any], callback: EventCallback) -> None:
        """
        Unregister an event callback.
        """
        ...

    @abstractmethod
    def emit(self, event_type: Union[str, Any], data: Dict[str, Any]) -> None:
        """
        Emit an event.
        """
        ...


@runtime_checkable
class SerializableProtocol(Protocol):
    """Protocol for serializable objects."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        ...

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SerializableProtocol":
        """Create instance from dictionary."""
        ...


@runtime_checkable
class ValidatorProtocol(Protocol[T]):
    """Protocol for validators."""

    @abstractmethod
    def validate(self, value: Any) -> T:
        """
        Validate and transform a value.
        """
        ...


@runtime_checkable
class MLOpsContextProtocol(Protocol):
    """Protocol for MLOps context implementations."""

    model_registry: ModelRegistryProtocol
    experiment_tracker: ExperimentTrackerProtocol
    storage: StorageBackendProtocol

    @classmethod
    def from_config(cls, config: Any) -> "MLOpsContextProtocol":
        """Create context from configuration."""
        ...

    def get_stats(self) -> Dict[str, Any]:
        """Get combined statistics from all components."""
        ...

    def cleanup(self) -> None:
        """Clean up resources."""
        ...
