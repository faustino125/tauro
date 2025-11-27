from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pathlib import Path
from contextlib import contextmanager

import pandas as pd  # type: ignore
from loguru import logger

from engine.mlops.storage import StorageBackend
from engine.mlops.concurrency import file_lock
from engine.mlops.validators import (
    validate_model_name,
    validate_framework,
    validate_artifact_type,
    validate_parameters,
    validate_tags,
    validate_description,
    PathValidator,
)
from engine.mlops.exceptions import (
    ModelNotFoundError,
    ModelRegistrationError,
    ArtifactNotFoundError,
)


class ModelStage(str, Enum):
    """Model lifecycle stage."""

    STAGING = "Staging"
    PRODUCTION = "Production"
    ARCHIVED = "Archived"


@dataclass
class ModelMetadata:
    """Model metadata."""

    name: str
    framework: str
    version: int
    created_at: str
    description: str = ""
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    stage: ModelStage = ModelStage.STAGING
    input_schema: Optional[Dict[str, str]] = None
    output_schema: Optional[Dict[str, str]] = None
    dependencies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = asdict(self)
        d["stage"] = self.stage.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelMetadata":
        """Create from dictionary with safe field filtering."""
        # Get valid field names for this dataclass
        valid_fields = {f.name for f in fields(cls)}

        # Filter only valid fields to avoid TypeError on unknown keys
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}

        # Handle stage enum conversion
        filtered_data["stage"] = ModelStage(filtered_data.get("stage", "Staging"))

        return cls(**filtered_data)


@dataclass
class ModelVersion:
    """Model version information."""

    model_id: str
    version: int
    metadata: ModelMetadata
    artifact_uri: str
    artifact_type: str  # "sklearn", "xgboost", "pytorch", "onnx", etc.
    created_at: str
    updated_at: str
    experiment_run_id: Optional[str] = None
    size_bytes: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_id": self.model_id,
            "version": self.version,
            "metadata": self.metadata.to_dict(),
            "artifact_uri": self.artifact_uri,
            "artifact_type": self.artifact_type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "experiment_run_id": self.experiment_run_id,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelVersion":
        """Create from dictionary."""
        data["metadata"] = ModelMetadata.from_dict(data["metadata"])
        return cls(**data)


class ModelRegistry:
    """
    Model Registry for versionado y gestión de modelos.
    """

    def __init__(self, storage: StorageBackend, registry_path: str = "model_registry"):
        """
        Initialize Model Registry.

        Args:
            storage: Storage backend for artifacts
            registry_path: Base path for registry data
        """
        self.storage = storage
        self.registry_path = registry_path
        self._ensure_registry_structure()
        logger.info(f"ModelRegistry initialized at {registry_path}")

    @contextmanager
    def _registry_lock(self, timeout: float = 30.0):
        """Context manager for registry write operations"""
        lock_path = f"{self.registry_path}/models.lock"
        with file_lock(lock_path, timeout=timeout):
            yield

    def _ensure_registry_structure(self) -> None:
        """Ensure registry directory structure exists."""
        paths = [
            f"{self.registry_path}/models",
            f"{self.registry_path}/versions",
            f"{self.registry_path}/artifacts",
            f"{self.registry_path}/metadata",
        ]
        for path in paths:
            if not self.storage.exists(path):
                # Create empty marker file
                try:
                    self.storage.write_json(
                        {"created": datetime.now(tz=timezone.utc).isoformat()},
                        f"{path}/.registry_marker.json",
                        mode="overwrite",
                    )
                except Exception:
                    pass

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
    ) -> ModelVersion:
        """
        Register a new model or version with validation and locking.
        """
        # VALIDATION: Perform all validations before acquiring lock
        try:
            # Validate inputs
            name = validate_model_name(name)
            framework = validate_framework(framework)
            artifact_type = validate_artifact_type(artifact_type)
            description = validate_description(description)
            hyperparameters = validate_parameters(hyperparameters)
            tags = validate_tags(tags)

            logger.debug(f"Input validation passed for model '{name}'")

        except Exception as e:
            logger.error(f"Validation failed for model registration: {e}")
            raise ModelRegistrationError(name, str(e)) from e

        # ARTIFACT VALIDATION: Check artifact exists and is valid
        try:
            artifact_file = Path(artifact_path)
            PathValidator.validate_file_exists(artifact_file)
            PathValidator.validate_is_file_or_dir(artifact_file)
            logger.debug(f"Artifact validation passed: {artifact_file}")

        except Exception as e:
            logger.error(f"Artifact validation failed: {e}")
            raise ArtifactNotFoundError(artifact_path) from e

        # ATOMIC OPERATION: Register under lock
        try:
            with self._registry_lock():
                model_id = str(uuid4())
                version = 1

                # Load current index
                try:
                    models_df = self._load_models_index()
                except Exception:
                    models_df = pd.DataFrame()

                # Check if model exists (increment version)
                if name in models_df.get("name", []).values:
                    model_rows = models_df[models_df["name"] == name]
                    version = int(model_rows["version"].max()) + 1
                    model_id = model_rows["model_id"].iloc[-1]
                    logger.debug(f"Model '{name}' exists, registering version {version}")
                else:
                    logger.debug(f"Registering new model '{name}' as version 1")

                now = datetime.now(tz=timezone.utc).isoformat()

                # Create metadata
                metadata = ModelMetadata(
                    name=name,
                    framework=framework,
                    version=version,
                    created_at=now,
                    description=description,
                    hyperparameters=hyperparameters or {},
                    metrics=metrics or {},
                    tags=tags or {},
                    input_schema=input_schema,
                    output_schema=output_schema,
                    dependencies=dependencies or [],
                )

                # Copy artifact to storage
                artifact_destination = f"{self.registry_path}/artifacts/{model_id}/v{version}"
                artifact_metadata = self.storage.write_artifact(
                    str(artifact_file), artifact_destination, mode="overwrite"
                )

                # Create version record
                model_version = ModelVersion(
                    model_id=model_id,
                    version=version,
                    metadata=metadata,
                    artifact_uri=artifact_destination,
                    artifact_type=artifact_type,
                    created_at=now,
                    updated_at=now,
                    experiment_run_id=experiment_run_id,
                    size_bytes=artifact_metadata.size_bytes,
                )

                # Persist metadata
                metadata_path = f"{self.registry_path}/metadata/{model_id}/v{version}.json"
                self.storage.write_json(model_version.to_dict(), metadata_path, mode="overwrite")

                # Update index (skip_lock=True since we're already under _registry_lock)
                self._update_models_index(model_version, skip_lock=True)

                logger.info(
                    f"Registered model '{name}' version {version} "
                    f"(ID: {model_id}, size: {artifact_metadata.size_bytes} bytes)"
                )

                return model_version

        except ArtifactNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Model registration failed: {e}")
            raise ModelRegistrationError(name, str(e)) from e

    def get_model_version(
        self,
        name: str,
        version: Optional[int] = None,
    ) -> ModelVersion:
        """
        Get specific model version.
        """
        models_df = self._load_models_index()
        model_rows = models_df[models_df["name"] == name]

        if model_rows.empty:
            raise ModelNotFoundError(name)

        if version is None:
            model_rows = model_rows.sort_values("version", ascending=False)
            row = model_rows.iloc[0]
        else:
            row = model_rows[model_rows["version"] == version]
            if row.empty:
                raise ModelNotFoundError(name, version)
            row = row.iloc[0]

        metadata_path = f"{self.registry_path}/metadata/{row['model_id']}/v{row['version']}.json"
        data = self.storage.read_json(metadata_path)
        return ModelVersion.from_dict(data)

    def get_model_by_stage(self, name: str, stage: ModelStage) -> ModelVersion:
        """Get latest model version for a given stage."""
        models_df = self._load_models_index()
        model_rows = models_df[models_df["name"] == name]

        if model_rows.empty:
            raise ModelNotFoundError(name)

        candidates: List[ModelVersion] = []
        for _, row in model_rows.iterrows():
            metadata_path = (
                f"{self.registry_path}/metadata/{row['model_id']}/v{row['version']}.json"
            )
            data = self.storage.read_json(metadata_path)
            mv = ModelVersion.from_dict(data)
            if mv.metadata.stage == stage:
                candidates.append(mv)

        if not candidates:
            raise ModelNotFoundError(f"{name} in stage {stage.value}")

        # Return highest version in the requested stage
        return sorted(candidates, key=lambda mv: mv.version, reverse=True)[0]

    def list_models(self) -> List[Dict[str, Any]]:
        """List all registered models with latest version."""
        models_df = self._load_models_index()
        if models_df.empty:
            return []

        latest = models_df.sort_values("version", ascending=False).drop_duplicates(
            "name", keep="first"
        )

        result = []
        for _, row in latest.iterrows():
            model_version = self.get_model_version(row["name"], int(row["version"]))
            result.append(
                {
                    "name": row["name"],
                    "model_id": row["model_id"],
                    "latest_version": int(row["version"]),
                    "stage": model_version.metadata.stage.value,
                    "created_at": model_version.created_at,
                    "framework": model_version.metadata.framework,
                }
            )

        return result

    def list_model_versions(self, name: str) -> List[Dict[str, Any]]:
        """List all versions of a model."""
        models_df = self._load_models_index()
        model_rows = models_df[models_df["name"] == name].sort_values("version", ascending=False)

        if model_rows.empty:
            raise ValueError(f"Model {name} not found")

        result = []
        for _, row in model_rows.iterrows():
            model_version = self.get_model_version(name, int(row["version"]))
            result.append(
                {
                    "version": int(row["version"]),
                    "stage": model_version.metadata.stage.value,
                    "created_at": model_version.created_at,
                    "artifact_type": model_version.artifact_type,
                    "metrics": model_version.metadata.metrics,
                }
            )

        return result

    def promote_model(self, name: str, version: int, stage: ModelStage) -> ModelVersion:
        """
        Promote model to new stage.
        """
        model_version = self.get_model_version(name, version)
        model_version.metadata.stage = stage
        model_version.updated_at = datetime.now(tz=timezone.utc).isoformat()

        metadata_path = f"{self.registry_path}/metadata/{model_version.model_id}/v{version}.json"
        self.storage.write_json(model_version.to_dict(), metadata_path, mode="overwrite")

        logger.info(f"Model {name} v{version} promoted to {stage.value}")
        return model_version

    def download_artifact(self, name: str, version: Optional[int], local_destination: str) -> None:
        """
        Download model artifact to local path.
        """
        model_version = self.get_model_version(name, version)
        self.storage.read_artifact(model_version.artifact_uri, local_destination)
        logger.info(f"Downloaded {name} v{model_version.version} to {local_destination}")

    def delete_model_version(self, name: str, version: int) -> None:
        """Delete specific model version."""
        model_version = self.get_model_version(name, version)

        # Delete artifact
        try:
            self.storage.delete(model_version.artifact_uri)
        except Exception as e:
            logger.warning(f"Could not delete artifact: {e}")

        # Delete metadata
        metadata_path = f"{self.registry_path}/metadata/{model_version.model_id}/v{version}.json"
        try:
            self.storage.delete(metadata_path)
        except Exception as e:
            logger.warning(f"Could not delete metadata: {e}")

        logger.info(f"Deleted {name} v{version}")

    def _load_models_index(self) -> pd.DataFrame:
        """Load models index."""
        try:
            index_path = f"{self.registry_path}/models/index.parquet"
            return self.storage.read_dataframe(index_path)
        except FileNotFoundError:
            return pd.DataFrame(columns=["model_id", "name", "version", "created_at"])

    def _update_models_index(self, model_version: ModelVersion, skip_lock: bool = False) -> None:
        """
        Update models index.
        """
        lock_path = f"{self.registry_path}/models/.index.lock"

        def _do_update():
            df = self._load_models_index()

            new_row = pd.DataFrame(
                [
                    {
                        "model_id": model_version.model_id,
                        "name": model_version.metadata.name,
                        "version": model_version.version,
                        "created_at": model_version.created_at,
                    }
                ]
            )

            df = pd.concat([df, new_row], ignore_index=True)
            df = df.drop_duplicates(subset=["model_id", "version"], keep="last")

            index_path = f"{self.registry_path}/models/index.parquet"
            self.storage.write_dataframe(df, index_path, mode="overwrite")

        if skip_lock:
            _do_update()
        else:
            with file_lock(lock_path, timeout=30.0):
                _do_update()
