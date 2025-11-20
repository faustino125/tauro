import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

import pandas as pd  # type: ignore
from loguru import logger

from tauro.core.mlops.validators import PathValidator


@dataclass
class StorageMetadata:
    """Metadata for stored objects."""

    path: str
    created_at: str
    updated_at: str
    size_bytes: Optional[int] = None
    format: str = "parquet"
    tags: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def write_dataframe(
        self, df: pd.DataFrame, path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write DataFrame to storage."""
        pass

    @abstractmethod
    def read_dataframe(self, path: str) -> pd.DataFrame:
        """Read DataFrame from storage."""
        pass

    @abstractmethod
    def write_json(
        self, data: Dict[str, Any], path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write JSON object to storage."""
        pass

    @abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON object from storage."""
        pass

    @abstractmethod
    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write artifact (file or directory) to storage."""
        pass

    @abstractmethod
    def read_artifact(self, path: str, local_destination: str) -> None:
        """Download artifact from storage to local path."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if path exists."""
        pass

    @abstractmethod
    def list_paths(self, prefix: str) -> List[str]:
        """List all paths with given prefix."""
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete path (file or directory)."""
        pass


class LocalStorageBackend(StorageBackend):
    """Local file system storage backend using Parquet for DataFrames."""

    def __init__(self, base_path: str):
        """
        Initialize local storage backend.

        Args:
            base_path: Root directory for all MLOps data
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorageBackend initialized at {self.base_path}")

    def _get_full_path(self, path: str) -> Path:
        """
        Get full path with security validation.

        Prevents:
        - Absolute paths
        - Parent directory traversal
        - Path traversal attacks
        - Paths outside base_path
        """
        # Use validator to sanitize path
        try:
            full_path = PathValidator.validate_path(path, self.base_path)
        except Exception as e:
            logger.error(f"Invalid path '{path}': {e}")
            raise ValueError(f"Invalid path '{path}': {e}")

        return full_path

    def write_dataframe(
        self, df: pd.DataFrame, path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write DataFrame to Parquet file."""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Add .parquet extension if not present
        if not full_path.suffix:
            full_path = full_path.with_suffix(".parquet")

        if full_path.exists() and mode != "overwrite":
            raise FileExistsError(f"File {full_path} already exists")

        df.to_parquet(str(full_path), engine="pyarrow", index=False)
        size_bytes = full_path.stat().st_size if full_path.exists() else 0

        metadata = StorageMetadata(
            path=str(full_path.relative_to(self.base_path)),
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            size_bytes=size_bytes,
            format="parquet",
        )
        logger.info(f"DataFrame written to {full_path}")
        return metadata

    def read_dataframe(self, path: str) -> pd.DataFrame:
        """Read DataFrame from Parquet file."""
        full_path = self._get_full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File {full_path} not found")

        df = pd.read_parquet(str(full_path), engine="pyarrow")
        logger.info(f"DataFrame read from {full_path}")
        return df

    def write_json(
        self, data: Dict[str, Any], path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write JSON object to file."""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Add .json extension if not present
        if not full_path.suffix:
            full_path = full_path.with_suffix(".json")

        if full_path.exists() and mode != "overwrite":
            raise FileExistsError(f"File {full_path} already exists")

        with open(full_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

        size_bytes = full_path.stat().st_size if full_path.exists() else 0

        metadata = StorageMetadata(
            path=str(full_path.relative_to(self.base_path)),
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            size_bytes=size_bytes,
            format="json",
        )
        logger.info(f"JSON written to {full_path}")
        return metadata

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON object from file."""
        full_path = self._get_full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File {full_path} not found")

        with open(full_path, "r") as f:
            data = json.load(f)
        logger.info(f"JSON read from {full_path}")
        return data

    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Copy artifact to storage."""
        import shutil

        src = Path(artifact_path)
        dest = self._get_full_path(destination)

        if not src.exists():
            raise FileNotFoundError(f"Source artifact {src} not found")

        if dest.exists() and mode != "overwrite":
            raise FileExistsError(f"Destination {dest} already exists")

        dest.parent.mkdir(parents=True, exist_ok=True)

        if src.is_file():
            shutil.copy2(src, dest)
        else:
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest)

        size_bytes = (
            sum(f.stat().st_size for f in dest.rglob("*") if f.is_file()) if dest.exists() else 0
        )

        metadata = StorageMetadata(
            path=str(dest.relative_to(self.base_path)),
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            size_bytes=size_bytes,
            format="artifact",
        )
        logger.info(f"Artifact copied from {src} to {dest}")
        return metadata

    def read_artifact(self, path: str, local_destination: str) -> None:
        """Download artifact to local path."""
        import shutil

        src = self._get_full_path(path)
        dest = Path(local_destination)

        if not src.exists():
            raise FileNotFoundError(f"Source artifact {src} not found")

        dest.parent.mkdir(parents=True, exist_ok=True)

        if src.is_file():
            shutil.copy2(src, dest)
        else:
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest)

        logger.info(f"Artifact copied from {src} to {dest}")

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        full_path = self._get_full_path(path)
        return full_path.exists()

    def list_paths(self, prefix: str) -> List[str]:
        """List all paths with given prefix."""
        base = self._get_full_path(prefix)
        if not base.exists():
            return []

        paths = []
        for path in base.rglob("*"):
            if path.is_file():
                paths.append(str(path.relative_to(self.base_path)))
        return sorted(paths)

    def delete(self, path: str) -> None:
        """Delete path (file or directory)."""
        import shutil

        full_path = self._get_full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"Path {full_path} not found")

        if full_path.is_file():
            full_path.unlink()
        else:
            shutil.rmtree(full_path)

        logger.info(f"Deleted {full_path}")


class DatabricksStorageBackend(StorageBackend):
    """
    Databricks Unity Catalog storage backend.

    ⚠️ WARNING: This backend is currently EXPERIMENTAL and partially implemented.
    Most operations will raise NotImplementedError or return placeholder data.

    For production use with Databricks:
    1. Use Spark DataFrame API directly: spark.table(f"{catalog}.{schema}.{table}")
    2. Use UC Volumes API for artifacts: dbfs:/Volumes/{catalog}/{schema}/{path}
    3. Consider implementing full integration with databricks-sdk

    Status:
    - ❌ read_dataframe: NotImplemented
    - ❌ read_json: NotImplemented
    - ❌ read_artifact: NotImplemented
    - ⚠️ write_dataframe: Placeholder only
    - ⚠️ write_json: Placeholder only
    - ⚠️ write_artifact: Placeholder only
    - ❌ exists/list_paths/delete: NotImplemented
    """

    def __init__(self, catalog: str, schema: str, workspace_url: str = None, token: str = None):
        """
        Initialize Databricks storage backend.

        Args:
            catalog: Unity Catalog name
            schema: Schema name in the catalog
            workspace_url: Databricks workspace URL (optional, from env DATABRICKS_HOST)
            token: Databricks API token (optional, from env DATABRICKS_TOKEN)
        """
        self.catalog = catalog
        self.schema = schema
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")

        if not self.workspace_url or not self.token:
            raise ValueError(
                "Databricks workspace_url and token must be provided or "
                "set via DATABRICKS_HOST and DATABRICKS_TOKEN environment variables"
            )

        # Try to import databricks SDK
        try:
            from databricks.sql import connect  # type: ignore

            self.connect = connect
        except ImportError:
            raise ImportError(
                "databricks-sql-connector is required for DatabricksStorageBackend. "
                "Install it with: pip install databricks-sql-connector"
            )

        logger.info(f"DatabricksStorageBackend initialized: {catalog}.{schema}")

    def _get_table_path(self, path: str) -> tuple:
        """Parse path into (schema_path, table_name)."""
        parts = path.strip("/").split("/")
        table_name = parts[-1].replace(".parquet", "").replace(".json", "")
        schema_path = "/".join(parts[:-1])
        return schema_path, table_name

    def write_dataframe(
        self, df: pd.DataFrame, path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write DataFrame to UC volume or table."""
        # For now, fall back to local storage with metadata
        # In production, this would write to UC volumes via dbfs:/Volumes/...
        logger.warning(
            f"DatabricksStorageBackend.write_dataframe: "
            f"Writing to UC via Spark session is recommended. Path: {path}"
        )
        return StorageMetadata(
            path=f"{self.catalog}.{self.schema}.{path}",
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            format="parquet",
        )

    def read_dataframe(self, path: str) -> pd.DataFrame:
        """Read DataFrame from UC table."""
        raise NotImplementedError(
            "Use Spark DataFrame API to read from Unity Catalog. "
            "Example: spark.table(f'{catalog}.{schema}.{path}')"
        )

    def write_json(
        self, data: Dict[str, Any], path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write JSON to UC volume."""
        logger.warning(f"DatabricksStorageBackend.write_json: Path: {path}")
        return StorageMetadata(
            path=f"{self.catalog}.{self.schema}.{path}",
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            format="json",
        )

    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON from UC volume."""
        raise NotImplementedError("Use UC volumes API to read JSON from Databricks")

    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Upload artifact to UC volume."""
        logger.warning(
            f"DatabricksStorageBackend.write_artifact: "
            f"Artifact path: {artifact_path}, destination: {destination}"
        )
        return StorageMetadata(
            path=f"dbfs:/Volumes/{self.catalog}/{self.schema}/{destination}",
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            format="artifact",
        )

    def read_artifact(self, path: str, local_destination: str) -> None:
        """Download artifact from UC volume."""
        raise NotImplementedError("Use Databricks Volumes API to download artifacts")

    def exists(self, path: str) -> bool:
        """Check if UC object exists."""
        # This would require SQL queries against UC metadata
        return False

    def list_paths(self, prefix: str) -> List[str]:
        """List UC objects with prefix."""
        # This would require UC volumes listing API
        return []

    def delete(self, path: str) -> None:
        """Delete UC object."""
        raise NotImplementedError("Use UC volumes API to delete objects")
