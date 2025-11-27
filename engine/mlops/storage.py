import json
import os
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass

import pandas as pd  # type: ignore
from loguru import logger

from engine.mlops.validators import PathValidator
from engine.mlops.resilience import (
    with_retry,
    STORAGE_RETRY_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
)
from engine.mlops.exceptions import StorageBackendError


@dataclass
class StorageMetadata:
    """Metadata for stored objects."""

    path: str
    created_at: str
    updated_at: str
    size_bytes: Optional[int] = None
    format: str = "parquet"
    tags: Optional[Dict[str, str]] = None
    checksum: Optional[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    """

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

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics (optional override)."""
        return {}


# Constants
PARQUET_EXT = ".parquet"
JSON_EXT = ".json"
TEMP_SUFFIX = ".tmp"


class LocalStorageBackend(StorageBackend):
    """
    Local file system storage backend using Parquet for DataFrames.
    """

    def __init__(
        self,
        base_path: str,
        retry_config: Optional[RetryConfig] = None,
        enable_circuit_breaker: bool = True,
    ):
        """
        Initialize local storage backend.
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        self._retry_config = retry_config or STORAGE_RETRY_CONFIG

        # Circuit breaker for I/O operations
        self._circuit_breaker: Optional[CircuitBreaker] = None
        if enable_circuit_breaker:
            self._circuit_breaker = CircuitBreaker(
                name=f"local_storage_{base_path}",
                config=CircuitBreakerConfig(
                    failure_threshold=10,
                    timeout=60.0,
                ),
            )

        # Statistics tracking
        self._stats = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "errors": 0,
        }

        logger.info(f"LocalStorageBackend initialized at {self.base_path}")

    def _get_full_path(self, path: str) -> Path:
        """
        Get full path with security validation.
        """
        try:
            full_path = PathValidator.validate_path(path, self.base_path)
        except Exception as e:
            logger.error(f"Invalid path '{path}': {e}")
            raise ValueError(f"Invalid path '{path}': {e}") from e

        return full_path

    def _check_circuit_breaker(self) -> None:
        """Check circuit breaker before operation."""
        if self._circuit_breaker:
            self._circuit_breaker.check()

    def _record_success(self) -> None:
        """Record successful operation."""
        if self._circuit_breaker:
            self._circuit_breaker.record_success()

    def _record_failure(self, error: Exception) -> None:
        """Record failed operation."""
        self._stats["errors"] += 1
        if self._circuit_breaker:
            self._circuit_breaker.record_failure(error)

    def _create_metadata(
        self,
        path: Path,
        fmt: str,
        checksum: Optional[str] = None,
    ) -> StorageMetadata:
        """Create metadata for a stored object."""
        now = datetime.now(timezone.utc).isoformat()
        size_bytes = 0

        if path.exists():
            if path.is_file():
                size_bytes = path.stat().st_size
            else:
                size_bytes = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

        return StorageMetadata(
            path=str(path.relative_to(self.base_path)),
            created_at=now,
            updated_at=now,
            size_bytes=size_bytes,
            format=fmt,
            checksum=checksum,
        )

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="write_dataframe")
    def write_dataframe(
        self, df: pd.DataFrame, path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write DataFrame to Parquet file."""
        self._check_circuit_breaker()

        try:
            full_path = self._get_full_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)

            # Add .parquet extension if not present
            if not full_path.suffix:
                full_path = full_path.with_suffix(PARQUET_EXT)

            if full_path.exists() and mode != "overwrite":
                raise FileExistsError(f"File {full_path} already exists")

            # Write to temp file first for atomic operation
            temp_path = full_path.with_suffix(PARQUET_EXT + TEMP_SUFFIX)
            try:
                df.to_parquet(str(temp_path), engine="pyarrow", index=False)

                # Atomic rename
                if full_path.exists():
                    full_path.unlink()
                temp_path.rename(full_path)

            except Exception:
                if temp_path.exists():
                    temp_path.unlink()
                raise

            self._stats["writes"] += 1
            metadata = self._create_metadata(full_path, "parquet")

            self._record_success()
            logger.debug(f"DataFrame written to {full_path}")
            return metadata

        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_dataframe", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="read_dataframe")
    def read_dataframe(self, path: str) -> pd.DataFrame:
        """Read DataFrame from Parquet file."""
        self._check_circuit_breaker()

        try:
            full_path = self._get_full_path(path)
            if not full_path.exists():
                raise FileNotFoundError(f"File {full_path} not found")

            df = pd.read_parquet(str(full_path), engine="pyarrow")

            self._stats["reads"] += 1
            self._record_success()
            logger.debug(f"DataFrame read from {full_path}")
            return df

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_dataframe", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="write_json")
    def write_json(
        self, data: Dict[str, Any], path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Write JSON object to file."""
        self._check_circuit_breaker()

        try:
            full_path = self._get_full_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)

            # Add .json extension if not present
            if not full_path.suffix:
                full_path = full_path.with_suffix(JSON_EXT)

            if full_path.exists() and mode != "overwrite":
                raise FileExistsError(f"File {full_path} already exists")

            # Write to temp file first
            temp_path = full_path.with_suffix(JSON_EXT + TEMP_SUFFIX)
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, default=str, ensure_ascii=False)

                # Atomic rename
                if full_path.exists():
                    full_path.unlink()
                temp_path.rename(full_path)

            except Exception:
                if temp_path.exists():
                    temp_path.unlink()
                raise

            self._stats["writes"] += 1
            metadata = self._create_metadata(full_path, "json")

            self._record_success()
            logger.debug(f"JSON written to {full_path}")
            return metadata

        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_json", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="read_json")
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON object from file."""
        self._check_circuit_breaker()

        try:
            full_path = self._get_full_path(path)
            if not full_path.exists():
                raise FileNotFoundError(f"File {full_path} not found")

            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self._stats["reads"] += 1
            self._record_success()
            logger.debug(f"JSON read from {full_path}")
            return data

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_json", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="write_artifact")
    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """Copy artifact to storage."""
        self._check_circuit_breaker()

        try:
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

            self._stats["writes"] += 1
            metadata = self._create_metadata(dest, "artifact")

            self._record_success()
            logger.debug(f"Artifact copied from {src} to {dest}")
            return metadata

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_artifact", destination, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="read_artifact")
    def read_artifact(self, path: str, local_destination: str) -> None:
        """Download artifact to local path."""
        self._check_circuit_breaker()

        try:
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

            self._stats["reads"] += 1
            self._record_success()
            logger.debug(f"Artifact copied from {src} to {dest}")

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_artifact", path, e) from e

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        try:
            full_path = self._get_full_path(path)
            return full_path.exists()
        except Exception:
            return False

    def list_paths(self, prefix: str) -> List[str]:
        """List all paths with given prefix."""
        try:
            base = self._get_full_path(prefix)
            if not base.exists():
                return []

            paths = []
            for path in base.rglob("*"):
                if path.is_file():
                    paths.append(str(path.relative_to(self.base_path)))
            return sorted(paths)

        except Exception as e:
            logger.warning(f"Error listing paths with prefix '{prefix}': {e}")
            return []

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="delete")
    def delete(self, path: str) -> None:
        """Delete path (file or directory)."""
        self._check_circuit_breaker()

        try:
            full_path = self._get_full_path(path)
            if not full_path.exists():
                raise FileNotFoundError(f"Path {full_path} not found")

            if full_path.is_file():
                full_path.unlink()
            else:
                shutil.rmtree(full_path)

            self._stats["deletes"] += 1
            self._record_success()
            logger.debug(f"Deleted {full_path}")

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("delete", path, e) from e

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        stats = self._stats.copy()
        if self._circuit_breaker:
            stats["circuit_breaker_state"] = self._circuit_breaker.state.value
        return stats


class DatabricksStorageBackend(StorageBackend):
    """
    Databricks Unity Catalog storage backend.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        volume_name: str = "mlops_artifacts",
        enable_circuit_breaker: bool = True,
        local_cache: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize Databricks storage backend.
        """
        self.catalog = catalog
        self.schema = schema
        self.volume_name = volume_name

        self._spark: Optional[Any] = None
        cache_path = local_cache or f"/tmp/mlops_cache/{catalog}_{schema}"
        self._local_cache_path = Path(cache_path)
        self._local_cache_path.mkdir(parents=True, exist_ok=True)

        # Circuit breaker for Databricks operations
        self._circuit_breaker: Optional[CircuitBreaker] = None
        if enable_circuit_breaker:
            self._circuit_breaker = CircuitBreaker(
                name=f"databricks_storage_{catalog}_{schema}",
                config=CircuitBreakerConfig(
                    failure_threshold=5,
                    timeout=120.0,
                ),
            )

        # Statistics
        self._stats = {
            "spark_reads": 0,
            "spark_writes": 0,
            "local_fallbacks": 0,
            "errors": 0,
        }

        # Initialize Spark session
        self._init_spark_session()

        logger.info(f"DatabricksStorageBackend initialized for {catalog}.{schema}")

    def _init_spark_session(self) -> None:
        """Initialize Spark session using databricks-connect or runtime."""
        try:
            # Try databricks-connect first (local development)
            from databricks.connect import DatabricksSession  # type: ignore

            self._spark = DatabricksSession.builder.getOrCreate()
            logger.info("Using databricks-connect session")
            return
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"databricks-connect not available: {e}")

        try:
            # Fallback to regular SparkSession (Databricks Runtime)
            from pyspark.sql import SparkSession  # type: ignore

            self._spark = SparkSession.builder.getOrCreate()
            logger.info("Using existing SparkSession")
            return
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"SparkSession not available: {e}")

        logger.warning(
            "No Spark session available. Some operations will use local fallback. "
            "Install databricks-connect or run in Databricks environment."
        )

    @property
    def spark(self) -> Optional[Any]:
        """Get the Spark session."""
        if self._spark is None:
            self._init_spark_session()
        return self._spark

    def _check_circuit_breaker(self) -> None:
        """Check circuit breaker before operation."""
        if self._circuit_breaker:
            self._circuit_breaker.check()

    def _record_success(self) -> None:
        """Record successful operation."""
        if self._circuit_breaker:
            self._circuit_breaker.record_success()

    def _record_failure(self, error: Exception) -> None:
        """Record failed operation."""
        self._stats["errors"] += 1
        if self._circuit_breaker:
            self._circuit_breaker.record_failure(error)

    def _get_volume_path(self, path: str) -> str:
        """Get the full Unity Catalog volume path."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}/{path}"

    def _get_table_name(self, path: str) -> str:
        """Get full table name from path."""
        # Sanitize path to valid table name
        table_name = (
            path.replace("/", "_")
            .replace(PARQUET_EXT, "")
            .replace(JSON_EXT, "")
            .replace("-", "_")
            .replace(".", "_")
        )
        return f"{self.catalog}.{self.schema}.{table_name}"

    def _get_local_cache_path(self, path: str) -> Path:
        """Get local cache path for a given storage path."""
        return self._local_cache_path / path

    def _use_spark(self) -> bool:
        """Check if Spark should be used for operations."""
        return self.spark is not None

    def _get_dbutils(self) -> Optional[Any]:
        """Get dbutils if available."""
        if not self._use_spark():
            return None
        try:
            from pyspark.dbutils import DBUtils  # type: ignore

            return DBUtils(self.spark)
        except Exception:
            return None

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_write_dataframe")
    def write_dataframe(
        self, df: pd.DataFrame, path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """
        Write DataFrame to Unity Catalog table.
        """
        self._check_circuit_breaker()

        try:
            now = datetime.now(timezone.utc).isoformat()

            if self._use_spark():
                # Convert to Spark DataFrame and write as Delta
                spark_df = self.spark.createDataFrame(df)
                table_name = self._get_table_name(path)

                spark_df.write.format("delta").mode(mode).saveAsTable(table_name)

                self._stats["spark_writes"] += 1
                self._record_success()

                logger.info(f"DataFrame written to table: {table_name}")

                return StorageMetadata(
                    path=table_name,
                    created_at=now,
                    updated_at=now,
                    format="delta",
                    size_bytes=df.memory_usage(deep=True).sum(),
                )
            else:
                # Fallback to local cache
                self._stats["local_fallbacks"] += 1
                local_path = self._get_local_cache_path(path)
                local_path.parent.mkdir(parents=True, exist_ok=True)

                if not local_path.suffix:
                    local_path = local_path.with_suffix(PARQUET_EXT)

                df.to_parquet(str(local_path), engine="pyarrow", index=False)

                logger.warning(f"Spark unavailable, DataFrame cached locally: {local_path}")

                return StorageMetadata(
                    path=str(local_path),
                    created_at=now,
                    updated_at=now,
                    format="parquet",
                    size_bytes=local_path.stat().st_size if local_path.exists() else 0,
                )

        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_dataframe", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_read_dataframe")
    def read_dataframe(self, path: str) -> pd.DataFrame:
        """
        Read DataFrame from Unity Catalog table.
        """
        self._check_circuit_breaker()

        try:
            if self._use_spark():
                table_name = self._get_table_name(path)

                spark_df = self.spark.table(table_name)
                df = spark_df.toPandas()

                self._stats["spark_reads"] += 1
                self._record_success()

                logger.info(f"DataFrame read from table: {table_name}")
                return df
            else:
                # Try local cache
                local_path = self._get_local_cache_path(path)
                if not local_path.suffix:
                    local_path = local_path.with_suffix(PARQUET_EXT)

                if local_path.exists():
                    self._stats["local_fallbacks"] += 1
                    logger.warning(f"Reading from local cache: {local_path}")
                    return pd.read_parquet(str(local_path), engine="pyarrow")

                raise FileNotFoundError(
                    f"No Spark session available and no local cache found for: {path}. "
                    f"Run in Databricks environment or use from_context() with proper config."
                )

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_dataframe", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_write_json")
    def write_json(
        self, data: Dict[str, Any], path: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """
        Write JSON to Unity Catalog volume.
        """
        self._check_circuit_breaker()

        try:
            now = datetime.now(timezone.utc).isoformat()
            json_content = json.dumps(data, indent=2, default=str, ensure_ascii=False)

            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(path)
                if not volume_path.endswith(JSON_EXT):
                    volume_path += JSON_EXT

                dbutils.fs.put(volume_path, json_content, overwrite=(mode == "overwrite"))

                self._stats["spark_writes"] += 1
                self._record_success()

                logger.info(f"JSON written to volume: {volume_path}")

                return StorageMetadata(
                    path=volume_path,
                    created_at=now,
                    updated_at=now,
                    format="json",
                    size_bytes=len(json_content.encode("utf-8")),
                )

            # Local cache fallback
            self._stats["local_fallbacks"] += 1
            local_path = self._get_local_cache_path(path)
            if not local_path.suffix:
                local_path = local_path.with_suffix(JSON_EXT)

            local_path.parent.mkdir(parents=True, exist_ok=True)

            with open(local_path, "w", encoding="utf-8") as f:
                f.write(json_content)

            logger.warning(f"JSON cached locally: {local_path}")

            return StorageMetadata(
                path=str(local_path),
                created_at=now,
                updated_at=now,
                format="json",
                size_bytes=local_path.stat().st_size if local_path.exists() else 0,
            )

        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_json", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_read_json")
    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Read JSON from Unity Catalog volume.
        """
        self._check_circuit_breaker()

        try:
            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(path)
                if not volume_path.endswith(JSON_EXT):
                    volume_path += JSON_EXT

                content = dbutils.fs.head(volume_path, 10 * 1024 * 1024)  # 10MB max

                self._stats["spark_reads"] += 1
                self._record_success()

                return json.loads(content)

            # Try local cache
            local_path = self._get_local_cache_path(path)
            if not local_path.suffix:
                local_path = local_path.with_suffix(JSON_EXT)

            if local_path.exists():
                self._stats["local_fallbacks"] += 1
                with open(local_path, "r", encoding="utf-8") as f:
                    return json.load(f)

            raise FileNotFoundError(f"JSON not found at volume path or local cache: {path}")

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_json", path, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_write_artifact")
    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> StorageMetadata:
        """
        Upload artifact to Unity Catalog volume.
        """
        self._check_circuit_breaker()

        try:
            now = datetime.now(timezone.utc).isoformat()
            src = Path(artifact_path)

            if not src.exists():
                raise FileNotFoundError(f"Source artifact not found: {artifact_path}")

            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(destination)

                # Copy to volume
                local_path = f"file:{src.absolute()}"
                dbutils.fs.cp(local_path, volume_path, recurse=src.is_dir())

                self._stats["spark_writes"] += 1
                self._record_success()

                logger.info(f"Artifact uploaded to volume: {volume_path}")

                size_bytes = (
                    src.stat().st_size
                    if src.is_file()
                    else sum(f.stat().st_size for f in src.rglob("*") if f.is_file())
                )

                return StorageMetadata(
                    path=volume_path,
                    created_at=now,
                    updated_at=now,
                    format="artifact",
                    size_bytes=size_bytes,
                )

            # Local cache fallback
            self._stats["local_fallbacks"] += 1
            local_dest = self._get_local_cache_path(destination)
            local_dest.parent.mkdir(parents=True, exist_ok=True)

            if src.is_file():
                shutil.copy2(src, local_dest)
            else:
                if local_dest.exists():
                    shutil.rmtree(local_dest)
                shutil.copytree(src, local_dest)

            logger.warning(f"Artifact cached locally: {local_dest}")

            size_bytes = (
                local_dest.stat().st_size
                if local_dest.is_file()
                else sum(f.stat().st_size for f in local_dest.rglob("*") if f.is_file())
            )

            return StorageMetadata(
                path=str(local_dest),
                created_at=now,
                updated_at=now,
                format="artifact",
                size_bytes=size_bytes,
            )

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("write_artifact", destination, e) from e

    @with_retry(config=STORAGE_RETRY_CONFIG, operation_name="databricks_read_artifact")
    def read_artifact(self, path: str, local_destination: str) -> None:
        """
        Download artifact from Unity Catalog volume.
        """
        self._check_circuit_breaker()

        try:
            dest = Path(local_destination)
            dest.parent.mkdir(parents=True, exist_ok=True)

            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(path)

                local_path = f"file:{dest.absolute()}"
                dbutils.fs.cp(volume_path, local_path, recurse=True)

                self._stats["spark_reads"] += 1
                self._record_success()

                logger.info(f"Artifact downloaded from volume: {volume_path}")
                return

            # Try local cache
            local_src = self._get_local_cache_path(path)

            if local_src.exists():
                self._stats["local_fallbacks"] += 1

                if local_src.is_file():
                    shutil.copy2(local_src, dest)
                else:
                    if dest.exists():
                        shutil.rmtree(dest)
                    shutil.copytree(local_src, dest)

                logger.warning(f"Artifact read from local cache: {local_src}")
                return

            raise FileNotFoundError(f"Artifact not found at volume path or local cache: {path}")

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("read_artifact", path, e) from e

    def exists(self, path: str) -> bool:
        """Check if path exists in volume or local cache."""
        try:
            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(path)
                try:
                    dbutils.fs.ls(volume_path)
                    return True
                except Exception:
                    pass

            # Check local cache
            local_path = self._get_local_cache_path(path)
            return local_path.exists()

        except Exception:
            return False

    def list_paths(self, prefix: str) -> List[str]:
        """List paths in volume or local cache."""
        paths: List[str] = []

        try:
            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(prefix)
                try:
                    for file_info in dbutils.fs.ls(volume_path):
                        paths.append(file_info.path)
                except Exception:
                    pass

            # Also check local cache
            local_base = self._get_local_cache_path(prefix)
            if local_base.exists():
                for p in local_base.rglob("*"):
                    if p.is_file():
                        paths.append(str(p.relative_to(self._local_cache_path)))

        except Exception as e:
            logger.warning(f"Error listing paths: {e}")

        return sorted(set(paths))

    def delete(self, path: str) -> None:
        """Delete path from volume and local cache."""
        self._check_circuit_breaker()

        deleted = False

        try:
            dbutils = self._get_dbutils()
            if dbutils is not None:
                volume_path = self._get_volume_path(path)
                try:
                    dbutils.fs.rm(volume_path, recurse=True)
                    deleted = True
                    logger.info(f"Deleted from volume: {volume_path}")
                except Exception as e:
                    logger.debug(f"dbutils delete failed: {e}")

            # Also delete from local cache
            local_path = self._get_local_cache_path(path)
            if local_path.exists():
                if local_path.is_file():
                    local_path.unlink()
                else:
                    shutil.rmtree(local_path)
                deleted = True
                logger.info(f"Deleted from local cache: {local_path}")

            if not deleted:
                raise FileNotFoundError(f"Path not found: {path}")

            self._record_success()

        except FileNotFoundError:
            raise
        except Exception as e:
            self._record_failure(e)
            raise StorageBackendError("delete", path, e) from e

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        stats = self._stats.copy()
        stats["spark_available"] = self._use_spark()
        if self._circuit_breaker:
            stats["circuit_breaker_state"] = self._circuit_breaker.state.value
        return stats

    def sync_to_volume(self) -> int:
        """
        Sync local cache to Unity Catalog volume.
        """
        if not self._use_spark():
            logger.warning("Cannot sync: Spark not available")
            return 0

        synced = 0

        for local_file in self._local_cache_path.rglob("*"):
            if local_file.is_file():
                relative_path = str(local_file.relative_to(self._local_cache_path))

                try:
                    self.write_artifact(str(local_file), relative_path)
                    synced += 1
                except Exception as e:
                    logger.error(f"Failed to sync {relative_path}: {e}")

        logger.info(f"Synced {synced} files to volume")
        return synced
