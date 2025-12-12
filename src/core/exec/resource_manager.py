"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Comprehensive resource management for pipelines.
Handles connections, file handles, temporary files, and other resources.
"""
import atexit
import os
import shutil
import tempfile
import threading
import weakref
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from loguru import logger  # type: ignore


class ResourceType:
    """Resource type constants."""

    CONNECTION = "connection"
    FILE_HANDLE = "file_handle"
    TEMP_FILE = "temp_file"
    TEMP_DIR = "temp_dir"
    SPARK_DF = "spark_dataframe"
    PANDAS_DF = "pandas_dataframe"
    GPU_RESOURCE = "gpu"
    THREAD_POOL = "thread_pool"
    PROCESS_POOL = "process_pool"
    GENERIC = "generic"


class ManagedResource:
    """
    Wrapper for a managed resource with cleanup callback.
    """

    def __init__(
        self,
        resource: Any,
        resource_type: str,
        cleanup_callback: Optional[Callable] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize managed resource.

        Args:
            resource: The actual resource object
            resource_type: Type of resource (from ResourceType)
            cleanup_callback: Optional custom cleanup function
            metadata: Optional metadata about the resource
        """
        self.resource = resource
        self.resource_type = resource_type
        self.cleanup_callback = cleanup_callback
        self.metadata = metadata or {}
        self.cleaned_up = False
        self._lock = threading.Lock()

    def cleanup(self) -> bool:
        """
        Clean up the resource.

        Returns:
            True if cleanup succeeded, False otherwise
        """
        with self._lock:
            if self.cleaned_up:
                logger.debug(f"Resource already cleaned up: {self.resource_type}")
                return True

            try:
                if self.cleanup_callback:
                    self.cleanup_callback(self.resource)
                else:
                    self._default_cleanup()

                self.cleaned_up = True
                logger.debug(f"Successfully cleaned up {self.resource_type} resource")
                return True

            except Exception as e:
                logger.error(f"Error cleaning up {self.resource_type} resource: {e}")
                return False

    def _default_cleanup(self) -> None:
        """Default cleanup based on resource type."""
        if self.resource_type == ResourceType.CONNECTION:
            self._cleanup_connection()
        elif self.resource_type == ResourceType.FILE_HANDLE:
            self._cleanup_file_handle()
        elif self.resource_type == ResourceType.TEMP_FILE:
            self._cleanup_temp_file()
        elif self.resource_type == ResourceType.TEMP_DIR:
            self._cleanup_temp_dir()
        elif self.resource_type == ResourceType.SPARK_DF:
            self._cleanup_spark_df()
        elif self.resource_type == ResourceType.PANDAS_DF:
            self._cleanup_pandas_df()
        elif self.resource_type == ResourceType.GPU_RESOURCE:
            self._cleanup_gpu()
        elif self.resource_type in (ResourceType.THREAD_POOL, ResourceType.PROCESS_POOL):
            self._cleanup_executor()

    def _cleanup_connection(self) -> None:
        """Cleanup database connection."""
        if hasattr(self.resource, "close"):
            self.resource.close()
        elif hasattr(self.resource, "disconnect"):
            self.resource.disconnect()

    def _cleanup_file_handle(self) -> None:
        """Cleanup file handle."""
        if hasattr(self.resource, "close"):
            self.resource.close()

    def _cleanup_temp_file(self) -> None:
        """Cleanup temporary file."""
        path = (
            self.resource if isinstance(self.resource, (str, Path)) else self.metadata.get("path")
        )
        if path and os.path.exists(path):
            try:
                os.unlink(path)
                logger.debug(f"Deleted temporary file: {path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {path}: {e}")

    def _cleanup_temp_dir(self) -> None:
        """Cleanup temporary directory."""
        path = (
            self.resource if isinstance(self.resource, (str, Path)) else self.metadata.get("path")
        )
        if path and os.path.exists(path):
            try:
                shutil.rmtree(path)
                logger.debug(f"Deleted temporary directory: {path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary directory {path}: {e}")

    def _cleanup_spark_df(self) -> None:
        """Cleanup Spark DataFrame."""
        if hasattr(self.resource, "unpersist"):
            try:
                self.resource.unpersist()
            except Exception:
                pass

    def _cleanup_pandas_df(self) -> None:
        """Cleanup Pandas DataFrame."""
        # Pandas DataFrames are garbage collected automatically
        # Just ensure references are cleared
        pass

    def _cleanup_gpu(self) -> None:
        """Cleanup GPU resources."""
        if hasattr(self.resource, "reset"):
            self.resource.reset()
        elif hasattr(self.resource, "clear"):
            self.resource.clear()

    def _cleanup_executor(self) -> None:
        """Cleanup thread/process pool executor."""
        if hasattr(self.resource, "shutdown"):
            self.resource.shutdown(wait=True)


class ResourceManager:
    """
    Central resource manager for tracking and cleanup of all resources.

    Thread-safe singleton that manages the lifecycle of various resources
    including connections, files, DataFrames, and more.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._resources: Dict[str, List[ManagedResource]] = {}
        self._global_resources: List[ManagedResource] = []
        self._lock = threading.RLock()
        self._temp_dirs: Set[Path] = set()
        self._cleanup_registered = False
        self._initialized = True

        # Register cleanup on exit
        self._register_exit_cleanup()

    def _register_exit_cleanup(self) -> None:
        """Register cleanup to run on program exit."""
        if not self._cleanup_registered:
            atexit.register(self.cleanup_all)
            self._cleanup_registered = True
            logger.debug("Registered resource cleanup on exit")

    @contextmanager
    def resource_context(self, context_id: str):
        """
        Context manager for resource scoping.

        Args:
            context_id: Identifier for the resource context

        Example:
            with resource_manager.resource_context("pipeline_123"):
                # All resources registered here are tied to this context
                conn = create_connection()
                resource_manager.register(conn, ResourceType.CONNECTION, context_id)
            # Resources automatically cleaned up when context exits
        """
        try:
            yield self
        finally:
            self.cleanup_context(context_id)

    def register(
        self,
        resource: Any,
        resource_type: str,
        context_id: Optional[str] = None,
        cleanup_callback: Optional[Callable] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ManagedResource:
        """
        Register a resource for managed cleanup.

        Args:
            resource: The resource to manage
            resource_type: Type of resource (from ResourceType)
            context_id: Optional context identifier for scoped cleanup
            cleanup_callback: Optional custom cleanup function
            metadata: Optional metadata about the resource

        Returns:
            ManagedResource wrapper
        """
        managed = ManagedResource(resource, resource_type, cleanup_callback, metadata)

        with self._lock:
            if context_id:
                if context_id not in self._resources:
                    self._resources[context_id] = []
                self._resources[context_id].append(managed)
                logger.debug(f"Registered {resource_type} resource for context '{context_id}'")
            else:
                self._global_resources.append(managed)
                logger.debug(f"Registered global {resource_type} resource")

        return managed

    def cleanup_context(self, context_id: str) -> None:
        """
        Clean up all resources for a specific context.

        Args:
            context_id: Context identifier
        """
        with self._lock:
            resources = self._resources.get(context_id, [])
            if not resources:
                return

            logger.info(f"Cleaning up {len(resources)} resources for context '{context_id}'")

            # Cleanup in reverse order (LIFO)
            for managed in reversed(resources):
                try:
                    managed.cleanup()
                except Exception as e:
                    logger.error(f"Error during cleanup of {managed.resource_type}: {e}")

            # Remove context
            del self._resources[context_id]
            logger.debug(f"Context '{context_id}' cleaned up")

    def cleanup_all(self) -> None:
        """Clean up all managed resources."""
        with self._lock:
            total_resources = sum(len(r) for r in self._resources.values()) + len(
                self._global_resources
            )

            if total_resources == 0:
                return

            logger.info(f"Cleaning up {total_resources} total resources")

            # Cleanup context resources
            for context_id in tuple(self._resources.keys()):
                self.cleanup_context(context_id)

            # Cleanup global resources
            for managed in reversed(self._global_resources):
                try:
                    managed.cleanup()
                except Exception as e:
                    logger.error(f"Error during cleanup of global {managed.resource_type}: {e}")

            self._global_resources.clear()
            logger.info("All resources cleaned up")

    def create_temp_file(
        self,
        context_id: Optional[str] = None,
        suffix: str = "",
        prefix: str = "tauro_",
        dir: Optional[str] = None,
    ) -> Path:
        """
        Create a managed temporary file.

        Args:
            context_id: Optional context identifier
            suffix: File suffix
            prefix: File prefix
            dir: Directory for temp file

        Returns:
            Path to temporary file
        """
        fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
        os.close(fd)  # Close file descriptor immediately

        temp_path = Path(path)
        self.register(
            temp_path,
            ResourceType.TEMP_FILE,
            context_id,
            metadata={"path": str(temp_path)},
        )

        logger.debug(f"Created managed temporary file: {temp_path}")
        return temp_path

    def create_temp_dir(
        self,
        context_id: Optional[str] = None,
        suffix: str = "",
        prefix: str = "tauro_",
        dir: Optional[str] = None,
    ) -> Path:
        """
        Create a managed temporary directory.

        Args:
            context_id: Optional context identifier
            suffix: Directory suffix
            prefix: Directory prefix
            dir: Parent directory

        Returns:
            Path to temporary directory
        """
        path = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
        temp_path = Path(path)

        self.register(
            temp_path,
            ResourceType.TEMP_DIR,
            context_id,
            metadata={"path": str(temp_path)},
        )

        with self._lock:
            self._temp_dirs.add(temp_path)

        logger.debug(f"Created managed temporary directory: {temp_path}")
        return temp_path

    def get_resource_count(self, context_id: Optional[str] = None) -> int:
        """
        Get count of managed resources.

        Args:
            context_id: Optional context to count resources for

        Returns:
            Number of managed resources
        """
        with self._lock:
            if context_id:
                return len(self._resources.get(context_id, []))
            else:
                return sum(len(r) for r in self._resources.values()) + len(self._global_resources)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get resource management statistics.

        Returns:
            Dictionary with resource statistics
        """
        with self._lock:
            context_counts = {ctx: len(res) for ctx, res in self._resources.items()}
            type_counts: Dict[str, int] = {}

            # Count by type
            for resources in self._resources.values():
                for managed in resources:
                    rtype = managed.resource_type
                    type_counts[rtype] = type_counts.get(rtype, 0) + 1

            for managed in self._global_resources:
                rtype = managed.resource_type
                type_counts[rtype] = type_counts.get(rtype, 0) + 1

            return {
                "total_resources": self.get_resource_count(),
                "global_resources": len(self._global_resources),
                "contexts": len(self._resources),
                "context_counts": context_counts,
                "by_type": type_counts,
                "temp_directories": len(self._temp_dirs),
            }


# Global instance
_global_manager: Optional[ResourceManager] = None


def get_resource_manager() -> ResourceManager:
    """
    Get the global ResourceManager instance.

    Returns:
        ResourceManager singleton instance
    """
    global _global_manager
    if _global_manager is None:
        _global_manager = ResourceManager()
    return _global_manager


@contextmanager
def managed_resource(
    resource: Any,
    resource_type: str,
    context_id: Optional[str] = None,
    cleanup_callback: Optional[Callable] = None,
):
    """
    Context manager for automatic resource cleanup.

    Args:
        resource: The resource to manage
        resource_type: Type of resource
        context_id: Optional context identifier
        cleanup_callback: Optional cleanup function

    Example:
        with managed_resource(connection, ResourceType.CONNECTION):
            # Use connection
            pass
        # Connection automatically closed
    """
    manager = get_resource_manager()
    managed = manager.register(resource, resource_type, context_id, cleanup_callback)
    try:
        yield resource
    finally:
        managed.cleanup()
