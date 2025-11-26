"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger  # type: ignore


class ResourceHandle:
    """Handle to a tracked resource for cleanup management."""

    def __init__(
        self,
        resource_id: str,
        resource: Any,
        resource_type: str,
        cleanup_fn: Optional[Callable[[Any], None]] = None,
    ):
        """Initialize resource handle.

        Args:
            resource_id: Unique identifier for this resource
            resource: The actual resource object
            resource_type: Type of resource (spark, pandas, gpu, connection, temp_file)
            cleanup_fn: Optional custom cleanup function
        """
        self.resource_id = resource_id
        self.resource = resource
        self.resource_type = resource_type
        self.cleanup_fn = cleanup_fn
        self.is_released = False

    def release(self) -> None:
        """Release this resource using registered cleanup function or default."""
        if self.is_released:
            logger.debug(f"Resource {self.resource_id} already released")
            return

        try:
            if self.cleanup_fn:
                logger.debug(
                    f"Releasing {self.resource_type} resource {self.resource_id} with custom cleanup"
                )
                self.cleanup_fn(self.resource)
            else:
                self._default_cleanup()

            self.is_released = True
            logger.debug(f"Successfully released {self.resource_type} resource {self.resource_id}")

        except Exception as e:
            logger.error(
                f"Error releasing {self.resource_type} resource {self.resource_id}: {str(e)}"
            )

    def _default_cleanup(self) -> None:
        """Default cleanup based on resource type."""
        if self.resource is None:
            return

        resource_type = self.resource_type.lower()

        # Dispatch to appropriate cleanup based on type
        cleanup_handlers = {
            "spark": self._cleanup_spark,
            "pandas": self._cleanup_pandas,
            "gpu": self._cleanup_gpu,
            "connection": self._cleanup_connection,
            "temp_file": self._cleanup_temp_file,
        }

        handler = cleanup_handlers.get(resource_type)
        if handler:
            handler()
        elif hasattr(self.resource, "close"):
            self.resource.close()

    def _cleanup_spark(self) -> None:
        """Cleanup Spark resource."""
        if hasattr(self.resource, "unpersist"):
            self.resource.unpersist()

    def _cleanup_pandas(self) -> None:
        """Cleanup Pandas resource."""
        if hasattr(self.resource, "close"):
            self.resource.close()

    def _cleanup_gpu(self) -> None:
        """Cleanup GPU resource."""
        if hasattr(self.resource, "reset"):
            self.resource.reset()
        elif hasattr(self.resource, "clear"):
            self.resource.clear()

    def _cleanup_connection(self) -> None:
        """Cleanup database connection resource."""
        if hasattr(self.resource, "close"):
            self.resource.close()
        elif hasattr(self.resource, "disconnect"):
            self.resource.disconnect()

    def _cleanup_temp_file(self) -> None:
        """Cleanup temporary file resource."""
        import os

        if isinstance(self.resource, str) and os.path.exists(self.resource):
            try:
                os.remove(self.resource)
            except Exception:
                pass


class ResourcePool:
    """Centralized resource pool for tracking and cleanup management.

    Thread-safe registry of resources with guaranteed cleanup on release.
    Supports multiple resource types: Spark, Pandas, GPU, Connections, Temp Files.
    """

    def __init__(self):
        """Initialize resource pool with thread safety."""
        self._lock = threading.RLock()
        self._resources: Dict[str, ResourceHandle] = {}
        self._node_resources: Dict[str, List[str]] = {}  # node_name -> resource_ids

    def node_context(self, node_id: str):
        """Context manager for automatic resource cleanup.

        Usage:
            with resource_pool.node_context('my_node'):
                # Work with resources
                resource = load_data()
                pool.register_resource('my_node', resource, 'spark')
                # Automatic cleanup on exit
        """
        return _NodeResourceContext(self, node_id)


class _NodeResourceContext:
    """Context manager for node resource lifecycle."""

    def __init__(self, pool: ResourcePool, node_id: str):
        self.pool = pool
        self.node_id = node_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources even on exception."""
        try:
            self.pool.release_node_resources(self.node_id)
        except Exception as e:
            logger.error(f"Error during context cleanup for '{self.node_id}': {e}")
        return False  # Don't suppress exceptions

    def register_resource(
        self,
        node_id: str,
        resource: Any,
        resource_type: str,
        cleanup_fn: Optional[Callable[[Any], None]] = None,
    ) -> str:
        """Register a resource for tracking and automatic cleanup.

        Args:
            node_id: Identifier of the node owning this resource
            resource: The resource object to track
            resource_type: Type of resource (spark, pandas, gpu, connection, temp_file)
            cleanup_fn: Optional custom cleanup function(resource) -> None

        Returns:
            Unique resource ID for later reference
        """
        with self._lock:
            resource_id = f"{node_id}_{len(self._resources)}"

            handle = ResourceHandle(resource_id, resource, resource_type, cleanup_fn)
            self._resources[resource_id] = handle

            if node_id not in self._node_resources:
                self._node_resources[node_id] = []
            self._node_resources[node_id].append(resource_id)

            logger.debug(f"Registered {resource_type} resource {resource_id} for node '{node_id}'")

            return resource_id

    def release_resource(self, resource_id: str) -> None:
        """Release a specific resource by ID.

        Args:
            resource_id: The resource ID returned from register_resource()
        """
        with self._lock:
            if resource_id not in self._resources:
                logger.warning(f"Resource {resource_id} not found in pool")
                return

            handle = self._resources.pop(resource_id)
            handle.release()

    def release_node_resources(self, node_id: str) -> None:
        """Release all resources owned by a specific node.

        Args:
            node_id: The node identifier
        """
        with self._lock:
            resource_ids = self._node_resources.get(node_id, [])

            if not resource_ids:
                logger.debug(f"No resources to release for node '{node_id}'")
                return

            logger.info(f"Releasing {len(resource_ids)} resources for node '{node_id}'")

            for resource_id in resource_ids.copy():
                if resource_id in self._resources:
                    handle = self._resources.pop(resource_id)
                    handle.release()

            self._node_resources.pop(node_id, None)

    def release_all(self) -> None:
        """Release all tracked resources.

        This is typically called during shutdown or cleanup operations.
        """
        with self._lock:
            resource_ids = list(self._resources.keys())

            if not resource_ids:
                logger.debug("No resources to release")
                return

            logger.info(f"Releasing {len(resource_ids)} tracked resources")

            for resource_id in resource_ids:
                handle = self._resources.pop(resource_id)
                handle.release()

            self._node_resources.clear()

    def get_resource_stats(self) -> Dict[str, Any]:
        """Get statistics about tracked resources.

        Returns:
            Dictionary with resource counts and breakdown by type
        """
        with self._lock:
            total_resources = len(self._resources)
            by_type: Dict[str, int] = {}

            for handle in self._resources.values():
                resource_type = handle.resource_type.lower()
                by_type[resource_type] = by_type.get(resource_type, 0) + 1

            return {
                "total_resources": total_resources,
                "nodes_tracked": len(self._node_resources),
                "breakdown_by_type": by_type,
            }

    def validate_all_released(self) -> Tuple[bool, List[str]]:
        """Validate that all resources have been released.

        Returns:
            Tuple of (all_released: bool, unreleased_ids: List[str])
        """
        with self._lock:
            unreleased = [rid for rid, handle in self._resources.items() if not handle.is_released]

            return len(unreleased) == 0, unreleased


# Global singleton instance (optional - for convenience)
_default_pool: Optional[ResourcePool] = None
_pool_lock = threading.Lock()


def get_default_resource_pool() -> ResourcePool:
    """Get or create the default resource pool instance.

    Returns:
        The default ResourcePool singleton
    """
    global _default_pool

    if _default_pool is None:
        with _pool_lock:
            if _default_pool is None:
                _default_pool = ResourcePool()
                logger.debug("Initialized default ResourcePool instance")

    return _default_pool


def reset_default_resource_pool() -> None:
    """Reset the default resource pool (useful for testing)."""
    global _default_pool

    with _pool_lock:
        if _default_pool is not None:
            _default_pool.release_all()
        _default_pool = None
        logger.debug("Reset default ResourcePool instance")
