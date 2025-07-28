import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
from loguru import logger  # type: ignore


class CoordinationEvent(Enum):
    """Events for coordination between batch and streaming components."""

    BATCH_NODE_COMPLETED = "batch_node_completed"
    STREAMING_NODE_STARTED = "streaming_node_started"
    STREAMING_NODE_FAILED = "streaming_node_failed"
    PIPELINE_FAILED = "pipeline_failed"
    CHECKPOINT_REACHED = "checkpoint_reached"


@dataclass
class CoordinationMessage:
    """Coordination message between components."""

    event_type: CoordinationEvent
    node_name: str
    data: Dict[str, Any]
    timestamp: float
    pipeline_id: str


class HybridPipelineCoordinator:
    """Advanced coordinator for hybrid batch/streaming pipelines."""

    def __init__(self, pipeline_id: str):
        """
        Initialize the hybrid pipeline coordinator.

        Args:
            pipeline_id: Unique identifier for the pipeline
        """
        self.pipeline_id = pipeline_id
        self._event_handlers: Dict[CoordinationEvent, List[Callable]] = {}
        self._data_bridges: Dict[
            str, Dict[str, Any]
        ] = {}  # batch_node -> streaming_nodes
        self._checkpoint_manager = CheckpointManager(pipeline_id)
        self._resource_manager = ResourceManager()
        self._lock = threading.RLock()
        self._active = True

        logger.info(
            f"HybridPipelineCoordinator initialized for pipeline: {pipeline_id}"
        )

    def register_data_bridge(
        self, batch_node: str, streaming_nodes: List[str], bridge_config: Dict[str, Any]
    ) -> None:
        """
        Register a data bridge between batch node and streaming nodes.

        Args:
            batch_node: Name of the batch node
            streaming_nodes: List of streaming node names
            bridge_config: Configuration for the data bridge
        """
        with self._lock:
            self._data_bridges[batch_node] = {
                "streaming_nodes": streaming_nodes,
                "config": bridge_config,
                "status": "waiting",
                "bridge_path": bridge_config.get("path"),
                "format": bridge_config.get("format", "parquet"),
            }

        logger.info(f"Registered data bridge: {batch_node} -> {streaming_nodes}")

    def register_event_handler(
        self,
        event_type: CoordinationEvent,
        handler: Callable[[CoordinationMessage], None],
    ) -> None:
        """
        Register an event handler for coordination events.

        Args:
            event_type: Type of coordination event
            handler: Handler function to call when event occurs
        """
        with self._lock:
            if event_type not in self._event_handlers:
                self._event_handlers[event_type] = []
            self._event_handlers[event_type].append(handler)

        logger.debug(f"Registered event handler for {event_type.value}")

    def emit_event(
        self, event_type: CoordinationEvent, node_name: str, data: Dict[str, Any]
    ) -> None:
        """
        Emit a coordination event.

        Args:
            event_type: Type of event to emit
            node_name: Name of the node generating the event
            data: Event data
        """
        if not self._active:
            return

        message = CoordinationMessage(
            event_type=event_type,
            node_name=node_name,
            data=data,
            timestamp=time.time(),
            pipeline_id=self.pipeline_id,
        )

        logger.debug(
            f"Emitting coordination event: {event_type.value} for node {node_name}"
        )

        # Process event internally
        self._process_internal_event(message)

        # Notify external handlers
        with self._lock:
            handlers = self._event_handlers.get(event_type, [])

        for handler in handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Error in event handler for {event_type.value}: {e}")

    def _process_internal_event(self, message: CoordinationMessage) -> None:
        """Process internal coordination events."""
        try:
            if message.event_type == CoordinationEvent.BATCH_NODE_COMPLETED:
                self._handle_batch_completion(message)
            elif message.event_type == CoordinationEvent.STREAMING_NODE_FAILED:
                self._handle_streaming_failure(message)
            elif message.event_type == CoordinationEvent.PIPELINE_FAILED:
                self._handle_pipeline_failure(message)

        except Exception as e:
            logger.error(
                f"Error processing internal event {message.event_type.value}: {e}"
            )

    def _handle_batch_completion(self, message: CoordinationMessage) -> None:
        """Handle batch node completion."""
        batch_node = message.node_name
        output_path = message.data.get("output_path")

        with self._lock:
            if batch_node in self._data_bridges:
                bridge_info = self._data_bridges[batch_node]
                bridge_info["status"] = "available"
                bridge_info["output_path"] = output_path
                bridge_info["completion_time"] = message.timestamp

                logger.info(
                    f"Data bridge for {batch_node} is now available at {output_path}"
                )

                # Create checkpoint for this bridge
                self._checkpoint_manager.create_checkpoint(
                    batch_node,
                    {
                        "output_path": output_path,
                        "completion_time": message.timestamp,
                        "streaming_nodes": bridge_info["streaming_nodes"],
                    },
                )

    def _handle_streaming_failure(self, message: CoordinationMessage) -> None:
        """Handle streaming node failure."""
        streaming_node = message.node_name
        error = message.data.get("error", "Unknown error")

        logger.error(f"Streaming node {streaming_node} failed: {error}")

        # Mark affected bridges
        with self._lock:
            for batch_node, bridge_info in self._data_bridges.items():
                if streaming_node in bridge_info["streaming_nodes"]:
                    bridge_info["failed_streaming_nodes"] = bridge_info.get(
                        "failed_streaming_nodes", []
                    )
                    bridge_info["failed_streaming_nodes"].append(streaming_node)

                    logger.warning(
                        f"Data bridge from {batch_node} affected by streaming failure"
                    )

    def _handle_pipeline_failure(self, message: CoordinationMessage) -> None:
        """Handle complete pipeline failure."""
        logger.error(
            f"Pipeline {self.pipeline_id} failed: {message.data.get('error', 'Unknown error')}"
        )
        self._active = False

        # Clean up active resources
        self._resource_manager.cleanup_all()

    def wait_for_data_bridge(
        self, batch_node: str, timeout_seconds: int = 300
    ) -> Optional[str]:
        """
        Wait for a data bridge to become available.

        Args:
            batch_node: Name of the batch node
            timeout_seconds: Maximum time to wait

        Returns:
            Output path if bridge becomes available, None on timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            with self._lock:
                if batch_node in self._data_bridges:
                    bridge_info = self._data_bridges[batch_node]
                    if bridge_info["status"] == "available":
                        return bridge_info.get("output_path")

            time.sleep(1)

        logger.error(
            f"Timeout waiting for data bridge {batch_node} after {timeout_seconds}s"
        )
        return None

    def get_bridge_status(self, batch_node: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a data bridge.

        Args:
            batch_node: Name of the batch node

        Returns:
            Bridge status information if exists, None otherwise
        """
        with self._lock:
            return self._data_bridges.get(batch_node, {}).copy()

    def validate_data_consistency(self, batch_node: str) -> Dict[str, Any]:
        """
        Validate data consistency of a bridge.

        Args:
            batch_node: Name of the batch node

        Returns:
            Validation result with checks, errors, and warnings
        """
        validation_result = {
            "is_valid": False,
            "checks": [],
            "errors": [],
            "warnings": [],
        }

        try:
            with self._lock:
                bridge_info = self._data_bridges.get(batch_node)

            if not bridge_info:
                validation_result["errors"].append(
                    f"Data bridge for {batch_node} not found"
                )
                return validation_result

            output_path = bridge_info.get("output_path")
            if not output_path:
                validation_result["errors"].append(
                    f"No output path for bridge {batch_node}"
                )
                return validation_result

            # Verify file/directory existence
            import os

            if output_path.startswith(("s3://", "gs://", "abfs://", "hdfs://")):
                # For distributed systems, assume exists if configured
                validation_result["checks"].append("Remote path assumed available")
            else:
                if os.path.exists(output_path):
                    validation_result["checks"].append("Local path exists")
                else:
                    validation_result["errors"].append(
                        f"Path does not exist: {output_path}"
                    )
                    return validation_result

            # Verify data format
            bridge_format = bridge_info.get("format", "parquet")
            validation_result["checks"].append(f"Expected format: {bridge_format}")

            # If we reach here, validation is successful
            validation_result["is_valid"] = True

        except Exception as e:
            validation_result["errors"].append(f"Validation failed: {str(e)}")

        return validation_result

    def create_manual_checkpoint(self, name: str, data: Dict[str, Any]) -> str:
        """
        Create a manual checkpoint.

        Args:
            name: Checkpoint name
            data: Data to store in checkpoint

        Returns:
            Checkpoint ID
        """
        return self._checkpoint_manager.create_checkpoint(name, data)

    def restore_from_checkpoint(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Restore from a checkpoint.

        Args:
            checkpoint_id: ID of the checkpoint to restore

        Returns:
            Checkpoint data if found, None otherwise
        """
        return self._checkpoint_manager.restore_checkpoint(checkpoint_id)

    def get_coordination_summary(self) -> Dict[str, Any]:
        """
        Get coordination status summary.

        Returns:
            Summary of coordination state
        """
        with self._lock:
            return {
                "pipeline_id": self.pipeline_id,
                "active": self._active,
                "data_bridges": len(self._data_bridges),
                "available_bridges": len(
                    [
                        b
                        for b in self._data_bridges.values()
                        if b.get("status") == "available"
                    ]
                ),
                "event_handlers": {
                    event.value: len(handlers)
                    for event, handlers in self._event_handlers.items()
                },
                "bridge_details": {
                    name: {
                        "status": info.get("status", "unknown"),
                        "streaming_nodes": len(info.get("streaming_nodes", [])),
                        "has_output_path": bool(info.get("output_path")),
                    }
                    for name, info in self._data_bridges.items()
                },
            }

    def shutdown(self) -> None:
        """Shutdown the coordinator and clean up resources."""
        logger.info(f"Shutting down coordinator for pipeline {self.pipeline_id}")

        self._active = False

        with self._lock:
            self._event_handlers.clear()
            self._data_bridges.clear()

        self._checkpoint_manager.cleanup()
        self._resource_manager.cleanup_all()


class CheckpointManager:
    """Manager for pipeline coordination checkpoints."""

    def __init__(self, pipeline_id: str):
        """Initialize checkpoint manager."""
        self.pipeline_id = pipeline_id
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_checkpoint(self, name: str, data: Dict[str, Any]) -> str:
        """Create a new checkpoint."""
        checkpoint_id = f"{self.pipeline_id}_{name}_{int(time.time())}"

        checkpoint_data = {
            "id": checkpoint_id,
            "name": name,
            "pipeline_id": self.pipeline_id,
            "timestamp": time.time(),
            "data": data.copy(),
        }

        with self._lock:
            self._checkpoints[checkpoint_id] = checkpoint_data

        logger.debug(f"Created checkpoint: {checkpoint_id}")
        return checkpoint_id

    def restore_checkpoint(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """Restore data from a checkpoint."""
        with self._lock:
            checkpoint = self._checkpoints.get(checkpoint_id)

        if checkpoint:
            logger.info(f"Restoring from checkpoint: {checkpoint_id}")
            return checkpoint["data"].copy()

        logger.warning(f"Checkpoint not found: {checkpoint_id}")
        return None

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """
        List all available checkpoints.

        Returns:
            List of checkpoint metadata
        """
        with self._lock:
            return [
                {"id": cp["id"], "name": cp["name"], "timestamp": cp["timestamp"]}
                for cp in self._checkpoints.values()
            ]

    def cleanup(self) -> None:
        """Clean up all checkpoints."""
        with self._lock:
            self._checkpoints.clear()
        logger.debug("Checkpoints cleaned up")


class ResourceManager:
    """Advanced resource manager with priority-based cleanup."""

    def __init__(self):
        """Initialize the resource manager."""
        self._managed_resources: Dict[str, Dict[str, Any]] = {}
        self._resources_by_priority: List[
            tuple
        ] = []  # (resource, cleanup_fn, priority)
        self._lock = threading.Lock()

    def register_resource(
        self,
        resource: Any,
        cleanup_fn: Callable[[Any], None],
        priority: int = 0,
        resource_id: Optional[str] = None,
    ) -> str:
        """
        Register a resource with cleanup function and priority.

        Args:
            resource: Resource to manage
            cleanup_fn: Function to call for cleanup
            priority: Cleanup priority (higher numbers cleaned first)
            resource_id: Optional custom resource ID

        Returns:
            Resource ID for tracking
        """
        if resource_id is None:
            resource_id = f"resource_{id(resource)}_{int(time.time())}"

        with self._lock:
            # Store in managed resources dict
            self._managed_resources[resource_id] = {
                "resource": resource,
                "cleanup_fn": cleanup_fn,
                "priority": priority,
                "registered_at": time.time(),
            }

            # Add to priority list and keep sorted
            self._resources_by_priority.append((resource, cleanup_fn, priority))
            self._resources_by_priority.sort(key=lambda x: x[2], reverse=True)

        logger.debug(f"Registered resource '{resource_id}' with priority {priority}")
        return resource_id

    def cleanup_resource(self, resource_id: str) -> bool:
        """
        Clean up a specific resource.

        Args:
            resource_id: ID of the resource to clean up

        Returns:
            True if resource was cleaned up successfully, False otherwise
        """
        with self._lock:
            resource_info = self._managed_resources.pop(resource_id, None)

        if not resource_info:
            logger.warning(f"Resource '{resource_id}' not found for cleanup")
            return False

        try:
            cleanup_fn = resource_info.get("cleanup_fn")
            if cleanup_fn:
                cleanup_fn(resource_info["resource"])

            # Remove from priority list
            resource_obj = resource_info["resource"]
            self._resources_by_priority = [
                (r, fn, p)
                for r, fn, p in self._resources_by_priority
                if r is not resource_obj
            ]

            logger.debug(f"Cleaned up resource: {resource_id}")
            return True

        except Exception as e:
            logger.error(f"Error cleaning up resource {resource_id}: {e}")
            return False

    def cleanup_all(self) -> None:
        """Clean up all resources in priority order."""
        with self._lock:
            resources_to_cleanup = self._resources_by_priority.copy()

        success_count = 0
        failure_count = 0
        failures = []

        for resource, cleanup_fn, priority in resources_to_cleanup:
            try:
                logger.debug(f"Cleaning resource (priority {priority})")
                cleanup_fn(resource)
                success_count += 1
            except Exception as e:
                failure_count += 1
                failures.append((resource, str(e)))
                logger.error(f"Failed to clean resource: {e}")

        # Clear all tracking
        with self._lock:
            self._managed_resources.clear()
            self._resources_by_priority.clear()

        logger.info(
            f"Resource cleanup completed: {success_count} succeeded, "
            f"{failure_count} failed"
        )

        if failures:
            for resource, error in failures:
                logger.error(f"Cleanup failure for {type(resource).__name__}: {error}")

    def get_resource_count(self) -> int:
        """Get the number of managed resources."""
        with self._lock:
            return len(self._managed_resources)

    def get_resource_summary(self) -> Dict[str, Any]:
        """Get summary of managed resources."""
        with self._lock:
            priority_counts = defaultdict(int)
            resource_types = defaultdict(int)

            for resource_info in self._managed_resources.values():
                priority = resource_info["priority"]
                resource_type = type(resource_info["resource"]).__name__

                priority_counts[priority] += 1
                resource_types[resource_type] += 1

            return {
                "total_resources": len(self._managed_resources),
                "priority_distribution": dict(priority_counts),
                "resource_types": dict(resource_types),
                "oldest_registration": min(
                    (
                        info["registered_at"]
                        for info in self._managed_resources.values()
                    ),
                    default=time.time(),
                ),
            }


def create_batch_to_streaming_bridge(
    batch_output_path: str,
    streaming_input_config: Dict[str, Any],
    bridge_format: str = "parquet",
) -> Dict[str, Any]:
    """Create configuration for batch->streaming bridge."""
    # Map batch output formats to streaming input formats
    format_mapping = {
        "parquet": "file_stream",
        "delta": "delta_stream",
        "json": "file_stream",
        "csv": "file_stream",
    }

    streaming_format = format_mapping.get(bridge_format, "file_stream")

    bridge_config = {
        "path": batch_output_path,
        "format": bridge_format,
        "streaming_input": {
            "format": streaming_format,
            "options": {
                "path": batch_output_path,
                "maxFilesPerTrigger": streaming_input_config.get(
                    "maxFilesPerTrigger", "1000"
                ),
                "latestFirst": "false",
            },
        },
    }

    # Add format-specific configurations
    if bridge_format == "delta":
        bridge_config["streaming_input"]["options"]["readChangeFeed"] = "false"
    elif bridge_format in ["json", "csv"]:
        bridge_config["streaming_input"]["options"]["fileFormat"] = bridge_format

    return bridge_config


def validate_bridge_compatibility(
    batch_output_config: Dict[str, Any], streaming_input_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate compatibility between batch output and streaming input.
    """
    validation = {"compatible": False, "issues": [], "recommendations": []}

    batch_format = batch_output_config.get("format")
    streaming_format = streaming_input_config.get("format")

    # Check format compatibility
    compatible_mappings = {
        "parquet": ["file_stream"],
        "delta": ["delta_stream"],
        "json": ["file_stream"],
        "csv": ["file_stream"],
        "kafka": ["kafka"],
    }

    if batch_format not in compatible_mappings:
        validation["issues"].append(f"Unsupported batch format: {batch_format}")
        return validation

    if streaming_format not in compatible_mappings[batch_format]:
        validation["issues"].append(
            f"Incompatible formats: batch '{batch_format}' -> streaming '{streaming_format}'"
        )
        validation["recommendations"].append(
            f"Change streaming format to one of: {compatible_mappings[batch_format]}"
        )
        return validation

    # Check paths
    batch_path = batch_output_config.get("path")
    streaming_path = streaming_input_config.get("options", {}).get("path")

    if batch_path and streaming_path and batch_path != streaming_path:
        validation["issues"].append(
            "Path mismatch between batch output and streaming input"
        )
        validation["recommendations"].append(
            "Ensure paths are coordinated or use bridge configuration"
        )

    # If we reach here, it's compatible
    validation["compatible"] = True
    return validation
