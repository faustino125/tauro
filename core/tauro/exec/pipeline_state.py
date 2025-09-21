"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from loguru import logger  # type: ignore


class NodeStatus(Enum):
    """Status of a node in the pipeline execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class NodeType(Enum):
    """Type of pipeline node."""

    BATCH = "batch"
    STREAMING = "streaming"


@dataclass
class NodeExecutionInfo:
    """Information about a node's execution state."""

    node_name: str
    node_type: NodeType
    status: NodeStatus = NodeStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    output_path: Optional[str] = None
    error: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    dependents: Set[str] = field(default_factory=set)
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: float = 5.0  # seconds
    execution_metadata: Dict[str, Any] = field(default_factory=dict)
    resources: List[Any] = field(default_factory=list)


class UnifiedPipelineState:
    """Unified state for coordinating batch and streaming pipeline execution."""

    def __init__(self, circuit_breaker_threshold: int = 3):
        """Initialize the unified pipeline state."""
        self._lock = threading.RLock()
        self._nodes: Dict[str, NodeExecutionInfo] = {}
        self._circuit_breaker_threshold = circuit_breaker_threshold
        self._failure_counts: Dict[str, int] = defaultdict(int)
        self._circuit_open: bool = False

        self._pipeline_status: str = "initializing"
        self._batch_outputs: Dict[str, str] = {}
        self._streaming_queries: Dict[str, Any] = {}
        self._streaming_stopper: Optional[Callable[[str], bool]] = None
        self._cross_dependencies: Dict[str, Set[str]] = {}

    def register_node(
        self,
        node_name: str,
        node_type: NodeType,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        """Register a node in the unified state.

        This method ensures that `dependents` relationships are consistent regardless
        of the order in which nodes are registered. If a node A depends on B but B
        is registered later, when B is registered it will also link A as dependent.
        """
        with self._lock:
            if node_name in self._nodes:
                raise ValueError(f"Node '{node_name}' already registered")

            deps = dependencies or []
            self._nodes[node_name] = NodeExecutionInfo(
                node_name=node_name,
                node_type=node_type,
                dependencies=list(deps),
            )

            for dep in deps:
                if dep in self._nodes:
                    self._nodes[dep].dependents.add(node_name)

            for other_name, other_info in self._nodes.items():
                if other_name == node_name:
                    continue
                if node_name in other_info.dependencies:
                    self._nodes[node_name].dependents.add(other_name)

            if node_type == NodeType.STREAMING:
                batch_deps = [
                    dep
                    for dep in deps
                    if dep in self._nodes
                    and self._nodes[dep].node_type == NodeType.BATCH
                ]
                if batch_deps:
                    self._cross_dependencies[node_name] = set(batch_deps)

            logger.debug(
                f"Registered {node_type.value} node '{node_name}' with dependencies: {dependencies}"
            )

    def start_node_execution(self, node_name: str) -> bool:
        """Start execution of a node if dependencies are ready."""
        with self._lock:
            if node_name not in self._nodes:
                raise ValueError(f"Node '{node_name}' not registered")

            node = self._nodes[node_name]

            if not self._are_dependencies_ready(node_name):
                return False

            node.status = NodeStatus.RUNNING
            node.start_time = time.time()

            logger.info(
                f"Started execution of {node.node_type.value} node '{node_name}'"
            )
            return True

    def complete_node_execution(
        self,
        node_name: str,
        output_path: Optional[str] = None,
        execution_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Mark a node as completed successfully."""
        with self._lock:
            if node_name not in self._nodes:
                raise ValueError(f"Node '{node_name}' not registered")

            node = self._nodes[node_name]
            node.status = NodeStatus.COMPLETED
            node.end_time = time.time()
            node.output_path = output_path
            node.execution_metadata = execution_metadata or {}

            if node.node_type == NodeType.BATCH and output_path:
                self._batch_outputs[node_name] = output_path

            execution_time = node.end_time - (node.start_time or node.end_time)
            logger.info(
                f"Completed {node.node_type.value} node '{node_name}' in {execution_time:.2f}s"
            )

            self._notify_dependents(node_name)

    def fail_node_execution(self, node_name: str, error: str) -> None:
        """Handle node failure with retry logic and circuit breaker."""
        with self._lock:
            if node_name not in self._nodes:
                raise ValueError(f"Node '{node_name}' not registered")

            node = self._nodes[node_name]
            self._failure_counts[node_name] += 1

            if self._failure_counts[node_name] > self._circuit_breaker_threshold:
                logger.critical(f"Circuit breaker triggered for node '{node_name}'")
                self._circuit_open = True
                node.status = NodeStatus.FAILED
                node.error = "Circuit breaker triggered"
                node.end_time = time.time()
                self._propagate_failure(node_name)
                return

            if node.retry_count < node.max_retries:
                node.retry_count += 1
                node.status = NodeStatus.RETRYING
                logger.warning(
                    f"Node '{node_name}' failed (attempt {node.retry_count}/"
                    f"{node.max_retries}). Retrying in {node.retry_delay}s"
                )
                threading.Timer(
                    node.retry_delay, self._retry_node, args=[node_name]
                ).start()
            else:
                node.status = NodeStatus.FAILED
                node.error = error
                node.end_time = time.time()
                self._propagate_failure(node_name)

    def _retry_node(self, node_name: str) -> None:
        """Retry node execution after failure."""
        with self._lock:
            if node_name not in self._nodes:
                return

            node = self._nodes[node_name]
            if node.status == NodeStatus.RETRYING:
                node.status = NodeStatus.PENDING
                logger.info(f"Retrying node '{node_name}'")

    def register_streaming_query(self, node_name: str, query: Any) -> None:
        """Register a streaming query for tracking."""
        with self._lock:
            self._streaming_queries[node_name] = query
            logger.debug(f"Registered streaming query for node '{node_name}'")

    def set_streaming_stopper(self, stopper: Callable[[str], bool]) -> None:
        """Register a stopper function to stop queries by execution_id (string)."""
        with self._lock:
            self._streaming_stopper = stopper

    def get_batch_output_path(self, node_name: str) -> Optional[str]:
        """Get the output path of a batch node."""
        with self._lock:
            return self._batch_outputs.get(node_name)

    def is_node_ready(self, node_name: str) -> bool:
        """Check if a node is ready to execute."""
        with self._lock:
            return self._are_dependencies_ready(node_name)

    def get_node_status(self, node_name: str) -> Optional[NodeStatus]:
        """Get the status of a node."""
        with self._lock:
            node = self._nodes.get(node_name)
            return node.status if node else None

    def get_ready_nodes(self) -> List[str]:
        """Get list of nodes ready for execution."""
        with self._lock:
            ready_nodes = []
            for node_name, node in self._nodes.items():
                if node.status == NodeStatus.PENDING and self._are_dependencies_ready(
                    node_name
                ):
                    ready_nodes.append(node_name)
            return ready_nodes

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get summary of the pipeline state."""
        with self._lock:
            summary = {
                "total_nodes": len(self._nodes),
                "batch_nodes": len(
                    [n for n in self._nodes.values() if n.node_type == NodeType.BATCH]
                ),
                "streaming_nodes": len(
                    [
                        n
                        for n in self._nodes.values()
                        if n.node_type == NodeType.STREAMING
                    ]
                ),
                "status_breakdown": {},
                "cross_dependencies": len(self._cross_dependencies),
                "pipeline_status": self._pipeline_status,
                "circuit_breaker_open": self._circuit_open,
            }

            for status in NodeStatus:
                count = len([n for n in self._nodes.values() if n.status == status])
                summary["status_breakdown"][status.value] = count

            return summary

    def validate_cross_dependencies(self) -> List[str]:
        """Validate cross-dependencies between batch and streaming nodes."""
        warnings = []

        with self._lock:
            for streaming_node, batch_deps in self._cross_dependencies.items():
                for batch_dep in batch_deps:
                    batch_node = self._nodes.get(batch_dep)
                    if not batch_node:
                        warnings.append(
                            f"Streaming node '{streaming_node}' depends on "
                            f"non-existent batch node '{batch_dep}'"
                        )
                    elif batch_node.node_type != NodeType.BATCH:
                        warnings.append(
                            f"Cross-dependency error: '{streaming_node}' -> '{batch_dep}' "
                            f"but '{batch_dep}' is not a batch node"
                        )

        return warnings

    def _attempt_stop_by_handle(
        self,
        streaming_node: str,
        streaming_query: Any,
        stopped_nodes: List[str],
        failed_batch_node: str,
    ) -> None:
        try:
            streaming_query.stop()
            self._nodes[streaming_node].status = NodeStatus.CANCELLED
            stopped_nodes.append(streaming_node)
            logger.warning(
                f"Stopped streaming node '{streaming_node}' due to "
                f"failed dependency '{failed_batch_node}'"
            )
        except Exception as e:
            logger.error(f"Failed to stop streaming node '{streaming_node}': {e}")

    def _attempt_stop_by_id(
        self,
        streaming_node: str,
        query_id: str,
        stopped_nodes: List[str],
        failed_batch_node: str,
    ) -> None:
        try:
            if not self._streaming_stopper:
                logger.error(
                    f"No stopper configured to stop streaming node '{streaming_node}'"
                )
                return

            if self._streaming_stopper(query_id):
                self._nodes[streaming_node].status = NodeStatus.CANCELLED
                stopped_nodes.append(streaming_node)
                logger.warning(
                    f"Stopped streaming node '{streaming_node}' by id due to "
                    f"failed dependency '{failed_batch_node}'"
                )
            else:
                logger.error(
                    f"Stopper failed to stop streaming node '{streaming_node}'"
                )
        except Exception as e:
            logger.error(f"Error invoking stopper for '{streaming_node}': {e}")

    def stop_dependent_streaming_nodes(self, failed_batch_node: str) -> List[str]:
        """Stop streaming nodes that depend on a failed batch node."""
        stopped_nodes = []

        with self._lock:
            for streaming_node, batch_deps in self._cross_dependencies.items():
                if failed_batch_node not in batch_deps:
                    continue

                streaming_query = self._streaming_queries.get(streaming_node)
                if not streaming_query:
                    continue

                if hasattr(streaming_query, "stop"):
                    self._attempt_stop_by_handle(
                        streaming_node,
                        streaming_query,
                        stopped_nodes,
                        failed_batch_node,
                    )
                elif isinstance(streaming_query, str):
                    self._attempt_stop_by_id(
                        streaming_node,
                        streaming_query,
                        stopped_nodes,
                        failed_batch_node,
                    )

        return stopped_nodes

    def _are_dependencies_ready(self, node_name: str) -> bool:
        """Check if all dependencies of a node are completed."""
        node = self._nodes.get(node_name)
        if not node:
            return False

        for dep in node.dependencies:
            dep_node = self._nodes.get(dep)
            if not dep_node or dep_node.status != NodeStatus.COMPLETED:
                return False

        return True

    def _notify_dependents(self, completed_node: str) -> None:
        """Notify dependent nodes that a node has completed."""
        node = self._nodes.get(completed_node)
        if not node:
            return

        for dependent in node.dependents:
            if self._are_dependencies_ready(dependent):
                logger.debug(
                    f"Node '{dependent}' is now ready (dependency '{completed_node}' completed)"
                )

    def _propagate_failure(self, failed_node: str) -> None:
        """Propagate failure to dependent nodes with circuit breaker awareness."""
        if self._circuit_open:
            logger.error("Circuit open - propagating failure to all dependent nodes")

        node = self._nodes.get(failed_node)
        if node and node.node_type == NodeType.BATCH:
            self.stop_dependent_streaming_nodes(failed_node)

    def set_pipeline_status(self, status: str) -> None:
        """Set the overall pipeline status."""
        with self._lock:
            self._pipeline_status = status
            logger.info(f"Pipeline status changed to: {status}")

    def register_resource(self, node_name: str, resource_type: str, resource: Any):
        """Register a resource for cleanup tracking"""
        with self._lock:
            if node_name not in self._nodes:
                raise ValueError(f"Node '{node_name}' not registered")

            self._nodes[node_name].resources.append((resource_type, resource))
            logger.debug(f"Registered {resource_type} resource for node '{node_name}'")

    def _stop_query_by_handle(self, node_name: str, query: Any) -> None:
        try:
            query.stop()
            logger.info(f"Stopped streaming query handle for node '{node_name}'")
        except Exception as e:
            logger.warning(
                f"Error stopping streaming query handle for '{node_name}': {e}"
            )

    def _stop_query_by_id(self, node_name: str, query_id: str) -> None:
        try:
            if self._streaming_stopper:
                self._streaming_stopper(query_id)
                logger.info(f"Requested stop by id for streaming node '{node_name}'")
            else:
                logger.error(
                    f"No stopper configured to stop streaming node '{node_name}'"
                )
        except Exception as e:
            logger.warning(f"Error requesting stop by id for '{node_name}': {e}")

    def _stop_query(self, node_name: str, query: Any) -> None:
        if hasattr(query, "stop"):
            self._stop_query_by_handle(node_name, query)
        elif isinstance(query, str):
            self._stop_query_by_id(node_name, query)

    def _release_resource(self, node, res_type: str, resource: Any) -> None:
        try:
            if res_type == "spark_rdd" and hasattr(resource, "unpersist"):
                resource.unpersist()
            elif hasattr(resource, "close"):
                resource.close()
            logger.debug(f"Released {res_type} for node '{node.node_name}'")
        except Exception as e:
            logger.error(f"Error releasing resource for '{node.node_name}': {str(e)}")

    def cleanup(self) -> None:
        """Clean up all resources and stop active streaming queries"""
        with self._lock:
            for node_name, query in list(self._streaming_queries.items()):
                self._stop_query(node_name, query)

            for node in list(self._nodes.values()):
                for res_type, resource in list(node.resources):
                    self._release_resource(node, res_type, resource)

            self._reset_state()
            logger.info("Pipeline state has been reset")

    def _reset_state(self):
        """Reset all state containers"""
        self._streaming_queries.clear()
        self._nodes.clear()
        self._batch_outputs.clear()
        self._cross_dependencies.clear()
        self._failure_counts.clear()
        self._pipeline_status = "initializing"
        self._circuit_open = False
