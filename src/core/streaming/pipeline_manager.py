"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Event, Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger  # type: ignore

try:
    from pyspark.sql.streaming import StreamingQuery  # type: ignore
except ImportError:
    StreamingQuery = Any  # type: ignore

from core.streaming.exceptions import (
    StreamingError,
    StreamingPipelineError,
    create_error_context,
    handle_streaming_error,
)
from core.streaming.query_manager import StreamingQueryManager
from core.streaming.validators import StreamingValidator


class QueryHealthMonitor:
    """
    Monitor health of streaming queries.
    """

    def __init__(self, query: Any, query_name: str, timeout_seconds: float = 300):
        """
        Initialize query health monitor.

        Args:
            query: StreamingQuery object to monitor
            query_name: Human-readable query name
            timeout_seconds: Timeout for detecting stalls (no progress)
        """
        self.query = query
        self.query_name = query_name
        self.timeout_seconds = timeout_seconds
        self.last_progress_time = time.time()
        self.last_batch_id = None
        self.error_message = None

    def check_health(self) -> Tuple[bool, Optional[str]]:
        """
        Check if query is healthy.

        Returns:
            (is_healthy, error_message) where error_message is None if healthy
        """
        try:
            # Check if query is still active
            is_active = self._is_query_active()
            if not is_active:
                # Query stopped - check if it failed
                exception = self._get_query_exception()
                if exception:
                    self.error_message = f"Query failed with exception: {str(exception)}"
                    return False, self.error_message
                else:
                    # Gracefully stopped, not an error for health purposes
                    self.error_message = None
                    return False, None

            # Check for stalls (no progress in timeout)
            if not self._check_progress():
                elapsed = time.time() - self.last_progress_time
                self.error_message = (
                    f"Query stalled: no progress for {elapsed:.1f}s "
                    f"(timeout: {self.timeout_seconds}s)"
                )
                return False, self.error_message

            # Query is healthy
            self.error_message = None
            return True, None

        except Exception as e:
            self.error_message = f"Error checking query health: {str(e)}"
            logger.error(f"Health check failed for '{self.query_name}': {self.error_message}")
            return False, self.error_message

    def _is_query_active(self) -> bool:
        """Check if query is currently active."""
        try:
            is_active_attr = getattr(self.query, "isActive", None)
            if is_active_attr is None:
                return False

            # Handle both attribute and method
            if callable(is_active_attr):
                return bool(is_active_attr())
            else:
                return bool(is_active_attr)

        except Exception:
            return False

    def _get_query_exception(self) -> Optional[Exception]:
        """Get query exception if available."""
        try:
            exc_attr = getattr(self.query, "exception", None)
            if exc_attr is None:
                return None

            if callable(exc_attr):
                return exc_attr()
            else:
                return exc_attr

        except Exception:
            return None

    def _check_progress(self) -> bool:
        """
        Check if query has made progress recently.

        Returns True if progress detected or timeout not exceeded.
        """
        try:
            last_progress = getattr(self.query, "lastProgress", None)
            if not last_progress:
                # No progress info available - assume healthy
                return True

            current_batch_id = last_progress.get("batchId")
            current_time = time.time()

            # If batch ID changed, update progress time
            if current_batch_id is not None and current_batch_id != self.last_batch_id:
                self.last_batch_id = current_batch_id
                self.last_progress_time = current_time
                return True

            # No progress since last check - verify timeout
            elapsed = current_time - self.last_progress_time
            if elapsed > self.timeout_seconds:
                return False

            return True

        except Exception:
            # If we can't check progress, assume healthy
            return True

    def get_status_dict(self) -> Dict[str, Any]:
        """Get status as dictionary."""
        try:
            last_progress = getattr(self.query, "lastProgress", None)
            return {
                "query_name": self.query_name,
                "is_active": self._is_query_active(),
                "error": self.error_message,
                "last_batch_id": self.last_batch_id,
                "last_progress": last_progress,
            }
        except Exception:
            return {
                "query_name": self.query_name,
                "error": "Failed to get status",
            }


class StreamingPipelineManager:
    """Manages streaming pipelines with lifecycle control and monitoring."""

    def __init__(
        self,
        context,
        max_concurrent_pipelines: int = 5,
        validator: Optional[StreamingValidator] = None,
    ):
        self.context = context
        self.max_concurrent_pipelines = max_concurrent_pipelines
        policy = getattr(context, "format_policy", None)
        self.validator = validator or StreamingValidator(policy)
        self.query_manager = StreamingQueryManager(context, validator=self.validator)

        # ✅ CAPTURE ACTIVE ENVIRONMENT from context (managed by src/core/exec)
        # This ensures streaming operations use the same environment as executor
        active_env = getattr(context, "env", None) or getattr(context, "environment", None)
        if active_env:
            logger.debug(f"StreamingPipelineManager initialized for environment: '{active_env}'")
            self.active_environment = active_env
        else:
            logger.debug("StreamingPipelineManager initialized without explicit environment")
            self.active_environment = None

        self._running_pipelines: Dict[str, Dict[str, Any]] = {}
        self._pipeline_threads: Dict[str, Any] = {}
        self._shutdown_event = Event()
        self._lock = Lock()

        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_pipelines,
            thread_name_prefix="streaming_pipeline",
        )

        # Background monitor thread
        self._monitor_thread = Thread(
            target=self._monitor_loop, name="streaming_monitor", daemon=True
        )
        self._monitor_thread.start()

        logger.info(
            f"StreamingPipelineManager initialized with max {max_concurrent_pipelines} concurrent pipelines"
        )

    @handle_streaming_error
    def start_pipeline(
        self,
        pipeline_name: str,
        pipeline_config: Dict[str, Any],
        execution_id: Optional[str] = None,
    ) -> str:
        """Start a streaming pipeline with comprehensive error handling."""
        try:
            execution_id = execution_id or self._generate_execution_id(pipeline_name)

            self._validate_pipeline_start(execution_id, pipeline_name)

            self.validator.validate_streaming_pipeline_config(pipeline_config)

            logger.info(
                f"Starting streaming pipeline '{pipeline_name}' with execution_id: {execution_id}"
            )

            # Initialize pipeline info
            pipeline_info = {
                "pipeline_name": pipeline_name,
                "execution_id": execution_id,
                "config": pipeline_config,
                "start_time": time.time(),
                "status": "starting",
                "queries": {},
                "error": None,
                "nodes_count": len(pipeline_config.get("nodes", [])),
                "completed_nodes": 0,
                "last_health_check": time.time(),
            }

            with self._lock:
                self._running_pipelines[execution_id] = pipeline_info

            # Submit pipeline execution to thread pool
            future = self._executor.submit(
                self._execute_streaming_pipeline,
                execution_id,
                pipeline_name,
                pipeline_config,
            )

            # Protect modification of _pipeline_threads with lock
            with self._lock:
                self._pipeline_threads[execution_id] = future

            return execution_id

        except Exception as e:
            context = create_error_context(
                operation="start_pipeline",
                component="StreamingPipelineManager",
                pipeline_name=pipeline_name,
                execution_id=execution_id,
            )

            if isinstance(e, StreamingError):
                e.add_context("operation_context", context)
                raise
            else:
                raise StreamingPipelineError(
                    f"Failed to start pipeline '{pipeline_name}': {str(e)}",
                    pipeline_name=pipeline_name,
                    execution_id=execution_id,
                    context=context,
                    cause=e,
                ) from e

    def _validate_pipeline_start(self, execution_id: str, pipeline_name: str) -> None:
        """Validate pipeline can be started."""
        with self._lock:
            if len(self._running_pipelines) >= self.max_concurrent_pipelines:
                active_pipelines = [
                    info["pipeline_name"] for info in self._running_pipelines.values()
                ]
                raise StreamingPipelineError(
                    f"Maximum concurrent pipelines ({self.max_concurrent_pipelines}) reached. "
                    f"Active pipelines: {active_pipelines}",
                    pipeline_name=pipeline_name,
                    context={
                        "active_pipelines": active_pipelines,
                        "max_concurrent": self.max_concurrent_pipelines,
                    },
                )

            if execution_id in self._running_pipelines:
                raise StreamingPipelineError(
                    f"Pipeline with execution_id '{execution_id}' is already running",
                    pipeline_name=pipeline_name,
                    execution_id=execution_id,
                )

    @handle_streaming_error
    def stop_pipeline(
        self, execution_id: str, graceful: bool = True, timeout_seconds: float = 60.0
    ) -> bool:
        """Stop a streaming pipeline with enhanced error handling."""
        try:
            with self._lock:
                pipeline_info = self._running_pipelines.get(execution_id)
                if not pipeline_info:
                    logger.warning(f"Pipeline '{execution_id}' not found or not running")
                    return False
                pipeline_info["status"] = "stopping"

            pipeline_name = pipeline_info["pipeline_name"]
            logger.info(
                f"Stopping streaming pipeline '{pipeline_name}' (ID: {execution_id}, graceful={graceful})"
            )

            stopped_queries, failed_queries = self._stop_pipeline_queries(
                pipeline_info, execution_id, graceful, timeout_seconds
            )

            # Handle pipeline thread
            if not graceful:
                with self._lock:
                    future = self._pipeline_threads.get(execution_id)
                if future:
                    future.cancel()

            self._update_pipeline_stop_status(execution_id, stopped_queries, failed_queries)

            if failed_queries:
                logger.warning(
                    f"Pipeline '{execution_id}' stopped with {len(failed_queries)} failed queries: {failed_queries}"
                )
            else:
                logger.info(f"Pipeline '{execution_id}' stopped successfully")

            return len(failed_queries) == 0

        except Exception as e:
            logger.error(f"Error stopping pipeline '{execution_id}': {str(e)}")
            with self._lock:
                if execution_id in self._running_pipelines:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)

            raise StreamingPipelineError(
                f"Failed to stop pipeline '{execution_id}': {str(e)}",
                execution_id=execution_id,
                cause=e,
            ) from e

    def _stop_pipeline_queries(
        self,
        pipeline_info: Dict[str, Any],
        execution_id: str,
        graceful: bool,
        timeout_seconds: float,
    ):
        """Helper to stop all queries in a pipeline."""
        stopped_queries = []
        failed_queries = []
        for query_name, query in pipeline_info["queries"].items():
            try:
                if isinstance(query, StreamingQuery) and query.isActive:
                    logger.info(f"Stopping query '{query_name}' in pipeline '{execution_id}'")
                    success = self.query_manager.stop_query(query, graceful, timeout_seconds)
                    if success:
                        stopped_queries.append(query_name)
                    else:
                        failed_queries.append(query_name)
            except Exception as e:
                logger.error(f"Error stopping query '{query_name}': {str(e)}")
                failed_queries.append(query_name)
        return stopped_queries, failed_queries

    def _update_pipeline_stop_status(
        self, execution_id: str, stopped_queries: list, failed_queries: list
    ):
        """Helper to update pipeline status after stopping."""
        with self._lock:
            if execution_id in self._running_pipelines:
                self._running_pipelines[execution_id]["status"] = "stopped"
                self._running_pipelines[execution_id]["end_time"] = time.time()
                self._running_pipelines[execution_id]["stopped_queries"] = stopped_queries
                self._running_pipelines[execution_id]["failed_queries"] = failed_queries

    def get_pipeline_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific pipeline with enhanced information."""
        try:
            with self._lock:
                pipeline_info = self._running_pipelines.get(execution_id)
                if not pipeline_info:
                    return None
                status = pipeline_info.copy()

            (
                query_statuses,
                active_queries,
                failed_queries,
            ) = self._collect_query_statuses(pipeline_info.get("queries", {}))

            status["query_statuses"] = query_statuses
            status["active_queries"] = active_queries
            status["failed_queries"] = failed_queries
            status["total_queries"] = len(pipeline_info.get("queries", {}))

            # Calculate uptime
            if "start_time" in status:
                end_time = status.get("end_time", time.time())
                status["uptime_seconds"] = end_time - status["start_time"]

            return status

        except Exception as e:
            logger.error(f"Error getting pipeline status for '{execution_id}': {str(e)}")
            return {
                "execution_id": execution_id,
                "status": "error",
                "error": f"Failed to get status: {str(e)}",
            }

    def _collect_query_statuses(self, queries: Dict[str, Any]):
        """Helper to collect statuses for all queries in a pipeline."""
        query_statuses = {}
        active_queries = 0
        failed_queries = 0

        for query_name, query in queries.items():
            status, is_active, is_failed = self._get_single_query_status(query)
            query_statuses[query_name] = status
            if is_active:
                active_queries += 1
            if is_failed:
                failed_queries += 1

        return query_statuses, active_queries, failed_queries

    def _get_single_query_status(self, query):
        """Extract status for a single query."""
        if not isinstance(query, StreamingQuery):
            return {"status": "unknown"}, False, False
        try:
            is_active = query.isActive
            status = {
                "id": getattr(query, "id", None),
                "runId": str(getattr(query, "runId", "")),
                "isActive": is_active,
                "lastProgress": query.lastProgress if is_active else None,
            }
            if is_active:
                return status, True, False
            exception = None
            try:
                exception = query.exception()
            except Exception:
                pass
            if exception:
                status["exception"] = str(exception)
                return status, False, True
            return status, False, False
        except Exception as e:
            return {"error": str(e)}, False, True

    def list_running_pipelines(self) -> List[Dict[str, Any]]:
        """List all running pipelines with their status."""
        try:
            with self._lock:
                pipeline_ids = list(self._running_pipelines.keys())

            pipelines = []
            for execution_id in pipeline_ids:
                status = self.get_pipeline_status(execution_id)
                if status:
                    pipelines.append(status)

            return pipelines

        except Exception as e:
            logger.error(f"Error listing running pipelines: {str(e)}")
            return []

    def get_pipeline_metrics(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed metrics for a pipeline."""
        try:
            pipeline_info = self.get_pipeline_status(execution_id)
            if not pipeline_info:
                return None

            metrics = {
                "execution_id": execution_id,
                "pipeline_name": pipeline_info["pipeline_name"],
                "uptime_seconds": pipeline_info.get("uptime_seconds", 0),
                "status": pipeline_info["status"],
                "total_queries": pipeline_info.get("total_queries", 0),
                "active_queries": pipeline_info.get("active_queries", 0),
                "failed_queries": pipeline_info.get("failed_queries", 0),
                "query_metrics": {},
                "performance_metrics": {},
            }

            # Collect query-specific metrics
            for query_name, query_status in pipeline_info.get("query_statuses", {}).items():
                if (
                    query_status.get("isActive")
                    and "lastProgress" in query_status
                    and query_status["lastProgress"]
                ):
                    progress = query_status["lastProgress"]
                    metrics["query_metrics"][query_name] = {
                        "batchId": progress.get("batchId"),
                        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                        "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                        "timestamp": progress.get("timestamp"),
                        "durationMs": progress.get("durationMs", {}),
                        "eventTime": progress.get("eventTime", {}),
                        "stateOperators": progress.get("stateOperators", []),
                    }

            # Calculate performance metrics
            total_input_rate = sum(
                float(qm.get("inputRowsPerSecond", 0) or 0)
                for qm in metrics["query_metrics"].values()
            )
            total_processing_rate = sum(
                float(qm.get("processedRowsPerSecond", 0) or 0)
                for qm in metrics["query_metrics"].values()
            )

            metrics["performance_metrics"] = {
                "total_input_rate": total_input_rate,
                "total_processing_rate": total_processing_rate,
                "processing_efficiency": (
                    (total_processing_rate / total_input_rate * 100) if total_input_rate > 0 else 0
                ),
                "health_score": self._calculate_health_score(pipeline_info),
            }

            return metrics

        except Exception as e:
            logger.error(f"Error getting pipeline metrics for '{execution_id}': {str(e)}")
            return None

    def _calculate_health_score(self, pipeline_info: Dict[str, Any]) -> float:
        """Calculate a health score for the pipeline (0-100)."""
        try:
            total_queries = pipeline_info.get("total_queries", 0)
            active_queries = pipeline_info.get("active_queries", 0)
            failed_queries = pipeline_info.get("failed_queries", 0)

            if total_queries == 0:
                return 100.0

            # Base score based on active queries
            active_score = (active_queries / total_queries) * 70

            # Penalty for failed queries
            failure_penalty = (failed_queries / total_queries) * 30

            # Bonus for successful queries
            success_bonus = ((total_queries - failed_queries) / total_queries) * 30

            health_score = max(0, min(100, active_score + success_bonus - failure_penalty))
            return round(health_score, 2)

        except Exception:
            return 0.0

    @handle_streaming_error
    def shutdown(self, timeout_seconds: int = 30) -> Dict[str, bool]:
        """Shutdown the streaming pipeline manager with comprehensive cleanup."""
        logger.info("Shutting down StreamingPipelineManager...")

        self._shutdown_event.set()
        shutdown_results = {}

        try:
            # Stop all running pipelines
            with self._lock:
                execution_ids = list(self._running_pipelines.keys())

            logger.info(f"Stopping {len(execution_ids)} running pipelines...")

            for execution_id in execution_ids:
                try:
                    result = self.stop_pipeline(
                        execution_id,
                        graceful=True,
                        timeout_seconds=timeout_seconds // 2,
                    )
                    shutdown_results[execution_id] = result
                except Exception as e:
                    logger.error(
                        f"Error stopping pipeline '{execution_id}' during shutdown: {str(e)}"
                    )
                    shutdown_results[execution_id] = False

            # Wait for threads to complete with timeout
            logger.info("Waiting for pipeline threads to complete...")
            completed_threads = 0

            # Take a snapshot of futures safely
            with self._lock:
                futures = list(self._pipeline_threads.items())

            num_futures = len(futures)
            per_future_timeout = max(1.0, float(timeout_seconds) / max(1, num_futures))

            for execution_id, future in futures:
                try:
                    future.result(timeout=per_future_timeout)
                    completed_threads += 1
                except FutureTimeoutError:
                    logger.warning(
                        f"Pipeline thread '{execution_id}' did not complete within timeout"
                    )
                    try:
                        future.cancel()
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning(f"Error waiting for pipeline '{execution_id}' to finish: {e}")

            logger.info(f"Completed {completed_threads}/{len(futures)} pipeline threads")

            # Shutdown executor: ThreadPoolExecutor.shutdown does not accept a timeout kwarg
            logger.info("Shutting down thread pool executor...")
            try:
                # First, prevent new tasks and attempt a non-blocking shutdown
                self._executor.shutdown(wait=False)
            except Exception:
                try:
                    self._executor.shutdown(wait=True)
                except Exception:
                    logger.warning("Executor shutdown encountered an issue")

            # Clear internal state
            with self._lock:
                self._running_pipelines.clear()
                self._pipeline_threads.clear()

            logger.info("StreamingPipelineManager shutdown complete")
            return shutdown_results

        except Exception as e:
            logger.error(f"Error during StreamingPipelineManager shutdown: {str(e)}")
            raise StreamingError(
                f"Failed to shutdown StreamingPipelineManager: {str(e)}",
                error_code="SHUTDOWN_ERROR",
                cause=e,
            ) from e

    def _generate_execution_id(self, pipeline_name: str) -> str:
        """Generate unique execution ID."""
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]
        return f"{pipeline_name}_{timestamp}_{unique_id}"

    def _execute_streaming_pipeline(
        self, execution_id: str, pipeline_name: str, pipeline_config: Dict[str, Any]
    ) -> None:
        """Execute a streaming pipeline in a separate thread with comprehensive error handling."""
        try:
            with self._lock:
                if execution_id not in self._running_pipelines:
                    logger.error(f"Pipeline {execution_id} not found in running pipelines")
                    return
                self._running_pipelines[execution_id]["status"] = "running"

            logger.info(
                f"Executing streaming pipeline '{pipeline_name}' with execution_id: {execution_id}"
            )

            nodes = pipeline_config.get("nodes", [])
            if not nodes:
                raise StreamingPipelineError(
                    f"No nodes defined in streaming pipeline '{pipeline_name}'",
                    pipeline_name=pipeline_name,
                    execution_id=execution_id,
                )

            processed_nodes = self._process_pipeline_nodes(execution_id, pipeline_name, nodes)

            if processed_nodes:
                logger.info(
                    f"Pipeline '{execution_id}' started {len(processed_nodes)} queries. Monitoring handled by background thread."
                )
                # Monitoring is now decoupled and handled by _monitor_loop
            else:
                logger.error(f"Pipeline '{execution_id}' failed to start any queries")
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "failed"

        except Exception as e:
            logger.error(f"Error executing streaming pipeline '{execution_id}': {str(e)}")
            with self._lock:
                if execution_id in self._running_pipelines:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)
            raise

    def _process_pipeline_nodes(
        self, execution_id: str, pipeline_name: str, nodes: List[Any]
    ) -> List[str]:
        """
        Process pipeline nodes respecting dependencies.
        """
        try:
            # Build dependency graph
            node_graph = self._build_node_dependency_graph(nodes)

            # Validate graph for cycles
            self._validate_no_cycles(node_graph)

            # Execute nodes in dependency order
            processed_nodes = self._execute_nodes_respecting_dependencies(
                execution_id, pipeline_name, nodes, node_graph
            )

            return processed_nodes

        except Exception as e:
            logger.error(f"Error processing pipeline nodes for '{execution_id}': {str(e)}")
            with self._lock:
                self._running_pipelines[execution_id]["status"] = "error"
                self._running_pipelines[execution_id]["error"] = str(e)
            raise

    def _build_node_dependency_graph(self, nodes: List[Any]) -> Dict[str, List[str]]:
        """
        Build directed acyclic graph of node dependencies.
        """
        graph = {}
        node_names = set()

        # Build graph and collect node names
        for i, node_config in enumerate(nodes):
            node_name, dependencies = self._extract_node_and_dependencies(node_config, i)
            node_names.add(node_name)
            graph[node_name] = dependencies

        # Validate dependencies reference existing nodes
        self._validate_node_dependencies(graph, node_names)

        return graph

    def _extract_node_and_dependencies(self, node_config: Any, idx: int) -> Tuple[str, List[str]]:
        """Extract node name and its dependencies from configuration."""
        try:
            node_name = self._get_node_name(node_config, idx)

            if isinstance(node_config, dict):
                depends_on = node_config.get("depends_on", [])
                if depends_on and not isinstance(depends_on, list):
                    depends_on = [depends_on]
            else:
                depends_on = []

            return node_name, depends_on

        except Exception as e:
            raise StreamingPipelineError(
                f"Error building dependency graph at node {idx}: {str(e)}",
                pipeline_name=None,
                execution_id=None,
            ) from e

    def _validate_node_dependencies(self, graph: Dict[str, List[str]], node_names: set) -> None:
        """Validate that all dependencies reference existing nodes."""
        for node_name, dependencies in graph.items():
            for dep in dependencies:
                if dep not in node_names:
                    raise StreamingPipelineError(
                        f"Node '{node_name}' depends on '{dep}' which is not defined",
                        pipeline_name=None,
                        execution_id=None,
                    )

    def _get_node_name(self, node_config: Any, idx: int) -> str:
        """Extract node name from configuration."""
        if isinstance(node_config, str):
            return node_config
        elif isinstance(node_config, dict):
            return node_config.get("name", f"node_{idx}")
        else:
            raise ValueError(f"Invalid node configuration type: {type(node_config)}")

    def _validate_no_cycles(self, graph: Dict[str, List[str]]) -> None:
        """
        Validate that dependency graph has no cycles using DFS.
        """
        visited = set()
        visiting = set()

        def has_cycle(node: str) -> bool:
            if node in visiting:
                return True
            if node in visited:
                return False

            visiting.add(node)
            for dependency in graph.get(node, []):
                if has_cycle(dependency):
                    return True
            visiting.remove(node)
            visited.add(node)
            return False

        for node in graph:
            if node not in visited and has_cycle(node):
                raise StreamingPipelineError(
                    "Circular dependency detected in pipeline graph",
                    pipeline_name=None,
                    execution_id=None,
                    context={"graph": graph},
                )

    def _execute_nodes_respecting_dependencies(
        self,
        execution_id: str,
        pipeline_name: str,
        nodes: List[Any],
        graph: Dict[str, List[str]],
    ) -> List[str]:
        """
        Execute nodes in dependency order.
        """
        processed_nodes = []
        completed_nodes = dict.fromkeys(graph.keys(), False)
        remaining_nodes = set(graph.keys())
        max_iterations = len(nodes) + 1
        iterations = 0

        while remaining_nodes and iterations < max_iterations:
            iterations += 1

            if self._shutdown_event.is_set():
                logger.info(f"Shutdown requested, stopping pipeline '{execution_id}'")
                break

            ready_nodes = self._find_ready_nodes(remaining_nodes, graph, completed_nodes)

            if not ready_nodes:
                self._log_missing_nodes(remaining_nodes, graph, completed_nodes)
                break

            for node_name in ready_nodes:
                self._execute_single_node(
                    node_name,
                    execution_id,
                    pipeline_name,
                    nodes,
                    graph,
                    processed_nodes,
                    completed_nodes,
                    remaining_nodes,
                )

        if remaining_nodes:
            logger.warning(f"Failed to start all nodes. Remaining: {remaining_nodes}")

        return processed_nodes

    def _find_ready_nodes(
        self, remaining_nodes: set, graph: Dict[str, List[str]], completed_nodes: Dict[str, bool]
    ) -> List[str]:
        """Find nodes ready to execute (all dependencies completed)."""
        ready_nodes = []
        for node_name in remaining_nodes:
            dependencies = graph.get(node_name, [])
            if all(completed_nodes.get(dep, False) for dep in dependencies):
                ready_nodes.append(node_name)
        return ready_nodes

    def _log_missing_nodes(
        self, remaining_nodes: set, graph: Dict[str, List[str]], completed_nodes: Dict[str, bool]
    ) -> None:
        """Log nodes that cannot progress."""
        missing = [
            n
            for n in remaining_nodes
            if not all(completed_nodes.get(d, False) for d in graph.get(n, []))
        ]
        logger.error(f"Cannot make progress on nodes: {missing}")

    def _execute_single_node(
        self,
        node_name: str,
        execution_id: str,
        pipeline_name: str,
        nodes: List[Any],
        graph: Dict[str, List[str]],
        processed_nodes: List[str],
        completed_nodes: Dict[str, bool],
        remaining_nodes: set,
    ) -> None:
        """Execute a single node and update tracking structures."""
        try:
            node_config = self._find_node_config(node_name, nodes)
            if not node_config:
                raise ValueError(f"Node configuration not found for '{node_name}'")

            logger.info(
                f"Starting node '{node_name}' in pipeline '{execution_id}' "
                f"(dependencies completed: {graph.get(node_name, [])})"
            )

            query = self.query_manager.create_and_start_query(
                node_config, execution_id, pipeline_name
            )

            with self._lock:
                self._running_pipelines[execution_id]["queries"][node_name] = query
                self._running_pipelines[execution_id]["completed_nodes"] += 1

            processed_nodes.append(node_name)
            completed_nodes[node_name] = True
            remaining_nodes.discard(node_name)
            logger.info(f"Successfully started query '{node_name}'")

        except Exception as e:
            self._handle_node_execution_error(execution_id, node_name, str(e))
            completed_nodes[node_name] = True
            remaining_nodes.discard(node_name)

    def _find_node_config(self, node_name: str, nodes: List[Any]) -> Optional[Dict[str, Any]]:
        """Find configuration for a specific node."""
        for idx, node_cfg in enumerate(nodes):
            cfg_name = self._get_node_name(node_cfg, idx)
            if cfg_name == node_name:
                return self._get_node_config(node_cfg, idx)
        return None

    def _handle_node_execution_error(self, execution_id: str, node_name: str, error: str) -> None:
        """Handle errors during node execution."""
        logger.error(f"Error starting node '{node_name}' in pipeline '{execution_id}': {error}")
        with self._lock:
            if execution_id in self._running_pipelines:
                self._running_pipelines[execution_id]["status"] = "partial_failure"
                self._running_pipelines[execution_id]["error"] = error

    def _get_node_config(self, node_config: Any, idx: int) -> Dict[str, Any]:
        """Ensure node has proper configuration."""
        if isinstance(node_config, str):
            node_name = node_config
            actual_config = self.context.nodes_config.get(node_name)
            if not actual_config:
                raise StreamingPipelineError(
                    f"Node configuration '{node_name}' not found",
                    pipeline_name=None,
                    execution_id=None,
                )
            return {**actual_config, "name": node_name}
        elif isinstance(node_config, dict):
            if "name" not in node_config:
                node_config["name"] = f"node_{idx}"
            return node_config
        else:
            raise StreamingPipelineError(
                f"Invalid node configuration type: {type(node_config)}",
                pipeline_name=None,
                execution_id=None,
            )

    def _monitor_loop(self) -> None:
        """Background thread to monitor all running pipelines."""
        logger.info("Starting streaming pipeline monitor thread")
        while not self._shutdown_event.is_set():
            try:
                with self._lock:
                    # Copy keys to avoid modification during iteration
                    execution_ids = list(self._running_pipelines.keys())

                for execution_id in execution_ids:
                    self._check_pipeline_health(execution_id)

                time.sleep(5)  # Monitoring interval
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(5)

    def _check_pipeline_health(self, execution_id: str) -> None:
        """Check health of a single pipeline."""
        with self._lock:
            pipeline_info = self._running_pipelines.get(execution_id)

        if not pipeline_info or pipeline_info["status"] not in ("running", "starting"):
            return

        try:
            (
                active_queries,
                failed_queries,
                completed_queries,
            ) = self._collect_query_states(pipeline_info)

            self._update_pipeline_query_status(
                execution_id, active_queries, completed_queries, failed_queries
            )

            if self._handle_failed_queries(execution_id, failed_queries):
                return

            if self._handle_completed_queries(execution_id, active_queries, completed_queries):
                return

            # Periodic health check logging
            current_time = time.time()
            last_check = pipeline_info.get("last_health_check", 0)
            if current_time - last_check > 30:  # 30 seconds interval
                self._periodic_health_check(
                    execution_id,
                    active_queries,
                    completed_queries,
                    last_check,
                    30,
                )
                with self._lock:
                    if execution_id in self._running_pipelines:
                        self._running_pipelines[execution_id]["last_health_check"] = current_time

        except Exception as e:
            logger.error(f"Error monitoring pipeline '{execution_id}': {str(e)}")
            with self._lock:
                if execution_id in self._running_pipelines:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)

    def _collect_query_states(
        self, pipeline_info: Dict[str, Any]
    ) -> Tuple[int, List[Tuple[str, str]], List[str]]:
        """
        Collect states of all queries in a pipeline using QueryHealthMonitor.
        """
        active_queries = 0
        failed_queries = []
        completed_queries = []

        for query_name, query in pipeline_info["queries"].items():
            if query is None:
                failed_queries.append((query_name, "Query object is None"))
                continue

            try:
                # Use health monitor for comprehensive check
                monitor = QueryHealthMonitor(query, query_name, timeout_seconds=300)
                is_healthy, error_message = monitor.check_health()

                if is_healthy:
                    # Query is running and healthy
                    active_queries += 1
                elif error_message is None:
                    # Query stopped cleanly (not an error)
                    completed_queries.append(query_name)
                else:
                    # Query failed or stalled
                    failed_queries.append((query_name, error_message))

            except Exception as e:
                logger.error(f"Error monitoring query '{query_name}': {str(e)}")
                failed_queries.append((query_name, f"Monitoring error: {str(e)}"))

        return active_queries, failed_queries, completed_queries

    def _get_query_state(self, query_name, query):
        """Return state for a query using duck-typing to support Py4J proxies."""
        if query is None:
            return None, None

        try:
            if self._query_is_active(query):
                return "active", None

            exception = self._query_exception(query)
            if exception:
                return "failed", str(exception)

            return "completed", None

        except Exception as e:
            logger.error(f"Error checking query '{query_name}' status: {str(e)}")
            return "failed", str(e)

    def _query_is_active(self, query) -> bool:
        """Determine whether a query is active using duck-typing."""
        is_active_attr = getattr(query, "isActive", None)
        if is_active_attr is None:
            return False
        try:
            return bool(is_active_attr)
        except TypeError:
            if callable(is_active_attr):
                try:
                    return bool(is_active_attr())
                except Exception:
                    return False
            return False

    def _query_exception(self, query):
        """Return query exception if available, otherwise None."""
        exc_call = getattr(query, "exception", None)
        if callable(exc_call):
            try:
                return exc_call()
            except Exception:
                return None
        return None

    def _update_pipeline_query_status(
        self, execution_id, active_queries, completed_queries, failed_queries
    ):
        with self._lock:
            if execution_id in self._running_pipelines:
                self._running_pipelines[execution_id]["active_queries"] = active_queries
                self._running_pipelines[execution_id]["completed_queries"] = len(completed_queries)
                self._running_pipelines[execution_id]["failed_queries"] = len(failed_queries)

    def _handle_failed_queries(self, execution_id, failed_queries):
        if failed_queries:
            error_msg = f"Queries failed: {failed_queries}"
            logger.error(f"Pipeline '{execution_id}' has failed queries: {error_msg}")
            with self._lock:
                self._running_pipelines[execution_id]["status"] = "error"
                self._running_pipelines[execution_id]["error"] = error_msg
            return True
        return False

    def _handle_completed_queries(self, execution_id, active_queries, completed_queries):
        if active_queries == 0:
            if completed_queries:
                logger.info(
                    f"All queries completed successfully for pipeline '{execution_id}': {completed_queries}"
                )
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "completed"
            else:
                logger.warning(
                    f"No active queries remaining for pipeline '{execution_id}' but none completed successfully"
                )
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "stopped"
            return True
        return False

    def _periodic_health_check(
        self,
        execution_id,
        active_queries,
        completed_queries,
        last_health_check,
        health_check_interval,
    ):
        current_time = time.time()
        if current_time - last_health_check > health_check_interval:
            health_score = self._calculate_health_score(self._running_pipelines[execution_id])
            logger.debug(
                f"Pipeline '{execution_id}' health score: {health_score}% (active: {active_queries}, completed: {len(completed_queries)})"
            )

    def get_pipeline_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary of all managed pipelines."""
        try:
            with self._lock:
                total_pipelines = len(self._running_pipelines)

                if total_pipelines == 0:
                    return {
                        "total_pipelines": 0,
                        "healthy_pipelines": 0,
                        "unhealthy_pipelines": 0,
                        "overall_health_score": 100.0,
                        "status": "idle",
                    }

                status_counts = {}
                health_scores = []

                for pipeline_info in self._running_pipelines.values():
                    status = pipeline_info.get("status", "unknown")
                    status_counts[status] = status_counts.get(status, 0) + 1

                    health_score = self._calculate_health_score(pipeline_info)
                    health_scores.append(health_score)

                avg_health_score = sum(health_scores) / len(health_scores) if health_scores else 0
                healthy_count = sum(1 for score in health_scores if score >= 80)

                if avg_health_score >= 80:
                    overall_status = "healthy"
                elif avg_health_score >= 50:
                    overall_status = "degraded"
                else:
                    overall_status = "critical"

                return {
                    "total_pipelines": total_pipelines,
                    "healthy_pipelines": healthy_count,
                    "unhealthy_pipelines": total_pipelines - healthy_count,
                    "overall_health_score": round(avg_health_score, 2),
                    "status_breakdown": status_counts,
                    "individual_health_scores": health_scores,
                    "status": overall_status,
                }

        except Exception as e:
            logger.error(f"Error calculating pipeline health summary: {str(e)}")
            return {"total_pipelines": 0, "error": str(e), "status": "error"}
