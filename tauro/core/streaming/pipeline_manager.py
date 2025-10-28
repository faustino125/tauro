"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Event, Lock
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore
from pyspark.sql.streaming import StreamingQuery  # type: ignore

from tauro.core.streaming.exceptions import (
    StreamingError,
    StreamingPipelineError,
    create_error_context,
    handle_streaming_error,
)
from tauro.core.streaming.query_manager import StreamingQueryManager
from tauro.core.streaming.validators import StreamingValidator


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

        self._running_pipelines: Dict[str, Dict[str, Any]] = {}
        self._pipeline_threads: Dict[str, Any] = {}
        self._shutdown_event = Event()
        self._lock = Lock()

        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_pipelines,
            thread_name_prefix="streaming_pipeline",
        )

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
                    f"Pipeline '{execution_id}' started {len(processed_nodes)} queries, beginning monitoring..."
                )
                self._monitor_pipeline_queries(execution_id)
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
        """Process each node configuration for the pipeline."""
        processed_nodes = []
        for i, node_config in enumerate(nodes):
            if self._shutdown_event.is_set():
                logger.info(f"Shutdown requested, stopping pipeline '{execution_id}'")
                break
            try:
                node_config = self._get_node_config(node_config, i)
                query = self.query_manager.create_and_start_query(
                    node_config, execution_id, pipeline_name
                )
                node_name = node_config.get("name", f"node_{i}")
                with self._lock:
                    self._running_pipelines[execution_id]["queries"][node_name] = query
                    self._running_pipelines[execution_id]["completed_nodes"] += 1
                processed_nodes.append(node_name)
                logger.info(f"Started streaming query '{node_name}' in pipeline '{execution_id}'")
            except Exception as e:
                logger.error(f"Error processing node {i} in pipeline '{execution_id}': {str(e)}")
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "partial_failure"
                    self._running_pipelines[execution_id]["error"] = str(e)
                break
        return processed_nodes

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

    def _monitor_pipeline_queries(self, execution_id: str) -> None:
        """Monitor queries in a pipeline until completion or error with enhanced monitoring."""
        pipeline_info = self._running_pipelines.get(execution_id)
        if not pipeline_info:
            return

        logger.info(f"Monitoring queries for pipeline '{execution_id}'")
        monitoring_interval = 5  # seconds
        last_health_check = time.time()
        health_check_interval = 30  # seconds

        while not self._shutdown_event.is_set():
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
                    break

                if self._handle_completed_queries(execution_id, active_queries, completed_queries):
                    break

                self._periodic_health_check(
                    execution_id,
                    active_queries,
                    completed_queries,
                    last_health_check,
                    health_check_interval,
                )
                last_health_check = time.time()

                time.sleep(monitoring_interval)

            except Exception as e:
                logger.error(f"Error monitoring pipeline '{execution_id}': {str(e)}")
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)
                break

        logger.info(f"Stopped monitoring pipeline '{execution_id}'")

    def _collect_query_states(self, pipeline_info):
        active_queries = 0
        failed_queries = []
        completed_queries = []

        for query_name, query in pipeline_info["queries"].items():
            state, info = self._get_query_state(query_name, query)
            if state == "active":
                active_queries += 1
            elif state == "failed":
                failed_queries.append((query_name, info))
            elif state == "completed":
                completed_queries.append(query_name)

        return active_queries, failed_queries, completed_queries

    def _get_query_state(self, query_name, query):
        if not isinstance(query, StreamingQuery):
            return None, None
        try:
            if query.isActive:
                return "active", None
            try:
                exception = query.exception()
            except Exception:
                exception = None
            if exception:
                return "failed", str(exception)
            else:
                return "completed", None
        except Exception as e:
            logger.error(f"Error checking query '{query_name}' status: {str(e)}")
            return "failed", str(e)

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
