import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Event, Lock
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore
from pyspark.sql.streaming import StreamingQuery  # type: ignore

from tauro.streaming.exceptions import (
    StreamingError,
    StreamingPipelineError,
    create_error_context,
    handle_streaming_error,
)
from tauro.streaming.query_manager import StreamingQueryManager
from tauro.streaming.validators import StreamingValidator


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
                )

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
                    logger.warning(
                        f"Pipeline '{execution_id}' not found or not running"
                    )
                    return False

                pipeline_info["status"] = "stopping"

            pipeline_name = pipeline_info["pipeline_name"]
            logger.info(
                f"Stopping streaming pipeline '{pipeline_name}' (ID: {execution_id}, graceful={graceful})"
            )

            # Stop all streaming queries
            stopped_queries = []
            failed_queries = []

            for query_name, query in pipeline_info["queries"].items():
                try:
                    if isinstance(query, StreamingQuery) and query.isActive:
                        logger.info(
                            f"Stopping query '{query_name}' in pipeline '{execution_id}'"
                        )

                        success = self.query_manager.stop_query(
                            query, graceful, timeout_seconds
                        )
                        if success:
                            stopped_queries.append(query_name)
                        else:
                            failed_queries.append(query_name)

                except Exception as e:
                    logger.error(f"Error stopping query '{query_name}': {str(e)}")
                    failed_queries.append(query_name)

            # Handle pipeline thread
            if not graceful and execution_id in self._pipeline_threads:
                future = self._pipeline_threads[execution_id]
                future.cancel()

            # Update pipeline status
            with self._lock:
                if execution_id in self._running_pipelines:
                    self._running_pipelines[execution_id]["status"] = "stopped"
                    self._running_pipelines[execution_id]["end_time"] = time.time()
                    self._running_pipelines[execution_id][
                        "stopped_queries"
                    ] = stopped_queries
                    self._running_pipelines[execution_id][
                        "failed_queries"
                    ] = failed_queries

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
            )

    def get_pipeline_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific pipeline with enhanced information."""
        try:
            with self._lock:
                pipeline_info = self._running_pipelines.get(execution_id)
                if not pipeline_info:
                    return None

                # Enrich with real-time query status
                status = pipeline_info.copy()
                query_statuses = {}
                active_queries = 0
                failed_queries = 0

                for query_name, query in pipeline_info["queries"].items():
                    if isinstance(query, StreamingQuery):
                        try:
                            is_active = query.isActive
                            query_status = {
                                "id": query.id,
                                "runId": str(query.runId),
                                "isActive": is_active,
                                "lastProgress": (
                                    query.lastProgress if is_active else None
                                ),
                            }

                            if is_active:
                                active_queries += 1
                            else:
                                # Check if query failed
                                try:
                                    exception = query.exception()
                                    if exception:
                                        query_status["exception"] = str(exception)
                                        failed_queries += 1
                                except Exception:
                                    pass

                            query_statuses[query_name] = query_status

                        except Exception as e:
                            query_statuses[query_name] = {"error": str(e)}
                            failed_queries += 1
                    else:
                        query_statuses[query_name] = {"status": "unknown"}

                status["query_statuses"] = query_statuses
                status["active_queries"] = active_queries
                status["failed_queries"] = failed_queries
                status["total_queries"] = len(pipeline_info["queries"])

                # Calculate uptime
                if "start_time" in status:
                    end_time = status.get("end_time", time.time())
                    status["uptime_seconds"] = end_time - status["start_time"]

                return status

        except Exception as e:
            logger.error(
                f"Error getting pipeline status for '{execution_id}': {str(e)}"
            )
            return {
                "execution_id": execution_id,
                "status": "error",
                "error": f"Failed to get status: {str(e)}",
            }

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
            for query_name, query_status in pipeline_info.get(
                "query_statuses", {}
            ).items():
                if (
                    query_status.get("isActive")
                    and "lastProgress" in query_status
                    and query_status["lastProgress"]
                ):
                    progress = query_status["lastProgress"]
                    metrics["query_metrics"][query_name] = {
                        "batchId": progress.get("batchId"),
                        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                        "processedRowsPerSecond": progress.get(
                            "processedRowsPerSecond"
                        ),
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
                    (total_processing_rate / total_input_rate * 100)
                    if total_input_rate > 0
                    else 0
                ),
                "health_score": self._calculate_health_score(pipeline_info),
            }

            return metrics

        except Exception as e:
            logger.error(
                f"Error getting pipeline metrics for '{execution_id}': {str(e)}"
            )
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

            health_score = max(
                0, min(100, active_score + success_bonus - failure_penalty)
            )
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

            for execution_id, future in list(self._pipeline_threads.items()):
                try:
                    future.result(
                        timeout=(
                            timeout_seconds // len(self._pipeline_threads)
                            if self._pipeline_threads
                            else timeout_seconds
                        )
                    )
                    completed_threads += 1
                except FutureTimeoutError:
                    logger.warning(
                        f"Pipeline thread '{execution_id}' did not complete within timeout"
                    )
                    future.cancel()
                except Exception as e:
                    logger.warning(
                        f"Error waiting for pipeline '{execution_id}' to finish: {e}"
                    )

            logger.info(
                f"Completed {completed_threads}/{len(self._pipeline_threads)} pipeline threads"
            )

            # Shutdown executor
            logger.info("Shutting down thread pool executor...")
            self._executor.shutdown(wait=True, timeout=timeout_seconds)

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
            )

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
                    logger.error(
                        f"Pipeline {execution_id} not found in running pipelines"
                    )
                    return
                self._running_pipelines[execution_id]["status"] = "running"

            logger.info(
                f"Executing streaming pipeline '{pipeline_name}' with execution_id: {execution_id}"
            )

            # Get pipeline nodes in execution order
            nodes = pipeline_config.get("nodes", [])
            if not nodes:
                raise StreamingPipelineError(
                    f"No nodes defined in streaming pipeline '{pipeline_name}'",
                    pipeline_name=pipeline_name,
                    execution_id=execution_id,
                )

            # Process each node configuration
            processed_nodes = []
            for i, node_config in enumerate(nodes):
                if self._shutdown_event.is_set():
                    logger.info(
                        f"Shutdown requested, stopping pipeline '{execution_id}'"
                    )
                    break

                try:
                    # Ensure node has proper configuration
                    if isinstance(node_config, str):
                        # Node reference - load from context
                        node_name = node_config
                        actual_config = self.context.nodes_config.get(node_name)
                        if not actual_config:
                            raise StreamingPipelineError(
                                f"Node configuration '{node_name}' not found",
                                pipeline_name=pipeline_name,
                                execution_id=execution_id,
                            )
                        node_config = {**actual_config, "name": node_name}
                    elif isinstance(node_config, dict):
                        if "name" not in node_config:
                            node_config["name"] = f"node_{i}"
                    else:
                        raise StreamingPipelineError(
                            f"Invalid node configuration type: {type(node_config)}",
                            pipeline_name=pipeline_name,
                            execution_id=execution_id,
                        )

                    # Create and start streaming query
                    query = self.query_manager.create_and_start_query(
                        node_config, execution_id, pipeline_name
                    )

                    node_name = node_config.get("name", f"node_{i}")
                    with self._lock:
                        self._running_pipelines[execution_id]["queries"][
                            node_name
                        ] = query
                        self._running_pipelines[execution_id]["completed_nodes"] += 1

                    processed_nodes.append(node_name)
                    logger.info(
                        f"Started streaming query '{node_name}' in pipeline '{execution_id}'"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing node {i} in pipeline '{execution_id}': {str(e)}"
                    )
                    # Mark pipeline as failed but continue monitoring existing queries
                    with self._lock:
                        self._running_pipelines[execution_id][
                            "status"
                        ] = "partial_failure"
                        self._running_pipelines[execution_id]["error"] = str(e)
                    break

            # Monitor queries until shutdown or failure
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
            logger.error(
                f"Error executing streaming pipeline '{execution_id}': {str(e)}"
            )
            with self._lock:
                if execution_id in self._running_pipelines:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)
            raise

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
                active_queries = 0
                failed_queries = []
                completed_queries = []

                for query_name, query in pipeline_info["queries"].items():
                    if isinstance(query, StreamingQuery):
                        try:
                            if query.isActive:
                                active_queries += 1
                            else:
                                # Query has stopped - check if it failed
                                try:
                                    exception = query.exception()
                                    if exception:
                                        failed_queries.append(
                                            (query_name, str(exception))
                                        )
                                    else:
                                        completed_queries.append(query_name)
                                except Exception:
                                    # Query might have completed successfully
                                    completed_queries.append(query_name)
                        except Exception as e:
                            logger.error(
                                f"Error checking query '{query_name}' status: {str(e)}"
                            )
                            failed_queries.append((query_name, str(e)))

                # Update pipeline status based on query states
                with self._lock:
                    if execution_id in self._running_pipelines:
                        self._running_pipelines[execution_id][
                            "active_queries"
                        ] = active_queries
                        self._running_pipelines[execution_id]["completed_queries"] = (
                            len(completed_queries)
                        )
                        self._running_pipelines[execution_id]["failed_queries"] = len(
                            failed_queries
                        )

                # Handle failed queries
                if failed_queries:
                    error_msg = f"Queries failed: {failed_queries}"
                    logger.error(
                        f"Pipeline '{execution_id}' has failed queries: {error_msg}"
                    )
                    with self._lock:
                        self._running_pipelines[execution_id]["status"] = "error"
                        self._running_pipelines[execution_id]["error"] = error_msg
                    break

                # Check if all queries completed
                if active_queries == 0:
                    if completed_queries:
                        logger.info(
                            f"All queries completed successfully for pipeline '{execution_id}': {completed_queries}"
                        )
                        with self._lock:
                            self._running_pipelines[execution_id][
                                "status"
                            ] = "completed"
                    else:
                        logger.warning(
                            f"No active queries remaining for pipeline '{execution_id}' but none completed successfully"
                        )
                        with self._lock:
                            self._running_pipelines[execution_id]["status"] = "stopped"
                    break

                # Periodic health check
                current_time = time.time()
                if current_time - last_health_check > health_check_interval:
                    health_score = self._calculate_health_score(
                        self._running_pipelines[execution_id]
                    )
                    logger.debug(
                        f"Pipeline '{execution_id}' health score: {health_score}% (active: {active_queries}, completed: {len(completed_queries)})"
                    )
                    last_health_check = current_time

                # Sleep before next check
                time.sleep(monitoring_interval)

            except Exception as e:
                logger.error(f"Error monitoring pipeline '{execution_id}': {str(e)}")
                with self._lock:
                    self._running_pipelines[execution_id]["status"] = "error"
                    self._running_pipelines[execution_id]["error"] = str(e)
                break

        logger.info(f"Stopped monitoring pipeline '{execution_id}'")

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

                avg_health_score = (
                    sum(health_scores) / len(health_scores) if health_scores else 0
                )
                healthy_count = sum(1 for score in health_scores if score >= 80)

                return {
                    "total_pipelines": total_pipelines,
                    "healthy_pipelines": healthy_count,
                    "unhealthy_pipelines": total_pipelines - healthy_count,
                    "overall_health_score": round(avg_health_score, 2),
                    "status_breakdown": status_counts,
                    "individual_health_scores": health_scores,
                    "status": (
                        "healthy"
                        if avg_health_score >= 80
                        else "degraded" if avg_health_score >= 50 else "critical"
                    ),
                }

        except Exception as e:
            logger.error(f"Error calculating pipeline health summary: {str(e)}")
            return {"total_pipelines": 0, "error": str(e), "status": "error"}
