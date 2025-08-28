import gc
import time
from typing import Any, Dict, List, Optional, Set, Union

from loguru import logger  # type: ignore

from tauro.config.contexts import Context
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.node_executor import NodeExecutor
from tauro.exec.pipeline_state import NodeType, UnifiedPipelineState
from tauro.exec.pipeline_validator import PipelineValidator
from tauro.exec.utils import extract_pipeline_nodes, get_node_dependencies
from tauro.io.input import InputLoader
from tauro.io.output import OutputManager
from tauro.streaming.constants import PipelineType
from tauro.streaming.pipeline_manager import StreamingPipelineManager


class BaseExecutor:
    """Base class for pipeline executors."""

    def __init__(self, context: Context):
        self.context = context
        self.input_loader = InputLoader(self.context)
        self.output_manager = OutputManager(self.context)
        self.is_ml_layer = getattr(self.context, "is_ml_layer", False)
        gs = getattr(self.context, "global_settings", {}) or {}
        self.max_workers = gs.get("max_parallel_nodes", 4)
        self.node_executor = NodeExecutor(
            self.context, self.input_loader, self.output_manager, self.max_workers
        )
        self.unified_state = None

    def _prepare_ml_info(
        self,
        pipeline_name: str,
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Prepare ML-specific information."""
        ml_info: Dict[str, Any] = {}
        pipeline_ml_config: Dict[str, Any] = {}

        final_hyperparams = dict(hyperparams or {})
        final_model_version = model_version or getattr(
            self.context, "default_model_version", None
        )

        if hasattr(self.context, "get_pipeline_ml_config"):
            pipeline_ml_config = (
                self.context.get_pipeline_ml_config(pipeline_name) or {}
            )
            final_model_version = (
                model_version
                or pipeline_ml_config.get("model_version")
                or getattr(self.context, "default_model_version", None)
            )

            final_hyperparams.update(
                getattr(self.context, "default_hyperparams", {}) or {}
            )
            final_hyperparams.update(pipeline_ml_config.get("hyperparams", {}) or {})
            if hyperparams:
                final_hyperparams.update(hyperparams)

            if hasattr(self.context, "get_model_registry"):
                model_registry = self.context.get_model_registry()
                try:
                    ml_info["model"] = model_registry.get_model(
                        pipeline_ml_config.get("model_name"),
                        version=final_model_version,
                    )
                except Exception:
                    pass

            ml_info = {
                "model_version": final_model_version,
                "hyperparams": final_hyperparams,
                "pipeline_config": pipeline_ml_config,
                "project_name": getattr(self.context, "project_name", ""),
                "is_experiment": self._is_experiment_pipeline(pipeline_name),
            }

        elif self.is_ml_layer:
            final_hyperparams.update(
                getattr(self.context, "default_hyperparams", {}) or {}
            )
            ml_info = {
                "model_version": final_model_version,
                "hyperparams": final_hyperparams,
                "pipeline_config": pipeline_ml_config,
                "is_experiment": self._is_experiment_pipeline(pipeline_name),
            }

        return ml_info

    def _is_experiment_pipeline(self, pipeline_name: str) -> bool:
        """Check if it's an experimentation pipeline."""
        return (
            "experiment" in pipeline_name.lower() or "tuning" in pipeline_name.lower()
        )

    def _log_pipeline_start(
        self, pipeline_name: str, ml_info: Dict[str, Any], pipeline_type: str
    ) -> None:
        """Log pipeline execution start."""
        if self.is_ml_layer:
            logger.info("=" * 60)
            logger.info(f"🚀 Starting {pipeline_type} ML Pipeline: '{pipeline_name}'")
            logger.info(f"📦 Project: {ml_info.get('project_name', 'Unknown')}")
            logger.info(f"🏷️  Model Version: {ml_info.get('model_version', 'Unknown')}")
        else:
            logger.info(f"Running {pipeline_type.lower()} pipeline '{pipeline_name}'")

    def _get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get pipeline configuration from context."""
        pipeline = self.context.pipelines.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        return pipeline

    def _extract_pipeline_nodes(self, pipeline: Dict[str, Any]) -> List[str]:
        """Extract node names from pipeline configuration."""
        return extract_pipeline_nodes(pipeline)

    def _get_node_configs(self, pipeline_nodes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all pipeline nodes."""
        return {
            node_name: self.context.nodes_config[node_name]
            for node_name in pipeline_nodes
            if node_name in self.context.nodes_config
        }


class BatchExecutor(BaseExecutor):
    """Executor for batch pipelines."""

    def execute(
        self,
        pipeline_name: str,
        node_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> None:
        logger.info(f"Executing batch pipeline: {pipeline_name}")

        pipeline = self._get_pipeline_config(pipeline_name)

        start_date = start_date or self.context.global_settings.get("start_date")
        end_date = end_date or self.context.global_settings.get("end_date")

        ml_info = self._prepare_ml_info(pipeline_name, model_version, hyperparams)
        self._log_pipeline_start(pipeline_name, ml_info, "BATCH")

        self.unified_state = UnifiedPipelineState()
        self.unified_state.set_pipeline_status("running")

        try:
            self._execute_batch_flow(pipeline, node_name, start_date, end_date, ml_info)
            self.unified_state.set_pipeline_status("completed")
        except Exception:
            self.unified_state.set_pipeline_status("failed")
            raise
        finally:
            if self.unified_state:
                self.unified_state.cleanup()

    def _execute_batch_flow(
        self,
        pipeline: Dict[str, Any],
        node_name: Optional[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute batch flow logic."""
        if node_name:
            self.node_executor.execute_single_node(
                node_name, start_date, end_date, ml_info
            )
        else:
            pipeline_nodes = self._extract_pipeline_nodes(pipeline)
            self._execute_pipeline_nodes(pipeline_nodes, start_date, end_date, ml_info)

    def _execute_pipeline_nodes(
        self,
        pipeline_nodes: List[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute all nodes in batch pipeline."""
        node_configs = self._get_node_configs(pipeline_nodes)
        PipelineValidator.validate_node_configs(pipeline_nodes, node_configs)

        dag = DependencyResolver.build_dependency_graph(pipeline_nodes, node_configs)
        execution_order = DependencyResolver.topological_sort(dag)

        self.node_executor.execute_nodes_parallel(
            execution_order, node_configs, dag, start_date, end_date, ml_info
        )


class StreamingExecutor(BaseExecutor):
    """Executor for streaming pipelines."""

    def __init__(self, context: Context):
        super().__init__(context)
        max_streaming_pipelines = context.global_settings.get(
            "max_streaming_pipelines", 5
        )
        # Inyectar policy del contexto dentro del manager (el manager crea su validador con policy)
        self.streaming_manager = StreamingPipelineManager(
            context, max_streaming_pipelines
        )

    def execute(
        self,
        pipeline_name: str,
        execution_mode: Optional[str] = "async",
    ) -> str:
        logger.info(f"Executing streaming pipeline: {pipeline_name}")

        pipeline = self._get_pipeline_config(pipeline_name)
        # Validación con el validador del manager (único punto de verdad)
        self.streaming_manager.validator.validate_streaming_pipeline_config(pipeline)

        running_pipelines = self.streaming_manager.list_running_pipelines()
        conflicts = self._check_resource_conflicts(pipeline, running_pipelines)

        if conflicts:
            logger.warning(f"Potential resource conflicts detected: {conflicts}")

        execution_id = self.streaming_manager.start_pipeline(pipeline_name, pipeline)

        logger.info(
            f"Streaming pipeline '{pipeline_name}' started with execution_id: {execution_id}"
        )

        if execution_mode == "sync":
            self._wait_for_streaming_pipeline(execution_id)

        return execution_id

    def _check_resource_conflicts(
        self, pipeline: Dict[str, Any], running_pipelines: List[Dict[str, Any]]
    ) -> List[str]:
        """Check resource conflicts with running pipelines."""
        conflicts = []
        current_resources = self._extract_pipeline_resources(pipeline)

        for running in running_pipelines:
            running_resources = self._extract_pipeline_resources(running)

            common_topics = (
                current_resources["kafka_topics"] & running_resources["kafka_topics"]
            )
            if common_topics:
                conflicts.append(
                    f"Kafka topic conflict: topics {', '.join(common_topics)}"
                )

            common_paths = (
                current_resources["file_paths"] & running_resources["file_paths"]
            )
            if common_paths:
                conflicts.append(f"File path conflict: paths {', '.join(common_paths)}")

            common_tables = (
                current_resources["delta_tables"] & running_resources["delta_tables"]
            )
            if common_tables:
                conflicts.append(
                    f"Delta table conflict: tables {', '.join(common_tables)}"
                )

        return conflicts

    def _extract_pipeline_resources(
        self, pipeline: Dict[str, Any]
    ) -> Dict[str, Set[str]]:
        """Extract critical resources from pipeline configuration."""
        resources = {"kafka_topics": set(), "file_paths": set(), "delta_tables": set()}

        pipeline_nodes = self._extract_pipeline_nodes(pipeline)
        for node_name in pipeline_nodes:
            node_config = self.context.nodes_config.get(node_name, {})

            # Input resources
            input_config = node_config.get("input", {})
            input_format = input_config.get("format")
            if input_format == "kafka":
                topics = input_config.get("options", {}).get("topics", [])
                if isinstance(topics, str):
                    topics = [topics]
                resources["kafka_topics"].update(topics)
            elif input_format == "file_stream":
                path = input_config.get("options", {}).get("path")
                if path:
                    resources["file_paths"].add(path)
            elif input_format == "delta":
                path = input_config.get("options", {}).get("path")
                if path:
                    resources["delta_tables"].add(path)

            output_config = node_config.get("output", {})
            output_format = output_config.get("format")
            if output_format == "kafka":
                topic = output_config.get("options", {}).get("topic")
                if topic:
                    resources["kafka_topics"].add(topic)
            elif output_format == "delta":
                path = output_config.get("options", {}).get("path")
                if path:
                    resources["delta_tables"].add(path)

        return resources

    def _wait_for_streaming_pipeline(
        self, execution_id: str, timeout_seconds: int = 300
    ) -> None:
        """Wait for streaming pipeline to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status = self.streaming_manager.get_pipeline_status(execution_id)
            if status in ["completed", "error", "stopped"]:
                break
            time.sleep(5)


class HybridExecutor(BaseExecutor):
    """Executor for hybrid pipelines."""

    def __init__(self, context: Context):
        super().__init__(context)
        max_streaming_pipelines = context.global_settings.get(
            "max_streaming_pipelines", 5
        )
        self.streaming_manager = StreamingPipelineManager(
            context, max_streaming_pipelines
        )
        self.max_retries = context.global_settings.get("max_retries", 3)
        self.retry_delay = context.global_settings.get("retry_delay", 5)

    def execute(
        self,
        pipeline_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
        execution_mode: Optional[str] = "async",
    ) -> Dict[str, Any]:
        logger.info(f"Executing hybrid pipeline: {pipeline_name}")

        pipeline = self._get_pipeline_config(pipeline_name)
        pipeline_nodes = self._extract_pipeline_nodes(pipeline)
        node_configs = self._get_node_configs(pipeline_nodes)

        # Usar la misma política de formatos del contexto via PipelineValidator (batch/streaming/híbrido)
        validation_result = PipelineValidator.validate_hybrid_pipeline(
            pipeline, node_configs, self.context.format_policy
        )

        if not validation_result["is_valid"]:
            raise ValueError("Hybrid pipeline validation failed")

        self.unified_state = UnifiedPipelineState()
        ml_info = self._prepare_ml_info(pipeline_name, model_version, hyperparams)

        try:
            self._register_nodes_in_unified_state(
                validation_result["batch_nodes"],
                validation_result["streaming_nodes"],
                node_configs,
            )
            self.unified_state.set_streaming_stopper(
                lambda eid: self.streaming_manager.stop_pipeline(eid, graceful=True)
            )
            return self._execute_unified_hybrid_pipeline(
                validation_result["batch_nodes"],
                validation_result["streaming_nodes"],
                node_configs,
                start_date or self.context.global_settings.get("start_date"),
                end_date or self.context.global_settings.get("end_date"),
                ml_info,
                execution_mode,
            )
        finally:
            if self.unified_state:
                self.unified_state.cleanup()

    def _register_nodes_in_unified_state(
        self,
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Register all nodes in unified state."""
        for node_name in batch_nodes:
            dependencies = get_node_dependencies(node_configs[node_name])
            self.unified_state.register_node(node_name, NodeType.BATCH, dependencies)

        for node_name in streaming_nodes:
            dependencies = get_node_dependencies(node_configs[node_name])
            self.unified_state.register_node(
                node_name, NodeType.STREAMING, dependencies
            )

    def _execute_unified_hybrid_pipeline(
        self,
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
        execution_mode: str,
    ) -> Dict[str, Any]:
        """Execute hybrid pipeline with enhanced error handling."""
        execution_result = {
            "batch_execution": {},
            "streaming_execution_ids": [],
            "status": "success",
            "errors": [],
        }

        try:
            batch_results = self._execute_batch_phase(
                batch_nodes, node_configs, start_date, end_date, ml_info
            )
            execution_result["batch_execution"] = batch_results

            batch_failures = [
                node
                for node, result in batch_results.items()
                if result["status"] != "completed"
            ]

            if batch_failures:
                execution_result["status"] = "failed"
                execution_result["errors"] = [
                    f"Batch node failed: {node} - {batch_results[node].get('error')}"
                    for node in batch_failures
                ]
                logger.error("Batch phase failed, skipping streaming execution")
                return execution_result

            streaming_execution_ids = self._execute_streaming_phase(
                streaming_nodes, node_configs, execution_mode
            )
            execution_result["streaming_execution_ids"] = streaming_execution_ids

        except Exception as e:
            execution_result["status"] = "failed"
            execution_result["errors"].append(f"Hybrid pipeline failed: {str(e)}")
            logger.error(f"Hybrid pipeline execution failed: {str(e)}")

        return execution_result

    def _execute_batch_phase(
        self,
        batch_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """Execute batch nodes with retries and dependency resolution."""
        results = {}
        dag = DependencyResolver.build_dependency_graph(batch_nodes, node_configs)
        execution_order = DependencyResolver.topological_sort(dag)

        for node in execution_order:
            if not self.unified_state.start_node_execution(node):
                continue

            for attempt in range(self.max_retries + 1):
                try:
                    self._execute_node_with_retry(
                        node, start_date, end_date, ml_info, attempt
                    )
                    results[node] = {"status": "completed"}
                    self.unified_state.complete_node_execution(node)
                    break

                except Exception as e:
                    if attempt < self.max_retries:
                        logger.warning(
                            f"Retrying node '{node}' (attempt {attempt+1}/{self.max_retries})"
                        )
                        time.sleep(self.retry_delay * (attempt + 1))
                    else:
                        results[node] = {"status": "failed", "error": str(e)}
                        self.unified_state.fail_node_execution(node, str(e))
                        self._handle_batch_failure(node, results)
                        raise

        return results

    def _execute_node_with_retry(
        self,
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
        attempt: int,
    ) -> None:
        """Execute a node with retry logic."""
        try:
            self.node_executor.execute_single_node(
                node_name, start_date, end_date, ml_info
            )
        except Exception:
            if attempt < self.max_retries:
                logger.warning(
                    f"Attempt {attempt+1} failed for node '{node_name}'. Retrying..."
                )
                raise
            else:
                logger.error(
                    f"Node '{node_name}' failed after {self.max_retries} attempts"
                )
                raise

    def _handle_batch_failure(self, failed_node: str, results: Dict[str, Any]):
        """Handle batch node failure: cancel dependents and stop related streaming nodes."""
        logger.error(f"Batch node '{failed_node}' failed, cleaning up dependents")

        for node in list(results.keys()):
            if node != failed_node and self.unified_state.is_node_pending(node):
                if failed_node in self.unified_state.get_node_dependencies(node):
                    self.unified_state.cancel_node_execution(node)
                    results[node] = {
                        "status": "cancelled",
                        "reason": f"Dependency {failed_node} failed",
                    }
                    logger.warning(f"Cancelled dependent node: {node}")

        stopped_streaming = self.unified_state.stop_dependent_streaming_nodes(
            failed_node
        )
        for node in stopped_streaming:
            logger.warning(f"Stopped streaming node due to batch failure: {node}")

    def _execute_streaming_phase(
        self,
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        execution_mode: str,
    ) -> List[str]:
        """Start all streaming nodes and manage their lifecycle."""
        execution_ids = []
        for node in streaming_nodes:
            if not self.unified_state.start_node_execution(node):
                continue

            try:
                execution_id = self.streaming_manager.start_streaming_node(
                    node, node_configs[node]
                )
                execution_ids.append(execution_id)
                self.unified_state.register_streaming_query(node, execution_id)
                self.unified_state.complete_node_execution(node)
            except Exception as e:
                logger.error(f"Failed to start streaming node '{node}': {str(e)}")
                self.unified_state.fail_node_execution(node, str(e))

        if execution_mode == "sync":
            self._wait_for_streaming_completion(execution_ids)

        return execution_ids

    def _wait_for_streaming_completion(
        self, execution_ids: List[str], timeout_minutes=60
    ):
        """Wait for streaming queries to complete (for sync execution)."""
        start_time = time.time()
        while time.time() - start_time < timeout_minutes * 60:
            active_queries = [
                eid
                for eid in execution_ids
                if self.streaming_manager.is_query_active(eid)
            ]
            if not active_queries:
                return
            time.sleep(5)

        logger.warning("Timeout reached while waiting for streaming queries")


class PipelineExecutor:
    """Orchestrator that delegates to specialized executors."""

    def __init__(self, context: Context):
        self.context = context
        self.batch_executor = BatchExecutor(context)
        self.streaming_executor = StreamingExecutor(context)
        self.hybrid_executor = HybridExecutor(context)

    def run_pipeline(
        self,
        pipeline_name: str,
        node_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
        execution_mode: Optional[str] = "async",
    ) -> Union[None, str, Dict[str, Any]]:
        PipelineValidator.validate_required_params(
            pipeline_name,
            start_date,
            end_date,
            self.context.global_settings.get("start_date"),
            self.context.global_settings.get("end_date"),
        )

        pipeline = self.batch_executor._get_pipeline_config(pipeline_name)
        pipeline_type = pipeline.get("type", PipelineType.BATCH.value)

        logger.info(f"Executing {pipeline_type} pipeline: '{pipeline_name}'")

        if pipeline_type == PipelineType.BATCH.value:
            return self.batch_executor.execute(
                pipeline_name,
                node_name,
                start_date,
                end_date,
                model_version,
                hyperparams,
            )
        elif pipeline_type == PipelineType.STREAMING.value:
            return self.streaming_executor.execute(pipeline_name, execution_mode)
        elif pipeline_type == PipelineType.HYBRID.value:
            return self.hybrid_executor.execute(
                pipeline_name,
                start_date,
                end_date,
                model_version,
                hyperparams,
                execution_mode,
            )
        else:
            raise ValueError(f"Unsupported pipeline type: {pipeline_type}")

    # Helpers para CLI de streaming (status/stop/metrics/list)

    def get_streaming_pipeline_status(self, execution_id: str) -> Dict[str, Any]:
        """Return status info for a streaming pipeline execution."""
        try:
            return (
                self.streaming_executor.streaming_manager.get_pipeline_status(
                    execution_id
                )
                or {}
            )
        except Exception:
            return {}

    def list_streaming_pipelines(self) -> List[Dict[str, Any]]:
        """List running streaming pipelines."""
        try:
            return self.streaming_executor.streaming_manager.list_running_pipelines()
        except Exception:
            return []

    def stop_streaming_pipeline(self, execution_id: str, graceful: bool = True) -> bool:
        """Stop a running streaming pipeline."""
        try:
            return self.streaming_executor.streaming_manager.stop_pipeline(
                execution_id, graceful
            )
        except Exception:
            return False

    def get_streaming_pipeline_metrics(self, execution_id: str) -> Dict[str, Any]:
        """Get metrics for a streaming pipeline."""
        try:
            return (
                self.streaming_executor.streaming_manager.get_pipeline_metrics(
                    execution_id
                )
                or {}
            )
        except Exception:
            return {}

    def shutdown(self, timeout_seconds: int = 30) -> None:
        """Unified shutdown with resource cleanup"""
        shutdown_sequence = [
            (self._stop_streaming_queries, 5),
            (self._release_connection_pools, 10),
            (self._release_gpu_resources, 15),
            (self._cleanup_memory, 20),
        ]

        for step, timeout in shutdown_sequence:
            try:
                logger.info(f"Executing shutdown step: {step.__name__}")
                step(timeout)
            except Exception as e:
                logger.error(f"Shutdown error in {step.__name__}: {str(e)}")

    def _stop_streaming_queries(self, _):
        """Stop active streaming queries if the manager supports it"""
        if hasattr(self.streaming_executor, "streaming_manager"):
            try:
                self.streaming_executor.streaming_manager.stop_all()
            except Exception:
                pass

    def _release_connection_pools(self, _):
        """Release all database connections"""
        if hasattr(self.context, "connection_pools"):
            for pool in self.context.connection_pools.values():
                pool.shutdown()

    def _release_gpu_resources(self, _):
        """Placeholder for GPU/accelerator cleanup"""
        pass

    def _cleanup_memory(self, _):
        """Force memory cleanup"""
        gc.collect()
        if getattr(self.context, "spark", None):
            try:
                self.context.spark.catalog.clearCache()
            except Exception:
                pass
