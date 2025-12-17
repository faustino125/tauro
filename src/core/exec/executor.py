"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import gc
import json
import time
from typing import Any, Dict, List, Optional, Set, Union, Tuple

from loguru import logger  # type: ignore

from core.config.contexts import Context
from core.exec.dependency_resolver import DependencyResolver
from core.exec.mlops_auto_config import MLOpsAutoConfigurator
from core.exec.node_executor import NodeExecutor
from core.exec.pipeline_state import NodeType, UnifiedPipelineState
from core.exec.resilience import RetryPolicy

try:
    from core.exec.mlflow_node_executor import MLflowNodeExecutor
    from core.mlops.mlflow import MLflowPipelineTracker, is_mlflow_available

    MLFLOW_INTEGRATION_AVAILABLE = True
except ImportError:
    MLFLOW_INTEGRATION_AVAILABLE = False
    MLflowNodeExecutor = None
    MLflowPipelineTracker = None
from core.exec.pipeline_validator import PipelineValidator
from core.exec.mlops_integration import MLOpsExecutorIntegration
from core.exec.utils import extract_pipeline_nodes, get_node_dependencies
from core.io.input import InputLoader
from core.io.output import DataOutputManager
from core.streaming.constants import PipelineType
from core.streaming.pipeline_manager import StreamingPipelineManager


class BaseExecutor:
    """Base class for pipeline executors."""

    # Default execution timeout (1 hour)
    DEFAULT_TIMEOUT_SECONDS = 3600
    # Maximum reasonable timeout (24 hours)
    MAX_TIMEOUT_SECONDS = 86400

    def __init__(self, context: Context):
        self.context = context
        self.input_loader = InputLoader(self.context)
        self.output_manager = DataOutputManager(self.context)
        self.is_ml_layer = getattr(self.context, "is_ml_layer", False)
        gs = getattr(self.context, "global_settings", {}) or {}
        self.max_workers = gs.get("max_parallel_nodes", 4)

        # Configure timeout
        configured_timeout = gs.get("execution_timeout_seconds", self.DEFAULT_TIMEOUT_SECONDS)
        self.timeout_seconds = min(configured_timeout, self.MAX_TIMEOUT_SECONDS)
        if self.timeout_seconds != configured_timeout:
            logger.warning(
                f"Configured timeout {configured_timeout}s exceeds maximum, "
                f"using {self.timeout_seconds}s instead"
            )
        # Check if MLflow is enabled
        self._mlflow_enabled = self._should_enable_mlflow()
        self._mlflow_tracker = None

        # Create node executor (MLflow-enabled if configured)
        if self._mlflow_enabled and MLFLOW_INTEGRATION_AVAILABLE:
            try:
                self._mlflow_tracker = MLflowPipelineTracker.from_context(self.context)
                self.node_executor = MLflowNodeExecutor(
                    self.context,
                    self.input_loader,
                    self.output_manager,
                    self._mlflow_tracker,
                    self.max_workers,
                )
                logger.info("MLflow integration enabled for pipeline execution")
            except Exception as e:
                logger.warning(f"Could not enable MLflow: {e}. Falling back to standard executor.")
                self.node_executor = NodeExecutor(
                    self.context, self.input_loader, self.output_manager, self.max_workers
                )
        else:
            self.node_executor = NodeExecutor(
                self.context, self.input_loader, self.output_manager, self.max_workers
            )

        self.unified_state = None

        # Lazy MLOps initialization - only load when needed
        self._mlops_context = None
        self._mlops_auto_config = MLOpsAutoConfigurator()
        self._mlops_init_attempted = False

    def _should_enable_mlflow(self) -> bool:
        """
        Determine if MLflow should be enabled.
        """
        if not MLFLOW_INTEGRATION_AVAILABLE:
            return False

        import os

        # Check environment variable
        env_enabled = os.getenv("TAURO_MLFLOW_ENABLED", "false").lower() == "true"
        if env_enabled:
            return True

        # Check global settings
        gs = getattr(self.context, "global_settings", {}) or {}
        mlflow_config = gs.get("mlflow", {}) or {}

        return mlflow_config.get("enabled", False)

    def _should_init_mlops(self) -> bool:
        """
        Determine if MLOps should be initialized for this execution.
        """
        gs = getattr(self.context, "global_settings", {}) or {}

        # Use auto-configurator to decide
        return self._mlops_auto_config.should_init_mlops_for_pipeline(self.context.nodes_config, gs)

    def _init_mlops_if_needed(self) -> None:
        """
        Initialize MLOps lazily if needed.
        """
        if self._mlops_init_attempted:
            return

        self._mlops_init_attempted = True

        if not self._should_init_mlops():
            logger.debug("MLOps initialization skipped (not needed for this pipeline)")
            return

        try:
            from core.mlops.config import MLOpsContext

            self._mlops_context = MLOpsContext.from_context(
                self.context,
                auto_init=True,
            )
            logger.info("MLOps initialized successfully (auto-detected ML workload)")
        except Exception as e:
            logger.warning(f"MLOps initialization failed (non-critical): {e}")
            self._mlops_context = None

    @property
    def mlops_context(self):
        """
        Get MLOps context (lazy initialization).
        """
        if not self._mlops_init_attempted:
            self._init_mlops_if_needed()

        return self._mlops_context

    def _prepare_ml_info(
        self,
        pipeline_name: str,
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Prepare ML-specific information."""
        ml_info: Dict[str, Any] = {}
        pipeline_ml_config: Dict[str, Any] = {}
        initial_hyperparams = dict(hyperparams or {})
        final_model_version = model_version or getattr(self.context, "default_model_version", None)

        if hasattr(self.context, "get_pipeline_ml_config"):
            pipeline_ml_config = self.context.get_pipeline_ml_config(pipeline_name) or {}
            final_model_version = self._resolve_model_version(pipeline_ml_config, model_version)
            final_hyperparams = self._merge_hyperparams(
                pipeline_ml_config, initial_hyperparams, hyperparams
            )

            model = self._try_get_model(pipeline_ml_config, final_model_version)
            if model is not None:
                ml_info["model"] = model

            ml_info.update(
                {
                    "model_version": final_model_version,
                    "hyperparams": final_hyperparams,
                    "pipeline_config": pipeline_ml_config,
                    "project_name": getattr(self.context, "project_name", ""),
                    "is_experiment": self._is_experiment_pipeline(pipeline_name),
                }
            )

        elif self.is_ml_layer:
            final_hyperparams = self._merge_hyperparams({}, initial_hyperparams, hyperparams)
            ml_info = {
                "model_version": final_model_version,
                "hyperparams": final_hyperparams,
                "pipeline_config": pipeline_ml_config,
                "is_experiment": self._is_experiment_pipeline(pipeline_name),
            }

        return ml_info

    def _resolve_model_version(
        self, pipeline_ml_config: Dict[str, Any], model_version: Optional[str]
    ) -> Optional[str]:
        """Resolve final model version from args, pipeline config or context defaults."""
        return (
            model_version
            or pipeline_ml_config.get("model_version")
            or getattr(self.context, "default_model_version", None)
        )

    def _merge_hyperparams(
        self,
        pipeline_ml_config: Dict[str, Any],
        initial_hyperparams: Dict[str, Any],
        explicit_hyperparams: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge default, pipeline and explicit hyperparameters into a single dict."""
        final = dict(initial_hyperparams or {})
        final.update(getattr(self.context, "default_hyperparams", {}) or {})
        final.update(pipeline_ml_config.get("hyperparams", {}) or {})
        if explicit_hyperparams:
            final.update(explicit_hyperparams)
        return final

    def _try_get_model(
        self, pipeline_ml_config: Dict[str, Any], final_model_version: Optional[str]
    ) -> Optional[Any]:
        """Attempt to fetch a model from the model registry, returning None on failure."""
        if not hasattr(self.context, "get_model_registry"):
            return None
        try:
            model_registry = self.context.get_model_registry()
            return model_registry.get_model(
                pipeline_ml_config.get("model_name"), version=final_model_version
            )
        except Exception:
            return None

    def _is_experiment_pipeline(self, pipeline_name: str) -> bool:
        """Check if it's an experimentation pipeline."""
        return "experiment" in pipeline_name.lower() or "tuning" in pipeline_name.lower()

    def _log_pipeline_start(
        self, pipeline_name: str, ml_info: Dict[str, Any], pipeline_type: str
    ) -> None:
        """Log pipeline execution start."""
        if self.is_ml_layer:
            logger.info(f"🚀 Starting {pipeline_type} ML Pipeline: '{pipeline_name}'")
            logger.info(f"📦 Project: {ml_info.get('project_name', 'Unknown')}")

            model_version = ml_info.get("model_version")
            if model_version and str(model_version).lower() != "none":
                logger.info(f"🏷️  Model Version: {model_version}")
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
        pipeline = self._get_pipeline_config(pipeline_name)

        start_date = start_date or self.context.global_settings.get("start_date")
        end_date = end_date or self.context.global_settings.get("end_date")

        # Prepare ml_info and allow CLI overrides
        ml_info = self._build_ml_info(pipeline_name, model_version, hyperparams)

        self._log_pipeline_start(pipeline_name, ml_info, "BATCH")

        # Initialize unified state
        self.unified_state = UnifiedPipelineState()
        self.unified_state.set_pipeline_status("running")

        # Initialize MLOps integration (experiment/run lifecycle)
        mlops_integration, mlops_run_id, _ = self._start_mlops_integration(
            pipeline, pipeline_name, ml_info
        )

        try:
            # inject MLOps context into ml_info for nodes
            if mlops_integration and mlops_run_id:
                ml_info["mlops_integration"] = mlops_integration
                ml_info["mlops_run_id"] = mlops_run_id

            self._execute_batch_flow(pipeline, node_name, start_date, end_date, ml_info)
            self.unified_state.set_pipeline_status("completed")
            self._end_mlops_run_success(mlops_integration, mlops_run_id)

        except Exception:
            self.unified_state.set_pipeline_status("failed")
            self._end_mlops_run_failed(mlops_integration, mlops_run_id)
            raise
        finally:
            if self.unified_state:
                self.unified_state.cleanup()

    def _end_mlops_run_success(
        self, mlops_integration: Optional[MLOpsExecutorIntegration], mlops_run_id: Optional[str]
    ) -> None:
        """Close MLOps run on success with explicit exception handling."""
        if mlops_integration and mlops_run_id:
            try:
                mlops_integration.end_pipeline_run(mlops_run_id)
            except Exception as e:
                logger.error(f"Failed to end MLOps run {mlops_run_id}: {e}")
                raise RuntimeError(f"Pipeline completed but failed to record in MLOps: {e}") from e

    def _end_mlops_run_failed(
        self, mlops_integration: Optional[MLOpsExecutorIntegration], mlops_run_id: Optional[str]
    ) -> None:
        """Close MLOps run as failed with explicit exception handling."""
        if mlops_integration and mlops_run_id:
            try:
                from core.mlops.experiment_tracking import RunStatus

                mlops_integration.end_pipeline_run(mlops_run_id, status=RunStatus.FAILED)
            except Exception as mlops_error:
                logger.error(f"Failed to record pipeline failure in MLOps: {mlops_error}")

    def _build_ml_info(
        self,
        pipeline_name: str,
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build or fetch the ml_info and apply CLI overrides."""
        if hasattr(self.context, "get_pipeline_ml_info"):
            ml_info = self.context.get_pipeline_ml_info(pipeline_name) or {}
        else:
            ml_info = self._prepare_ml_info(pipeline_name, model_version, hyperparams)

        # Allow CLI overrides to take precedence over config
        if model_version is not None:
            ml_info["model_version"] = model_version

        if hyperparams:
            merged_h = dict(ml_info.get("hyperparams", {}) or {})
            merged_h.update(hyperparams)
            ml_info["hyperparams"] = merged_h

        return ml_info

    def _start_mlops_integration(
        self,
        pipeline: Dict[str, Any],
        pipeline_name: str,
        ml_info: Dict[str, Any],
    ) -> Tuple[Optional[MLOpsExecutorIntegration], Optional[str], Optional[str]]:
        """Initialize MLOps integration and start a pipeline run if available."""
        mlops_integration: Optional[MLOpsExecutorIntegration] = None
        mlops_run_id: Optional[str] = None
        experiment_id: Optional[str] = None

        # Check if MLOps is explicitly disabled in global settings
        mlops_enabled = self.context.global_settings.get("mlops_enabled", True)
        if not mlops_enabled:
            logger.debug("MLOps tracking disabled via global settings (mlops_enabled: false)")
            return None, None, None

        try:
            mlops_integration = MLOpsExecutorIntegration(context=self.context, auto_init=True)
            if not mlops_integration.is_available():
                return mlops_integration, None, None

            pipeline_type = pipeline.get("type", PipelineType.BATCH.value)
            description = ml_info.get("description") or ""
            tags = {
                "project_name": ml_info.get("project_name", ""),
                "model_name": ml_info.get("model_name", pipeline_name),
                "pipeline_type": str(pipeline_type),
            }

            experiment_id = mlops_integration.create_pipeline_experiment(
                pipeline_name=pipeline_name,
                pipeline_type=str(pipeline_type),
                description=description,
                tags=tags,
            )

            if experiment_id:
                mlops_run_id = mlops_integration.start_pipeline_run(
                    experiment_id=experiment_id,
                    pipeline_name=pipeline_name,
                    model_version=str(ml_info.get("model_version") or ""),
                    hyperparams=ml_info.get("hyperparams") or {},
                    tags={
                        "env": str(self.context.global_settings.get("env", "")),
                        "execution_mode": str(getattr(self.context, "execution_mode", "")),
                    },
                )

        except Exception as e:
            # Log MLOps initialization failure but do not fail the whole execution
            # ER4 FIX: Explicit logging instead of silent failure
            logger.warning(
                f"MLOps integration initialization failed: {e}. "
                f"Pipeline will execute without experiment tracking."
            )
            mlops_integration = None
            mlops_run_id = None
            experiment_id = None

        return mlops_integration, mlops_run_id, experiment_id

    def _execute_batch_flow(
        self,
        pipeline: Dict[str, Any],
        node_name: Optional[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute batch flow logic with optional MLflow tracking."""
        pipeline_name = pipeline.get("name", "batch_pipeline")

        # Execute with MLflow tracking if enabled
        if self._mlflow_enabled and self._mlflow_tracker:
            with self._mlflow_tracker.start_pipeline_run(
                pipeline_name=pipeline_name,
                parameters={
                    "start_date": start_date,
                    "end_date": end_date,
                    "model_version": ml_info.get("model_version"),
                    "node_name": node_name or "all",
                },
                tags={
                    "pipeline_type": "batch",
                    "executor": "BatchExecutor",
                },
            ) as run_id:
                logger.info(
                    f"Batch pipeline '{pipeline_name}' tracked in MLflow (run_id: {run_id})"
                )
                self._execute_batch_nodes(node_name, pipeline, start_date, end_date, ml_info)
        else:
            # Standard execution without MLflow
            self._execute_batch_nodes(node_name, pipeline, start_date, end_date, ml_info)

    def _execute_batch_nodes(
        self,
        node_name: Optional[str],
        pipeline: Dict[str, Any],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute batch nodes (single or all)."""
        if node_name:
            self.node_executor.execute_single_node(node_name, start_date, end_date, ml_info)
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
        max_streaming_pipelines = context.global_settings.get("max_streaming_pipelines", 5)
        # Inyectar policy del contexto dentro del manager (el manager crea su validador con policy)
        self.streaming_manager = StreamingPipelineManager(context, max_streaming_pipelines)

    def execute(
        self,
        pipeline_name: str,
        execution_mode: Optional[str] = "async",
    ) -> str:
        logger.info(f"Executing streaming pipeline: {pipeline_name}")

        pipeline = self._get_pipeline_config(pipeline_name)
        # Validation via manager's validator (single source of truth)
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

            common_topics = current_resources["kafka_topics"] & running_resources["kafka_topics"]
            if common_topics:
                conflicts.append(f"Kafka topic conflict: topics {', '.join(common_topics)}")

            common_paths = current_resources["file_paths"] & running_resources["file_paths"]
            if common_paths:
                conflicts.append(f"File path conflict: paths {', '.join(common_paths)}")

            common_tables = current_resources["delta_tables"] & running_resources["delta_tables"]
            if common_tables:
                conflicts.append(f"Delta table conflict: tables {', '.join(common_tables)}")

        return conflicts

    def _add_kafka_from_subscribe(
        self, resources: Dict[str, Set[str]], subscribe_value: Any
    ) -> None:
        if isinstance(subscribe_value, str):
            topics = [t.strip() for t in subscribe_value.split(",") if t.strip()]
        elif isinstance(subscribe_value, (list, tuple, set)):
            topics = [str(t).strip() for t in subscribe_value if str(t).strip()]
        else:
            topics = []
        for t in topics:
            resources["kafka_topics"].add(t)

    def _add_kafka_from_assign(self, resources: Dict[str, Set[str]], assign_value: Any) -> None:
        try:
            mapping = json.loads(assign_value) if isinstance(assign_value, str) else assign_value
            if isinstance(mapping, dict):
                for t in mapping.keys():
                    resources["kafka_topics"].add(t)
        except Exception:
            pass

    def _add_kafka_from_opts(self, resources: Dict[str, Set[str]], opts: Dict[str, Any]) -> None:
        if not opts:
            return
        if "subscribe" in opts:
            self._add_kafka_from_subscribe(resources, opts["subscribe"])
            return
        if "assign" in opts:
            self._add_kafka_from_assign(resources, opts["assign"])
            return
        if "subscribePattern" in opts:
            pattern = str(opts["subscribePattern"]).strip()
            if pattern:
                resources["kafka_topics"].add(f"pattern:{pattern}")

    def _extract_path_from_config(self, cfg: Dict[str, Any]) -> Optional[str]:
        path = cfg.get("path")
        if path:
            return path
        opts = cfg.get("options", {}) or {}
        return opts.get("path")

    def _process_input_config(
        self, resources: Dict[str, Set[str]], node_cfg: Dict[str, Any]
    ) -> None:
        input_config = node_cfg.get("input", {}) or {}
        input_format = (input_config.get("format") or "").lower()
        if input_format == "kafka":
            self._add_kafka_from_opts(resources, input_config.get("options", {}) or {})
            return
        if input_format == "file_stream":
            path = self._extract_path_from_config(input_config)
            if path:
                resources["file_paths"].add(path)
            return
        if input_format in ("delta_stream", "delta"):
            path = self._extract_path_from_config(input_config)
            if path:
                resources["delta_tables"].add(path)

    def _process_output_config(
        self, resources: Dict[str, Set[str]], node_cfg: Dict[str, Any]
    ) -> None:
        output_config = node_cfg.get("output", {}) or {}
        output_format = (output_config.get("format") or "").lower()
        if output_format == "kafka":
            opar = output_config.get("options", {}) or {}
            topic = opar.get("topic") or opar.get("kafka.topic")
            if topic:
                resources["kafka_topics"].add(str(topic))
            return
        if output_format == "delta":
            out_path = self._extract_path_from_config(output_config)
            if out_path:
                resources["delta_tables"].add(out_path)

    def _extract_pipeline_resources(self, pipeline: Dict[str, Any]) -> Dict[str, Set[str]]:
        """Extract critical resources from pipeline configuration."""
        resources: Dict[str, Set[str]] = {
            "kafka_topics": set(),
            "file_paths": set(),
            "delta_tables": set(),
        }

        pipeline_nodes = self._extract_pipeline_nodes(pipeline)
        for node_name in pipeline_nodes:
            node_config = self.context.nodes_config.get(node_name, {}) or {}
            self._process_input_config(resources, node_config)
            self._process_output_config(resources, node_config)

        return resources

    def _wait_for_streaming_pipeline(self, execution_id: str, timeout_seconds: int = 300) -> None:
        """Wait for streaming pipeline to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_info = self.streaming_manager.get_pipeline_status(execution_id)
            state = status_info.get("state") if isinstance(status_info, dict) else str(status_info)
            if state in ["completed", "error", "stopped"]:
                break
            time.sleep(5)


class HybridExecutor(BaseExecutor):
    """Executor for hybrid pipelines."""

    def __init__(self, context: Context):
        super().__init__(context)
        max_streaming_pipelines = context.global_settings.get("max_streaming_pipelines", 5)
        self.streaming_manager = StreamingPipelineManager(context, max_streaming_pipelines)
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

        # Use the same format policy from context via PipelineValidator (batch/streaming/hybrid)
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
            self.unified_state.register_node(node_name, NodeType.STREAMING, dependencies)

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
                node for node, result in batch_results.items() if result["status"] != "completed"
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

        retry_policy = RetryPolicy(
            max_retries=self.max_retries, delay=self.retry_delay, backoff_factor=1.5
        )

        for node in execution_order:
            if not self.unified_state.start_node_execution(node):
                continue

            try:
                retry_policy.execute(
                    self.node_executor.execute_single_node, node, start_date, end_date, ml_info
                )
                results[node] = {"status": "completed"}
                self.unified_state.complete_node_execution(node)

            except Exception as e:
                results[node] = {"status": "failed", "error": str(e)}
                self.unified_state.fail_node_execution(node, str(e))
                self._handle_batch_failure(node, results)
                raise

        return results

    def _handle_batch_failure(self, failed_node: str, results: Dict[str, Any]):
        """Handle batch node failure: cancel dependents and stop related streaming nodes."""
        logger.error(f"Batch node '{failed_node}' failed, cleaning up dependents")

        for node in results.keys():
            if node != failed_node and self.unified_state.is_node_pending(node):
                if failed_node in self.unified_state.get_node_dependencies(node):
                    self.unified_state.cancel_node_execution(node)
                    results[node] = {
                        "status": "cancelled",
                        "reason": f"Dependency {failed_node} failed",
                    }
                    logger.warning(f"Cancelled dependent node: {node}")

        stopped_streaming = self.unified_state.stop_dependent_streaming_nodes(failed_node)
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
                execution_id = self.streaming_manager.start_streaming_node(node, node_configs[node])
                execution_ids.append(execution_id)
                self.unified_state.register_streaming_query(node, execution_id)
                self.unified_state.complete_node_execution(node)
            except Exception as e:
                logger.error(f"Failed to start streaming node '{node}': {str(e)}")
                self.unified_state.fail_node_execution(node, str(e))

        if execution_mode == "sync":
            self._wait_for_streaming_completion(execution_ids)

        return execution_ids

    def _wait_for_streaming_completion(self, execution_ids: List[str], timeout_minutes=60):
        """Wait for streaming queries to complete (for sync execution)."""
        start_time = time.time()
        while time.time() - start_time < timeout_minutes * 60:
            active_queries = [
                eid for eid in execution_ids if self.streaming_manager.is_query_active(eid)
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
        self._streaming_executor: Optional[StreamingExecutor] = None
        self._hybrid_executor: Optional[HybridExecutor] = None

    @property
    def streaming_executor(self) -> StreamingExecutor:
        """Lazy initialization of StreamingExecutor."""
        if self._streaming_executor is None:
            self._streaming_executor = StreamingExecutor(self.context)
        return self._streaming_executor

    @property
    def hybrid_executor(self) -> HybridExecutor:
        """Lazy initialization of HybridExecutor."""
        if self._hybrid_executor is None:
            self._hybrid_executor = HybridExecutor(self.context)
        return self._hybrid_executor

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
        pipeline = self.batch_executor._get_pipeline_config(pipeline_name)
        pipeline_type = pipeline.get("type", PipelineType.BATCH.value)

        # Only validate dates for batch, ml and hybrid pipelines, not for streaming
        if pipeline_type in [
            PipelineType.BATCH.value,
            PipelineType.ML.value,
            PipelineType.HYBRID.value,
        ]:
            # Check if pipeline requires dates (default True, but can be overridden for static data like catalogs)
            requires_dates = pipeline.get("requires_dates", True)
            PipelineValidator.validate_required_params(
                pipeline_name,
                start_date,
                end_date,
                self.context.global_settings.get("start_date"),
                self.context.global_settings.get("end_date"),
                requires_dates=requires_dates,
            )

        if pipeline_type == PipelineType.BATCH.value or pipeline_type == PipelineType.ML.value:
            # ML pipelines son ejecutados como batch con MLOps habilitado
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
            return self.streaming_executor.streaming_manager.get_pipeline_status(execution_id) or {}
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
            return self.streaming_executor.streaming_manager.stop_pipeline(execution_id, graceful)
        except Exception:
            return False

    def get_streaming_pipeline_metrics(self, execution_id: str) -> Dict[str, Any]:
        """Get metrics for a streaming pipeline."""
        try:
            return (
                self.streaming_executor.streaming_manager.get_pipeline_metrics(execution_id) or {}
            )
        except Exception:
            return {}

    def run_streaming_pipeline(
        self,
        pipeline_name: str,
        mode: str = "async",
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Execute a streaming pipeline (convenience wrapper for CLI)."""
        execution_mode = "sync" if mode == "sync" else "async"
        result = self.run_pipeline(
            pipeline_name=pipeline_name,
            model_version=model_version,
            hyperparams=hyperparams,
            execution_mode=execution_mode,
        )
        return result if isinstance(result, str) else ""

    def get_running_execution_ids(self) -> List[str]:
        """Get list of running execution IDs from streaming manager."""
        try:
            pipelines = self.streaming_executor.streaming_manager.list_running_pipelines()
            return [p.get("execution_id") for p in pipelines if p.get("execution_id")]
        except Exception:
            return []

    def shutdown(self) -> None:
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
