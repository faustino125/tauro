from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from tauro.config.context import Context
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.node_executor import NodeExecutor
from tauro.exec.pipeline_validator import PipelineValidator
from tauro.io.input import InputLoader
from tauro.io.output import OutputManager


class PipelineExecutor:
    """Executes data pipelines based on the layer type (ML or standard)."""

    def __init__(self, context: Context):
        """Initialize the pipeline executor with application context."""
        self.context = context
        self.input_loader = InputLoader(context)
        self.output_manager = OutputManager(context)
        self.is_ml_layer = context.layer == "ml"
        self.max_workers = context.global_settings.get("max_parallel_nodes", 4)

        self.node_executor = NodeExecutor(
            context, self.input_loader, self.output_manager, self.max_workers
        )

    def run_pipeline(
        self,
        env: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        node_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Main entry point for executing pipelines of any type."""
        PipelineValidator.validate_required_params(
            pipeline_name,
            start_date,
            end_date,
            self.context.global_settings.get("start_date"),
            self.context.global_settings.get("end_date"),
        )

        pipeline = self._get_pipeline_config(pipeline_name)
        PipelineValidator.validate_pipeline_config(pipeline)

        start_date = start_date or self.context.global_settings.get("start_date")
        end_date = end_date or self.context.global_settings.get("end_date")

        ml_info = self._prepare_ml_info(model_version, hyperparams)

        self._log_pipeline_start(pipeline_name, ml_info)

        self._execute_flow(pipeline, node_name, start_date, end_date, ml_info)

    def _get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get pipeline configuration from the context."""
        pipeline = self.context.pipelines.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        return pipeline

    def _prepare_ml_info(
        self, model_version: Optional[str], hyperparams: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Prepare ML-specific information if this is an ML layer."""
        ml_info = {}

        if self.is_ml_layer:
            model_version = model_version or self.context.global_settings.get(
                "default_model_version", "latest"
            )
            hyperparams = hyperparams or {}

            if not hyperparams:
                logger.warning("Executing ML pipeline without hyperparameters")

            ml_info = {"model_version": model_version, "hyperparams": hyperparams}

        return ml_info

    def _log_pipeline_start(self, pipeline_name: str, ml_info: Dict[str, Any]) -> None:
        """Log the start of pipeline execution."""
        if self.is_ml_layer:
            logger.info(
                f"Running ML pipeline '{pipeline_name}' with model version: "
                f"{ml_info['model_version']}"
            )
        else:
            logger.info(f"Running standard pipeline '{pipeline_name}'")

    def _execute_flow(
        self,
        pipeline: Dict[str, Any],
        node_name: Optional[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute pipeline with proper dependency resolution and parallel execution."""
        if node_name:
            logger.info(f"Running single node: '{node_name}'")
            self.node_executor.execute_single_node(
                node_name, start_date, end_date, ml_info
            )
        else:
            pipeline_nodes = self._extract_pipeline_nodes(pipeline)
            self._execute_pipeline_nodes(pipeline_nodes, start_date, end_date, ml_info)

    def _extract_pipeline_nodes(self, pipeline: Dict[str, Any]) -> List[str]:
        """Extract node names from pipeline configuration."""
        pipeline_nodes_raw = pipeline.get("nodes", [])
        pipeline_nodes = []

        for node in pipeline_nodes_raw:
            if isinstance(node, str):
                pipeline_nodes.append(node)
            elif isinstance(node, dict):
                if len(node) == 1:
                    pipeline_nodes.append(list(node.keys())[0])
                elif "name" in node:
                    pipeline_nodes.append(node["name"])
                else:
                    logger.error(f"Cannot extract node name from dict: {node}")
                    raise ValueError(f"Invalid node format in pipeline: {node}")
            else:
                pipeline_nodes.append(str(node))

        if not pipeline_nodes:
            raise ValueError("No nodes found in pipeline")

        return pipeline_nodes

    def _execute_pipeline_nodes(
        self,
        pipeline_nodes: List[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute all nodes in a pipeline with dependency resolution."""
        node_configs = self._get_node_configs(pipeline_nodes)

        PipelineValidator.validate_node_configs(pipeline_nodes, node_configs)

        dag = DependencyResolver.build_dependency_graph(pipeline_nodes, node_configs)

        execution_order = DependencyResolver.topological_sort(dag)

        if not execution_order:
            raise ValueError("Pipeline has circular dependencies - cannot execute")

        self._log_execution_order(execution_order)

        self.node_executor.execute_nodes_parallel(
            execution_order, node_configs, dag, start_date, end_date, ml_info
        )

    def _get_node_configs(self, pipeline_nodes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all pipeline nodes."""
        node_configs = {}

        for node_name in pipeline_nodes:
            node_config = self.context.nodes_config.get(node_name)
            if node_config:
                node_configs[node_name] = node_config

        return node_configs

    def _log_execution_order(self, execution_order: List[str]) -> None:
        """Log the determined execution order."""
        try:
            execution_order_str = " -> ".join(str(node) for node in execution_order)
            logger.info(f"Execution order determined: {execution_order_str}")
        except Exception as e:
            logger.error(f"Error creating execution order string: {e}")
            logger.error(
                f"Execution order items: {[type(item) for item in execution_order]}"
            )
            raise
