"""
MLflow Node Executor for Tauro.

Ejecuta nodos de pipeline como MLflow steps, con tracking automático
de métricas, parámetros y artefactos.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""

import time
from typing import Any, Dict, Optional

from loguru import logger

from tauro.core.exec.node_executor import NodeExecutor
from tauro.core.mlops.mlflow_adapter import (
    MLflowPipelineTracker,
    is_mlflow_available,
)


class MLflowNodeExecutor(NodeExecutor):
    """
    Enhanced NodeExecutor con integración MLflow.
    
    Cada nodo se ejecuta dentro de un MLflow step context, permitiendo:
    - Tracking automático de duración
    - Logging de métricas y parámetros por nodo
    - Artifacts por nodo
    - Visualización jerárquica en MLflow UI
    
    Example:
        >>> executor = MLflowNodeExecutor(
        ...     context=context,
        ...     mlflow_tracker=tracker,
        ...     input_loader=loader,
        ...     output_manager=manager,
        ... )
        >>> executor.execute_single_node("train", start_date, end_date, ml_info)
    """
    
    def __init__(
        self,
        context,
        input_loader,
        output_manager,
        mlflow_tracker: Optional[MLflowPipelineTracker] = None,
        max_workers: int = 4,
        enable_mlflow: bool = True,
    ):
        """
        Initialize MLflow Node Executor.
        
        Args:
            context: Tauro execution context
            input_loader: Input data loader
            output_manager: Output data manager
            mlflow_tracker: MLflow tracker instance (se crea automáticamente si no se provee)
            max_workers: Max parallel workers
            enable_mlflow: Enable MLflow tracking
        """
        super().__init__(context, input_loader, output_manager, max_workers)
        
        self.enable_mlflow = enable_mlflow and is_mlflow_available()
        self.mlflow_tracker = mlflow_tracker
        
        if self.enable_mlflow and not self.mlflow_tracker:
            # Auto-create tracker from context
            try:
                self.mlflow_tracker = MLflowPipelineTracker.from_context(context)
                logger.info("Created MLflow tracker from context")
            except Exception as e:
                logger.warning(f"Could not create MLflow tracker: {e}")
                self.enable_mlflow = False
    
    def execute_single_node(
        self,
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """
        Execute single node con MLflow step tracking.
        
        Args:
            node_name: Node name
            start_date: Start date
            end_date: End date
            ml_info: ML configuration and metadata
        """
        if not self.enable_mlflow or not self.mlflow_tracker:
            # Fallback to standard execution
            return super().execute_single_node(node_name, start_date, end_date, ml_info)
        
        # Execute dentro de MLflow step context
        node_config = self._get_node_config(node_name)
        
        # Prepare node parameters for MLflow
        node_params = {
            "start_date": start_date,
            "end_date": end_date,
            "node_type": node_config.get("type", "batch"),
            "module": node_config.get("module", "unknown"),
            "function": node_config.get("function", "unknown"),
        }
        
        # Add ML info
        if "model_version" in ml_info:
            node_params["model_version"] = ml_info["model_version"]
        
        # Add hyperparams
        if "hyperparams" in ml_info:
            for key, value in ml_info["hyperparams"].items():
                node_params[f"hyperparam_{key}"] = value
        
        # Execute with MLflow tracking
        with self.mlflow_tracker.start_node_step(
            node_name=node_name,
            parameters=node_params,
        ) as node_run_id:
            logger.info(f"Executing node '{node_name}' with MLflow tracking (run_id: {node_run_id})")
            
            # Inject MLflow context into ml_info
            ml_info["mlflow_run_id"] = node_run_id
            ml_info["mlflow_tracker"] = self.mlflow_tracker
            
            try:
                # Execute node (parent implementation)
                start_time = time.perf_counter()
                super().execute_single_node(node_name, start_date, end_date, ml_info)
                duration = time.perf_counter() - start_time
                
                # Log success metrics
                self.mlflow_tracker.log_node_metric("execution_time_seconds", duration, node_name=node_name)
                self.mlflow_tracker.log_node_metric("success", 1.0, node_name=node_name)
                
                logger.info(f"Node '{node_name}' completed successfully (duration: {duration:.2f}s)")
                
            except Exception as e:
                # Log failure
                self.mlflow_tracker.log_node_param("error_type", type(e).__name__, node_name=node_name)
                self.mlflow_tracker.log_node_param("error_message", str(e), node_name=node_name)
                self.mlflow_tracker.log_node_metric("success", 0.0, node_name=node_name)
                
                logger.error(f"Node '{node_name}' failed: {e}")
                raise
    
    def execute_nodes_parallel(
        self,
        execution_order: list,
        node_configs: Dict[str, Dict[str, Any]],
        dag: Dict[str, set],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """
        Execute nodes in parallel con MLflow tracking.
        
        Cada nodo se ejecuta como un MLflow step independiente.
        """
        if not self.enable_mlflow or not self.mlflow_tracker:
            # Fallback to standard execution
            return super().execute_nodes_parallel(
                execution_order, node_configs, dag, start_date, end_date, ml_info
            )
        
        # Log pipeline-level metrics
        self.mlflow_tracker.log_pipeline_metric("total_nodes", len(execution_order))
        self.mlflow_tracker.log_pipeline_metric("parallel_workers", self.max_workers)
        
        # Execute with parent implementation (each node will use MLflow context)
        try:
            super().execute_nodes_parallel(
                execution_order, node_configs, dag, start_date, end_date, ml_info
            )
            
            # Log pipeline success
            self.mlflow_tracker.log_pipeline_metric("pipeline_success", 1.0)
            
        except Exception:
            # Log pipeline failure
            self.mlflow_tracker.log_pipeline_metric("pipeline_success", 0.0)
            raise


def create_mlflow_executor(
    context,
    input_loader,
    output_manager,
    mlflow_config: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
) -> NodeExecutor:
    """
    Factory function para crear MLflow-enabled executor.
    
    Args:
        context: Tauro execution context
        input_loader: Input data loader
        output_manager: Output data manager
        mlflow_config: MLflow configuration override
        max_workers: Max parallel workers
        
    Returns:
        MLflowNodeExecutor instance
        
    Example:
        >>> executor = create_mlflow_executor(
        ...     context=context,
        ...     input_loader=loader,
        ...     output_manager=manager,
        ...     mlflow_config={
        ...         "tracking_uri": "http://localhost:5000",
        ...         "experiment_name": "my_pipeline",
        ...     },
        ... )
    """
    if not is_mlflow_available():
        logger.warning("MLflow not available, falling back to standard executor")
        return NodeExecutor(context, input_loader, output_manager, max_workers)
    
    # Create MLflow tracker
    tracker = None
    if mlflow_config:
        tracker = MLflowPipelineTracker(
            experiment_name=mlflow_config.get("experiment_name", "tauro_pipeline"),
            tracking_uri=mlflow_config.get("tracking_uri"),
            artifact_location=mlflow_config.get("artifact_location"),
            enable_autolog=mlflow_config.get("enable_autolog", True),
            nested_runs=mlflow_config.get("nested_runs", True),
            tags=mlflow_config.get("tags"),
        )
    
    return MLflowNodeExecutor(
        context=context,
        input_loader=input_loader,
        output_manager=output_manager,
        mlflow_tracker=tracker,
        max_workers=max_workers,
        enable_mlflow=True,
    )
