import os
from loguru import logger  # type: ignore

import time
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from tauro.config.context import Context
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.node_executor import NodeExecutor
from tauro.exec.pipeline_validator import PipelineValidator
from tauro.exec.pipeline_state import UnifiedPipelineState, NodeType, NodeStatus
from tauro.io.input import InputLoader
from tauro.io.output import OutputManager
from tauro.streaming.constants import PipelineType
from tauro.streaming.pipeline_manager import StreamingPipelineManager
from tauro.streaming.validators import StreamingValidator


class PipelineExecutor:
    """Executor mejorado que soporta correctamente pipelines batch, streaming e híbridos."""

    def __init__(self, context: Context):
        """Inicializa el executor mejorado."""
        self.context = context
        self.input_loader = InputLoader(context)
        self.output_manager = OutputManager(context)
        self.is_ml_layer = context.is_ml_layer
        self.max_workers = context.global_settings.get("max_parallel_nodes", 4)

        self.node_executor = NodeExecutor(
            context, self.input_loader, self.output_manager, self.max_workers
        )

        max_streaming_pipelines = context.global_settings.get(
            "max_streaming_pipelines", 5
        )
        self.streaming_manager = StreamingPipelineManager(
            context, max_streaming_pipelines
        )
        self.streaming_validator = StreamingValidator()

        self.unified_state = None

        logger.info("PipelineExecutor initialized with unified batch/streaming support")

    def run_pipeline(
        self,
        env: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        node_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
        execution_mode: Optional[str] = "async",
    ) -> Union[None, str, Dict[str, Any]]:
        """Punto de entrada principal para ejecutar pipelines."""

        PipelineValidator.validate_required_params(
            pipeline_name,
            start_date,
            end_date,
            self.context.global_settings.get("start_date"),
            self.context.global_settings.get("end_date"),
        )

        pipeline = self._get_pipeline_config(pipeline_name)
        PipelineValidator.validate_pipeline_config(pipeline)

        pipeline_type = pipeline.get("type", PipelineType.BATCH.value)

        logger.info(f"Executing pipeline '{pipeline_name}' of type: {pipeline_type}")

        try:
            if pipeline_type == PipelineType.BATCH.value:
                return self._execute_batch_pipeline(
                    pipeline,
                    pipeline_name,
                    node_name,
                    start_date,
                    end_date,
                    model_version,
                    hyperparams,
                )

            elif pipeline_type == PipelineType.STREAMING.value:
                return self._execute_streaming_pipeline(
                    pipeline, pipeline_name, execution_mode
                )

            elif pipeline_type == PipelineType.HYBRID.value:
                return self._execute_hybrid_pipeline(
                    pipeline,
                    pipeline_name,
                    node_name,
                    start_date,
                    end_date,
                    model_version,
                    hyperparams,
                    execution_mode,
                )

            else:
                raise ValueError(f"Unsupported pipeline type: {pipeline_type}")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            if self.unified_state:
                self.unified_state.cleanup()
            raise

    def _execute_batch_pipeline(
        self,
        pipeline: Dict[str, Any],
        pipeline_name: str,
        node_name: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
    ) -> None:
        """Ejecuta pipeline batch usando la lógica existente."""

        logger.info(f"Executing batch pipeline: {pipeline_name}")

        start_date = start_date or self.context.global_settings.get("start_date")
        end_date = end_date or self.context.global_settings.get("end_date")

        ml_info = self._prepare_ml_info(pipeline_name, model_version, hyperparams)
        self._log_pipeline_start(pipeline_name, ml_info, "BATCH")

        self.unified_state = UnifiedPipelineState()
        self.unified_state.set_pipeline_status("running")

        try:
            self._execute_batch_flow(pipeline, node_name, start_date, end_date, ml_info)
            self.unified_state.set_pipeline_status("completed")
        except Exception as e:
            self.unified_state.set_pipeline_status("failed")
            raise
        finally:
            if self.unified_state:
                self.unified_state.cleanup()

    def _execute_streaming_pipeline(
        self,
        pipeline: Dict[str, Any],
        pipeline_name: str,
        execution_mode: Optional[str] = "async",
    ) -> str:
        """Ejecuta pipeline streaming puro."""

        logger.info(f"Executing streaming pipeline: {pipeline_name}")

        self.streaming_validator.validate_streaming_pipeline_config(pipeline)

        running_pipelines = self.streaming_manager.list_running_pipelines()
        conflicts = self._check_resource_conflicts(pipeline, running_pipelines)

        if conflicts:
            logger.warning(f"Potential resource conflicts detected: {conflicts}")

        execution_id = self.streaming_manager.start_pipeline(pipeline_name, pipeline)

        logger.info(
            f"Streaming pipeline '{pipeline_name}' started with execution_id: {execution_id}"
        )

        if execution_mode == "sync":
            logger.info("Synchronous mode - waiting for pipeline completion...")
            self._wait_for_streaming_pipeline(execution_id)

        return execution_id

    def _execute_hybrid_pipeline(
        self,
        pipeline: Dict[str, Any],
        pipeline_name: str,
        node_name: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
        execution_mode: Optional[str] = "async",
    ) -> Dict[str, Any]:
        """Ejecuta pipeline híbrido con coordinación mejorada."""

        logger.info(f"Executing hybrid pipeline: {pipeline_name}")

        start_date = start_date or self.context.global_settings.get("start_date")
        end_date = end_date or self.context.global_settings.get("end_date")
        ml_info = self._prepare_ml_info(pipeline_name, model_version, hyperparams)

        pipeline_nodes = self._extract_pipeline_nodes(pipeline)
        node_configs = self._get_node_configs(pipeline_nodes)

        validation_result = PipelineValidator.validate_hybrid_pipeline(
            pipeline, node_configs
        )

        if not validation_result["is_valid"]:
            error_msg = "Hybrid pipeline validation failed:\n" + "\n".join(
                validation_result["errors"]
            )
            raise ValueError(error_msg)

        if validation_result["warnings"]:
            for warning in validation_result["warnings"]:
                logger.warning(f"Pipeline validation warning: {warning}")

        self.unified_state = UnifiedPipelineState()
        self.unified_state.set_pipeline_status("initializing")

        try:
            self._register_nodes_in_unified_state(
                validation_result["batch_nodes"],
                validation_result["streaming_nodes"],
                node_configs,
            )

            cross_warnings = self.unified_state.validate_cross_dependencies()
            for warning in cross_warnings:
                logger.warning(f"Cross-dependency warning: {warning}")

            self.unified_state.set_pipeline_status("running")

            execution_result = self._execute_unified_hybrid_pipeline(
                validation_result["batch_nodes"],
                validation_result["streaming_nodes"],
                node_configs,
                start_date,
                end_date,
                ml_info,
                execution_mode,
            )

            self.unified_state.set_pipeline_status("completed")
            return execution_result

        except Exception as e:
            logger.error(f"Hybrid pipeline execution failed: {str(e)}")
            self.unified_state.set_pipeline_status("failed")
            raise
        finally:
            if self.unified_state:
                self.unified_state.cleanup()

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
        """Ejecuta pipeline híbrido con coordinación unificada."""

        execution_result = {
            "batch_execution": None,
            "streaming_execution_ids": [],
            "total_nodes": len(batch_nodes) + len(streaming_nodes),
            "completed_nodes": 0,
            "failed_nodes": 0,
        }

        # Construir DAG unificado para determinar orden de ejecución
        all_nodes = batch_nodes + streaming_nodes
        dag = DependencyResolver.build_dependency_graph(all_nodes, node_configs)
        execution_order = DependencyResolver.topological_sort(dag)

        logger.info(f"Unified execution order: {' -> '.join(execution_order)}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for node_name in execution_order:
                self._wait_for_node_dependencies(node_name)

                if node_name in batch_nodes:
                    future = executor.submit(
                        self._execute_batch_node_unified,
                        node_name,
                        node_configs[node_name],
                        start_date,
                        end_date,
                        ml_info,
                    )
                    futures[future] = ("batch", node_name)

                elif node_name in streaming_nodes:
                    future = executor.submit(
                        self._execute_streaming_node_unified,
                        node_name,
                        node_configs[node_name],
                        execution_mode,
                    )
                    futures[future] = ("streaming", node_name)

            for future in as_completed(futures):
                node_type, node_name = futures[future]

                try:
                    result = future.result()

                    if node_type == "batch":
                        self.unified_state.complete_node_execution(
                            node_name,
                            output_path=result.get("output_path"),
                            execution_metadata=result.get("metadata", {}),
                        )
                        execution_result["completed_nodes"] += 1

                    elif node_type == "streaming":
                        self.unified_state.complete_node_execution(node_name)
                        execution_result["streaming_execution_ids"].append(
                            result["execution_id"]
                        )
                        execution_result["completed_nodes"] += 1

                except Exception as e:
                    logger.error(f"Node '{node_name}' ({node_type}) failed: {str(e)}")
                    self.unified_state.fail_node_execution(node_name, str(e))
                    execution_result["failed_nodes"] += 1

                    if execution_mode == "sync":
                        break

        self._log_hybrid_execution_summary(execution_result)

        return execution_result

    def _execute_batch_node_unified(
        self,
        node_name: str,
        node_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Ejecuta un nodo batch individual en contexto unificado."""

        logger.info(f"Executing batch node '{node_name}' in unified context")

        if not self.unified_state.start_node_execution(node_name):
            raise RuntimeError(
                f"Cannot start node '{node_name}' - dependencies not ready"
            )

        try:
            self.node_executor.execute_single_node(
                node_name, start_date, end_date, ml_info
            )

            output_config = node_config.get("output", {})
            output_path = output_config.get("path")

            return {
                "status": "completed",
                "output_path": output_path,
                "metadata": {"execution_time": time.time()},
            }

        except Exception as e:
            logger.error(f"Batch node '{node_name}' execution failed: {str(e)}")
            raise

    def _execute_streaming_node_unified(
        self,
        node_name: str,
        node_config: Dict[str, Any],
        execution_mode: str,
    ) -> Dict[str, Any]:
        """Ejecuta un nodo streaming individual en contexto unificado."""

        logger.info(f"Executing streaming node '{node_name}' in unified context")

        if not self.unified_state.start_node_execution(node_name):
            raise RuntimeError(
                f"Cannot start streaming node '{node_name}' - dependencies not ready"
            )

        try:
            self._ensure_batch_dependencies_available(node_name, node_config)

            temp_pipeline = {
                "type": PipelineType.STREAMING.value,
                "nodes": [node_config],
            }

            execution_id = self.streaming_manager.start_pipeline(
                f"unified_{node_name}", temp_pipeline
            )

            pipeline_status = self.streaming_manager.get_pipeline_status(execution_id)
            if pipeline_status and "queries" in pipeline_status:
                for query_name, query in pipeline_status["queries"].items():
                    self.unified_state.register_streaming_query(node_name, query)
                    break

            return {
                "status": "started",
                "execution_id": execution_id,
                "metadata": {"start_time": time.time()},
            }

        except Exception as e:
            logger.error(f"Streaming node '{node_name}' execution failed: {str(e)}")
            raise

    def _ensure_batch_dependencies_available(
        self, streaming_node: str, node_config: Dict[str, Any]
    ) -> None:
        """Asegura que las dependencias batch estén disponibles para nodo streaming."""

        dependencies = self._get_node_dependencies(node_config)

        for dep in dependencies:
            dep_status = self.unified_state.get_node_status(dep)

            if dep_status != NodeStatus.COMPLETED:
                raise RuntimeError(
                    f"Streaming node '{streaming_node}' cannot start: "
                    f"batch dependency '{dep}' not completed (status: {dep_status})"
                )

            output_path = self.unified_state.get_batch_output_path(dep)
            if not output_path:
                raise RuntimeError(
                    f"Streaming node '{streaming_node}' cannot start: "
                    f"batch dependency '{dep}' has no output path"
                )

            logger.info(f"Batch dependency '{dep}' is available at: {output_path}")

    def _wait_for_node_dependencies(self, node_name: str) -> None:
        """Espera a que las dependencias de un nodo estén listas."""

        max_wait_time = 300  # 5 minutos máximo
        check_interval = 2  # Verificar cada 2 segundos
        waited_time = 0

        while (
            not self.unified_state.is_node_ready(node_name)
            and waited_time < max_wait_time
        ):
            time.sleep(check_interval)
            waited_time += check_interval

            if waited_time % 10 == 0:  # Log cada 10 segundos
                logger.debug(
                    f"Waiting for dependencies of node '{node_name}' ({waited_time}s)"
                )

        if not self.unified_state.is_node_ready(node_name):
            raise TimeoutError(
                f"Timeout waiting for dependencies of node '{node_name}' after {max_wait_time}s"
            )

    def _register_nodes_in_unified_state(
        self,
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Registra todos los nodos en el estado unificado."""

        for node_name in batch_nodes:
            node_config = node_configs[node_name]
            dependencies = self._get_node_dependencies(node_config)
            self.unified_state.register_node(node_name, NodeType.BATCH, dependencies)

        for node_name in streaming_nodes:
            node_config = node_configs[node_name]
            dependencies = self._get_node_dependencies(node_config)
            self.unified_state.register_node(
                node_name, NodeType.STREAMING, dependencies
            )

        logger.info(
            f"Registered {len(batch_nodes)} batch and {len(streaming_nodes)} streaming nodes"
        )

    def _log_hybrid_execution_summary(self, execution_result: Dict[str, Any]) -> None:
        """Log resumen de ejecución de pipeline híbrido."""

        logger.info("=" * 60)
        logger.info("🔄 HYBRID PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)

        summary = self.unified_state.get_pipeline_summary()

        logger.info(f"📊 Total nodes: {summary['total_nodes']}")
        logger.info(f"📦 Batch nodes: {summary['batch_nodes']}")
        logger.info(f"🌊 Streaming nodes: {summary['streaming_nodes']}")
        logger.info(f"🔗 Cross-dependencies: {summary['cross_dependencies']}")

        logger.info(f"✅ Completed: {execution_result['completed_nodes']}")
        logger.info(f"❌ Failed: {execution_result['failed_nodes']}")

        if execution_result["streaming_execution_ids"]:
            logger.info(
                f"🚀 Streaming executions: {execution_result['streaming_execution_ids']}"
            )

        logger.info(f"🏁 Pipeline status: {summary['pipeline_status']}")
        logger.info("=" * 60)

    def _get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Obtiene configuración del pipeline del contexto."""
        pipeline = self.context.pipelines.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        return pipeline

    def _extract_pipeline_nodes(self, pipeline: Dict[str, Any]) -> List[str]:
        """Extrae nombres de nodos del pipeline."""
        return PipelineValidator._extract_pipeline_nodes(pipeline)

    def _get_node_configs(self, pipeline_nodes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Obtiene configuraciones para todos los nodos del pipeline."""
        node_configs = {}
        for node_name in pipeline_nodes:
            node_config = self.context.nodes_config.get(node_name)
            if node_config:
                node_configs[node_name] = node_config
        return node_configs

    def _get_node_dependencies(self, node_config: Dict[str, Any]) -> List[str]:
        """Extrae dependencias de la configuración del nodo."""
        return PipelineValidator._get_node_dependencies(node_config)

    def _prepare_ml_info(
        self,
        pipeline_name: str,
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Prepara información específica de ML."""
        ml_info = {}

        if self.is_ml_layer:
            pipeline_ml_config = self.context.get_pipeline_ml_config(pipeline_name)

            final_model_version = (
                model_version
                or pipeline_ml_config.get("model_version")
                or self.context.default_model_version
            )

            final_hyperparams = {}
            final_hyperparams.update(self.context.default_hyperparams)
            final_hyperparams.update(pipeline_ml_config.get("hyperparams", {}))
            if hyperparams:
                final_hyperparams.update(hyperparams)

            ml_info = {
                "model_version": final_model_version,
                "hyperparams": final_hyperparams,
                "pipeline_config": pipeline_ml_config,
                "project_name": self.context.project_name,
                "is_experiment": self._is_experiment_pipeline(pipeline_name),
            }

            if not final_hyperparams:
                logger.warning("Executing ML pipeline without hyperparameters")
            else:
                logger.info(f"ML hyperparameters: {final_hyperparams}")

        return ml_info

    def _is_experiment_pipeline(self, pipeline_name: str) -> bool:
        """Verifica si es un pipeline de experimentación."""
        return (
            "experiment" in pipeline_name.lower() or "tuning" in pipeline_name.lower()
        )

    def _log_pipeline_start(
        self, pipeline_name: str, ml_info: Dict[str, Any], pipeline_type: str
    ) -> None:
        """Log del inicio de ejecución del pipeline."""
        if self.is_ml_layer:
            logger.info("=" * 60)
            logger.info(f"🚀 Starting {pipeline_type} ML Pipeline: '{pipeline_name}'")
            logger.info(f"📦 Project: {ml_info.get('project_name', 'Unknown')}")
            logger.info(f"🏷️  Model Version: {ml_info['model_version']}")

            if ml_info.get("is_experiment"):
                logger.info("🧪 Experiment Mode: ENABLED")

            pipeline_desc = ml_info.get("pipeline_config", {}).get("description", "")
            if pipeline_desc:
                logger.info(f"📋 Description: {pipeline_desc}")

            logger.info("=" * 60)
        else:
            logger.info(f"Running {pipeline_type.lower()} pipeline '{pipeline_name}'")

    def _execute_batch_flow(
        self,
        pipeline: Dict[str, Any],
        node_name: Optional[str],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Ejecuta flujo batch (lógica existente)."""
        if node_name:
            logger.info(f"Running single node: '{node_name}'")
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
        """Ejecuta todos los nodos en un pipeline con resolución de dependencias."""
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

    def _log_execution_order(self, execution_order: List[str]) -> None:
        """Log del orden de ejecución determinado."""
        try:
            execution_order_str = " -> ".join(str(node) for node in execution_order)
            logger.info(f"Execution order determined: {execution_order_str}")
        except Exception as e:
            logger.error(f"Error creating execution order string: {e}")
            logger.error(
                f"Execution order items: {[type(item) for item in execution_order]}"
            )
            raise

    def _check_resource_conflicts(
        self, pipeline: Dict[str, Any], running_pipelines: List[Dict[str, Any]]
    ) -> List[str]:
        """Verifica conflictos de recursos con pipelines en ejecución."""
        conflicts = []

        new_outputs = set()
        nodes = pipeline.get("nodes", [])

        for node in nodes:
            if isinstance(node, dict):
                output_config = node.get("output", {})
                if output_config.get("path"):
                    new_outputs.add(output_config["path"])
                if output_config.get("topic"):
                    new_outputs.add(f"kafka:{output_config['topic']}")

        for running_pipeline in running_pipelines:
            if running_pipeline.get("status") in ["running", "starting"]:
                running_config = running_pipeline.get("config", {})
                running_nodes = running_config.get("nodes", [])

                for running_node in running_nodes:
                    if isinstance(running_node, dict):
                        running_output = running_node.get("output", {})
                        if running_output.get("path") in new_outputs:
                            conflicts.append(f"Path conflict: {running_output['path']}")
                        if f"kafka:{running_output.get('topic')}" in new_outputs:
                            conflicts.append(
                                f"Kafka topic conflict: {running_output['topic']}"
                            )

        return conflicts

    def _wait_for_streaming_pipeline(
        self, execution_id: str, timeout_seconds: int = 300
    ) -> None:
        """Espera a que el pipeline streaming se complete (para ejecución síncrona)."""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            status = self.streaming_manager.get_pipeline_status(execution_id)

            if not status:
                logger.error(f"Pipeline {execution_id} not found")
                break

            pipeline_status = status.get("status")

            if pipeline_status in ["completed", "error", "stopped"]:
                logger.info(
                    f"Pipeline {execution_id} finished with status: {pipeline_status}"
                )
                break

            time.sleep(5)

        else:
            logger.warning(f"Timeout waiting for pipeline {execution_id} to complete")

    def stop_streaming_pipeline(self, execution_id: str, graceful: bool = True) -> bool:
        """Detiene un pipeline streaming en ejecución."""
        logger.info(f"Stopping streaming pipeline: {execution_id}")
        return self.streaming_manager.stop_pipeline(execution_id, graceful)

    def get_streaming_pipeline_status(
        self, execution_id: str
    ) -> Optional[Dict[str, Any]]:
        """Obtiene estado de un pipeline streaming."""
        return self.streaming_manager.get_pipeline_status(execution_id)

    def list_streaming_pipelines(self) -> List[Dict[str, Any]]:
        """Lista todos los pipelines streaming en ejecución."""
        return self.streaming_manager.list_running_pipelines()

    def get_streaming_pipeline_metrics(
        self, execution_id: str
    ) -> Optional[Dict[str, Any]]:
        """Obtiene métricas para un pipeline streaming."""
        return self.streaming_manager.get_pipeline_metrics(execution_id)

    def get_unified_pipeline_status(self) -> Optional[Dict[str, Any]]:
        """Obtiene estado del pipeline unificado actual."""
        if self.unified_state:
            return self.unified_state.get_pipeline_summary()
        return None

    def shutdown(self, timeout_seconds: int = 30) -> None:
        """Shutdown with prioritized resource cleanup."""
        logger.info("Initiating graceful shutdown...")

        shutdown_sequence = [
            (self._stop_streaming_queries, 10),
            (self._release_compute_resources, 20),
            (self._close_database_connections, 30),
            (self._release_file_handles, 40),
            (self._cleanup_temporary_files, 50),
            (self._finalize_logging, 100),
        ]

        # Sort by priority (higher priority first)
        shutdown_sequence.sort(key=lambda x: x[1])

        for step, priority in shutdown_sequence:
            try:
                logger.debug(f"Executing shutdown step (priority {priority})")
                step(timeout_seconds)
            except Exception as e:
                logger.error(f"Shutdown step failed: {str(e)}")

        logger.info("Shutdown completed successfully")

    def _stop_streaming_queries(self, timeout: int) -> None:
        """Stop all active streaming queries."""
        logger.info("Stopping streaming queries...")
        if self.streaming_manager:
            self.streaming_manager.shutdown(timeout)

    def _release_compute_resources(self, _) -> None:
        """Release compute resources (GPU/CPU allocations)."""
        logger.info("Releasing compute resources...")
        if hasattr(self.context, "gpu_allocations"):
            for allocation in self.context.gpu_allocations:
                allocation.release()
        if hasattr(self.context, "thread_pools"):
            for pool in self.context.thread_pools.values():
                pool.shutdown(wait=False)

    def _close_database_connections(self, _) -> None:
        """Close all database connections."""
        logger.info("Closing database connections...")
        if hasattr(self.context, "database_pool"):
            for conn in self.context.database_pool:
                try:
                    conn.close()
                except Exception:
                    pass

    def _release_file_handles(self, _) -> None:
        """Release all file handles."""
        logger.info("Releasing file handles...")
        if hasattr(self.context, "open_files"):
            for file in self.context.open_files.values():
                try:
                    file.close()
                except Exception:
                    pass

    def _cleanup_temporary_files(self, _) -> None:
        """Clean up temporary files."""
        logger.info("Cleaning temporary files...")
        if hasattr(self.context, "temp_files"):
            for temp_file in self.context.temp_files:
                try:
                    os.remove(temp_file)
                except Exception:
                    pass

    def _finalize_logging(self, _) -> None:
        """Finalize logging systems."""
        logger.info("Finalizing logging...")
        logger.complete()
