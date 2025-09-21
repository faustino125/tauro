"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import inspect
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from concurrent.futures import as_completed as thread_as_completed
from functools import lru_cache
from typing import Any, Callable, Dict, List, Set

from loguru import logger  # type: ignore

from tauro.exec.commands import Command, MLNodeCommand, NodeCommand
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.pipeline_validator import PipelineValidator


@lru_cache(maxsize=64)
def _import_module_cached(module_path: str):
    import importlib

    logger.debug(f"Importing module: {module_path}")
    return importlib.import_module(module_path)


class NodeExecutor:
    """Enhanced node executor with ML support and optimized loading."""

    def __init__(self, context, input_loader, output_manager, max_workers: int = 4):
        self.context = context
        self.input_loader = input_loader
        self.output_manager = output_manager
        self.max_workers = max_workers
        self.is_ml_layer = getattr(context, "is_ml_layer", False)

    def execute_single_node(
        self,
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute a single node with enhanced ML support and error handling."""
        start_time = time.perf_counter()
        input_dfs = []
        result_df = None
        try:
            node_config = self._get_node_config(node_name)
            function = self._load_node_function(node_config)
            input_dfs = self.input_loader.load_inputs(node_config)

            command = self._create_enhanced_command(
                function,
                input_dfs,
                start_date,
                end_date,
                node_name,
                ml_info,
                node_config,
            )

            result_df = command.execute()

            self._validate_and_save_enhanced_output(
                result_df,
                node_config,
                node_name,
                start_date,
                end_date,
                ml_info,
            )

        except Exception as e:
            logger.error(f"Failed to execute node '{node_name}': {str(e)}")
            raise
        finally:
            self._release_resources(input_dfs, result_df)
            duration = time.perf_counter() - start_time
            logger.debug(f"Node '{node_name}' executed in {duration:.2f}s")

    def _release_resources(self, *dataframes):
        """Explicitly release resources for memory management"""
        for df in dataframes:
            if df is None:
                continue

            try:
                if hasattr(df, "unpersist"):
                    df.unpersist()  # Spark DataFrame
                elif hasattr(df, "close"):
                    df.close()  # Conexiones
                elif hasattr(df, "clear"):
                    df.clear()  # Colecciones
            except Exception as e:
                logger.debug(f"Resource release warning: {str(e)}")

            try:
                del df
            except Exception:
                pass

    def execute_nodes_parallel(
        self,
        execution_order: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        dag: Dict[str, Set[str]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute nodes in parallel while respecting dependencies with ML enhancements."""
        completed = set()
        running = {}
        ready_queue = deque()
        failed = False
        execution_results = {}

        for node in execution_order:
            node_config = node_configs[node]
            dependencies = DependencyResolver.get_node_dependencies(node_config)
            if not dependencies:
                ready_queue.append(node)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while (ready_queue or running) and not failed:
                    self._submit_ready_nodes(
                        ready_queue,
                        running,
                        executor,
                        start_date,
                        end_date,
                        ml_info,
                        node_configs,
                    )

                    if running:
                        failed = self._process_completed_nodes(
                            running,
                            completed,
                            dag,
                            ready_queue,
                            node_configs,
                            execution_results,
                        )

                if running:
                    logger.warning(
                        f"Pipeline ended with {len(running)} unfinished futures"
                    )
                    self._handle_unfinished_futures(running)

            except Exception as e:
                logger.error(f"Pipeline execution failed: {str(e)}")
                self._cancel_all_futures(running)
                raise
            finally:
                self._cleanup_futures(running)

        if failed:
            raise RuntimeError("Pipeline execution failed due to node failures")

        if self.is_ml_layer:
            self._log_ml_pipeline_summary(execution_results, ml_info)

        logger.info(f"Pipeline execution completed. Processed {len(completed)} nodes.")

    def _create_enhanced_command(
        self,
        function: Callable,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
        ml_info: Dict[str, Any],
        node_config: Dict[str, Any],
    ) -> Command:
        """Create appropriate command based on layer type with enhanced ML features."""
        if self.is_ml_layer:
            return self._create_ml_command(
                function,
                input_dfs,
                start_date,
                end_date,
                node_name,
                ml_info,
                node_config,
            )
        else:
            return NodeCommand(
                function=function,
                input_dfs=input_dfs,
                start_date=start_date,
                end_date=end_date,
                node_name=node_name,
            )

    def _create_ml_command(
        self,
        function: Callable,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
        ml_info: Dict[str, Any],
        node_config: Dict[str, Any],
    ) -> Command:
        """Create ML command (either standard or experiment)."""
        common_params = {
            "function": function,
            "input_dfs": input_dfs,
            "start_date": start_date,
            "end_date": end_date,
            "node_name": node_name,
            "model_version": ml_info["model_version"],
            "hyperparams": ml_info["hyperparams"],
            "node_config": node_config,
            "pipeline_config": ml_info.get("pipeline_config", {}),
        }

        if hasattr(self.context, "spark"):
            common_params["spark"] = self.context.spark

        if self._is_experiment_node(node_config):
            logger.info(
                f"Node '{node_name}' marked experimental but ExperimentCommand is disabled; running as MLNodeCommand"
            )
        return MLNodeCommand(**common_params)

    def _is_experiment_node(self, node_config: Dict[str, Any]) -> bool:
        """Check if a node is configured for experimentation using explicit flag."""
        return node_config.get("experimental", False)

    def _submit_ready_nodes(
        self,
        ready_queue: deque,
        running: Dict,
        executor: ThreadPoolExecutor,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Submit ready nodes for execution with enhanced context."""
        while ready_queue and len(running) < self.max_workers:
            node_name = ready_queue.popleft()
            logger.info(f"Starting execution of node: {node_name}")

            node_ml_info = self._prepare_node_ml_info(node_name, ml_info)

            future = executor.submit(
                self.execute_single_node, node_name, start_date, end_date, node_ml_info
            )
            running[future] = {
                "node_name": node_name,
                "start_time": time.time(),  # Get current time
                "config": node_configs.get(node_name, {}),
            }

    def _prepare_node_ml_info(
        self, node_name: str, ml_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare node-specific ML information."""
        if not self.is_ml_layer:
            return ml_info

        node_ml_config = self.context.get_node_ml_config(node_name)

        enhanced_ml_info = ml_info.copy()

        node_hyperparams = enhanced_ml_info.get("hyperparams", {}).copy()
        node_hyperparams.update(node_ml_config.get("hyperparams", {}))
        enhanced_ml_info["hyperparams"] = node_hyperparams

        enhanced_ml_info["node_config"] = node_ml_config

        return enhanced_ml_info

    def _process_completed_nodes(
        self,
        running: Dict,
        completed: Set[str],
        dag: Dict[str, Set[str]],
        ready_queue: deque,
        node_configs: Dict[str, Dict[str, Any]],
        execution_results: Dict[str, Any],
    ) -> bool:
        """Process completed nodes and update ready queue with enhanced tracking."""
        if not running:
            return False

        future_list = list(running.keys())
        completed_futures = []
        failed = False

        try:
            for future in thread_as_completed(future_list, timeout=10):
                completed_futures.append(future)
                node_info = running.get(future)
                if not node_info:
                    continue
                node_name = node_info["node_name"]

                try:
                    future.result()
                    completed.add(node_name)

                    execution_results[node_name] = {
                        "status": "success",
                        "start_time": node_info["start_time"],
                        "end_time": time.time(),
                        "config": node_info["config"],
                    }

                    logger.info(f"Node '{node_name}' completed successfully")

                    newly_ready = self._find_newly_ready_nodes(
                        node_name, dag, completed, ready_queue, running, node_configs
                    )
                    ready_queue.extend(newly_ready)

                except Exception as e:
                    execution_results[node_name] = {
                        "status": "failed",
                        "error": str(e),
                        "start_time": node_info["start_time"],
                        "end_time": time.time(),
                        "config": node_info["config"],
                    }
                    logger.error(f"Node '{node_name}' failed: {str(e)}")
                    failed = True
                    break

        except TimeoutError:
            logger.debug("Timeout waiting for node completion, will retry...")
        except Exception as e:
            logger.error(f"Unexpected error in _process_completed_nodes: {str(e)}")
            failed = True

        for future in completed_futures:
            running.pop(future, None)

        if failed:
            self._cancel_all_futures(running)

        return failed

    def _log_ml_pipeline_summary(
        self, execution_results: Dict[str, Any], ml_info: Dict[str, Any]
    ) -> None:
        """Log comprehensive ML pipeline execution summary."""
        logger.info("=" * 60)
        logger.info("🎯 ML PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)

        successful_nodes = [
            name
            for name, result in execution_results.items()
            if result.get("status") == "success"
        ]
        failed_nodes = [
            name
            for name, result in execution_results.items()
            if result.get("status") == "failed"
        ]

        logger.info(f"✅ Successful nodes: {len(successful_nodes)}")
        logger.info(f"❌ Failed nodes: {len(failed_nodes)}")

        if successful_nodes:
            logger.info(f"Successful: {', '.join(successful_nodes)}")

        if failed_nodes:
            logger.error(f"Failed: {', '.join(failed_nodes)}")

        logger.info(f"🏷️  Model Version: {ml_info.get('model_version', 'Unknown')}")
        logger.info(f"📦 Project: {ml_info.get('project_name', 'Unknown')}")

        total_time = 0
        for result in execution_results.values():
            if "start_time" in result and "end_time" in result:
                total_time += result["end_time"] - result["start_time"]

        logger.info(f"⏱️  Total execution time: {total_time:.2f}s")
        logger.info("=" * 60)

    def _handle_unfinished_futures(self, running: Dict) -> None:
        """Handle any unfinished futures at the end of execution."""
        logger.warning("Handling unfinished futures...")

        import time

        time.sleep(5)

        remaining_futures = []
        for future, node_info in running.items():
            node_name = node_info["node_name"]
            if future.done():
                try:
                    future.result()
                    logger.info(f"Late completion of node '{node_name}'")
                except Exception as e:
                    logger.error(f"Late failure of node '{node_name}': {str(e)}")
                running.pop(future, None)
            else:
                remaining_futures.append((future, node_name))

        if remaining_futures:
            logger.warning(f"Cancelling {len(remaining_futures)} unfinished futures")
            for future, node_name in remaining_futures:
                logger.warning(f"Cancelling unfinished future for node '{node_name}'")
                future.cancel()

    def _cancel_all_futures(self, running: Dict) -> None:
        """Cancel all running futures."""
        logger.warning(f"Cancelling {len(running)} running futures")
        for future, node_info in running.items():
            node_name = node_info["node_name"]
            logger.warning(f"Cancelling future for node '{node_name}'")
            try:
                future.cancel()
            except Exception:
                logger.debug(f"Could not cancel future for node '{node_name}'")

    def _cleanup_futures(self, running: Dict) -> None:
        """Ensure all futures are properly cleaned up."""
        if not running:
            return

        logger.debug(f"Cleaning up {len(running)} remaining futures")

        for future, node_info in running.items():
            node_name = node_info["node_name"]
            try:
                if not future.done():
                    future.cancel()
                else:
                    try:
                        future.result(timeout=0.1)
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"Error during cleanup of future for '{node_name}': {e}")

    def _find_newly_ready_nodes(
        self,
        completed_node: str,
        dag: Dict[str, Set[str]],
        completed: Set[str],
        ready_queue: deque,
        running: Dict,
        node_configs: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Find nodes that became ready after completing a node."""
        newly_ready = []
        running_nodes = {info["node_name"] for info in running.values()}
        queued_nodes = set(ready_queue)

        for dependent in dag.get(completed_node, set()):
            if (
                dependent in completed
                or dependent in running_nodes
                or dependent in queued_nodes
            ):
                continue

            node_config = node_configs[dependent]
            dependencies = DependencyResolver.get_node_dependencies(node_config)

            if all(dep in completed for dep in dependencies):
                newly_ready.append(dependent)

        return newly_ready

    def _validate_and_save_enhanced_output(
        self,
        result_df: Any,
        node_config: Dict[str, Any],
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Enhanced validation and output saving with ML metadata."""
        PipelineValidator.validate_dataframe_schema(result_df)
        if hasattr(result_df, "printSchema"):
            logger.debug(f"Schema for node '{node_name}':")
            try:
                result_df.printSchema()
            except Exception:
                pass

        env = getattr(self.context, "env", None)
        if not env:
            gs = getattr(self.context, "global_settings", {}) or {}
            env = gs.get("env") or gs.get("environment")

        output_params = {
            "node": node_config,
            "df": result_df,
            "start_date": start_date,
            "end_date": end_date,
        }

        if self.is_ml_layer:
            output_params["model_version"] = ml_info["model_version"]

        self.output_manager.save_output(env, **output_params)
        logger.info(f"Output saved successfully for node '{node_name}'")

    def _get_node_config(self, node_name: str) -> Dict[str, Any]:
        """Get configuration for a specific node with enhanced error handling."""
        node = self.context.nodes_config.get(node_name)
        if not node:
            available_nodes = list(self.context.nodes_config.keys())
            available_str = ", ".join(available_nodes[:10])
            if len(available_nodes) > 10:
                available_str += f", ... (total: {len(available_nodes)} nodes)"
            raise ValueError(
                f"Node '{node_name}' not found in configuration. "
                f"Available nodes: {available_str}"
            )
        return node

    def _load_node_function(self, node: Dict[str, Any]) -> Callable:
        """Load a node's function with comprehensive validation."""
        module_path = node.get("module")
        function_name = node.get("function")

        if not module_path or not function_name:
            raise ValueError("Node configuration must include 'module' and 'function'")

        try:
            module = _import_module_cached(module_path)
        except ImportError as e:
            logger.error(f"Failed to import module '{module_path}': {str(e)}")
            raise ImportError(f"Cannot import module '{module_path}': {str(e)}")

        if not hasattr(module, function_name):
            available_funcs = [
                attr
                for attr in dir(module)
                if callable(getattr(module, attr)) and not attr.startswith("_")
            ]
            available_str = ", ".join(available_funcs[:5])
            if len(available_funcs) > 5:
                available_str += f", ... (total: {len(available_funcs)})"

            raise AttributeError(
                f"Function '{function_name}' not found in module '{module_path}'. "
                f"Available functions: {available_str}"
            )

        func = getattr(module, function_name)

        if not callable(func):
            raise TypeError(
                f"Object '{function_name}' in module '{module_path}' is not callable"
            )

        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())

            required_params = {"start_date", "end_date"}
            if not required_params.issubset(params):
                logger.warning(
                    f"Function '{function_name}' may not accept required parameters: "
                    f"start_date and end_date. Parameters found: {params}"
                )

            if self.is_ml_layer and "ml_context" not in params:
                accepts_kwargs = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in sig.parameters.values()
                )
                if not accepts_kwargs:
                    logger.info(
                        f"Function '{function_name}' doesn't accept 'ml_context' parameter nor **kwargs. "
                        "ML-specific features may not be available."
                    )

        except ValueError as e:
            logger.warning(
                f"Signature validation skipped for {function_name}: {str(e)}"
            )

        return func
