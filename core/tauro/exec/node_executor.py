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
from tauro.exec.resource_pool import ResourcePool, get_default_resource_pool


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
        self.resource_pool = get_default_resource_pool()

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

            # Register input resources for cleanup
            for idx, df in enumerate(input_dfs):
                if df is not None:
                    resource_type = self._detect_resource_type(df)
                    self.resource_pool.register_resource(
                        node_id=node_name,
                        resource=df,
                        resource_type=resource_type,
                    )

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

            # Register result resource for cleanup
            if result_df is not None:
                resource_type = self._detect_resource_type(result_df)
                self.resource_pool.register_resource(
                    node_id=node_name,
                    resource=result_df,
                    resource_type=resource_type,
                )

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
            # Release all node resources from pool
            self.resource_pool.release_node_resources(node_name)
            duration = time.perf_counter() - start_time
            logger.debug(f"Node '{node_name}' executed in {duration:.2f}s")

    def _release_resources(self, node_name: str, *dataframes: Any) -> None:
        """Register and mark resources for cleanup via resource pool.

        Args:
            node_name: Node identifier for resource grouping
            *dataframes: DataFrames and other resources to track
        """
        for idx, df in enumerate(dataframes):
            if df is None:
                continue

            # Detect resource type
            resource_type = self._detect_resource_type(df)

            # Register with pool for cleanup
            try:
                self.resource_pool.register_resource(
                    node_id=node_name,
                    resource=df,
                    resource_type=resource_type,
                )
                logger.debug(
                    f"Registered {resource_type} resource #{idx} for node '{node_name}'"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to register resource for node '{node_name}': {str(e)}"
                )

    def _detect_resource_type(self, resource: Any) -> str:
        """Detect the type of resource for proper cleanup.

        Args:
            resource: The resource object to analyze

        Returns:
            Resource type string (spark, pandas, gpu, connection, temp_file, generic)
        """
        # Check for Spark DataFrame/RDD
        if hasattr(resource, "unpersist") and hasattr(resource, "rdd"):
            return "spark"

        # Check for Pandas DataFrame
        if hasattr(resource, "columns") and hasattr(resource, "empty"):
            return "pandas"

        # Check for GPU resources
        if hasattr(resource, "device") and hasattr(resource, "reset"):
            return "gpu"

        # Check for database connections
        if hasattr(resource, "cursor") or hasattr(resource, "execute"):
            return "connection"

        # Check for file paths
        if isinstance(resource, str):
            import os

            if os.path.exists(resource):
                return "temp_file"

        # Generic resource
        return "generic"

    def execute_nodes_parallel(
        self,
        execution_order: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        dag: Dict[str, Set[str]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute nodes in parallel while respecting dependencies with ML enhancements.

        Orchestrates parallel execution by delegating to specialized phases.
        """
        # Phase 1: Initialize execution state
        execution_state = self._initialize_execution_state(
            execution_order, node_configs
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                # Phase 2: Coordinate parallel execution
                self._coordinate_parallel_execution(
                    executor=executor,
                    execution_state=execution_state,
                    dag=dag,
                    node_configs=node_configs,
                    start_date=start_date,
                    end_date=end_date,
                    ml_info=ml_info,
                )
            except Exception as e:
                logger.error(f"Pipeline execution failed: {str(e)}")
                self._cancel_all_futures(execution_state["running"])
                raise
            finally:
                # Phase 3: Cleanup resources
                self._cleanup_execution(
                    execution_state=execution_state,
                    ml_info=ml_info,
                )

    def _initialize_execution_state(
        self,
        execution_order: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Initialize execution state with queues and tracking structures.

        Returns:
            State dictionary containing ready_queue, running, completed, failed flag, and results.
        """
        ready_queue = deque()

        # Identify nodes with no dependencies (ready for immediate execution)
        for node in execution_order:
            node_config = node_configs[node]
            dependencies = DependencyResolver.get_node_dependencies(node_config)
            if not dependencies:
                ready_queue.append(node)

        logger.debug(f"Initial ready nodes: {list(ready_queue)}")

        return {
            "ready_queue": ready_queue,
            "running": {},
            "completed": set(),
            "failed": False,
            "execution_results": {},
        }

    def _coordinate_parallel_execution(
        self,
        executor: ThreadPoolExecutor,
        execution_state: Dict[str, Any],
        dag: Dict[str, Set[str]],
        node_configs: Dict[str, Dict[str, Any]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Coordinate the main execution loop until all nodes complete or failure occurs."""
        while (
            execution_state["ready_queue"] or execution_state["running"]
        ) and not execution_state["failed"]:
            # Submit newly ready nodes to executor
            self._submit_ready_nodes(
                ready_queue=execution_state["ready_queue"],
                running=execution_state["running"],
                executor=executor,
                start_date=start_date,
                end_date=end_date,
                ml_info=ml_info,
                node_configs=node_configs,
            )

            # Process completed nodes and check for failures
            if execution_state["running"]:
                execution_state["failed"] = self._process_completed_nodes(
                    running=execution_state["running"],
                    completed=execution_state["completed"],
                    dag=dag,
                    ready_queue=execution_state["ready_queue"],
                    node_configs=node_configs,
                    execution_results=execution_state["execution_results"],
                )

        # Handle any remaining unfinished futures
        if execution_state["running"]:
            logger.warning(
                f"Pipeline ended with {len(execution_state['running'])} unfinished futures"
            )
            self._handle_unfinished_futures(execution_state["running"])

    def _cleanup_execution(
        self,
        execution_state: Dict[str, Any],
        ml_info: Dict[str, Any],
    ) -> None:
        """Cleanup resources and handle final state after execution completes.

        Raises:
            RuntimeError: If execution failed due to node failures.
        """
        # Ensure all futures are cleaned up
        self._cleanup_futures(execution_state["running"])

        # Check if execution failed
        if execution_state["failed"]:
            raise RuntimeError("Pipeline execution failed due to node failures")

        # Log summary for ML pipelines
        if self.is_ml_layer:
            self._log_ml_pipeline_summary(
                execution_state["execution_results"],
                ml_info,
            )

        logger.info(
            f"Pipeline execution completed. Processed {len(execution_state['completed'])} nodes."
        )

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

        module = self._import_module_with_fallback(module_path)
        func = self._get_function_from_module(module, function_name, module_path)
        self._validate_function_signature(func, function_name)
        return func

    def _import_module_with_fallback(self, module_path: str):
        try:
            return _import_module_cached(module_path)
        except ImportError as e:
            logger.debug(
                f"Initial import failed for '{module_path}', trying fallback import paths: {e}"
            )
            import importlib
            import sys
            from pathlib import Path

            candidates = self._gather_import_candidates()
            uniq_candidates = self._deduplicate_candidates(candidates)
            added = self._add_candidates_to_syspath(uniq_candidates)

            try:
                module = importlib.import_module(module_path)
            except Exception as e2:
                logger.error(
                    f"Failed to import module '{module_path}' after adding candidates: {e2}"
                )
                self._remove_candidates_from_syspath(added, sys)
                raise ImportError(f"Cannot import module '{module_path}': {str(e)}")
            finally:
                self._remove_candidates_from_syspath(added, sys)
            return module

    def _gather_import_candidates(self):
        from pathlib import Path

        candidates = []
        try:
            cwd = Path.cwd()
            candidates.extend([str(cwd), str(cwd / "src"), str(cwd / "lib")])
        except Exception:
            pass

        try:
            cp = getattr(self.context, "config_paths", None)
            if isinstance(cp, dict) and cp:
                first = next(iter(cp.values()))
                p = Path(first)
                candidates.append(str(p.parent))
                candidates.append(str(p.parent / "src"))
        except Exception:
            pass
        return candidates

    def _deduplicate_candidates(self, candidates):
        seen = set()
        return [c for c in candidates if c and not (c in seen or seen.add(c))]

    def _add_candidates_to_syspath(self, uniq_candidates):
        import sys
        from pathlib import Path

        added = []
        for c in uniq_candidates:
            try:
                pc = Path(c)
                if pc.exists() and pc.is_dir() and c not in sys.path:
                    sys.path.insert(0, c)
                    added.append(c)
                    logger.debug(f"Temporarily added to sys.path: {c}")
            except Exception:
                continue
        return added

    def _remove_candidates_from_syspath(self, added, sys):
        for c in added:
            try:
                while c in sys.path:
                    sys.path.remove(c)
            except Exception:
                pass

    def _get_function_from_module(self, module, function_name, module_path):
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
        return func

    def _validate_function_signature(self, func, function_name):
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
