import importlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from concurrent.futures import as_completed as thread_as_completed
from typing import Any, Callable, Dict, List, Set

from loguru import logger  # type: ignore

from tauro.exec.commands import Command, MLNodeCommand, NodeCommand
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.pipeline_validator import PipelineValidator


class NodeExecutor:
    """Handles the execution of individual nodes and parallel execution coordination."""

    def __init__(self, context, input_loader, output_manager, max_workers: int = 4):
        self.context = context
        self.input_loader = input_loader
        self.output_manager = output_manager
        self.max_workers = max_workers
        self.is_ml_layer = context.layer == "ml"

    def execute_single_node(
        self,
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute a single node with proper error handling and logging."""
        try:
            node_config = self._get_node_config(node_name)
            function = self._load_node_function(node_config)
            input_dfs = self.input_loader.load_inputs(node_config)

            command = self._create_command(
                function, input_dfs, start_date, end_date, node_name, ml_info
            )

            result_df = command.execute()

            self._validate_and_save_output(
                result_df, node_config, node_name, start_date, end_date, ml_info
            )

        except Exception as e:
            logger.error(f"Failed to execute node '{node_name}': {str(e)}")
            raise

    def execute_nodes_parallel(
        self,
        execution_order: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        dag: Dict[str, Set[str]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute nodes in parallel while respecting dependencies."""
        completed = set()
        running = {}
        ready_queue = deque()
        failed = False

        for node in execution_order:
            node_config = node_configs[node]
            dependencies = DependencyResolver.get_node_dependencies(node_config)
            if not dependencies:
                ready_queue.append(node)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                while (ready_queue or running) and not failed:
                    self._submit_ready_nodes(
                        ready_queue, running, executor, start_date, end_date, ml_info
                    )

                    if running:
                        failed = self._process_completed_nodes(
                            running, completed, dag, ready_queue, node_configs
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

        logger.info(f"Pipeline execution completed. Processed {len(completed)} nodes.")

    def _create_command(
        self,
        function: Callable,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
        ml_info: Dict[str, Any],
    ) -> Command:
        """Create appropriate command based on layer type."""
        if self.is_ml_layer:
            return MLNodeCommand(
                function=function,
                input_dfs=input_dfs,
                start_date=start_date,
                end_date=end_date,
                node_name=node_name,
                model_version=ml_info["model_version"],
                hyperparams=ml_info["hyperparams"],
                spark=self.context.spark,
            )
        else:
            return NodeCommand(
                function=function,
                input_dfs=input_dfs,
                start_date=start_date,
                end_date=end_date,
                node_name=node_name,
            )

    def _submit_ready_nodes(
        self,
        ready_queue: deque,
        running: Dict,
        executor: ThreadPoolExecutor,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Submit ready nodes for execution."""
        while ready_queue and len(running) < self.max_workers:
            node_name = ready_queue.popleft()
            logger.info(f"Starting execution of node: {node_name}")

            future = executor.submit(
                self.execute_single_node, node_name, start_date, end_date, ml_info
            )
            running[future] = node_name

    def _process_completed_nodes(
        self,
        running: Dict,
        completed: Set[str],
        dag: Dict[str, Set[str]],
        ready_queue: deque,
        node_configs: Dict[str, Dict[str, Any]],
    ) -> bool:
        """
        Process completed nodes and update ready queue.
        """
        if not running:
            return False

        future_list = list(running.keys())
        completed_futures = []
        failed = False

        try:
            for future in thread_as_completed(future_list, timeout=10):
                completed_futures.append(future)
                node_name = running[future]

                try:
                    future.result()
                    completed.add(node_name)
                    logger.info(f"Node '{node_name}' completed successfully")

                    newly_ready = self._find_newly_ready_nodes(
                        node_name, dag, completed, ready_queue, running, node_configs
                    )
                    ready_queue.extend(newly_ready)

                except Exception as e:
                    logger.error(f"Node '{node_name}' failed: {str(e)}")
                    failed = True
                    break

        except TimeoutError:
            logger.debug("Timeout waiting for node completion, will retry...")
            pass
        except Exception as e:
            logger.error(f"Unexpected error in _process_completed_nodes: {str(e)}")
            failed = True

        for future in completed_futures:
            if future in running:
                del running[future]

        if failed:
            self._cancel_all_futures(running)

        return failed

    def _handle_unfinished_futures(self, running: Dict) -> None:
        """Handle any unfinished futures at the end of execution."""
        logger.warning("Handling unfinished futures...")

        import time

        time.sleep(5)

        remaining_futures = []
        for future, node_name in list(running.items()):
            if future.done():
                try:
                    future.result()
                    logger.info(f"Late completion of node '{node_name}'")
                except Exception as e:
                    logger.error(f"Late failure of node '{node_name}': {str(e)}")
                del running[future]
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
        for future, node_name in running.items():
            logger.warning(f"Cancelling future for node '{node_name}'")
            future.cancel()

    def _cleanup_futures(self, running: Dict) -> None:
        """Ensure all futures are properly cleaned up."""
        if not running:
            return

        logger.debug(f"Cleaning up {len(running)} remaining futures")

        for future, node_name in list(running.items()):
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
        running_nodes = set(running.values())
        queued_nodes = set(ready_queue)

        for dependent in dag[completed_node]:
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

    def _validate_and_save_output(
        self,
        result_df: Any,
        node_config: Dict[str, Any],
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Validate result DataFrame and save output."""
        PipelineValidator.validate_dataframe_schema(result_df)

        if hasattr(result_df, "printSchema"):
            logger.debug(f"Schema for node '{node_name}':")
            result_df.printSchema()

        output_params = {
            "node": node_config,
            "df": result_df,
            "start_date": start_date,
            "end_date": end_date,
        }

        if self.is_ml_layer:
            output_params["model_version"] = ml_info["model_version"]

        self.output_manager.save_output(**output_params)
        logger.info(f"Output saved successfully for node '{node_name}'")

    def _get_node_config(self, node_name: str) -> Dict[str, Any]:
        """Get configuration for a specific node with enhanced error handling."""
        node = self.context.nodes_config.get(node_name)
        if not node:
            available_nodes = list(self.context.nodes_config.keys())
            raise ValueError(
                f"Node '{node_name}' not found in configuration. "
                f"Available nodes: {', '.join(available_nodes[:10])}"
                f"{'...' if len(available_nodes) > 10 else ''}"
            )
        return node

    def _load_node_function(self, node: Dict[str, Any]) -> Callable:
        """Load a node's function from its corresponding module."""
        module_path = node.get("module")
        function_name = node.get("function")

        if not module_path or not function_name:
            raise ValueError(
                f"Missing module/function configuration in node. "
                f"Got module='{module_path}', function='{function_name}'"
            )

        try:
            logger.debug(
                f"Loading function '{function_name}' from module '{module_path}'"
            )
            module = importlib.import_module(module_path)

            if not hasattr(module, function_name):
                available_functions = [
                    attr for attr in dir(module) if callable(getattr(module, attr))
                ]
                raise AttributeError(
                    f"Function '{function_name}' not found in module '{module_path}'. "
                    f"Available functions: {', '.join(available_functions[:5])}"
                    f"{'...' if len(available_functions) > 5 else ''}"
                )

            return getattr(module, function_name)

        except ImportError as e:
            logger.error(f"Failed to import module '{module_path}': {str(e)}")
            raise ImportError(f"Cannot import module '{module_path}': {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error loading function: {str(e)}")
            raise
