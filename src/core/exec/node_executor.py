"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import inspect
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from concurrent.futures import as_completed as thread_as_completed
from typing import Any, Callable, Dict, List, Optional, Set
from pathlib import Path

from loguru import logger  # type: ignore

from core.exec.commands import Command, MLNodeCommand, NodeCommand
from core.exec.dependency_resolver import DependencyResolver
from core.exec.import_security import SecureModuleImporter, ModuleImportError
from core.exec.pipeline_validator import PipelineValidator
from core.exec.resource_pool import get_default_resource_pool
from core.exec.resource_manager import get_resource_manager, ResourceType


class ThreadSafeExecutionState:
    """Thread-safe execution state for parallel node execution."""

    def __init__(self, execution_order: List[str], node_configs: Dict[str, Dict[str, Any]]):
        self._lock = threading.RLock()
        self.ready_queue = deque()
        self.running: Dict = {}
        self.completed: Set[str] = set()
        self.failed = False
        self.execution_results: Dict[str, Any] = {}

        # Initialize ready queue with nodes that have no dependencies
        for node in execution_order:
            node_config = node_configs[node]
            dependencies = DependencyResolver.get_node_dependencies(node_config)
            if not dependencies:
                self.ready_queue.append(node)

    def mark_completed(self, node_name: str, result: Dict[str, Any]) -> None:
        """Thread-safe mark node as completed."""
        with self._lock:
            self.completed.add(node_name)
            self.execution_results[node_name] = result

    def mark_failed(self, node_name: str, error_info: Dict[str, Any]) -> None:
        """Thread-safe mark node as failed."""
        with self._lock:
            self.failed = True
            self.execution_results[node_name] = error_info

    def is_completed(self, node_name: str) -> bool:
        """Thread-safe check if node is completed."""
        with self._lock:
            return node_name in self.completed

    def add_to_ready_queue(self, nodes: List[str]) -> None:
        """Thread-safe add nodes to ready queue."""
        with self._lock:
            self.ready_queue.extend(nodes)

    def pop_ready_node(self) -> Optional[str]:
        """Thread-safe pop node from ready queue."""
        with self._lock:
            if self.ready_queue:
                return self.ready_queue.popleft()
            return None

    def add_running_future(self, future, node_info: Dict[str, Any]) -> None:
        """Thread-safe add running future."""
        with self._lock:
            self.running[future] = node_info

    def remove_running_future(self, future) -> Optional[Dict[str, Any]]:
        """Thread-safe remove running future."""
        with self._lock:
            return self.running.pop(future, None)

    def get_running_count(self) -> int:
        """Thread-safe get count of running nodes."""
        with self._lock:
            return len(self.running)

    def has_work_pending(self) -> bool:
        """Thread-safe check if there's work pending."""
        with self._lock:
            return bool(self.ready_queue or self.running) and not self.failed


class NodeExecutor:
    """Enhanced node executor with ML support and secure module loading."""

    # Default timeout for single node execution (30 minutes)
    DEFAULT_NODE_TIMEOUT = 1800

    def __init__(
        self,
        context,
        input_loader,
        output_manager,
        max_workers: int = 4,
        timeout: Optional[int] = None,
    ):
        self.context = context
        self.input_loader = input_loader
        self.output_manager = output_manager
        self.max_workers = max_workers
        self.is_ml_layer = getattr(context, "is_ml_layer", False)
        self.resource_pool = get_default_resource_pool()

        # Configure timeout for node execution
        gs = getattr(context, "global_settings", {}) or {}
        self.node_timeout = timeout or gs.get("node_timeout_seconds", self.DEFAULT_NODE_TIMEOUT)
        logger.debug(f"NodeExecutor initialized with timeout: {self.node_timeout}s")

        # Initialize secure module importer
        self._init_secure_importer()

    def _init_secure_importer(self) -> None:
        """Initialize secure module importer with context-specific configuration."""
        # Get allowed prefixes from context or use defaults
        allowed_prefixes = getattr(self.context, "allowed_module_prefixes", None)

        # Get additional search paths from context
        additional_paths = self._gather_search_paths()

        # Allow custom strict_mode from context, default to False (permissive)
        strict_mode = getattr(self.context, "strict_module_import", False)

        # Initialize secure importer
        self.secure_importer = SecureModuleImporter(
            allowed_prefixes=allowed_prefixes,
            additional_search_paths=additional_paths,
            strict_mode=strict_mode,
        )

        logger.debug(
            f"Secure module importer initialized (strict={strict_mode}) "
            f"with {len(additional_paths)} search paths"
        )

    def _gather_search_paths(self) -> List[Path]:
        """Gather additional module search paths from context and environment.

        Thread-safe: captures cwd once at the start to avoid race conditions.
        """
        paths = []

        # Current working directory - capture once to avoid race conditions
        try:
            cwd = Path.cwd()
            paths.append(cwd)
            paths.append(cwd / "src")
            paths.append(cwd / "lib")
        except Exception as e:
            logger.debug(f"Could not access current working directory: {e}")

        # Config directory from context
        try:
            config_paths = getattr(self.context, "config_paths", None)
            if isinstance(config_paths, dict) and config_paths:
                first_path = next(iter(config_paths.values()))
                parent = Path(first_path).parent
                paths.append(parent)
                paths.append(parent / "src")
        except Exception:
            pass

        # Config file path if available
        if hasattr(self.context, "_config_file_path"):
            try:
                config_file = Path(self.context._config_file_path)
                paths.append(config_file.parent)
                paths.append(config_file.parent / "src")
            except Exception:
                pass

        # Remove duplicates while preserving order
        seen = set()
        unique_paths = []
        for path in paths:
            path_str = str(path)
            if path_str not in seen:
                seen.add(path_str)
                unique_paths.append(path)

        return unique_paths

    def execute_single_node(
        self,
        node_name: str,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Execute a single node with enhanced ML support and error handling."""
        start_time = time.perf_counter()
        resource_manager = get_resource_manager()

        # Use context manager for automatic resource cleanup
        with resource_manager.resource_context(f"node_{node_name}"):
            try:
                node_config = self._get_node_config(node_name)
                function = self._load_node_function(node_config)
                input_dfs = self.input_loader.load_inputs(node_config)

                # Register input resources for cleanup
                for idx, df in enumerate(input_dfs):
                    if df is not None:
                        resource_type = self._detect_resource_type(df)
                        resource_manager.register(
                            resource=df,
                            resource_type=resource_type,
                            context_id=f"node_{node_name}",
                            metadata={"index": idx, "stage": "input"},
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
                    resource_manager.register(
                        resource=result_df,
                        resource_type=resource_type,
                        context_id=f"node_{node_name}",
                        metadata={"stage": "output"},
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
                # Usar el analizador de errores elegante para desarrolladores
                try:
                    from core.cli.error_analyzer import format_error_for_developer
                    from core.cli.rich_logger import RichLoggerManager

                    console = RichLoggerManager.get_console()
                    format_error_for_developer(e, node_name, console)
                except Exception:
                    # Fallback a log simple si falla el análisis
                    logger.error(f"Failed to execute node '{node_name}': {str(e)}")
                raise
            finally:
                duration = time.perf_counter() - start_time
                logger.debug(f"Node '{node_name}' executed in {duration:.2f}s")

    def _detect_resource_type(self, resource: Any) -> str:
        """Detect the type of resource for proper cleanup.

        Args:
            resource: The resource object to analyze

        Returns:
            Resource type string (from ResourceType constants)
        """
        # Check for Spark DataFrame/RDD
        if hasattr(resource, "unpersist") and hasattr(resource, "rdd"):
            return ResourceType.SPARK_DF

        # Check for Pandas DataFrame
        if hasattr(resource, "columns") and hasattr(resource, "empty"):
            return ResourceType.PANDAS_DF

        # Check for GPU resources
        if hasattr(resource, "device") and hasattr(resource, "reset"):
            return ResourceType.GPU_RESOURCE

        # Check for database connections
        if hasattr(resource, "cursor") or hasattr(resource, "execute"):
            return ResourceType.CONNECTION

        # Check for file handles
        if hasattr(resource, "read") and hasattr(resource, "close"):
            return ResourceType.FILE_HANDLE

        # Check for file paths
        if isinstance(resource, (str, Path)):
            import os

            if os.path.exists(str(resource)):
                return ResourceType.TEMP_FILE

        # Generic resource
        return ResourceType.GENERIC

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
        execution_state = self._initialize_execution_state(execution_order, node_configs)

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
                self._cancel_all_futures(execution_state.running)
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
    ) -> ThreadSafeExecutionState:
        """Initialize thread-safe execution state with queues and tracking structures."""
        state = ThreadSafeExecutionState(execution_order, node_configs)
        logger.debug(f"Initial ready nodes: {list(state.ready_queue)}")
        return state

    def _coordinate_parallel_execution(
        self,
        executor: ThreadPoolExecutor,
        execution_state: ThreadSafeExecutionState,
        dag: Dict[str, Set[str]],
        node_configs: Dict[str, Dict[str, Any]],
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
    ) -> None:
        """Coordinate the main execution loop until all nodes complete or failure occurs."""
        while execution_state.has_work_pending():
            # Submit newly ready nodes to executor
            self._submit_ready_nodes(
                execution_state=execution_state,
                executor=executor,
                start_date=start_date,
                end_date=end_date,
                ml_info=ml_info,
                node_configs=node_configs,
            )

            # Process completed nodes and check for failures
            if execution_state.get_running_count() > 0:
                self._process_completed_nodes(
                    execution_state=execution_state,
                    dag=dag,
                    node_configs=node_configs,
                )

        # Handle any remaining unfinished futures
        if execution_state.get_running_count() > 0:
            logger.warning(
                f"Pipeline ended with {execution_state.get_running_count()} unfinished futures"
            )
            self._handle_unfinished_futures(execution_state)

    def _cleanup_execution(
        self,
        execution_state: ThreadSafeExecutionState,
        ml_info: Dict[str, Any],
    ) -> None:
        """Cleanup resources and handle final state after execution completes.

        Raises:
            RuntimeError: If execution failed due to node failures.
        """
        # Ensure all futures are cleaned up
        self._cleanup_futures(execution_state.running)

        # Check if execution failed
        if execution_state.failed:
            raise RuntimeError("Pipeline execution failed due to node failures")

        # Log summary for ML pipelines
        if self.is_ml_layer:
            self._log_ml_pipeline_summary(
                execution_state.execution_results,
                ml_info,
            )

        # Print execution summary separator
        try:
            from core.cli.rich_logger import RichLoggerManager
            from rich.rule import Rule

            console = RichLoggerManager.get_console()
            console.print()
            from core.cli.rich_logger import print_process_separator

            total_nodes = len(execution_state.completed)
            print_process_separator("summary", "EXECUTION SUMMARY", f"{total_nodes} nodes", console)
            console.print()
        except Exception:
            # ignore non-critical logging/display errors
            pass

        logger.info(
            f"Pipeline execution completed. Processed {len(execution_state.completed)} nodes."
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
        execution_state: ThreadSafeExecutionState,
        executor: ThreadPoolExecutor,
        start_date: str,
        end_date: str,
        ml_info: Dict[str, Any],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Submit ready nodes for execution with enhanced context."""
        while execution_state.get_running_count() < self.max_workers:
            node_name = execution_state.pop_ready_node()
            if not node_name:
                break

            # Print node execution separator
            try:
                from core.cli.rich_logger import log_node_start

                node_display = node_name.split(".")[-1] if "." in node_name else node_name
                log_node_start(node_display, "")
            except Exception:
                logger.info(f"Starting execution of node: {node_name}")

            node_ml_info = self._prepare_node_ml_info(node_name, ml_info)

            future = executor.submit(
                self.execute_single_node, node_name, start_date, end_date, node_ml_info
            )
            execution_state.add_running_future(
                future,
                {
                    "node_name": node_name,
                    "start_time": time.time(),
                    "config": node_configs.get(node_name, {}),
                },
            )

    def _prepare_node_ml_info(self, node_name: str, ml_info: Dict[str, Any]) -> Dict[str, Any]:
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
        execution_state: ThreadSafeExecutionState,
        dag: Dict[str, Set[str]],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Process completed nodes with timeout protection to prevent deadlock."""
        if execution_state.get_running_count() == 0:
            return

        future_list = list(execution_state.running.keys())
        completed_futures = []

        # Use context timeout with safety margin for node processing
        max_timeout = getattr(self.context, "execution_timeout_seconds", 3600)
        processing_timeout = min(30, max_timeout / 2)  # 30s or half of max

        try:
            for future in thread_as_completed(future_list, timeout=processing_timeout):
                completed_futures.append(future)
                node_info = execution_state.remove_running_future(future)
                if not node_info:
                    continue

                node_name = node_info["node_name"]
                should_break = self._handle_completed_future(
                    future, node_name, node_info, execution_state, dag, node_configs
                )
                if should_break:
                    break

        except TimeoutError:
            # Timeout waiting for ANY node completion
            logger.warning(
                f"Timeout ({processing_timeout}s) waiting for node completion. "
                f"{execution_state.get_running_count()} nodes still running."
            )
        except Exception as e:
            logger.error(f"Unexpected error in _process_completed_nodes: {str(e)}")
            execution_state.failed = True

        # Ensure all completed futures are properly cleaned up
        for future in completed_futures:
            if execution_state.running.get(future):
                execution_state.remove_running_future(future)

        if execution_state.failed:
            self._cancel_all_futures(execution_state.running)

    def _handle_completed_future(
        self,
        future,
        node_name: str,
        node_info: Dict[str, Any],
        execution_state: ThreadSafeExecutionState,
        dag: Dict[str, Set[str]],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> bool:
        """Handle a single completed future. Returns True if execution should break."""
        try:
            # Get result with safety timeout to prevent hanging
            future.result(timeout=5)
            self._handle_node_success(node_name, node_info, execution_state, dag, node_configs)
            return False

        except TimeoutError as te:
            self._handle_node_timeout(node_name, node_info, execution_state, te)
            return True

        except Exception as e:
            self._handle_node_failure(node_name, node_info, execution_state, e)
            return True

    def _handle_node_success(
        self,
        node_name: str,
        node_info: Dict[str, Any],
        execution_state: ThreadSafeExecutionState,
        dag: Dict[str, Set[str]],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> None:
        """Handle successful node completion."""
        result_dict = {
            "status": "success",
            "start_time": node_info["start_time"],
            "end_time": time.time(),
            "config": node_info["config"],
        }
        execution_state.mark_completed(node_name, result_dict)

        # Print node completion
        self._log_node_completion(node_name, node_info)

        newly_ready = self._find_newly_ready_nodes(node_name, dag, execution_state, node_configs)
        execution_state.add_to_ready_queue(newly_ready)

    def _log_node_completion(self, node_name: str, node_info: Dict[str, Any]) -> None:
        """Log node completion with graceful fallback."""
        try:
            from core.cli.rich_logger import log_node_complete

            duration = time.time() - node_info["start_time"]
            node_display = node_name.split(".")[-1] if "." in node_name else node_name
            log_node_complete(node_display, duration)
        except Exception:
            logger.info(f"Node '{node_name}' completed successfully")

    def _handle_node_timeout(
        self,
        node_name: str,
        node_info: Dict[str, Any],
        execution_state: ThreadSafeExecutionState,
        timeout_error: TimeoutError,
    ) -> None:
        """Handle node timeout failure."""
        error_info = {
            "status": "failed",
            "error": "Node execution timeout exceeded",
            "error_type": "TimeoutError",
            "start_time": node_info["start_time"],
            "end_time": time.time(),
            "config": node_info["config"],
        }
        execution_state.mark_failed(node_name, error_info)
        logger.error(f"Node '{node_name}' exceeded timeout: {timeout_error}")
        execution_state.failed = True

    def _handle_node_failure(
        self,
        node_name: str,
        node_info: Dict[str, Any],
        execution_state: ThreadSafeExecutionState,
        error: Exception,
    ) -> None:
        """Handle node execution failure with elegant error reporting."""
        error_info = {
            "status": "failed",
            "error": str(error),
            "error_type": type(error).__name__,
            "start_time": node_info["start_time"],
            "end_time": time.time(),
            "config": node_info["config"],
        }
        execution_state.mark_failed(node_name, error_info)

        # Use elegant error analyzer
        try:
            from core.cli.error_analyzer import format_error_for_developer
            from core.cli.rich_logger import RichLoggerManager

            console = RichLoggerManager.get_console()
            format_error_for_developer(error, node_name, console)
        except Exception:
            # Fallback to simple logging if analysis fails
            logger.error(f"Node '{node_name}' failed: {str(error)}")

        execution_state.failed = True

    def _log_ml_pipeline_summary(
        self, execution_results: Dict[str, Any], ml_info: Dict[str, Any]
    ) -> None:
        """Log comprehensive ML pipeline execution summary."""
        logger.info("🎯 ML PIPELINE EXECUTION SUMMARY")

        successful_nodes = [
            name for name, result in execution_results.items() if result.get("status") == "success"
        ]
        failed_nodes = [
            name for name, result in execution_results.items() if result.get("status") == "failed"
        ]

        logger.info(f"✅ Successful nodes: {len(successful_nodes)}")
        logger.info(f"❌ Failed nodes: {len(failed_nodes)}")

        if successful_nodes:
            logger.info(f"Successful: {', '.join(successful_nodes)}")

        if failed_nodes:
            logger.error(f"Failed: {', '.join(failed_nodes)}")

        logger.info(f"📦 Project: {ml_info.get('project_name', 'Unknown')}")

        total_time = 0
        for result in execution_results.values():
            if "start_time" in result and "end_time" in result:
                total_time += result["end_time"] - result["start_time"]

        logger.info(f"⏱️  Total execution time: {total_time:.2f}s")

    def _handle_unfinished_futures(self, execution_state: ThreadSafeExecutionState) -> None:
        """Handle any unfinished futures at the end of execution."""
        logger.warning("Handling unfinished futures...")

        import time

        time.sleep(5)

        remaining_futures = []
        for future, node_info in execution_state.running.items():
            node_name = node_info["node_name"]
            if future.done():
                try:
                    future.result()
                    logger.info(f"Late completion of node '{node_name}'")
                except Exception as e:
                    logger.error(f"Late failure of node '{node_name}': {str(e)}")
                execution_state.remove_running_future(future)
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
        execution_state: ThreadSafeExecutionState,
        node_configs: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Find nodes that became ready after completing a node."""
        newly_ready = []
        running_nodes = {info["node_name"] for info in execution_state.running.values()}
        queued_nodes = set(execution_state.ready_queue)

        for dependent in dag.get(completed_node, set()):
            if (
                execution_state.is_completed(dependent)
                or dependent in running_nodes
                or dependent in queued_nodes
            ):
                continue

            node_config = node_configs[dependent]
            dependencies = DependencyResolver.get_node_dependencies(node_config)

            if all(execution_state.is_completed(dep) for dep in dependencies):
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
            # Capture schema output
            from io import StringIO
            import sys

            old_stdout = sys.stdout
            sys.stdout = StringIO()
            result_df.printSchema()
            schema_output = sys.stdout.getvalue()
            sys.stdout = old_stdout

            # Print schema section separator
            try:
                from core.cli.rich_logger import RichLoggerManager
                from rich.rule import Rule

                console = RichLoggerManager.get_console()
                console.print()
                from core.cli.rich_logger import print_process_separator

                node_display_name = node_name.replace(".", " › ")
                print_process_separator("schema", "DATA SCHEMA", node_display_name, console)
                console.print()
            except Exception as e:
                logger.debug(f"Could not print schema separator: {e}")

            # Display with elegant table formatting
            try:
                from core.cli.schema_formatter import print_spark_schema

                node_display_name = node_name.replace(".", " › ")
                print_spark_schema(schema_output, title=f"{node_display_name}")

            except ImportError as e:
                # If elegant formatting fails, show plain text
                logger.warning(f"Could not format schema as table: {e}")
                print(schema_output)
            except Exception as e:
                # If elegant formatting fails, show plain text
                logger.warning(f"Could not format schema as table: {e}")
                print(schema_output)

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

        # Print save completion
        try:
            from core.cli.rich_logger import RichLoggerManager
            from rich.text import Text

            console = RichLoggerManager.get_console()
            line = Text("  ")
            line.append("✓ ", style="bright_green")
            line.append(f"Output saved for node '{node_name}'", style="white")
            console.print(line)
            console.print()
        except Exception:
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
        """Load a node's function with comprehensive validation and security."""
        module_path = node.get("module")
        function_name = node.get("function")

        if not module_path or not function_name:
            raise ValueError("Node configuration must include 'module' and 'function'")

        try:
            # Use secure importer to load function
            func = self.secure_importer.get_function_from_module(module_path, function_name)

            # Validate function signature
            self._validate_function_signature(func, function_name)

            return func

        except ModuleImportError as e:
            logger.error(f"Security validation failed for module '{module_path}': {e}")
            raise ValueError(f"Cannot load node function: {e}") from e
        except Exception as e:
            logger.error(
                f"Unexpected error loading function '{function_name}' from '{module_path}': {e}"
            )
            raise

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
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                )
                if not accepts_kwargs:
                    logger.debug(
                        f"Function '{function_name}' doesn't accept 'ml_context' parameter nor **kwargs. "
                        "ML-specific features may not be available."
                    )

        except ValueError as e:
            logger.warning(f"Signature validation skipped for {function_name}: {str(e)}")
