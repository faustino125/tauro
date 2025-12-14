"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from core.cli.core import ConfigurationError, ExecutionError, PathManager
from core.cli.config import AppConfigManager, ConfigManager
from core.config.contexts import Context
from core.config.context_loader import ContextLoader
from core.exec.executor import PipelineExecutor as ExternalPipelineExecutor


class ContextInitializer:
    """Initializes execution context from configuration."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.context_loader = ContextLoader()

    def initialize(self, env: str) -> Context:
        """Initialize context for given environment."""
        # Print configuration loading separator
        try:
            from core.cli.rich_logger import RichLoggerManager, print_process_separator

            console = RichLoggerManager.get_console()
            console.print()
            print_process_separator(
                "configuration", "LOADING CONFIGURATION", f"Environment: {env}", console
            )
            console.print()
        except Exception:
            pass

        try:
            config_file_path = self.config_manager.get_config_file_path()
            app_config = AppConfigManager(config_file_path)
            config_paths = app_config.get_env_config(env)

            return self.context_loader.load_from_paths(config_paths, env)

        except Exception as e:
            raise ConfigurationError(f"Context initialization failed: {e}")


class PipelineExecutor:
    """Wraps external pipeline executor with enhanced error handling."""

    def __init__(self, context: Context, config_dir: Optional[Path] = None):
        self.context = context
        self.executor = ExternalPipelineExecutor(context)
        self.path_manager = PathManager(config_dir) if config_dir else None

    def execute(
        self,
        pipeline_name: str,
        node_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        """Execute pipeline with comprehensive error handling."""
        if dry_run:
            self._log_dry_run(pipeline_name, node_name, start_date, end_date)
            return

        try:
            if self.path_manager:
                self.path_manager.setup_import_paths()

            if node_name:
                logger.info(f"Target node: {node_name}")

            self.executor.run_pipeline(
                pipeline_name=pipeline_name,
                node_name=node_name,
                start_date=start_date,
                end_date=end_date,
            )

            try:
                from core.cli.rich_logger import RichLoggerManager, print_process_separator
                from rich.rule import Rule

                console = RichLoggerManager.get_console()
                print_process_separator(
                    "success", "PIPELINE COMPLETED SUCCESSFULLY", pipeline_name, console
                )
            except Exception:
                pass

            logger.success(f"Pipeline '{pipeline_name}' completed successfully")

        except ImportError as e:
            if self.path_manager:
                self.path_manager.diagnose_import_error(str(e))
            raise ExecutionError(f"Import error in pipeline '{pipeline_name}': {e}")

        except Exception as e:
            raise ExecutionError(f"Pipeline '{pipeline_name}' execution failed: {e}")

        finally:
            if self.path_manager:
                self.path_manager.cleanup()

    def _log_dry_run(
        self,
        pipeline_name: str,
        node_name: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Log dry run information."""
        logger.info(f"DRY RUN: Would execute pipeline '{pipeline_name}'")
        if node_name:
            logger.info(f"DRY RUN: Would execute node '{node_name}'")
        if start_date:
            logger.info(f"DRY RUN: Start date: {start_date}")
        if end_date:
            logger.info(f"DRY RUN: End date: {end_date}")

    def validate_pipeline(self, pipeline_name: str) -> bool:
        """Check if pipeline exists in context."""
        try:
            pipelines = getattr(self.context, "pipelines_config", {})
            if hasattr(pipelines, "get"):
                return pipeline_name in pipelines
            return True  # Assume valid if can't verify
        except Exception:
            return True

    def validate_node(self, pipeline_name: str, node_name: str) -> bool:
        """Check if node exists in the specified pipeline."""
        try:
            pipelines = getattr(self.context, "pipelines_config", {})
            if hasattr(pipelines, "get"):
                pipeline_config = pipelines.get(pipeline_name, {})
                nodes = pipeline_config.get("nodes", [])
                if isinstance(nodes, list):
                    node_names = [n.get("name") if isinstance(n, dict) else n for n in nodes]
                    return node_name in node_names
            return True
        except Exception:
            return True

    def get_pipeline_info(self, pipeline_name: str) -> Dict[str, Any]:
        """Get detailed information about a pipeline."""
        try:
            info = {
                "name": pipeline_name,
                "exists": self.validate_pipeline(pipeline_name),
                "nodes": [],
                "description": "No description available",
            }

            if hasattr(self.context, "pipelines_config"):
                pipelines = getattr(self.context, "pipelines_config", {})
                if hasattr(pipelines, "get"):
                    pipeline_config = pipelines.get(pipeline_name, {})
                    info["description"] = pipeline_config.get("description", info["description"])
                    nodes = pipeline_config.get("nodes", [])
                    info["nodes"] = [n.get("name") if isinstance(n, dict) else n for n in nodes]

            return info
        except Exception:
            return {
                "name": pipeline_name,
                "exists": True,
                "nodes": [],
                "description": "Unknown",
            }

    def list_pipelines(self) -> List[str]:
        """List all available pipelines."""
        try:
            if hasattr(self.context, "pipelines_config"):
                pipelines = getattr(self.context, "pipelines_config", {})
                if hasattr(pipelines, "keys"):
                    return list(pipelines.keys())
            return []
        except Exception:
            return []

    def run_streaming_pipeline(
        self,
        pipeline_name: str,
        mode: str = "async",
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Execute a streaming pipeline."""
        try:
            if self.path_manager:
                self.path_manager.setup_import_paths()

            return self.executor.run_streaming_pipeline(
                pipeline_name=pipeline_name,
                mode=mode,
                model_version=model_version,
                hyperparams=hyperparams,
            )
        except Exception as e:
            raise ExecutionError(f"Streaming pipeline execution failed: {e}")
        finally:
            if self.path_manager:
                self.path_manager.cleanup()

    def get_streaming_pipeline_status(self, execution_id: str) -> Dict[str, Any]:
        """Get status of a specific streaming pipeline."""
        try:
            return self.executor.get_streaming_pipeline_status(execution_id)
        except Exception as e:
            logger.error(f"Failed to get streaming status: {e}")
            return {}

    def list_streaming_pipelines(self) -> List[Dict[str, Any]]:
        """List all running streaming pipelines."""
        try:
            return self.executor.list_streaming_pipelines()
        except Exception as e:
            logger.error(f"Failed to list streaming pipelines: {e}")
            return []

    def stop_streaming_pipeline(self, execution_id: str) -> bool:
        """Stop a streaming pipeline."""
        try:
            return self.executor.stop_streaming_pipeline(execution_id, graceful=True)
        except Exception as e:
            logger.error(f"Failed to stop streaming pipeline: {e}")
            return False

    def get_running_execution_ids(self) -> List[str]:
        """Get list of running execution IDs."""
        try:
            return self.executor.get_running_execution_ids()
        except Exception:
            return []

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of execution context."""
        try:
            summary = {
                "context_type": type(self.context).__name__,
                "has_path_manager": self.path_manager is not None,
                "available_pipelines": self.list_pipelines(),
                "config_dir": (str(self.path_manager.config_dir) if self.path_manager else None),
            }

            if hasattr(self.context, "global_settings"):
                settings = getattr(self.context, "global_settings", {})
                if hasattr(settings, "get"):
                    summary["environment"] = settings.get("environment", "unknown")
                    summary["project_name"] = settings.get("project_name", "unknown")

            return summary
        except Exception:
            return {"context_type": "unknown", "has_path_manager": False}
