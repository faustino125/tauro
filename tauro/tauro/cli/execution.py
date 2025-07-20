from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from tauro.cli.config import AppConfigManager, ConfigManager
from tauro.cli.core import ConfigFormat, ConfigurationError, ExecutionError, PathManager

try:
    from tauro.config import Context
    from tauro.exec import PipelineExecutor as ExternalPipelineExecutor
except ImportError as e:
    logger.error(f"Missing required modules: {e}")
    raise ImportError("au_execute and au_setting modules required") from e


class ContextInitializer:
    """Initializes execution context from configuration."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config_loader = config_manager.create_loader()
        self.active_format = config_manager.get_active_format()

    def initialize(self, env: str) -> Context:
        """Initialize context for given environment."""
        try:
            config_file_path = self.config_manager.get_config_file_path()
            app_config = AppConfigManager(config_file_path)
            config_paths = app_config.get_env_config(env)

            if self.active_format == ConfigFormat.JSON:
                return self._init_unified_config(config_paths)
            else:
                return self._init_separate_files(config_paths)

        except Exception as e:
            raise ConfigurationError(f"Context initialization failed: {e}")

    def _init_unified_config(self, config_paths: Dict[str, str]) -> Context:
        """Initialize from unified JSON configuration."""
        if "config_path" not in config_paths:
            raise ConfigurationError("JSON config requires 'config_path'")

        logger.info("Loading unified JSON configuration")
        config_data = self.config_loader.load_config(config_paths["config_path"])

        return Context.from_json_config(
            global_settings=config_data.get("global_settings", {}),
            pipelines_config=config_data.get("pipeline", {}),
            nodes_config=config_data.get("node", {}),
            input_config=config_data.get("input", {}),
            output_config=config_data.get("output", {}),
        )

    def _init_separate_files(self, config_paths: Dict[str, str]) -> Context:
        """Initialize from separate configuration files."""
        required = [
            "global_settings_path",
            "pipelines_config_path",
            "nodes_config_path",
            "input_config_path",
            "output_config_path",
        ]

        missing = [path for path in required if path not in config_paths]
        if missing:
            raise ConfigurationError(f"Missing config paths: {missing}")

        logger.info(f"Loading separate {self.active_format.value.upper()} files")

        return Context(
            global_settings=config_paths["global_settings_path"],
            pipelines_config=config_paths["pipelines_config_path"],
            nodes_config=config_paths["nodes_config_path"],
            input_config=config_paths["input_config_path"],
            output_config=config_paths["output_config_path"],
        )


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

            logger.info(f"Executing pipeline: {pipeline_name}")
            if node_name:
                logger.info(f"Target node: {node_name}")

            self.executor.run_pipeline(
                pipeline_name=pipeline_name,
                node_name=node_name,
                start_date=start_date,
                end_date=end_date,
            )

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
        """Check if node exists in pipeline."""
        try:
            nodes = getattr(self.context, "nodes_config", {})
            if hasattr(nodes, "get"):
                pipeline_nodes = nodes.get(pipeline_name, {})
                return node_name in pipeline_nodes
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
                    info["description"] = pipeline_config.get(
                        "description", info["description"]
                    )
                    info["nodes"] = list(pipeline_config.get("nodes", {}).keys())

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

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of execution context."""
        try:
            summary = {
                "context_type": type(self.context).__name__,
                "has_path_manager": self.path_manager is not None,
                "available_pipelines": self.list_pipelines(),
                "config_dir": (
                    str(self.path_manager.config_dir) if self.path_manager else None
                ),
            }

            if hasattr(self.context, "global_settings"):
                settings = getattr(self.context, "global_settings", {})
                if hasattr(settings, "get"):
                    summary["environment"] = settings.get("environment", "unknown")
                    summary["project_name"] = settings.get("project_name", "unknown")

            return summary
        except Exception:
            return {"context_type": "unknown", "has_path_manager": False}
