"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import traceback
from typing import Any, Dict, List, Optional, Sequence, Union

from loguru import logger  # type: ignore

from core.cli.config import ConfigDiscovery, ConfigManager
from core.cli.core import (
    CLIConfig,
    ConfigCache,
    ExitCode,
    LoggerManager,
    TauroError,
    ValidationError,
    parse_iso_date,
    validate_date_range,
)
from core.cli.execution import ContextInitializer, PipelineExecutor
from core.cli.template import handle_template_command
from core.cli.formatters import PipelineStatusFormatter


# Constants for help text to avoid duplication
HELP_BASE_PATH = "Base path for config discovery"
HELP_LAYER_NAME = "Layer name for config discovery"
HELP_USE_CASE = "Use case name"
HELP_CONFIG_TYPE = "Preferred configuration type"
HELP_PIPELINE_NAME = "Pipeline name"
HELP_PIPELINE_NAME_TO_EXECUTE = "Pipeline name to execute"
HELP_TIMEOUT_SECONDS = "Timeout in seconds"
HELP_CONFIG_FILE = "Path to configuration file"


# ===== Argument Validation Functions =====
def validate_stream_run_arguments(args: argparse.Namespace) -> None:
    """
    Validate stream run subcommand arguments for consistency.
    """
    # Both config and pipeline are required (enforced by argparse)

    # Validate mode compatibility
    if args.mode not in ["sync", "async"]:
        raise ValidationError(f"Invalid mode '{args.mode}'. Use 'sync' or 'async'")

    # Validate hyperparams if provided
    if args.hyperparams:
        try:
            json.loads(args.hyperparams)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid hyperparams JSON: {e}")

    # Log-level validation (optional)
    if hasattr(args, "log_level") and args.log_level not in [
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]:
        raise ValidationError(f"Invalid log-level '{args.log_level}'")


def validate_stream_status_arguments(args: argparse.Namespace) -> None:
    """
    Validate stream status subcommand arguments.
    """
    # Config is required (enforced by argparse)

    # Validate format if provided
    if hasattr(args, "format") and args.format not in ["table", "json"]:
        raise ValidationError(f"Invalid format '{args.format}'. Use 'table' or 'json'")


def validate_stream_stop_arguments(args: argparse.Namespace) -> None:
    """
    Validate stream stop subcommand arguments.
    """
    # Both config and execution-id are required (enforced by argparse)

    # Validate timeout
    if hasattr(args, "timeout"):
        if args.timeout <= 0:
            raise ValidationError(f"Timeout must be positive, got {args.timeout}")
        if args.timeout > 3600:  # Max 1 hour
            logger.warning(f"Timeout {args.timeout}s is very long, consider reducing")


def validate_run_arguments(args: argparse.Namespace) -> None:
    """
    Validate run subcommand arguments.
    """
    # Environment and pipeline are required
    if not getattr(args, "env", None):
        raise ValidationError("--env is required for pipeline execution")

    if not getattr(args, "pipeline", None):
        raise ValidationError("--pipeline is required for pipeline execution")

    # Validate dates if provided
    if getattr(args, "start_date", None) or getattr(args, "end_date", None):
        try:
            start = parse_iso_date(args.start_date) if args.start_date else None
            end = parse_iso_date(args.end_date) if args.end_date else None
            validate_date_range(start, end)
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"Date validation error: {e}")

    # Warn about conflicting options
    if getattr(args, "validate_only", False) and getattr(args, "dry_run", False):
        logger.warning("Both --validate-only and --dry-run specified. Will validate only.")


def validate_template_arguments(args: argparse.Namespace) -> None:
    """
    Validate template subcommand arguments.
    """
    # Check if listing templates
    if getattr(args, "list_templates", False):
        return

    # For template generation, template and project-name are required
    if not getattr(args, "template", None):
        raise ValidationError("--template is required for template generation")

    if not getattr(args, "project_name", None):
        raise ValidationError("--project-name is required for template generation")

    # Validate project name (alphanumeric, underscores, hyphens)
    import re

    if not re.match(r"^[a-zA-Z0-9_-]+$", args.project_name):
        raise ValidationError(
            "Project name must contain only alphanumeric characters, underscores, or hyphens"
        )

    # Validate format
    if hasattr(args, "format") and args.format not in ["yaml", "json", "dsl"]:
        raise ValidationError(f"Invalid format '{args.format}'. Use 'yaml', 'json', or 'dsl'")

    # Check output path doesn't exist if provided
    if getattr(args, "output_path", None):
        output = Path(args.output_path)
        if output.exists() and any(output.iterdir()):
            raise ValidationError(f"Output path '{output}' already exists and is not empty")


def validate_config_arguments(args: argparse.Namespace) -> None:
    """
    Validate config subcommand arguments.
    """
    # Ensure a config subcommand was specified
    if not getattr(args, "config_command", None):
        raise ValidationError(
            "A config subcommand is required (e.g., list-configs, list-pipelines)"
        )


# Streaming CLI functions
def _load_context_from_dsl(config_path: Optional[Union[str, Path]]) -> Any:
    """Load the base context from a DSL/Python module and build a full Context."""
    if config_path is None:
        raise ValidationError("Configuration path must be provided")
    # Normalize to str
    config_path_str = str(config_path)
    from core.config.contexts import Context, ContextFactory

    base_ctx = Context.from_dsl(config_path_str)
    return ContextFactory.create_context(base_ctx)


def run_cli_impl(
    config: Optional[Union[str, Path]],
    pipeline: str,
    mode: str = "async",
    model_version: Optional[str] = None,
    hyperparams: Optional[str] = None,
) -> int:
    """Programmatic wrapper that normalizes types and calls _run_impl."""
    config_str = str(config) if config is not None else ""
    return _run_streaming_impl(config_str, pipeline, mode, model_version, hyperparams)


def _run_streaming_impl(
    config: str,
    pipeline: str,
    mode: str,
    model_version: Optional[str],
    hyperparams: Optional[str],
) -> int:
    """Core implementation for 'run' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from core.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        # Parse hyperparams if provided
        parsed_hyperparams = None
        if hyperparams:
            try:
                parsed_hyperparams = json.loads(hyperparams)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid hyperparams JSON: {e}")
                return ExitCode.VALIDATION_ERROR.value

        execution_id = executor.run_streaming_pipeline(
            pipeline_name=pipeline,
            mode=mode,
            model_version=model_version,
            hyperparams=parsed_hyperparams,
        )

        logger.info(f"Streaming pipeline '{pipeline}' started with execution ID: {execution_id}")
        return ExitCode.SUCCESS.value

    except Exception as e:
        logger.error(f"Error running streaming pipeline: {e}")
        return ExitCode.GENERAL_ERROR.value


def status_cli_impl(
    config: Optional[Union[str, Path]],
    execution_id: Optional[str] = None,
    format: str = "table",
) -> int:
    """Check status of streaming pipelines."""
    config_str = str(config) if config is not None else ""
    return _status_streaming_impl(config_str, execution_id, format)


def _status_streaming_impl(config: str, execution_id: Optional[str], format: str) -> int:
    """Core implementation for 'status' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from core.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        if execution_id:
            status_info = executor.get_streaming_pipeline_status(execution_id)

            if not status_info:
                logger.error(f"Pipeline with execution_id '{execution_id}' not found")
                return ExitCode.VALIDATION_ERROR.value

            if format == "json":
                print(json.dumps(status_info, indent=2, default=str))
            else:
                PipelineStatusFormatter.format_single_pipeline(status_info)
        else:
            status_list = _list_all_pipelines_status(executor)

            if format == "json":
                print(json.dumps(status_list, indent=2, default=str))
            else:
                PipelineStatusFormatter.format_multiple_pipelines(status_list)

        return ExitCode.SUCCESS.value

    except Exception as e:
        logger.error(f"Error fetching status: {e}")
        return ExitCode.GENERAL_ERROR.value


def stop_cli_impl(
    config: Optional[Union[str, Path]],
    execution_id: str,
    timeout: int = 60,
) -> int:
    """Stop a streaming pipeline gracefully."""
    config_str = str(config) if config is not None else ""
    return _stop_streaming_impl(config_str, execution_id, timeout)


def _stop_streaming_impl(config: str, execution_id: str, timeout: int) -> int:
    """Core implementation for 'stop' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from core.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        stopped = executor.stop_streaming_pipeline(execution_id, timeout)
        if stopped:
            logger.info(f"Pipeline '{execution_id}' stopped successfully.")
            return ExitCode.SUCCESS.value
        else:
            logger.error(f"Failed to stop pipeline '{execution_id}' within {timeout}s.")
            return ExitCode.EXECUTION_ERROR.value

    except Exception as e:
        logger.error(f"Error stopping pipeline: {e}")
        return ExitCode.GENERAL_ERROR.value


# Helper functions for streaming status display
def _list_all_pipelines_status(executor: Any) -> List[Dict[str, Any]]:
    """Retrieval of all streaming pipelines' status using explicit interface."""
    if hasattr(executor, "list_streaming_pipelines"):
        return executor.list_streaming_pipelines()
    return []


class UnifiedArgumentParser:
    """Unified argument parser for all Tauro CLI commands."""

    @staticmethod
    def create() -> argparse.ArgumentParser:
        """Create configured argument parser with subcommands."""
        parser = argparse.ArgumentParser(
            prog="tauro",
            description="Tauro - Data Pipeline Framework",
            epilog="""
            Unified CLI with subcommands for all Tauro operations.

            Examples:
            # Direct pipeline execution
            tauro run --env dev --pipeline data_processing

            # Streaming pipelines
            tauro stream run --config config/streaming.py --pipeline real_time_processing
            tauro stream status --config config/streaming.py

            # Template generation
            tauro template --template medallion_basic --project-name my_project

            # Configuration management
            tauro config list-pipelines --env dev

            Note: Orchestration management (schedules, runs) is now exclusively available
            through the API REST interface. Use the API endpoints to manage pipeline
            orchestration, scheduling, and run management.
            """,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        # Global options
        # Use a custom version flag to render an elegant signature
        parser.add_argument("--version", action="store_true", help="Show Tauro signature & version")

        # Create subparsers
        subparsers = parser.add_subparsers(
            dest="subcommand", help="Available subcommands", required=False
        )

        # Add subcommands
        UnifiedArgumentParser._add_run_subcommand(subparsers)
        UnifiedArgumentParser._add_stream_subcommand(subparsers)
        UnifiedArgumentParser._add_template_subcommand(subparsers)
        UnifiedArgumentParser._add_config_subcommand(subparsers)

        return parser

    @staticmethod
    def _add_run_subcommand(subparsers):
        """Add run subcommand for direct pipeline execution."""
        run_parser = subparsers.add_parser(
            "run",
            help="Execute pipelines directly",
            description="Execute data pipelines directly without orchestration",
        )

        # Environment and pipeline
        run_parser.add_argument(
            "--env",
            help="Execution environment (base, dev, sandbox, prod, or sandbox_<developer>)",
        )
        run_parser.add_argument("--pipeline", help=HELP_PIPELINE_NAME_TO_EXECUTE)
        run_parser.add_argument("--node", help="Specific node to execute (optional)")

        # Date range
        run_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
        run_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

        # Configuration discovery
        run_parser.add_argument("--base-path", help=HELP_BASE_PATH)
        run_parser.add_argument("--layer-name", help=HELP_LAYER_NAME)
        run_parser.add_argument("--use-case", dest="use_case_name", help=HELP_USE_CASE)
        run_parser.add_argument(
            "--config-type",
            choices=["yaml", "json", "dsl"],
            help=HELP_CONFIG_TYPE,
        )
        run_parser.add_argument(
            "--interactive", action="store_true", help="Interactive config selection"
        )

        # Logging
        run_parser.add_argument(
            "--log-level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        run_parser.add_argument("--log-file", help="Custom log file path")
        run_parser.add_argument(
            "--verbose", action="store_true", help="Enable verbose output (DEBUG)"
        )
        run_parser.add_argument("--quiet", action="store_true", help="Reduce output (ERROR only)")

        # Execution modes
        run_parser.add_argument(
            "--validate-only",
            action="store_true",
            help="Validate configuration without executing the pipeline",
        )
        run_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Log actions without executing the pipeline",
        )

    @staticmethod
    def _add_stream_subcommand(subparsers):
        """Add stream subcommand for streaming pipelines."""
        stream_parser = subparsers.add_parser(
            "stream",
            help="Manage streaming pipelines",
            description="Manage real-time streaming pipelines",
        )

        # Create stream sub-subcommands
        stream_subparsers = stream_parser.add_subparsers(
            dest="stream_command", help="Streaming commands", required=True
        )

        # stream run
        run_parser = stream_subparsers.add_parser("run", help="Run streaming pipeline")
        run_parser.add_argument("--config", "-c", required=True, help=HELP_CONFIG_FILE)
        run_parser.add_argument("--pipeline", "-p", required=True, help="Pipeline name to execute")
        run_parser.add_argument(
            "--mode",
            "-m",
            default="async",
            choices=["sync", "async"],
            help="Execution mode for streaming pipelines",
        )
        run_parser.add_argument("--model-version", help="Model version for ML pipelines")
        run_parser.add_argument("--hyperparams", help="Hyperparameters as JSON string")

        # stream status
        status_parser = stream_subparsers.add_parser(
            "status", help="Check streaming pipeline status"
        )
        status_parser.add_argument("--config", "-c", required=True, help=HELP_CONFIG_FILE)
        status_parser.add_argument("--execution-id", "-e", help="Specific execution ID to check")
        status_parser.add_argument(
            "--format",
            "-f",
            default="table",
            choices=["table", "json"],
            help="Output format",
        )

        # stream stop
        stop_parser = stream_subparsers.add_parser("stop", help="Stop streaming pipeline")
        stop_parser.add_argument("--config", "-c", required=True, help=HELP_CONFIG_FILE)
        stop_parser.add_argument("--execution-id", "-e", required=True, help="Execution ID to stop")
        stop_parser.add_argument("--timeout", "-t", type=int, default=60, help=HELP_TIMEOUT_SECONDS)

    @staticmethod
    def _add_template_subcommand(subparsers):
        """Add template subcommand for project generation."""
        template_parser = subparsers.add_parser(
            "template",
            help="Generate project templates",
            description="Generate Tauro project templates and boilerplate code",
        )

        template_parser.add_argument("--template", help="Template type to generate")
        template_parser.add_argument("--project-name", help="Project name for template")
        template_parser.add_argument("--output-path", help="Output path for generated files")
        template_parser.add_argument(
            "--format",
            choices=["yaml", "json", "dsl"],
            default="yaml",
            help="Config format for generated template",
        )
        template_parser.add_argument(
            "--sandbox-developers",
            nargs="*",
            help="List of developer names for sandbox environments",
        )
        template_parser.add_argument(
            "--no-sample-code",
            action="store_true",
            help="Do not include sample code in generated template",
        )
        template_parser.add_argument(
            "--list-templates", action="store_true", help="List available templates"
        )

    @staticmethod
    def _add_config_subcommand(subparsers):
        """Add config subcommand for configuration management."""
        config_parser = subparsers.add_parser(
            "config",
            help="Manage configuration",
            description="Manage Tauro configuration and discovery",
        )

        # Create config sub-subcommands
        config_subparsers = config_parser.add_subparsers(
            dest="config_command", help="Configuration commands", required=True
        )

        # config list-configs
        config_subparsers.add_parser("list-configs", help="List discovered configs")

        # config list-pipelines
        list_pipelines_parser = config_subparsers.add_parser(
            "list-pipelines", help="List available pipelines"
        )
        list_pipelines_parser.add_argument("--env", help="Environment to use for listing")

        # config pipeline-info
        pipeline_info_parser = config_subparsers.add_parser(
            "pipeline-info", help="Show pipeline information"
        )
        pipeline_info_parser.add_argument("--pipeline", required=True, help=HELP_PIPELINE_NAME)
        pipeline_info_parser.add_argument("--env", help="Environment to use")

        # config clear-cache
        config_subparsers.add_parser("clear-cache", help="Clear configuration cache")

        # Global config options
        config_parser.add_argument("--base-path", help=HELP_BASE_PATH)
        config_parser.add_argument("--layer-name", help=HELP_LAYER_NAME)
        config_parser.add_argument("--use-case", dest="use_case_name", help=HELP_USE_CASE)
        config_parser.add_argument(
            "--config-type",
            choices=["yaml", "json", "dsl"],
            help=HELP_CONFIG_TYPE,
        )
        config_parser.add_argument(
            "--interactive", action="store_true", help="Interactive config selection"
        )


class UnifiedCLI:
    """Unified CLI application class that handles all Tauro operations."""

    def __init__(self):
        self.config: Optional[CLIConfig] = None
        self.config_manager: Optional[ConfigManager] = None

    def parse_arguments(
        self,
        args: Optional[List[str]] = None,
        parsed_args: Optional[argparse.Namespace] = None,
    ) -> CLIConfig:
        """Parse command line arguments into configuration object."""
        if parsed_args is None:
            parser = UnifiedArgumentParser.create()
            parsed = parser.parse_args(args)
        else:
            parsed = parsed_args

        base_path = Path(parsed.base_path) if getattr(parsed, "base_path", None) else None
        log_file = Path(parsed.log_file) if getattr(parsed, "log_file", None) else None
        output_path = (
            Path(parsed.output_path)
            if hasattr(parsed, "output_path") and parsed.output_path
            else None
        )

        try:
            start_date = (
                parse_iso_date(parsed.start_date) if getattr(parsed, "start_date", None) else None
            )
        except Exception:
            start_date = parsed.start_date
        try:
            end_date = (
                parse_iso_date(parsed.end_date) if getattr(parsed, "end_date", None) else None
            )
        except Exception:
            end_date = parsed.end_date

        return CLIConfig(
            env=getattr(parsed, "env", ""),
            pipeline=getattr(parsed, "pipeline", ""),
            node=getattr(parsed, "node", None),
            start_date=start_date,
            end_date=end_date,
            base_path=base_path,
            layer_name=getattr(parsed, "layer_name", None),
            use_case_name=getattr(parsed, "use_case_name", None),
            config_type=getattr(parsed, "config_type", None),
            interactive=getattr(parsed, "interactive", False),
            list_configs=getattr(parsed, "list_configs", False),
            list_pipelines=getattr(parsed, "list_pipelines", False),
            pipeline_info=getattr(parsed, "pipeline_info", None),
            clear_cache=getattr(parsed, "clear_cache", False),
            log_level=getattr(parsed, "log_level", "INFO"),
            log_file=log_file,
            validate_only=getattr(parsed, "validate_only", False),
            dry_run=getattr(parsed, "dry_run", False),
            verbose=getattr(parsed, "verbose", False),
            quiet=getattr(parsed, "quiet", False),
            streaming=getattr(parsed, "streaming", False),
            streaming_command=getattr(parsed, "streaming_command", None),
            streaming_config=getattr(parsed, "streaming_config", None),
            streaming_pipeline=getattr(parsed, "streaming_pipeline", None),
            execution_id=getattr(parsed, "execution_id", None),
            streaming_mode=getattr(parsed, "streaming_mode", "async"),
            model_version=getattr(parsed, "model_version", None),
            hyperparams=getattr(parsed, "hyperparams", None),
            output_path=output_path,
        )

    def run(self, args: Optional[List[str]] = None) -> int:
        """Main entry point for unified CLI execution."""
        try:
            parsed_args = self._parse_and_setup_logging(args)
            return self._dispatch_subcommand(parsed_args)

        except TauroError as e:
            logger.error(f"Tauro error: {e}")
            if self.config and self.config.verbose:
                logger.debug(traceback.format_exc())
            return e.exit_code.value

        except KeyboardInterrupt:
            logger.warning("Execution interrupted by user")
            return ExitCode.GENERAL_ERROR.value

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if (self.config and self.config.verbose) or (
                hasattr(parsed_args, "verbose") and parsed_args.verbose
            ):
                logger.debug(traceback.format_exc())
            return ExitCode.GENERAL_ERROR.value

        finally:
            if self.config_manager:
                try:
                    self.config_manager.restore_original_directory()
                except Exception:
                    pass
            ConfigCache.clear()

    def _parse_and_setup_logging(self, args: Optional[List[str]]) -> argparse.Namespace:
        """Parse arguments and setup logging."""
        parser = UnifiedArgumentParser.create()
        parsed_args = parser.parse_args(args)

        # If only --version is requested, skip logger setup to keep output clean
        if getattr(parsed_args, "version", False) and not getattr(parsed_args, "subcommand", None):
            return parsed_args

        # Setup logging based on subcommand
        log_level = getattr(parsed_args, "log_level", "INFO")
        log_file = getattr(parsed_args, "log_file", None)
        verbose = getattr(parsed_args, "verbose", False)
        quiet = getattr(parsed_args, "quiet", False)

        LoggerManager.setup(
            level=log_level,
            log_file=log_file,
            verbose=verbose,
            quiet=quiet,
        )
        return parsed_args

    def _dispatch_subcommand(self, parsed_args: argparse.Namespace) -> int:
        """Dispatch to the appropriate subcommand handler with argument validation."""
        # Handle global --version before subcommands
        if getattr(parsed_args, "version", False) and not getattr(parsed_args, "subcommand", None):
            self._print_signature()
            return ExitCode.SUCCESS.value

        subcommand = parsed_args.subcommand

        try:
            if subcommand == "run":
                validate_run_arguments(parsed_args)
                return self._handle_run_command(parsed_args)
            elif subcommand == "stream":
                return self._handle_stream_command(parsed_args)
            elif subcommand == "template":
                validate_template_arguments(parsed_args)
                return self._handle_template_command(parsed_args)
            elif subcommand == "config":
                validate_config_arguments(parsed_args)
                return self._handle_config_command(parsed_args)
            else:
                logger.error(f"Unknown subcommand: {subcommand}")
                return ExitCode.GENERAL_ERROR.value
        except ValidationError as e:
            logger.error(f"Invalid arguments: {e}")
            return ExitCode.VALIDATION_ERROR.value

    def _handle_run_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle direct pipeline execution (legacy run command)."""
        # Normalize and validate dates
        try:
            if getattr(parsed_args, "start_date", None):
                parsed_args.start_date = parse_iso_date(parsed_args.start_date)
            if getattr(parsed_args, "end_date", None):
                parsed_args.end_date = parse_iso_date(parsed_args.end_date)
            validate_date_range(parsed_args.start_date, parsed_args.end_date)
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"Error validating dates: {e}")

        self.config = self.parse_arguments(parsed_args=parsed_args)

        # Validate configuration
        if not self.config.env:
            raise ValidationError("--env required for pipeline execution")
        if not self.config.pipeline:
            raise ValidationError("--pipeline required for pipeline execution")

        logger.info("Starting Tauro pipeline execution")

        # Print elegant header
        try:
            from core.cli.rich_logger import RichLoggerManager
            from rich.rule import Rule

            console = RichLoggerManager.get_console()
            console.print()
            from core.cli.rich_logger import print_process_separator

            print_process_separator(
                "execution", "PIPELINE EXECUTION", f"Environment: {self.config.env}", console
            )
            console.print()
        except Exception:
            pass

        logger.info(f"Environment: {self.config.env.upper()}")
        logger.info(f"Pipeline: {self.config.pipeline}")

        self._init_config_manager()
        context_init = ContextInitializer(self.config_manager)

        if self.config.validate_only:
            return self._handle_validate_only(context_init)
        return self._execute_pipeline(context_init)

    def _handle_stream_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle streaming pipeline commands with argument validation."""
        stream_cmd = parsed_args.stream_command

        try:
            # Validate stream subcommand arguments
            if stream_cmd == "run":
                validate_stream_run_arguments(parsed_args)
            elif stream_cmd == "status":
                validate_stream_status_arguments(parsed_args)
            elif stream_cmd == "stop":
                validate_stream_stop_arguments(parsed_args)
            else:
                logger.error(f"Unknown stream command: {stream_cmd}")
                return ExitCode.GENERAL_ERROR.value

            handlers = {
                "run": lambda: run_cli_impl(
                    config=getattr(parsed_args, "config", None),
                    pipeline=parsed_args.pipeline,
                    mode=getattr(parsed_args, "mode", "async"),
                    model_version=getattr(parsed_args, "model_version", None),
                    hyperparams=getattr(parsed_args, "hyperparams", None),
                ),
                "status": lambda: status_cli_impl(
                    config=getattr(parsed_args, "config", None),
                    execution_id=getattr(parsed_args, "execution_id", None),
                    format=getattr(parsed_args, "format", "table"),
                ),
                "stop": lambda: stop_cli_impl(
                    config=getattr(parsed_args, "config", None),
                    execution_id=parsed_args.execution_id,
                    timeout=getattr(parsed_args, "timeout", 60),
                ),
            }

            handler = handlers.get(stream_cmd)
            if not handler:
                logger.error(f"Unknown stream command: {stream_cmd}")
                return ExitCode.GENERAL_ERROR.value

            return handler()

        except ValidationError as e:
            logger.error(f"Stream command validation failed: {e}")
            return ExitCode.VALIDATION_ERROR.value

    def _handle_template_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle template generation commands."""
        return handle_template_command(parsed_args)

    def _handle_config_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle configuration management commands."""
        config_cmd = parsed_args.config_command

        if config_cmd == "list-configs":
            discovery = ConfigDiscovery(getattr(parsed_args, "base_path", None))
            discovery.list_all()
            return ExitCode.SUCCESS.value

        # Initialize config manager for other commands
        self._init_config_manager_for_config(parsed_args)

        if config_cmd == "list-pipelines":
            return self._handle_config_list_pipelines(parsed_args)
        elif config_cmd == "pipeline-info":
            return self._handle_config_pipeline_info(parsed_args)
        elif config_cmd == "clear-cache":
            ConfigCache.clear()
            logger.info("Configuration cache cleared")
            return ExitCode.SUCCESS.value
        else:
            logger.error(f"Unknown config command: {config_cmd}")
            return ExitCode.GENERAL_ERROR.value

    def _init_config_manager(self):
        """Initialize configuration manager."""
        if not self.config_manager:
            self.config_manager = ConfigManager(
                base_path=self.config.base_path,
                layer_name=self.config.layer_name,
                use_case=self.config.use_case_name,
                config_type=self.config.config_type,
                interactive=self.config.interactive,
            )
            self.config_manager.change_to_config_directory()

    def _init_config_manager_for_config(self, parsed_args: argparse.Namespace):
        """Initialize config manager for config commands."""
        if not self.config_manager:
            self.config_manager = ConfigManager(
                base_path=getattr(parsed_args, "base_path", None),
                layer_name=getattr(parsed_args, "layer_name", None),
                use_case=getattr(parsed_args, "use_case_name", None),
                config_type=getattr(parsed_args, "config_type", None),
                interactive=getattr(parsed_args, "interactive", False),
            )
            self.config_manager.change_to_config_directory()

    def _handle_config_list_pipelines(self, parsed_args: argparse.Namespace) -> int:
        """Handle config list-pipelines command."""
        try:
            env = getattr(parsed_args, "env", "dev")
            context_init = ContextInitializer(self.config_manager)
            context = context_init.initialize(env)
            executor = PipelineExecutor(context, self.config_manager.get_config_directory())
            pipelines = executor.list_pipelines()

            if pipelines:
                logger.info("Available pipelines:")
                for pipeline in sorted(pipelines):
                    logger.info(f"  - {pipeline}")
            else:
                logger.warning("No pipelines found")

            return ExitCode.SUCCESS.value
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            return ExitCode.EXECUTION_ERROR.value

    def _handle_config_pipeline_info(self, parsed_args: argparse.Namespace) -> int:
        """Handle config pipeline-info command."""
        try:
            env = getattr(parsed_args, "env", "dev")
            context_init = ContextInitializer(self.config_manager)
            context = context_init.initialize(env)
            executor = PipelineExecutor(context, self.config_manager.get_config_directory())
            info = executor.get_pipeline_info(parsed_args.pipeline)

            logger.info(f"Pipeline: {parsed_args.pipeline}")
            logger.info(f"  Exists: {info['exists']}")
            logger.info(f"  Description: {info['description']}")
            if info["nodes"]:
                logger.info(f"  Nodes: {', '.join(info['nodes'])}")
            else:
                logger.info("  Nodes: None found")

            return ExitCode.SUCCESS.value
        except Exception as e:
            logger.error(f"Failed to get pipeline info: {e}")
            return ExitCode.EXECUTION_ERROR.value

    def _handle_validate_only(self, context_init: ContextInitializer) -> int:
        """Handle validation-only mode."""
        logger.info("Validating configuration...")
        context = context_init.initialize(self.config.env)
        logger.success("Configuration validation successful")

        executor = PipelineExecutor(context, self.config_manager.get_config_directory())
        summary = executor.get_execution_summary()

        logger.info("Execution Summary:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")

        return ExitCode.SUCCESS.value

    def _execute_pipeline(self, context_init: ContextInitializer) -> int:
        """Execute the specified pipeline."""
        context = context_init.initialize(self.config.env)

        executor = PipelineExecutor(context, self.config_manager.get_config_directory())

        if not executor.validate_pipeline(self.config.pipeline):
            available = executor.list_pipelines()
            if available:
                logger.error(f"Pipeline '{self.config.pipeline}' not found")
                logger.info(f"Available: {', '.join(available)}")
            else:
                logger.warning("Could not validate pipeline existence")

        if self.config.node and not executor.validate_node(self.config.pipeline, self.config.node):
            logger.warning(
                f"Node '{self.config.node}' may not exist in pipeline '{self.config.pipeline}'"
            )

        executor.execute(
            pipeline_name=self.config.pipeline,
            node_name=self.config.node,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            dry_run=self.config.dry_run,
        )

        # Print elegant completion
        try:
            from core.cli.rich_logger import RichLoggerManager
            from rich.rule import Rule
            from rich.panel import Panel
            from rich.text import Text
            from rich import box

            console = RichLoggerManager.get_console()
            console.print()
            from core.cli.rich_logger import print_process_separator

            print_process_separator(
                "success", "EXECUTION COMPLETED", f"Pipeline: {self.config.pipeline}", console
            )
            console.print()
        except Exception:
            pass

        logger.success("Tauro pipeline execution completed successfully")
        return ExitCode.SUCCESS.value


def main() -> int:
    """Main entry point for unified Tauro CLI application."""
    cli = UnifiedCLI()
    return cli.run()


def _get_version() -> str:
    """Return the Tauro version string."""
    return "0.1.4"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _center(text: str, width: int = 64) -> str:
    return text.center(width)


def _ansi(color: str) -> str:
    colors = {
        "cyan": "\033[96m",
        "magenta": "\033[95m",
        "yellow": "\033[93m",
        "green": "\033[92m",
        "reset": "\033[0m",
    }
    return colors.get(color, colors["reset"])


def _signature_lines() -> List[str]:
    subtitle = "Data Pipeline Framework"
    version = f"v{_get_version()}"
    art = [
        "╔════════════════════════════════╗",
        "║  ══════════ TAURO ══════════   ║",
        "╚════════════════════════════════╝",
    ]

    lines = []
    lines.append(_center(_ansi("cyan") + art[0] + _ansi("reset"), 64))
    for line in art[1:]:
        lines.append(_center(_ansi("cyan") + line + _ansi("reset"), 64))
    lines.append("")
    lines.append(_center(_ansi("yellow") + subtitle + _ansi("reset"), 64))
    lines.append(_center(_ansi("green") + version + _ansi("reset"), 64))
    lines.append("")
    lines.append("")
    return lines


def print_signature() -> None:
    for line in _signature_lines():
        print(line)


# Attach to UnifiedCLI for convenience
setattr(UnifiedCLI, "_print_signature", staticmethod(print_signature))


if __name__ == "__main__":
    sys.exit(main())
