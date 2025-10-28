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

from tauro.cli.config import ConfigDiscovery, ConfigManager
from tauro.cli.core import (
    CLIConfig,
    ConfigCache,
    ExitCode,
    LoggerManager,
    TauroError,
    ValidationError,
    parse_iso_date,
    validate_date_range,
)
from tauro.cli.execution import ContextInitializer, PipelineExecutor
from tauro.cli.template import handle_template_command


# Constants for help text to avoid duplication
HELP_BASE_PATH = "Base path for config discovery"
HELP_LAYER_NAME = "Layer name for config discovery"
HELP_USE_CASE = "Use case name"
HELP_CONFIG_TYPE = "Preferred configuration type"
HELP_PIPELINE_NAME = "Pipeline name"
HELP_PIPELINE_NAME_TO_EXECUTE = "Pipeline name to execute"
HELP_TIMEOUT_SECONDS = "Timeout in seconds"
HELP_CONFIG_FILE = "Path to configuration file"


# Streaming CLI functions
def _load_context_from_dsl(config_path: Optional[Union[str, Path]]) -> Any:
    """Load the base context from a DSL/Python module and build a full Context."""
    if config_path is None:
        raise ValidationError("Configuration path must be provided")
    # Normalize to str
    config_path_str = str(config_path)
    from tauro.core.config.contexts import Context, ContextFactory

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

        from tauro.core.exec.executor import PipelineExecutor

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

        from tauro.core.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        if execution_id:
            status_info = executor.get_streaming_pipeline_status(execution_id)

            if not status_info:
                logger.error(f"Pipeline with execution_id '{execution_id}' not found")
                return ExitCode.VALIDATION_ERROR.value

            if format == "json":
                print(json.dumps(status_info, indent=2, default=str))
            else:
                _display_pipeline_status_table(status_info)
        else:
            status_list = _list_all_pipelines_status(executor)

            if format == "json":
                print(json.dumps(status_list, indent=2, default=str))
            else:
                _display_multiple_pipelines_status_table(status_list)

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

        from tauro.core.exec.executor import PipelineExecutor

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
    """Best-effort retrieval of all streaming pipelines' status."""
    method_candidates = [
        "list_streaming_pipelines_status",
        "get_streaming_pipelines_status",
        "get_all_streaming_pipelines_status",
        "list_pipelines_status",
        "list_status",
    ]

    result = _try_method_candidates(executor, method_candidates)
    if result is not None:
        return result

    return _fallback_running_ids(executor)


def _try_method_candidates(executor: Any, candidates: List[str]) -> Optional[List[Dict[str, Any]]]:
    for name in candidates:
        if not hasattr(executor, name):
            continue
        method = getattr(executor, name)
        res = _call_method_with_optional_none(method)
        normalized = _normalize_status_result(res)
        if normalized is not None:
            return normalized
    return None


def _call_method_with_optional_none(method) -> Any:
    try:
        return method()
    except TypeError:
        try:
            return method(None)
        except Exception:
            return None
    except Exception:
        return None


def _normalize_status_result(res: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(res, list):
        return [r for r in res if isinstance(r, dict)]
    if isinstance(res, dict):
        if "pipelines" in res and isinstance(res["pipelines"], list):
            return [r for r in res["pipelines"] if isinstance(r, dict)]
        if all(isinstance(v, dict) for v in res.values()):
            return list(res.values())
        return [res]
    if res is None:
        return []
    return None


def _fallback_running_ids(executor: Any) -> List[Dict[str, Any]]:
    if not (
        hasattr(executor, "get_running_execution_ids")
        and hasattr(executor, "get_streaming_pipeline_status")
    ):
        return []
    try:
        ids = executor.get_running_execution_ids() or []
        return [
            _safe_get_status(executor, eid)
            for eid in ids
            if _safe_get_status(executor, eid) is not None
        ]
    except Exception:
        return []


def _safe_get_status(executor: Any, eid: Any) -> Optional[Dict[str, Any]]:
    try:
        st = executor.get_streaming_pipeline_status(eid)
        if isinstance(st, dict):
            return st
    except Exception:
        return None
    return None


def _fmt_ts(ts: Optional[Union[str, int, float]]) -> str:
    """Format timestamp-like value into an ISO string, if possible."""
    if ts is None or ts == "":
        return "-"
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        except Exception:
            return str(ts)
    if isinstance(ts, str):
        return ts
    return str(ts)


def _fmt_seconds(total_seconds: Optional[Union[int, float]]) -> str:
    """Format seconds into a human-readable duration."""
    if total_seconds is None:
        return "-"
    try:
        s = int(total_seconds)
    except Exception:
        return str(total_seconds)

    days, rem = divmod(s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def _display_pipeline_status_table(status: Dict[str, Any]) -> None:
    """Render a single pipeline status as a small table."""
    execution_id = status.get("execution_id") or status.get("id") or "-"
    name = status.get("pipeline_name") or status.get("name") or "-"
    state = status.get("state") or status.get("status") or "-"
    start_time = _fmt_ts(status.get("start_time") or status.get("started_at"))
    last_update = _fmt_ts(status.get("last_update") or status.get("updated_at"))
    uptime = _fmt_seconds(status.get("uptime_seconds") or status.get("uptime"))

    print("")
    print(f"Execution ID : {execution_id}")
    print(f"Pipeline     : {name}")
    print(f"State        : {state}")
    print(f"Start Time   : {start_time}")
    print(f"Last Update  : {last_update}")
    print(f"Uptime       : {uptime}")
    print("")

    _display_nodes_table_if_present(status)


def _display_nodes_table_if_present(status: Dict[str, Any]) -> None:
    """Helper to display nodes table if nodes info is present in status."""
    nodes: Optional[Sequence[Dict[str, Any]]] = None
    for key in ("nodes", "node_status", "nodes_status"):
        if isinstance(status.get(key), (list, tuple)):
            nodes = status[key]
            break

    if not nodes:
        return

    headers = ["Node", "State", "Last Update", "Message"]
    rows: List[List[str]] = []
    for node in nodes:
        node_name = node.get("name") or node.get("node") or "-"
        node_state = node.get("state") or node.get("status") or "-"
        node_updated = _fmt_ts(node.get("last_update") or node.get("updated_at"))
        message = node.get("message") or node.get("error") or ""
        rows.append([str(node_name), str(node_state), str(node_updated), str(message)])

    _print_table(headers, rows)


def _display_multiple_pipelines_status_table(
    status_list: Union[List[Dict[str, Any]], Dict[str, Any]]
) -> None:
    """Render multiple pipeline statuses in a compact table."""
    pipelines = _normalize_pipelines_list(status_list)

    if not pipelines:
        print("No streaming pipelines found.")
        return

    headers = [
        "Execution ID",
        "Pipeline",
        "State",
        "Start Time",
        "Last Update",
        "Uptime",
    ]
    rows = [_build_pipeline_row(p) for p in pipelines]

    _print_table(headers, rows)


def _normalize_pipelines_list(
    status_list: Union[List[Dict[str, Any]], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Extract a list of pipeline dicts from various possible input formats."""
    if isinstance(status_list, dict):
        for key in ("pipelines", "items", "data"):
            value = status_list.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
        if status_list and all(isinstance(v, dict) for v in status_list.values()):
            return list(status_list.values())
    elif isinstance(status_list, list):
        return [r for r in status_list if isinstance(r, dict)]
    return []


def _build_pipeline_row(p: Dict[str, Any]) -> List[str]:
    """Build a table row for a pipeline status dict."""
    execution_id = p.get("execution_id") or p.get("id") or "-"
    name = p.get("pipeline_name") or p.get("name") or "-"
    state = p.get("state") or p.get("status") or "-"
    start_time = _fmt_ts(p.get("start_time") or p.get("started_at"))
    last_update = _fmt_ts(p.get("last_update") or p.get("updated_at"))
    uptime = _fmt_seconds(p.get("uptime_seconds") or p.get("uptime"))
    return [str(execution_id), str(name), str(state), start_time, last_update, uptime]


def _print_table(headers: List[str], rows: List[List[str]]) -> None:
    """Print a simple table with dynamic column widths using plain text."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    fmt_parts = [f"{{:{w}}}" for w in widths]
    fmt = "  ".join(fmt_parts)

    sep = "  ".join("-" * w for w in widths)

    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(c) for c in row]))


class UnifiedArgumentParser:
    """Unified argument parser for all Tauro CLI commands."""

    @staticmethod
    def create() -> argparse.ArgumentParser:
        """Create configured argument parser with subcommands."""
        parser = argparse.ArgumentParser(
            prog="tauro",
            description="Tauro - Unified Scalable Data Pipeline Execution Framework",
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
        parser.add_argument("--version", action="version", version="Tauro CLI 2.0.0")

        # Create subparsers
        subparsers = parser.add_subparsers(
            dest="subcommand", help="Available subcommands", required=True
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
        """Dispatch to the appropriate subcommand handler."""
        subcommand = parsed_args.subcommand

        if subcommand == "run":
            return self._handle_run_command(parsed_args)
        elif subcommand == "stream":
            return self._handle_stream_command(parsed_args)
        elif subcommand == "template":
            return self._handle_template_command(parsed_args)
        elif subcommand == "config":
            return self._handle_config_command(parsed_args)
        else:
            logger.error(f"Unknown subcommand: {subcommand}")
            return ExitCode.GENERAL_ERROR.value

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
        logger.info(f"Environment: {self.config.env.upper()}")
        logger.info(f"Pipeline: {self.config.pipeline}")

        self._init_config_manager()
        context_init = ContextInitializer(self.config_manager)

        if self.config.validate_only:
            return self._handle_validate_only(context_init)
        return self._execute_pipeline(context_init)

    def _handle_stream_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle streaming pipeline commands."""
        stream_cmd = parsed_args.stream_command

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

        logger.success("Tauro pipeline execution completed successfully")
        return ExitCode.SUCCESS.value


def main() -> int:
    """Main entry point for unified Tauro CLI application."""
    cli = UnifiedCLI()
    return cli.run()


if __name__ == "__main__":
    sys.exit(main())
