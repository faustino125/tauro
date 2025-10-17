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
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.runner import OrchestratorRunner
from tauro.orchest.scheduler import SchedulerService
from tauro.orchest.models import ScheduleKind, RunState


# Constants for help text to avoid duplication
HELP_BASE_PATH = "Base path for config discovery"
HELP_LAYER_NAME = "Layer name for config discovery"
HELP_USE_CASE = "Use case name"
HELP_CONFIG_TYPE = "Preferred configuration type"
HELP_PIPELINE_NAME = "Pipeline name"
HELP_PIPELINE_NAME_TO_EXECUTE = "Pipeline name to execute"
HELP_TIMEOUT_SECONDS = "Timeout in seconds"
HELP_CONFIG_FILE = "Path to configuration file"


# Orchestration CLI constants and functions
RUN_ID_REQUIRED = "--run-id is required"

# Detect if croniter is available for cron expression validation
try:
    from croniter import croniter  # type: ignore

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False


def _init_context_and_store(args: argparse.Namespace):
    """Initialize context and store for orchestrate commands."""
    cm = ConfigManager(
        base_path=Path(args.base_path) if args.base_path else None,
        layer_name=getattr(args, "layer_name", None),
        use_case=getattr(args, "use_case", None),
        config_type=getattr(args, "config_type", None),
        interactive=getattr(args, "interactive", False),
    )
    cm.change_to_config_directory()
    ctx = ContextInitializer(cm).initialize(args.env)
    store = OrchestratorStore()
    return cm, ctx, store


def _handle_run_create(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-create command."""
    if not args.pipeline:
        ap.error("--pipeline is required")
    runner = OrchestratorRunner(ctx, store)
    pr = runner.create_run(args.pipeline, params={})
    logger.info(f"Run created: {pr.id} (pipeline={pr.pipeline_id})")
    return ExitCode.SUCCESS.value


def _handle_run_start(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-start command."""
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    try:
        state = runner.start_run(
            args.run_id,
            retries=args.retries,
            retry_delay_sec=args.retry_delay_sec,
            concurrency=args.max_concurrency,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as e:
        logger.exception(f"Error starting run {args.run_id}: {e}")
        return ExitCode.EXECUTION_ERROR.value

    logger.info(f"Run {args.run_id} finished with state: {state}")
    if state == RunState.SUCCESS:
        return ExitCode.SUCCESS.value
    return ExitCode.EXECUTION_ERROR.value


def _handle_run_status(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-status command."""
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    pr = runner.get_run(args.run_id)
    if not pr:
        logger.error("Run not found")
        return ExitCode.CONFIGURATION_ERROR.value
    logger.info(
        f"Run: {pr.id} state={pr.state} created_at={pr.created_at} "
        f"started_at={pr.started_at} finished_at={pr.finished_at} error={pr.error}"
    )
    tasks = runner.list_task_runs(pr.id)
    if tasks:
        logger.info("Tasks:")
        for t in tasks:
            logger.info(f" - {t.task_id}: {t.state} tries={t.try_number} err={t.error}")
    return ExitCode.SUCCESS.value


def _handle_run_list(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-list command."""
    runner = OrchestratorRunner(ctx, store)
    runs = runner.list_runs(pipeline_id=args.pipeline)
    if not runs:
        logger.info("No runs")
        return ExitCode.SUCCESS.value
    for r in runs:
        logger.info(
            f"- {r.id} {r.pipeline_id} {r.state} created_at={r.created_at} "
            f"finished_at={r.finished_at} error={r.error}"
        )
    return ExitCode.SUCCESS.value


def _handle_run_tasks(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-tasks command."""
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    tasks = runner.list_task_runs(args.run_id)
    if not tasks:
        logger.info("No task runs")
        return ExitCode.SUCCESS.value
    for t in tasks:
        logger.info(
            f"- {t.task_id} {t.state} tries={t.try_number} started={t.started_at} "
            f"finished={t.finished_at} error={t.error}"
        )
    return ExitCode.SUCCESS.value


def _handle_run_cancel(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle run-cancel command."""
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    success = runner.cancel_run(args.run_id)
    if not success:
        return ExitCode.EXECUTION_ERROR.value
    return ExitCode.SUCCESS.value


def _handle_schedule_add(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle schedule-add command."""
    if not args.pipeline:
        ap.error("--pipeline is required")

    # Validar expresión cron si es necesario
    if args.schedule_kind == "CRON" and HAS_CRONITER:
        try:
            croniter(args.expression)
        except Exception as e:
            logger.error(f"Invalid cron expression: {e}")
            return ExitCode.CONFIGURATION_ERROR.value

    sched = store.create_schedule(
        pipeline_id=args.pipeline,
        kind=ScheduleKind(args.schedule_kind),
        expression=args.expression,
        max_concurrency=args.max_concurrency,
        retry_policy={"retries": args.retries, "delay": args.retry_delay_sec},
        timeout_seconds=args.timeout_seconds,
    )
    logger.info(f"Schedule created: {sched.id} ({sched.kind} '{sched.expression}')")
    return ExitCode.SUCCESS.value


def _handle_schedule_list(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle schedule-list command."""
    scheds = store.list_schedules(pipeline_id=args.pipeline)
    if not scheds:
        logger.info("No schedules")
        return ExitCode.SUCCESS.value
    for s in scheds:
        logger.info(
            f"- {s.id} pipeline={s.pipeline_id} {s.kind} expr='{s.expression}' "
            f"enabled={s.enabled} next={s.next_run_at} max_conc={s.max_concurrency}"
        )
    return ExitCode.SUCCESS.value


def _handle_schedule_start(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle schedule-start command."""
    service = SchedulerService(ctx, store)
    service.start()
    logger.info("Press Ctrl+C to stop scheduler...")
    try:
        while True:
            import time as _t

            _t.sleep(10)
            metrics = service.get_metrics()
            logger.info(
                f"Scheduler metrics: cycles={metrics['cycles']}, "
                f"runs_created={metrics['runs_created']}, "
                f"errors={metrics['errors']}, "
                f"avg_cycle_time={metrics['avg_cycle_time']:.3f}s"
            )
    except KeyboardInterrupt:
        service.stop()
    return ExitCode.SUCCESS.value


def _handle_schedule_stop(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle schedule-stop command."""
    logger.info("Use Ctrl+C to stop a foreground scheduler instance")
    return ExitCode.SUCCESS.value


def _handle_backfill(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle backfill command."""
    if not args.pipeline or not args.count:
        ap.error("--pipeline and --count are required")
    service = SchedulerService(ctx, store)
    service.backfill(args.pipeline, args.count)
    logger.info(f"Backfill enqueued: {args.count} runs")
    return ExitCode.SUCCESS.value


def _handle_db_stats(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle db-stats command."""
    stats = store.get_database_stats()
    logger.info("Database statistics:")
    logger.info(f"Pipeline runs by state: {stats['pipeline_runs_by_state']}")
    logger.info(f"Task runs by state: {stats['task_runs_by_state']}")
    logger.info(f"Schedules by status: {stats['schedules_by_status']}")
    logger.info(f"Database size: {stats['database_size_bytes'] / (1024*1024):.2f} MB")
    return ExitCode.SUCCESS.value


def _handle_db_cleanup(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle db-cleanup command."""
    result = store.cleanup_old_data(max_days=args.days, batch_size=1000)
    logger.info(f"Cleanup completed: {result}")
    return ExitCode.SUCCESS.value


def _handle_db_vacuum(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    """Handle db-vacuum command."""
    store.vacuum()
    logger.info("Database vacuum completed")
    return ExitCode.SUCCESS.value


# Streaming CLI functions
def _load_context_from_dsl(config_path: Optional[Union[str, Path]]) -> Any:
    """Load the base context from a DSL/Python module and build a full Context."""
    if config_path is None:
        raise ValidationError("Configuration path must be provided")
    # Normalize to str
    config_path_str = str(config_path)
    from tauro.config.contexts import Context, ContextFactory

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

        from tauro.exec.executor import PipelineExecutor

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

        logger.info(
            f"Streaming pipeline '{pipeline}' started with execution ID: {execution_id}"
        )
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


def _status_streaming_impl(
    config: str, execution_id: Optional[str], format: str
) -> int:
    """Core implementation for 'status' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from tauro.exec.executor import PipelineExecutor

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

        from tauro.exec.executor import PipelineExecutor

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


def _try_method_candidates(
    executor: Any, candidates: List[str]
) -> Optional[List[Dict[str, Any]]]:
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

            # Orchestration management
            tauro orchestrate run-create --pipeline mi_pipeline
            tauro orchestrate schedule-add --pipeline mi_pipeline --expression "3600"

            # Streaming pipelines
            tauro stream run --config config/streaming.py --pipeline real_time_processing
            tauro stream status --config config/streaming.py

            # Template generation
            tauro template --template medallion_basic --project-name my_project

            # Configuration management
            tauro config list-pipelines --env dev
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
        UnifiedArgumentParser._add_orchestrate_subcommand(subparsers)
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
        run_parser.add_argument(
            "--quiet", action="store_true", help="Reduce output (ERROR only)"
        )

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
    def _add_orchestrate_subcommand(subparsers):
        """Add orchestrate subcommand for orchestration management."""
        orchestrate_parser = subparsers.add_parser(
            "orchestrate",
            help="Manage pipeline orchestration",
            description="Manage scheduled pipeline runs and orchestration",
        )

        # Create orchestrate sub-subcommands
        orchestrate_subparsers = orchestrate_parser.add_subparsers(
            dest="orchestrate_command", help="Orchestration commands", required=True
        )

        # Run management commands
        UnifiedArgumentParser._add_orchestrate_run_commands(orchestrate_subparsers)

        # Schedule management commands
        UnifiedArgumentParser._add_orchestrate_schedule_commands(orchestrate_subparsers)

        # Database management commands
        UnifiedArgumentParser._add_orchestrate_db_commands(orchestrate_subparsers)

        # Global orchestrate options
        orchestrate_parser.add_argument(
            "--env",
            required=True,
            help="Execution environment (base, dev, sandbox, prod, or sandbox_<developer>)",
        )
        orchestrate_parser.add_argument("--base-path", help=HELP_BASE_PATH)
        orchestrate_parser.add_argument("--layer-name", help=HELP_LAYER_NAME)
        orchestrate_parser.add_argument(
            "--use-case", dest="use_case_name", help=HELP_USE_CASE
        )
        orchestrate_parser.add_argument(
            "--config-type",
            choices=["yaml", "json", "dsl"],
            help=HELP_CONFIG_TYPE,
        )

    @staticmethod
    def _add_orchestrate_run_commands(subparsers):
        """Add run management commands."""
        # run-create
        create_parser = subparsers.add_parser(
            "run-create", help="Create a new pipeline run"
        )
        create_parser.add_argument("--pipeline", required=True, help=HELP_PIPELINE_NAME)

        # run-start
        start_parser = subparsers.add_parser("run-start", help="Start a pipeline run")
        start_parser.add_argument("--run-id", required=True, help="Run ID to start")
        start_parser.add_argument(
            "--retries", type=int, default=0, help="Number of retries"
        )
        start_parser.add_argument(
            "--retry-delay-sec", type=int, default=0, help="Retry delay in seconds"
        )
        start_parser.add_argument(
            "--max-concurrency", type=int, default=1, help="Maximum concurrency"
        )
        start_parser.add_argument(
            "--timeout-seconds", type=int, help=HELP_TIMEOUT_SECONDS
        )

        # run-status
        status_parser = subparsers.add_parser("run-status", help="Get run status")
        status_parser.add_argument("--run-id", required=True, help="Run ID to check")

        # run-list
        list_parser = subparsers.add_parser("run-list", help="List pipeline runs")
        list_parser.add_argument("--pipeline", help=HELP_PIPELINE_NAME)
        list_parser.add_argument("--state", help="Filter by run state")
        list_parser.add_argument(
            "--limit", type=int, default=50, help="Maximum number of results"
        )

        # run-tasks
        tasks_parser = subparsers.add_parser("run-tasks", help="List tasks for a run")
        tasks_parser.add_argument("--run-id", required=True, help="Run ID")

        # run-cancel
        cancel_parser = subparsers.add_parser(
            "run-cancel", help="Cancel a running pipeline"
        )
        cancel_parser.add_argument("--run-id", required=True, help="Run ID to cancel")

    @staticmethod
    def _add_orchestrate_schedule_commands(subparsers):
        """Add schedule management commands."""
        # schedule-add
        add_parser = subparsers.add_parser("schedule-add", help="Add a new schedule")
        add_parser.add_argument("--pipeline", required=True, help=HELP_PIPELINE_NAME)
        add_parser.add_argument(
            "--schedule-kind",
            choices=["INTERVAL", "CRON"],
            default="INTERVAL",
            help="Schedule type",
        )
        add_parser.add_argument(
            "--expression", required=True, help="Interval seconds or cron expression"
        )
        add_parser.add_argument(
            "--max-concurrency", type=int, default=1, help="Maximum concurrency"
        )
        add_parser.add_argument(
            "--retries", type=int, default=0, help="Number of retries"
        )
        add_parser.add_argument(
            "--retry-delay-sec", type=int, default=0, help="Retry delay"
        )
        add_parser.add_argument(
            "--timeout-seconds", type=int, help=HELP_TIMEOUT_SECONDS
        )

        # schedule-list
        list_sched_parser = subparsers.add_parser(
            "schedule-list", help="List schedules"
        )
        list_sched_parser.add_argument("--pipeline", help=HELP_PIPELINE_NAME)

        # schedule-start
        start_sched_parser = subparsers.add_parser(
            "schedule-start", help="Start scheduler"
        )
        start_sched_parser.add_argument(
            "--poll-interval",
            type=float,
            default=1.0,
            help="Scheduler poll interval in seconds",
        )

        # schedule-stop
        subparsers.add_parser("schedule-stop", help="Stop scheduler")

        # backfill
        backfill_parser = subparsers.add_parser(
            "backfill", help="Backfill historical runs"
        )
        backfill_parser.add_argument(
            "--pipeline", required=True, help=HELP_PIPELINE_NAME
        )
        backfill_parser.add_argument(
            "--count", type=int, required=True, help="Number of runs to create"
        )

    @staticmethod
    def _add_orchestrate_db_commands(subparsers):
        """Add database management commands."""
        # db-stats
        subparsers.add_parser("db-stats", help="Show database statistics")

        # db-cleanup
        cleanup_parser = subparsers.add_parser("db-cleanup", help="Clean up old data")
        cleanup_parser.add_argument("--days", type=int, default=30, help="Days to keep")

        # db-vacuum
        subparsers.add_parser("db-vacuum", help="Optimize database")

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
        run_parser.add_argument(
            "--pipeline", "-p", required=True, help="Pipeline name to execute"
        )
        run_parser.add_argument(
            "--mode",
            "-m",
            default="async",
            choices=["sync", "async"],
            help="Execution mode for streaming pipelines",
        )
        run_parser.add_argument(
            "--model-version", help="Model version for ML pipelines"
        )
        run_parser.add_argument("--hyperparams", help="Hyperparameters as JSON string")

        # stream status
        status_parser = stream_subparsers.add_parser(
            "status", help="Check streaming pipeline status"
        )
        status_parser.add_argument(
            "--config", "-c", required=True, help=HELP_CONFIG_FILE
        )
        status_parser.add_argument(
            "--execution-id", "-e", help="Specific execution ID to check"
        )
        status_parser.add_argument(
            "--format",
            "-f",
            default="table",
            choices=["table", "json"],
            help="Output format",
        )

        # stream stop
        stop_parser = stream_subparsers.add_parser(
            "stop", help="Stop streaming pipeline"
        )
        stop_parser.add_argument("--config", "-c", required=True, help=HELP_CONFIG_FILE)
        stop_parser.add_argument(
            "--execution-id", "-e", required=True, help="Execution ID to stop"
        )
        stop_parser.add_argument(
            "--timeout", "-t", type=int, default=60, help=HELP_TIMEOUT_SECONDS
        )

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
        template_parser.add_argument(
            "--output-path", help="Output path for generated files"
        )
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
        list_pipelines_parser.add_argument(
            "--env", help="Environment to use for listing"
        )

        # config pipeline-info
        pipeline_info_parser = config_subparsers.add_parser(
            "pipeline-info", help="Show pipeline information"
        )
        pipeline_info_parser.add_argument(
            "--pipeline", required=True, help=HELP_PIPELINE_NAME
        )
        pipeline_info_parser.add_argument("--env", help="Environment to use")

        # config clear-cache
        config_subparsers.add_parser("clear-cache", help="Clear configuration cache")

        # Global config options
        config_parser.add_argument("--base-path", help=HELP_BASE_PATH)
        config_parser.add_argument("--layer-name", help=HELP_LAYER_NAME)
        config_parser.add_argument(
            "--use-case", dest="use_case_name", help=HELP_USE_CASE
        )
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

        base_path = (
            Path(parsed.base_path) if getattr(parsed, "base_path", None) else None
        )
        log_file = Path(parsed.log_file) if getattr(parsed, "log_file", None) else None
        output_path = (
            Path(parsed.output_path)
            if hasattr(parsed, "output_path") and parsed.output_path
            else None
        )

        try:
            start_date = (
                parse_iso_date(parsed.start_date)
                if getattr(parsed, "start_date", None)
                else None
            )
        except Exception:
            start_date = parsed.start_date
        try:
            end_date = (
                parse_iso_date(parsed.end_date)
                if getattr(parsed, "end_date", None)
                else None
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
        elif subcommand == "orchestrate":
            return self._handle_orchestrate_command(parsed_args)
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

    def _handle_orchestrate_command(self, parsed_args: argparse.Namespace) -> int:
        """Handle orchestration management commands."""
        orchestrate_cmd = parsed_args.orchestrate_command

        # Initialize config manager for orchestrate commands
        self._init_orchestrate_config_manager(parsed_args)

        # Dispatch to appropriate handler
        handlers = {
            "run-create": _handle_run_create,
            "run-start": _handle_run_start,
            "run-status": _handle_run_status,
            "run-list": _handle_run_list,
            "run-tasks": _handle_run_tasks,
            "run-cancel": _handle_run_cancel,
            "schedule-add": _handle_schedule_add,
            "schedule-list": _handle_schedule_list,
            "schedule-start": _handle_schedule_start,
            "schedule-stop": _handle_schedule_stop,
            "backfill": _handle_backfill,
            "db-stats": _handle_db_stats,
            "db-cleanup": _handle_db_cleanup,
            "db-vacuum": _handle_db_vacuum,
        }

        handler = handlers.get(orchestrate_cmd)
        if not handler:
            logger.error(f"Unknown orchestrate command: {orchestrate_cmd}")
            return ExitCode.GENERAL_ERROR.value

        # Create a mock parser for compatibility
        parser = argparse.ArgumentParser()
        return handler(parsed_args, parser, self.context, self.store)

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

    def _init_orchestrate_config_manager(self, parsed_args: argparse.Namespace):
        """Initialize config manager for orchestrate commands."""
        if not hasattr(self, "context") or not hasattr(self, "store"):
            self.config_manager = ConfigManager(
                base_path=getattr(parsed_args, "base_path", None),
                layer_name=getattr(parsed_args, "layer_name", None),
                use_case=getattr(parsed_args, "use_case_name", None),
                config_type=getattr(parsed_args, "config_type", None),
                interactive=False,
            )
            self.config_manager.change_to_config_directory()

            # Initialize context and store for orchestrate
            context_init = ContextInitializer(self.config_manager)
            self.context = context_init.initialize(parsed_args.env)

            from tauro.orchest import OrchestratorStore

            self.store = OrchestratorStore()

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
            executor = PipelineExecutor(
                context, self.config_manager.get_config_directory()
            )
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
            executor = PipelineExecutor(
                context, self.config_manager.get_config_directory()
            )
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

        if self.config.node and not executor.validate_node(
            self.config.pipeline, self.config.node
        ):
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
