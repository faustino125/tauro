"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union
from pathlib import Path

import click  # type: ignore
from loguru import logger  # type: ignore

from tauro.cli.core import ExitCode, ValidationError
from tauro.config.contexts import Context, ContextFactory


def _load_context_from_dsl(config_path: Optional[Union[str, Path]]) -> Context:
    """Load the base context from a DSL/Python module and build a full Context.
    Accepts str or Path or None (None will raise further down)."""
    if config_path is None:
        raise ValidationError("Configuration path must be provided")
    # Normalize to str
    config_path_str = str(config_path)
    base_ctx = Context.from_dsl(config_path_str)
    return ContextFactory.create_context(base_ctx)


def _in_click_context() -> bool:
    """Detect if command is invoked under a click context (standalone CLI)."""
    try:
        return click.get_current_context(silent=True) is not None
    except Exception:
        return False


@click.group()
def streaming():
    """Streaming pipeline management commands."""
    pass


def _run_impl(
    config: str,
    pipeline: str,
    mode: str,
    model_version: Optional[str],
    hyperparams: Optional[str],
) -> int:
    """Core implementation for 'run' that returns an exit code."""
    try:
        parsed_hyperparams = None
        if hyperparams:
            try:
                parsed_hyperparams = json.loads(hyperparams)
            except json.JSONDecodeError as e:
                click.echo(f"Error parsing hyperparameters: {e}", err=True)
                return ExitCode.VALIDATION_ERROR.value

        context = _load_context_from_dsl(config)

        from tauro.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        click.echo(f"Starting streaming pipeline '{pipeline}' in {mode} mode...")

        result = executor.run_pipeline(
            pipeline_name=pipeline,
            model_version=model_version,
            hyperparams=parsed_hyperparams,
            execution_mode=mode,
        )

        # The executor may return an execution id or truthy value
        if result:
            click.echo(f"Streaming pipeline started with execution_id: {result}")
            if mode == "sync":
                click.echo("Pipeline completed.")
            else:
                click.echo(
                    "Pipeline running in background. Use 'tauro streaming status' to monitor."
                )
        else:
            # If executor signals completion without id, print neutral message
            click.echo("Pipeline execution completed successfully.")

        return ExitCode.SUCCESS.value

    except ValidationError as e:
        click.echo(str(e), err=True)
        logger.exception("Validation error during streaming run")
        return ExitCode.VALIDATION_ERROR.value

    except Exception as e:
        click.echo(f"Error running pipeline: {e}", err=True)
        logger.exception("Pipeline execution failed")
        return ExitCode.EXECUTION_ERROR.value


# Public programmatic wrapper to be used by argparse-based CLI
def run_cli_impl(
    config: Optional[Union[str, Path]],
    pipeline: str,
    mode: str = "async",
    model_version: Optional[str] = None,
    hyperparams: Optional[str] = None,
) -> int:
    """Programmatic wrapper that normalizes types and calls _run_impl."""
    config_str = str(config) if config is not None else ""
    return _run_impl(config_str, pipeline, mode, model_version, hyperparams)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--pipeline", "-p", required=True, help="Pipeline name to execute")
@click.option(
    "--mode",
    "-m",
    default="async",
    type=click.Choice(["sync", "async"]),
    help="Execution mode for streaming pipelines",
)
@click.option("--model-version", help="Model version for ML pipelines")
@click.option("--hyperparams", help="Hyperparameters as JSON string")
def run(
    config: str,
    pipeline: str,
    mode: str,
    model_version: Optional[str],
    hyperparams: Optional[str],
):
    """Run a streaming pipeline (click entry point)."""
    code = _run_impl(config, pipeline, mode, model_version, hyperparams)
    if _in_click_context():
        sys.exit(code)
    return code


def _status_impl(config: str, execution_id: Optional[str], format: str) -> int:
    """Core implementation for 'status' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from tauro.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        if execution_id:
            status_info = executor.get_streaming_pipeline_status(execution_id)

            if not status_info:
                click.echo(
                    f"Pipeline with execution_id '{execution_id}' not found", err=True
                )
                return ExitCode.VALIDATION_ERROR.value

            if format == "json":
                click.echo(json.dumps(status_info, indent=2, default=str))
            else:
                _display_pipeline_status_table(status_info)
        else:
            status_list = _list_all_pipelines_status(executor)

            if format == "json":
                click.echo(json.dumps(status_list, indent=2, default=str))
            else:
                _display_multiple_pipelines_status_table(status_list)

        return ExitCode.SUCCESS.value

    except Exception as e:
        click.echo(f"Error fetching status: {e}", err=True)
        logger.exception("Status retrieval failed")
        return ExitCode.GENERAL_ERROR.value


# Public wrapper for status (programmatic usage)
def status_cli_impl(
    config: Optional[Union[str, Path]],
    execution_id: Optional[str] = None,
    format: str = "table",
) -> int:
    config_str = str(config) if config is not None else ""
    return _status_impl(config_str, execution_id, format)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--execution-id", "-e", help="Specific execution ID to check")
@click.option(
    "--format",
    "-f",
    default="table",
    type=click.Choice(["table", "json"]),
    help="Output format",
)
def status(config: str, execution_id: Optional[str], format: str):
    """Check status of streaming pipelines (click entry point)."""
    code = _status_impl(config, execution_id, format)
    if _in_click_context():
        sys.exit(code)
    return code


def _stop_impl(config: str, execution_id: str, timeout: int) -> int:
    """Core implementation for 'stop' that returns an exit code."""
    try:
        context = _load_context_from_dsl(config)

        from tauro.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        stopped = executor.stop_streaming_pipeline(execution_id, timeout)
        if stopped:
            click.echo(f"Pipeline '{execution_id}' stopped successfully.")
            return ExitCode.SUCCESS.value
        else:
            click.echo(
                f"Failed to stop pipeline '{execution_id}' within {timeout}s.", err=True
            )
            return ExitCode.EXECUTION_ERROR.value

    except Exception as e:
        click.echo(f"Error stopping pipeline: {e}", err=True)
        logger.exception("Stop pipeline failed")
        return ExitCode.EXECUTION_ERROR.value


# Public wrapper for stop (programmatic usage)
def stop_cli_impl(
    config: Optional[Union[str, Path]],
    execution_id: str,
    timeout: int = 60,
) -> int:
    config_str = str(config) if config is not None else ""
    return _stop_impl(config_str, execution_id, timeout)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--execution-id", "-e", required=True, help="Execution ID to stop")
@click.option("--timeout", "-t", default=60, help="Timeout in seconds")
def stop(config: str, execution_id: str, timeout: int):
    """Stop a streaming pipeline gracefully (click entry point)."""
    code = _stop_impl(config, execution_id, timeout)
    if _in_click_context():
        sys.exit(code)
    return code


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

    click.echo("")
    click.echo(f"Execution ID : {execution_id}")
    click.echo(f"Pipeline     : {name}")
    click.echo(f"State        : {state}")
    click.echo(f"Start Time   : {start_time}")
    click.echo(f"Last Update  : {last_update}")
    click.echo(f"Uptime       : {uptime}")
    click.echo("")

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
        click.echo("No streaming pipelines found.")
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

    click.echo(fmt.format(*headers))
    click.echo(sep)
    for row in rows:
        click.echo(fmt.format(*[str(c) for c in row]))


streaming_commands = streaming
