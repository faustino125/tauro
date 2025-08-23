import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union

import click  # type: ignore
from loguru import logger  # type: ignore

from tauro.cli.core import ExitCode, ValidationError
from tauro.config.contexts import Context, ContextFactory


def _load_context_from_dsl(config_path: str) -> Context:
    """Load the base context from a DSL/Python module and build a full Context."""
    base_ctx = Context.from_dsl(config_path)
    return ContextFactory.create_context(base_ctx)


@click.group()
def streaming():
    """Streaming pipeline management commands."""
    pass


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
    """Run a streaming pipeline."""
    try:
        parsed_hyperparams = None
        if hyperparams:
            try:
                parsed_hyperparams = json.loads(hyperparams)
            except json.JSONDecodeError as e:
                click.echo(f"Error parsing hyperparameters: {e}", err=True)
                sys.exit(ExitCode.VALIDATION_ERROR.value)

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

        if result:
            click.echo(f"Streaming pipeline started with execution_id: {result}")
            if mode == "sync":
                click.echo("Pipeline completed.")
            else:
                click.echo(
                    "Pipeline running in background. Use 'tauro streaming status' to monitor."
                )
        else:
            click.echo("Batch pipeline completed successfully.")

    except ValidationError as e:
        click.echo(str(e), err=True)
        sys.exit(ExitCode.VALIDATION_ERROR.value)
    except Exception as e:
        click.echo(f"Error running pipeline: {e}", err=True)
        logger.exception("Pipeline execution failed")
        sys.exit(ExitCode.EXECUTION_ERROR.value)


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
    """Check status of streaming pipelines."""
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
                sys.exit(ExitCode.VALIDATION_ERROR.value)

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

    except Exception as e:
        click.echo(f"Error fetching status: {e}", err=True)
        logger.exception("Status retrieval failed")
        sys.exit(ExitCode.GENERAL_ERROR.value)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--execution-id", "-e", required=True, help="Execution ID to stop")
@click.option("--timeout", "-t", default=60, help="Timeout in seconds")
def stop(config: str, execution_id: str, timeout: int):
    """Stop a streaming pipeline gracefully."""
    try:
        context = _load_context_from_dsl(config)

        from tauro.exec.executor import PipelineExecutor

        executor = PipelineExecutor(context)

        stopped = executor.stop_streaming_pipeline(execution_id, timeout=timeout)
        if stopped:
            click.echo(f"Pipeline '{execution_id}' stopped successfully.")
        else:
            click.echo(
                f"Failed to stop pipeline '{execution_id}' within {timeout}s.", err=True
            )
            sys.exit(ExitCode.EXECUTION_ERROR.value)

    except Exception as e:
        click.echo(f"Error stopping pipeline: {e}", err=True)
        logger.exception("Stop pipeline failed")
        sys.exit(ExitCode.EXECUTION_ERROR.value)


def _list_all_pipelines_status(executor: Any) -> List[Dict[str, Any]]:
    """Best-effort retrieval of all streaming pipelines' status.

    Tries several likely method names on the executor, normalizes the result to a list
    of dicts. Falls back to iterating running IDs if available.
    """
    method_candidates = [
        "list_streaming_pipelines_status",
        "get_streaming_pipelines_status",
        "get_all_streaming_pipelines_status",
        "list_pipelines_status",
        "list_status",
    ]

    for name in method_candidates:
        if hasattr(executor, name):
            try:
                res = getattr(executor, name)()
            except TypeError:
                try:
                    res = getattr(executor, name)(None)
                except Exception:
                    continue
            except Exception:
                continue

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
            continue

    try:
        if hasattr(executor, "get_running_execution_ids") and hasattr(
            executor, "get_streaming_pipeline_status"
        ):
            ids = executor.get_running_execution_ids() or []
            out: List[Dict[str, Any]] = []
            for eid in ids:
                try:
                    st = executor.get_streaming_pipeline_status(eid)
                    if isinstance(st, dict):
                        out.append(st)
                except Exception:
                    continue
            return out
    except Exception:
        pass

    return []


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

    # Optional nodes detail
    nodes: Optional[Sequence[Dict[str, Any]]] = None
    for key in ("nodes", "node_status", "nodes_status"):
        if isinstance(status.get(key), (list, tuple)):
            nodes = status[key]
            break

    if nodes:
        headers = ["Node", "State", "Last Update", "Message"]
        rows: List[List[str]] = []
        for node in nodes:
            node_name = node.get("name") or node.get("node") or "-"
            node_state = node.get("state") or node.get("status") or "-"
            node_updated = _fmt_ts(node.get("last_update") or node.get("updated_at"))
            message = node.get("message") or node.get("error") or ""
            rows.append(
                [str(node_name), str(node_state), str(node_updated), str(message)]
            )

        _print_table(headers, rows)


def _display_multiple_pipelines_status_table(
    status_list: Union[List[Dict[str, Any]], Dict[str, Any]]
) -> None:
    """Render multiple pipeline statuses in a compact table."""
    pipelines: List[Dict[str, Any]] = []
    if isinstance(status_list, dict):
        for key in ("pipelines", "items", "data"):
            if isinstance(status_list.get(key), list):
                pipelines = [r for r in status_list[key] if isinstance(r, dict)]
                break
        if not pipelines and status_list:
            if all(isinstance(v, dict) for v in status_list.values()):
                pipelines = list(status_list.values())
    elif isinstance(status_list, list):
        pipelines = [r for r in status_list if isinstance(r, dict)]

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
    rows: List[List[str]] = []

    for p in pipelines:
        execution_id = p.get("execution_id") or p.get("id") or "-"
        name = p.get("pipeline_name") or p.get("name") or "-"
        state = p.get("state") or p.get("status") or "-"
        start_time = _fmt_ts(p.get("start_time") or p.get("started_at"))
        last_update = _fmt_ts(p.get("last_update") or p.get("updated_at"))
        uptime = _fmt_seconds(p.get("uptime_seconds") or p.get("uptime"))
        rows.append(
            [str(execution_id), str(name), str(state), start_time, last_update, uptime]
        )

    _print_table(headers, rows)


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
