import click  # type: ignore
import json
from typing import Optional

from loguru import logger  # type: ignore

from tauro.config.streaming_context import StreamingContext
from tauro.exec.executor import PipelineExecutor


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
        # Parse hyperparameters if provided
        parsed_hyperparams = None
        if hyperparams:
            try:
                parsed_hyperparams = json.loads(hyperparams)
            except json.JSONDecodeError as e:
                click.echo(f"Error parsing hyperparameters: {e}", err=True)
                return

        # Load context
        context = StreamingContext.from_python_dsl(config)

        # Create executor
        executor = PipelineExecutor(context)

        # Execute pipeline
        click.echo(f"Starting streaming pipeline '{pipeline}' in {mode} mode...")

        result = executor.run_pipeline(
            pipeline_name=pipeline,
            model_version=model_version,
            hyperparams=parsed_hyperparams,
            execution_mode=mode,
        )

        if result:  # Streaming pipeline returns execution_id
            click.echo(f"Streaming pipeline started with execution_id: {result}")

            if mode == "sync":
                click.echo("Pipeline completed.")
            else:
                click.echo(
                    "Pipeline running in background. Use 'tauro streaming status' to monitor."
                )
        else:  # Batch pipeline
            click.echo("Batch pipeline completed successfully.")

    except Exception as e:
        click.echo(f"Error running pipeline: {e}", err=True)
        logger.exception("Pipeline execution failed")


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
        context = StreamingContext.from_python_dsl(config)
        executor = PipelineExecutor(context)

        if execution_id:
            # Show specific pipeline status
            status_info = executor.get_streaming_pipeline_status(execution_id)

            if not status_info:
                click.echo(
                    f"Pipeline with execution_id '{execution_id}' not found", err=True
                )
                return

            if format == "json":
                click.echo(json.dumps(status_info, indent=2))
            else:
                _display_pipeline_status_table(status_info)
        else:
            # Show all running pipelines
            pipelines = executor.list_streaming_pipelines()

            if not pipelines:
                click.echo("No streaming pipelines currently running.")
                return

            if format == "json":
                click.echo(json.dumps(pipelines, indent=2))
            else:
                _display_pipelines_table(pipelines)

    except Exception as e:
        click.echo(f"Error checking status: {e}", err=True)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--execution-id", "-e", required=True, help="Execution ID to stop")
@click.option("--graceful/--force", default=True, help="Graceful or forced shutdown")
def stop(config: str, execution_id: str, graceful: bool):
    """Stop a running streaming pipeline."""
    try:
        context = StreamingContext.from_python_dsl(config)
        executor = PipelineExecutor(context)

        click.echo(
            f"Stopping pipeline '{execution_id}' ({'graceful' if graceful else 'forced'})..."
        )

        success = executor.stop_streaming_pipeline(execution_id, graceful)

        if success:
            click.echo(f"Pipeline '{execution_id}' stopped successfully.")
        else:
            click.echo(f"Failed to stop pipeline '{execution_id}'.", err=True)

    except Exception as e:
        click.echo(f"Error stopping pipeline: {e}", err=True)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
@click.option("--execution-id", "-e", required=True, help="Execution ID to monitor")
@click.option(
    "--format",
    "-f",
    default="table",
    type=click.Choice(["table", "json"]),
    help="Output format",
)
def metrics(config: str, execution_id: str, format: str):
    """Show metrics for a streaming pipeline."""
    try:
        context = StreamingContext.from_python_dsl(config)
        executor = PipelineExecutor(context)

        metrics_info = executor.get_streaming_pipeline_metrics(execution_id)

        if not metrics_info:
            click.echo(
                f"Pipeline with execution_id '{execution_id}' not found", err=True
            )
            return

        if format == "json":
            click.echo(json.dumps(metrics_info, indent=2))
        else:
            _display_metrics_table(metrics_info)

    except Exception as e:
        click.echo(f"Error getting metrics: {e}", err=True)


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
def validate(config: str):
    """Validate streaming pipeline configurations."""
    try:
        click.echo("Validating streaming configurations...")

        context = StreamingContext.from_python_dsl(config)

        # Get configuration summary
        summary = context.get_streaming_configuration_summary()

        click.echo("✅ Streaming configurations validated successfully!")
        click.echo("\n📊 Configuration Summary:")
        click.echo(f"  • Total pipelines: {summary['total_pipelines']}")
        click.echo(f"  • Streaming pipelines: {summary['streaming_pipelines']}")
        click.echo(f"  • Batch pipelines: {summary['batch_pipelines']}")
        click.echo(f"  • Hybrid pipelines: {summary['hybrid_pipelines']}")
        click.echo(f"  • Streaming nodes: {summary['streaming_nodes']}")

        if summary["streaming_sources"]:
            click.echo(f"\n📥 Streaming Sources:")
            for source, count in summary["streaming_sources"].items():
                click.echo(f"  • {source}: {count} nodes")

        if summary["streaming_sinks"]:
            click.echo(f"\n📤 Streaming Sinks:")
            for sink, count in summary["streaming_sinks"].items():
                click.echo(f"  • {sink}: {count} nodes")

        # Check for dependency issues
        dep_errors = context.validate_streaming_node_dependencies()
        if dep_errors:
            click.echo("\n⚠️  Dependency Warnings:")
            for error in dep_errors:
                click.echo(f"  • {error}")

    except Exception as e:
        click.echo(f"❌ Validation failed: {e}", err=True)
        logger.exception("Configuration validation failed")


@streaming.command()
@click.option("--config", "-c", required=True, help="Path to configuration file")
def list_pipelines(config: str):
    """List all available pipelines by type."""
    try:
        context = StreamingContext.from_python_dsl(config)

        streaming_pipelines = context.get_streaming_pipelines()
        batch_pipelines = context.get_batch_pipelines()
        hybrid_pipelines = context.get_hybrid_pipelines()

        click.echo("📋 Available Pipelines:\n")

        if streaming_pipelines:
            click.echo("🌊 Streaming Pipelines:")
            for name, config in streaming_pipelines.items():
                desc = config.get("description", "No description")
                click.echo(f"  • {name}: {desc}")

        if batch_pipelines:
            click.echo("\n📦 Batch Pipelines:")
            for name, config in batch_pipelines.items():
                desc = config.get("description", "No description")
                click.echo(f"  • {name}: {desc}")

        if hybrid_pipelines:
            click.echo("\n🔄 Hybrid Pipelines:")
            for name, config in hybrid_pipelines.items():
                desc = config.get("description", "No description")
                click.echo(f"  • {name}: {desc}")

    except Exception as e:
        click.echo(f"Error listing pipelines: {e}", err=True)


def _display_pipeline_status_table(status_info):
    """Display pipeline status in table format"""
    from prettytable import PrettyTable  # type: ignore

    table = PrettyTable()
    table.field_names = ["Property", "Value"]
    table.align = "l"
    for key, value in status_info.items():
        table.add_row([key, str(value)])
    click.echo(table)


def _display_pipelines_table(pipelines):
    """Display running pipelines in table format"""
    from prettytable import PrettyTable  # type: ignore

    table = PrettyTable()
    table.field_names = ["Execution ID", "Pipeline", "Status", "Start Time"]
    for pipeline in pipelines:
        table.add_row(
            [
                pipeline.get("execution_id", "N/A"),
                pipeline.get("pipeline", "N/A"),
                pipeline.get("status", "N/A"),
                pipeline.get("start_time", "N/A"),
            ]
        )
    click.echo(table)


def _display_metrics_table(metrics_info):
    """Display metrics in table format"""
    from prettytable import PrettyTable  # type: ignore

    table = PrettyTable()
    table.field_names = ["Metric", "Value"]
    for metric, value in metrics_info.items():
        table.add_row([metric, value])
    click.echo(table)


streaming_commands = {
    "run": run.callback,
    "status": status.callback,
    "stop": stop.callback,
    "metrics": metrics.callback,
    "validate": validate.callback,
    "list": list_pipelines.callback,
}
