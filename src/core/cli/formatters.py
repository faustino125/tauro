"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List
from loguru import logger

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.tree import Tree
    from rich import box
    from rich.layout import Layout
    from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn, TimeElapsedColumn
    from rich.align import Align
    from rich.rule import Rule
    from rich.columns import Columns

    _USE_RICH = True
except ImportError:
    _USE_RICH = False
    logger.debug("Rich not available, using basic formatting")


class PipelineStatusFormatter:
    """Professional formatter for pipeline status with elegant visual design."""

    console = Console(force_terminal=True) if _USE_RICH else None

    @staticmethod
    def format_single_pipeline(status_info: Dict[str, Any]) -> None:
        """Format and print status for a single pipeline."""
        if not _USE_RICH or not PipelineStatusFormatter.console:
            logger.info(f"Pipeline Status: {status_info}")
            return

        # Extract information
        pipeline_name = status_info.get("pipeline_name", "Unknown")
        status = status_info.get("status", "Unknown")
        nodes = status_info.get("nodes", [])
        execution_time = status_info.get("execution_time", 0.0)
        environment = status_info.get("environment", "N/A")

        # Create rich table
        table = Table(
            title=f"[bold magenta]Pipeline:[/] [bold cyan]{pipeline_name}[/]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )

        table.add_column("Property", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")

        # Status with color
        status_color = {
            "completed": "green",
            "running": "yellow",
            "failed": "red",
            "pending": "blue",
        }.get(status.lower(), "white")

        table.add_row("Status", f"[bold {status_color}]{status}[/]")
        table.add_row("Environment", environment)
        table.add_row("Execution Time", f"{execution_time:.2f}s")
        table.add_row("Total Nodes", str(len(nodes)))

        if nodes:
            completed_nodes = sum(1 for n in nodes if n.get("status") == "completed")
            table.add_row("Completed Nodes", f"{completed_nodes}/{len(nodes)}")

        PipelineStatusFormatter.console.print(table)

        # Show nodes if available
        if nodes and len(nodes) > 0:
            nodes_table = Table(
                title="[bold]Nodes Execution Details[/]",
                box=box.SIMPLE,
                show_header=True,
                header_style="bold yellow",
            )

            nodes_table.add_column("Node", style="cyan")
            nodes_table.add_column("Status", justify="center")
            nodes_table.add_column("Duration", justify="right", style="green")

            for node in nodes:
                node_name = node.get("name", "Unknown")
                node_status = node.get("status", "unknown")
                node_duration = node.get("duration", 0.0)

                status_emoji = {
                    "completed": "✅",
                    "running": "🔄",
                    "failed": "❌",
                    "pending": "⏳",
                }.get(node_status.lower(), "❓")

                nodes_table.add_row(
                    node_name, f"{status_emoji} {node_status}", f"{node_duration:.2f}s"
                )

            PipelineStatusFormatter.console.print(nodes_table)

    @staticmethod
    def _build_pipeline_row(idx: int, status_info: Dict[str, Any]) -> tuple:
        """Build a single row for the pipelines table."""
        pipeline_name = status_info.get("pipeline_name", "Unknown")
        status = status_info.get("status", "Unknown")
        nodes = status_info.get("nodes", [])
        execution_time = status_info.get("execution_time", 0.0)
        environment = status_info.get("environment", "N/A")

        # Status with emoji and color
        status_map = {
            "completed": "[bright_green]✅ Completed[/]",
            "running": "[bright_yellow]🔄 Running[/]",
            "failed": "[bright_red]❌ Failed[/]",
            "pending": "[bright_blue]⏳ Pending[/]",
        }
        status_display = status_map.get(status.lower(), f"[white]❓ {status}[/]")

        # Progress calculation
        progress = PipelineStatusFormatter._calculate_progress(nodes)

        # Duration formatting
        duration_display = PipelineStatusFormatter._format_duration(execution_time)

        return (str(idx), pipeline_name, environment, status_display, progress, duration_display)

    @staticmethod
    def _calculate_progress(nodes: List[Dict[str, Any]]) -> str:
        """Calculate progress display for nodes."""
        if not nodes:
            return "[dim]-[/]"

        completed = sum(1 for n in nodes if n.get("status") == "completed")
        failed = sum(1 for n in nodes if n.get("status") == "failed")
        total = len(nodes)

        if failed > 0:
            return f"[bright_red]{completed}[/]/[dim]{total}[/] [bright_red]({failed}❌)[/]"
        return f"[bright_green]{completed}[/]/[dim]{total}[/]"

    @staticmethod
    def _format_duration(execution_time: float) -> str:
        """Format execution time for display."""
        if execution_time >= 60:
            return f"{execution_time/60:.1f}m"
        return f"{execution_time:.1f}s"

    @staticmethod
    def format_multiple_pipelines(status_list: List[Dict[str, Any]]) -> None:
        """Format and print status for multiple pipelines with elegant summary."""
        if not _USE_RICH or not PipelineStatusFormatter.console:
            for status in status_list:
                logger.info(f"Pipeline Status: {status}")
            return

        console = PipelineStatusFormatter.console

        # Print header
        console.print()
        console.print(
            Rule("[bold bright_cyan]🚀 Pipelines Status Dashboard[/]", style="bright_magenta")
        )
        console.print()

        # Create summary table
        table = Table(
            box=box.DOUBLE_EDGE,
            show_header=True,
            header_style="bold bright_white on bright_blue",
            border_style="bright_cyan",
            row_styles=["dim", ""],
            padding=(0, 1),
        )

        table.add_column("№", style="dim", width=4, justify="right")
        table.add_column("Pipeline", style="bright_cyan", no_wrap=True, min_width=20)
        table.add_column("Environment", style="bright_yellow", justify="center", width=12)
        table.add_column("Status", justify="center", width=18)
        table.add_column("Progress", justify="center", width=12)
        table.add_column("Duration", justify="right", style="bright_green", width=10)

        for idx, status_info in enumerate(status_list, 1):
            row = PipelineStatusFormatter._build_pipeline_row(idx, status_info)
            table.add_row(*row)

        console.print(table)

        # Summary statistics
        total_pipelines = len(status_list)
        completed_count = sum(1 for s in status_list if s.get("status", "").lower() == "completed")
        failed_count = sum(1 for s in status_list if s.get("status", "").lower() == "failed")
        running_count = sum(1 for s in status_list if s.get("status", "").lower() == "running")

        summary_table = Table.grid(padding=(0, 2))
        summary_table.add_column(style="dim")
        summary_table.add_column(style="bold bright_white")

        summary_table.add_row("Total:", str(total_pipelines))
        summary_table.add_row("Completed:", f"[bright_green]{completed_count}[/]")
        if running_count > 0:
            summary_table.add_row("Running:", f"[bright_yellow]{running_count}[/]")
        if failed_count > 0:
            summary_table.add_row("Failed:", f"[bright_red]{failed_count}[/]")

        console.print()
        console.print(
            Panel(
                summary_table,
                title="[bold]Summary[/]",
                border_style="dim",
                box=box.ROUNDED,
                padding=(0, 2),
            )
        )
        console.print()

    @staticmethod
    def _add_nodes_to_tree(tree: Tree, pipeline_config: Dict[str, Any]) -> None:
        """Add nodes section to the tree."""
        if "nodes" not in pipeline_config:
            return

        nodes_branch = tree.add("[bold bright_yellow]⚡ Nodes[/]")
        for node in pipeline_config.get("nodes", []):
            if isinstance(node, dict):
                node_name = node.get("name", "unnamed")
                node_type = node.get("type", "unknown")
                nodes_branch.add(f"[bright_cyan]▸ {node_name}[/] [dim]({node_type})[/]")
            else:
                nodes_branch.add(f"[bright_cyan]▸ {node}[/]")

    @staticmethod
    def _add_dependencies_to_tree(tree: Tree, pipeline_config: Dict[str, Any]) -> None:
        """Add dependencies section to the tree."""
        deps = pipeline_config.get("dependencies", [])
        if not deps:
            return

        deps_branch = tree.add("[bold bright_magenta]🔗 Dependencies[/]")
        for dep in deps:
            deps_branch.add(f"[bright_blue]• {dep}[/]")

    @staticmethod
    def _add_config_to_tree(tree: Tree, pipeline_config: Dict[str, Any]) -> None:
        """Add configuration section to the tree."""
        config = pipeline_config.get("config", {})
        if not config:
            return

        config_branch = tree.add("[bold bright_green]⚙️ Configuration[/]")
        for key, value in config.items():
            config_branch.add(f"[bright_green]{key}[/]: [white]{value}[/]")

    @staticmethod
    def format_pipeline_tree(pipeline_config: Dict[str, Any]) -> None:
        """Format pipeline configuration as an elegant tree structure."""
        if not _USE_RICH or not PipelineStatusFormatter.console:
            logger.info(f"Pipeline Config: {pipeline_config}")
            return

        console = PipelineStatusFormatter.console
        pipeline_name = pipeline_config.get("name", "Unknown Pipeline")

        tree = Tree(f"[bold bright_cyan]📊 {pipeline_name}[/]")

        PipelineStatusFormatter._add_nodes_to_tree(tree, pipeline_config)
        PipelineStatusFormatter._add_dependencies_to_tree(tree, pipeline_config)
        PipelineStatusFormatter._add_config_to_tree(tree, pipeline_config)

        panel = Panel(
            tree,
            border_style="bright_cyan",
            box=box.ROUNDED,
            padding=(1, 2),
        )
        console.print(panel)

    @staticmethod
    def print_success(message: str, details: str = "") -> None:
        """Print success message with elegant panel."""
        if _USE_RICH and PipelineStatusFormatter.console:
            content = Text()
            content.append("✅ Success\n\n", style="bold bright_green")
            content.append(message, style="bright_white")
            if details:
                content.append(f"\n\n[dim]{details}[/]")

            panel = Panel(
                content,
                border_style="bright_green",
                box=box.DOUBLE,
                padding=(1, 2),
            )
            PipelineStatusFormatter.console.print(panel)
        else:
            logger.success(message)

    @staticmethod
    def print_error(message: str, details: str = "", suggestion: str = "") -> None:
        """Print error message with elegant panel and suggestions."""
        if _USE_RICH and PipelineStatusFormatter.console:
            content = Text()
            content.append("❌ Error\n\n", style="bold bright_red")
            content.append(message, style="bright_white")

            if details:
                content.append(f"\n\n[dim]Details:[/]\n{details}")

            if suggestion:
                content.append(f"\n\n[bright_yellow]💡 Suggestion:[/]\n{suggestion}")

            panel = Panel(
                content,
                border_style="bright_red",
                box=box.HEAVY,
                padding=(1, 2),
            )
            PipelineStatusFormatter.console.print(panel)
        else:
            logger.error(message)

    @staticmethod
    def print_warning(message: str, action: str = "") -> None:
        """Print warning message with elegant panel."""
        if _USE_RICH and PipelineStatusFormatter.console:
            content = Text()
            content.append("⚠️ Warning\n\n", style="bold bright_yellow")
            content.append(message, style="bright_white")

            if action:
                content.append(f"\n\n[bright_cyan]Action required:[/]\n{action}")

            panel = Panel(
                content,
                border_style="bright_yellow",
                box=box.ROUNDED,
                padding=(1, 2),
            )
            PipelineStatusFormatter.console.print(panel)
        else:
            logger.warning(message)

    @staticmethod
    def create_progress_display():
        """Create a professional progress display."""
        if not _USE_RICH:
            return None

        return Progress(
            SpinnerColumn("dots"),
            TextColumn("[bold bright_cyan]{task.description}"),
            BarColumn(
                complete_style="bright_green",
                finished_style="bright_green",
                pulse_style="bright_yellow",
            ),
            TextColumn("[bright_white]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=PipelineStatusFormatter.console,
        )
