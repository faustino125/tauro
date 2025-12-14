"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Rich-enhanced logging utilities for beautiful, professional terminal output.
"""
import sys
from typing import Optional, Any, Dict
from pathlib import Path
from datetime import datetime

from loguru import logger
from rich.console import Console
from rich.theme import Theme
from rich.logging import RichHandler
from rich.traceback import install as install_rich_traceback
from rich.panel import Panel
from rich.text import Text
from rich.rule import Rule
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich import box
from rich.layout import Layout
from rich.live import Live
from rich.align import Align
from rich.columns import Columns


# Style constants to avoid duplication
STYLE_BRIGHT_RED = "bright_red"
STYLE_BOLD_BRIGHT_RED = "bold bright_red"
STYLE_BOLD_BRIGHT_GREEN = "bold bright_green"
STYLE_BOLD_BRIGHT_YELLOW = "bold bright_yellow"
STYLE_BOLD_BRIGHT_CYAN = "bold bright_cyan"
STYLE_BOLD_BRIGHT_WHITE = "bold bright_white"
STYLE_DIM = "dim"
STYLE_DIM_WHITE = "dim white"

# Professional theme for Tauro with gradients and modern colors
TAURO_THEME = Theme(
    {
        "info": "bright_cyan",
        "warning": "bright_yellow",
        "error": STYLE_BOLD_BRIGHT_RED,
        "critical": "bold white on red",
        "success": STYLE_BOLD_BRIGHT_GREEN,
        "debug": "dim bright_black",
        "tauro": "bold magenta",
        "pipeline": "bold bright_blue",
        "node": "bold cyan",
        "metric": "bright_yellow",
        "highlight": STYLE_BOLD_BRIGHT_WHITE,
        "dim": STYLE_DIM_WHITE,
        "accent": "bright_magenta",
        # Grupos de colores temáticos para procesos
        "group.pipeline": "bold bright_blue",
        "group.data": STYLE_BOLD_BRIGHT_CYAN,
        "group.execution": "bold bright_magenta",
        "group.validation": STYLE_BOLD_BRIGHT_YELLOW,
        "group.schema": "bold cyan",
        "group.success": STYLE_BOLD_BRIGHT_GREEN,
        "group.error": STYLE_BOLD_BRIGHT_RED,
    }
)

# Estilos de separadores para diferentes grupos de procesos
PROCESS_GROUPS = {
    "configuration": {
        "emoji": "⚙️",
        "style": "bright_yellow",
        "color": "bright_yellow",
        "box_style": "double",
    },
    "pipeline_start": {
        "emoji": "🚀",
        "style": "bright_blue",
        "color": "bright_blue",
        "box_style": "double",
    },
    "data_loading": {
        "emoji": "📥",
        "style": "bright_cyan",
        "color": "bright_cyan",
        "box_style": "rounded",
    },
    "execution": {
        "emoji": "⚡",
        "style": "bright_magenta",
        "color": "bright_magenta",
        "box_style": "heavy",
    },
    "schema": {
        "emoji": "📊",
        "style": "cyan",
        "color": "cyan",
        "box_style": "rounded",
    },
    "saving": {
        "emoji": "💾",
        "style": "bright_green",
        "color": "bright_green",
        "box_style": "double",
    },
    "summary": {
        "emoji": "📋",
        "style": "bright_blue",
        "color": "bright_blue",
        "box_style": "double_edge",
    },
    "success": {
        "emoji": "✓",
        "style": "bright_green",
        "color": "bright_green",
        "box_style": "heavy",
    },
    "error": {
        "emoji": "✗",
        "style": "bright_red",
        "color": "bright_red",
        "box_style": "heavy",
    },
}


class RichLoggerManager:
    """
    Professional logger configuration using Rich for elegant terminal output.
    Integrates with loguru for structured logging with modern visual design.
    """

    console = Console(theme=TAURO_THEME, stderr=True, force_terminal=True)

    @classmethod
    def setup(
        cls,
        level: str = "INFO",
        log_file: Optional[str] = None,
        verbose: bool = False,
        quiet: bool = False,
        show_time: bool = True,
        show_path: bool = False,
        enable_rich_tracebacks: bool = True,
    ) -> None:
        """
        Configure application logging with professional Rich formatting.

        Args:
            level: Default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional path to log file for persistent logging
            verbose: Enable verbose (DEBUG) logging
            quiet: Show only ERROR and above
            show_time: Show timestamp in console output
            show_path: Show file path and line number in console output
            enable_rich_tracebacks: Enable rich traceback formatting
        """
        # Remove existing handlers
        logger.remove()

        # Determine console log level
        if quiet:
            console_level = "ERROR"
        elif verbose:
            console_level = "DEBUG"
        else:
            console_level = level.upper()

        # Install rich tracebacks for better error formatting
        if enable_rich_tracebacks:
            install_rich_traceback(
                console=cls.console,
                show_locals=verbose,
                width=140,
                extra_lines=3,
                theme="monokai",
                word_wrap=True,
                suppress=["loguru"],
            )

        # Configure Rich handler for console output
        rich_handler = RichHandler(
            console=cls.console,
            show_time=show_time,
            show_level=True,
            show_path=show_path,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=verbose,
            tracebacks_width=140,
            log_time_format="[%X]",
            omit_repeated_times=False,
        )

        # Simple format for rich handler - Rich maneja el formato visual
        if show_path:
            log_format = "{name}:{function}:{line} - {message}"
        else:
            log_format = "{message}"

        # Add Rich handler to loguru (sin colorize para evitar conflictos)
        logger.add(
            rich_handler,
            format=log_format,
            level=console_level,
            colorize=False,  # Rich maneja los colores
        )

        # Add file logging if specified
        if log_file:
            log_path = Path(log_file)
        else:
            log_path = Path("logs/tauro.log")

        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_path,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            rotation="10 MB",
            retention="7 days",
            level="DEBUG",
            compression="zip",
            enqueue=True,  # Thread-safe logging
        )

        # Log initialization
        logger.debug(f"Rich logger initialized (console_level={console_level})")

    @classmethod
    def get_console(cls) -> Console:
        """Get the Rich console instance for custom output."""
        return cls.console


# Professional logging functions with elegant visual design
def print_banner(text: str = "TAURO", subtitle: str = "") -> None:
    """Display a professional banner with gradient effect."""
    console = RichLoggerManager.get_console()

    banner_text = Text()
    banner_text.append(
        "╔═══════════════════════════════════════════════════════════════════╗\n",
        style="bright_magenta",
    )
    banner_text.append("║  ", style="bright_magenta")
    banner_text.append(text, style=STYLE_BOLD_BRIGHT_CYAN)
    banner_text.append("  " * (60 - len(text)) + "║\n", style="bright_magenta")

    if subtitle:
        banner_text.append("║  ", style="bright_magenta")
        banner_text.append(subtitle, style="dim white")
        banner_text.append("  " * (60 - len(subtitle)) + "║\n", style="bright_magenta")

    banner_text.append(
        "╚═══════════════════════════════════════════════════════════════════╝",
        style="bright_magenta",
    )

    console.print(Align.center(banner_text))


def print_section(title: str, emoji: str = "⚡") -> None:
    """Print an elegant section separator."""
    console = RichLoggerManager.get_console()
    console.print()
    console.print(Rule(f"[{STYLE_BOLD_BRIGHT_CYAN}]{emoji} {title}[/]", style="bright_magenta"))
    console.print()


def print_process_separator(
    process_type: str,
    title: str,
    subtitle: Optional[str] = None,
    console: Optional[Console] = None,
) -> None:
    """
    Print a simple text separator for process groups without borders.

    Args:
        process_type: Process type (pipeline_start, data_loading, execution, schema, saving, success, error)
        title: Main process title
        subtitle: Optional subtitle with additional information
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    config = PROCESS_GROUPS.get(process_type, PROCESS_GROUPS["execution"])
    emoji = config["emoji"]
    style = config["style"]

    # Create simple text line
    text = Text()
    text.append(f"{emoji} ", style=f"bold {style}")
    text.append(title, style=f"bold {style}")

    if subtitle:
        text.append("  •  ", style=STYLE_DIM_WHITE)
        text.append(subtitle, style=f"dim {style}")

    console.print(text)


def print_group_start(
    process_type: str,
    title: str,
    subtitle: Optional[str] = None,
    console: Optional[Console] = None,
) -> None:
    """
    Print the start of a process group with top border.

    Args:
        process_type: Process type
        title: Group title
        subtitle: Optional subtitle
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    config = PROCESS_GROUPS.get(process_type, PROCESS_GROUPS["execution"])
    emoji = config["emoji"]
    style = config["style"]

    # Create header text
    header = Text()
    header.append(f"{emoji} ", style=f"bold {style}")
    header.append(title, style=f"bold {style}")

    if subtitle:
        header.append(f"  •  {subtitle}", style=STYLE_DIM_WHITE)

    # Print top border with title inside
    console.print()
    console.print("╔" + "═" * (console.width - 2) + "╗", style=style)
    console.print("║" + " " * (console.width - 2) + "║", style=style)

    # Center the title
    title_str = header.plain
    padding = (console.width - len(title_str) - 2) // 2
    line = Text()
    line.append("║", style=style)
    line.append(" " * padding)
    line.append(header)
    line.append(" " * (console.width - len(title_str) - padding - 2))
    line.append("║", style=style)
    console.print(line)

    console.print("║" + " " * (console.width - 2) + "║", style=style)
    console.print("╠" + "═" * (console.width - 2) + "╣", style=style)
    console.print()


def print_group_end(
    process_type: str = "execution",
    console: Optional[Console] = None,
) -> None:
    """
    Print the end of a process group with bottom border.

    Args:
        process_type: Process type
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    config = PROCESS_GROUPS.get(process_type, PROCESS_GROUPS["execution"])
    style = config["style"]

    # Print bottom border
    console.print()
    console.print("╚" + "═" * (console.width - 2) + "╝", style=style)
    console.print()


def print_info_panel(
    title: str,
    items: Dict[str, Any],
    emoji: str = "ℹ️",
    border_color: str = "bright_cyan",
    console: Optional[Console] = None,
) -> None:
    """
    Print an elegant information panel with key-value pairs.

    Args:
        title: Panel title
        items: Dictionary of items to display
        emoji: Decorative emoji
        border_color: Panel border color
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    content = Text()
    content.append(f"{emoji} ", style=f"bold {border_color}")
    content.append(title, style=f"bold {border_color}")
    content.append("\n\n", style="")

    for key, value in items.items():
        content.append(f"{key}: ", style="dim")
        content.append(f"{value}\n", style=STYLE_BOLD_BRIGHT_WHITE)

    panel = Panel(
        content,
        border_style=border_color,
        box=box.ROUNDED,
        padding=(1, 2),
    )

    console.print(panel)


def print_success_box(
    message: str,
    details: Optional[Dict[str, Any]] = None,
    console: Optional[Console] = None,
) -> None:
    """
    Print a success box with optional details.

    Args:
        message: Main message
        details: Optional additional details
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    content = Text()
    content.append("✓ ", style=STYLE_BOLD_BRIGHT_GREEN)
    content.append(message, style=STYLE_BOLD_BRIGHT_GREEN)

    if details:
        content.append("\n\n")
        for key, value in details.items():
            content.append(f"{key}: ", style="dim")
            content.append(f"{value}\n", style="bright_white")

    panel = Panel(
        content,
        border_style="bright_green",
        box=box.HEAVY,
        padding=(1, 2),
    )

    console.print(panel)


def print_warning_box(
    message: str,
    details: Optional[Dict[str, Any]] = None,
    console: Optional[Console] = None,
) -> None:
    """
    Print a warning box with optional details.

    Args:
        message: Main message
        details: Optional additional details
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    content = Text()
    content.append("⚠ ", style=STYLE_BOLD_BRIGHT_YELLOW)
    content.append(message, style=STYLE_BOLD_BRIGHT_YELLOW)

    if details:
        content.append("\n\n")
        for key, value in details.items():
            content.append(f"{key}: ", style="dim")
            content.append(f"{value}\n", style="bright_white")

    panel = Panel(
        content,
        border_style="bright_yellow",
        box=box.HEAVY,
        padding=(1, 2),
    )

    console.print(panel)


def print_error_box(
    message: str,
    details: Optional[Dict[str, Any]] = None,
    console: Optional[Console] = None,
) -> None:
    """
    Print an error box with optional details.

    Args:
        message: Main message
        details: Optional additional details
        console: Console instance (optional)
    """
    if console is None:
        console = RichLoggerManager.console

    content = Text()
    content.append("✗ ", style=STYLE_BOLD_BRIGHT_RED)
    content.append(message, style=STYLE_BOLD_BRIGHT_RED)

    if details:
        content.append("\n\n")
        for key, value in details.items():
            content.append(f"{key}: ", style="dim")
            content.append(f"{value}\n", style="bright_white")

    panel = Panel(
        content,
        border_style=STYLE_BRIGHT_RED,
        box=box.HEAVY,
        padding=(1, 2),
    )
    console.print(panel)


def log_pipeline_start(pipeline_name: str, environment: str = "", description: str = "") -> None:
    """Log pipeline start with professional formatting."""
    console = RichLoggerManager.get_console()

    content = Text()
    content.append("🚀 ", style="bright_cyan")
    content.append("Starting Pipeline", style=STYLE_BOLD_BRIGHT_WHITE)
    content.append("\n\n")
    content.append("Pipeline: ", style="dim")
    content.append(f"{pipeline_name}", style="bold bright_cyan")
    content.append("\n")

    if environment:
        content.append("Environment: ", style="dim")
        content.append(f"{environment}", style="bright_yellow")
        content.append("\n")

    if description:
        content.append("Description: ", style="dim")
        content.append(f"{description}", style="white")
        content.append("\n")

    content.append("Started: ", style="dim")
    content.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="dim")

    panel = Panel(
        content,
        border_style="bright_blue",
        box=box.DOUBLE,
        padding=(1, 2),
    )
    console.print(panel)


def log_pipeline_complete(
    pipeline_name: str, nodes_executed: int = 0, duration: float = 0.0, status: str = "success"
) -> None:
    """Log pipeline completion with professional formatting."""
    console = RichLoggerManager.get_console()

    emoji_map = {
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "partial": "⚡",
    }

    color_map = {
        "success": "bright_green",
        "warning": "bright_yellow",
        "error": "bright_red",
        "partial": "bright_cyan",
    }

    emoji = emoji_map.get(status, "✓")
    color = color_map.get(status, "bright_green")

    content = Text()
    content.append(f"{emoji} ", style=color)
    content.append("Pipeline Completed\n\n", style=f"bold {color}")
    content.append("Pipeline: ", style="dim")
    content.append(f"{pipeline_name}\n", style=STYLE_BOLD_BRIGHT_WHITE)
    content.append("Nodes Executed: ", style="dim")
    content.append(f"{nodes_executed}\n", style="bright_cyan")
    content.append("Duration: ", style="dim")
    content.append(f"{duration:.2f}s\n", style="bright_yellow")
    content.append("Completed: ", style="dim")
    content.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="bright_black")

    panel = Panel(
        content,
        border_style=color,
        box=box.DOUBLE,
        padding=(1, 2),
    )
    console.print(panel)


def log_node_start(node_name: str, node_type: str = "") -> None:
    """Log node execution start with elegant formatting."""
    console = RichLoggerManager.get_console()

    line = Text("  ")
    line.append("▸ ", style="bright_cyan")
    line.append(node_name, style=STYLE_BOLD_BRIGHT_WHITE)
    if node_type:
        line.append(f" [{node_type}]", style="dim")

    console.print(line)


def log_node_complete(node_name: str, duration: float = 0.0, records: int = 0) -> None:
    """Log node execution completion with metrics."""
    console = RichLoggerManager.get_console()

    line = Text("  ")
    line.append("✓ ", style="bright_green")
    line.append(node_name, style=STYLE_BOLD_BRIGHT_WHITE)
    line.append(f" ({duration:.2f}s", style="dim")
    if records > 0:
        line.append(f", {records:,} records", style="dim")
    line.append(")", style="dim")

    console.print(line)


def log_error_with_context(message: str, **context) -> None:
    """Log error with additional context in an elegant panel."""
    console = RichLoggerManager.get_console()

    content = Text()
    content.append("❌ Error\n\n", style=STYLE_BOLD_BRIGHT_RED)
    content.append(f"{message}\n", style="bright_white")

    if context:
        content.append("\nContext:\n", style="dim")
        for key, value in context.items():
            content.append(f"  • {key}: ", style="dim")
            content.append(f"{value}\n", style="bright_yellow")

    panel = Panel(
        content,
        border_style=STYLE_BOLD_BRIGHT_RED,
        box=box.HEAVY,
        padding=(1, 2),
    )
    console.print(panel)


def log_config_loaded(config_path: str, config_type: str = "") -> None:
    """Log configuration loading with professional style."""
    console = RichLoggerManager.get_console()

    line = Text("  ")
    line.append("✓ ", style="bright_green")
    line.append("Configuration loaded: ", style="white")
    line.append(config_path, style="bright_cyan")
    if config_type:
        line.append(f" [{config_type}]", style="bright_magenta")

    console.print(line)


def create_progress_bar() -> Progress:
    """Create a professional progress bar."""
    return Progress(
        SpinnerColumn("dots"),
        TextColumn("[bold bright_cyan]{task.description}"),
        BarColumn(complete_style="bright_green", finished_style="bright_green"),
        TaskProgressColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=RichLoggerManager.get_console(),
    )


def log_metrics_table(metrics: Dict[str, Any], title: str = "Metrics") -> None:
    """Display metrics in an elegant table."""
    console = RichLoggerManager.get_console()

    table = Table(
        title=f"[bold bright_cyan]{title}[/]",
        box=box.ROUNDED,
        border_style="bright_magenta",
        header_style=STYLE_BOLD_BRIGHT_WHITE,
        show_lines=False,
    )

    table.add_column("Metric", style="bright_cyan", no_wrap=True)
    table.add_column("Value", style="bright_yellow", justify="right")

    for key, value in metrics.items():
        # Format numbers nicely
        if isinstance(value, float):
            formatted_value = f"{value:,.2f}"
        elif isinstance(value, int):
            formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)

        table.add_row(key, formatted_value)

    console.print(table)


def log_warning_panel(message: str, details: str = "") -> None:
    """Display a warning in an elegant panel."""
    console = RichLoggerManager.get_console()

    content = Text()
    content.append("⚠️  Warning\n\n", style=STYLE_BOLD_BRIGHT_YELLOW)
    content.append(f"{message}\n", style="bright_white")

    if details:
        content.append(f"\n{details}", style="dim")

    panel = Panel(
        content,
        border_style="bright_yellow",
        box=box.ROUNDED,
        padding=(1, 2),
    )
    console.print(panel)


def log_info_panel(title: str, message: str, icon: str = "ℹ️") -> None:
    """Display information in an elegant panel."""
    console = RichLoggerManager.get_console()

    content = Text()
    content.append(f"{icon} {title}\n\n", style=STYLE_BOLD_BRIGHT_CYAN)
    content.append(message, style=STYLE_BOLD_BRIGHT_WHITE)

    panel = Panel(
        content,
        border_style="bright_cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )
    console.print(panel)
