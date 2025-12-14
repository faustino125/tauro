"""
Schema visualization utilities for professional data structure display.
"""
from typing import Any, Dict, List, Union, Optional, Tuple
from rich.console import Console
from rich.tree import Tree
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from rich.syntax import Syntax
import json
import re

# Style constants
BOLD_BRIGHT_WHITE = "bold bright_white"
HEADER_STYLE = BOLD_BRIGHT_WHITE
HEADER_STYLE_ON_BLUE = f"{HEADER_STYLE} on bright_blue"


def format_value(value: Any, max_length: int = 80) -> str:
    """Format a value for display, truncating if needed."""
    str_value = str(value)
    if len(str_value) > max_length:
        return str_value[: max_length - 3] + "..."
    return str_value


def print_schema(schema: Union[Dict, List], title: str = "Schema", console: Console = None) -> None:
    """
    Display a data schema in a professional, clean format.

    Args:
        schema: Dictionary or list representing the schema
        title: Title for the schema display
        console: Rich console instance (optional)
    """
    if console is None:
        console = Console()

    if isinstance(schema, dict):
        _print_dict_schema(schema, title, console)
    elif isinstance(schema, list):
        _print_list_schema(schema, title, console)
    else:
        console.print(f"[yellow]Schema type not supported: {type(schema)}[/]")


def _print_dict_schema(schema: Dict, title: str, console: Console) -> None:
    """Print dictionary schema as a clean table."""

    table = Table(
        title=f"[bold bright_cyan]{title}[/]",
        box=box.ROUNDED,
        border_style="bright_cyan",
        header_style=HEADER_STYLE,
        show_lines=False,
    )

    table.add_column("Field", style="bright_cyan", no_wrap=True, width=30)
    table.add_column("Type", style="bright_yellow", justify="center", width=15)
    table.add_column("Value/Description", style="white", width=50)

    for key, value in schema.items():
        value_type = type(value).__name__

        if isinstance(value, (dict, list)) and len(str(value)) > 50:
            value_display = f"[dim]{value_type} with {len(value)} items[/]"
        else:
            value_display = format_value(value, max_length=50)

        table.add_row(str(key), value_type, value_display)

    console.print(table)


def _print_list_schema(schema: List, title: str, console: Console) -> None:
    """Print list schema as a clean table."""

    if not schema:
        console.print(f"[dim]{title}: Empty list[/]")
        return

    # If all items are dicts with same keys, show as table
    if all(isinstance(item, dict) for item in schema):
        first_keys = set(schema[0].keys()) if schema else set()
        if all(set(item.keys()) == first_keys for item in schema):
            _print_list_of_dicts(schema, title, console)
            return

    # Otherwise show as numbered list
    table = Table(
        title=f"[bold bright_cyan]{title}[/]",
        box=box.SIMPLE,
        border_style="bright_cyan",
        show_header=False,
    )

    table.add_column("Index", style="dim", width=6, justify="right")
    table.add_column("Value", style="white")

    for idx, item in enumerate(schema):
        table.add_row(str(idx), format_value(item, max_length=80))

    console.print(table)


def _print_list_of_dicts(items: List[Dict], title: str, console: Console) -> None:
    """Print list of dictionaries as a structured table."""

    if not items:
        return

    table = Table(
        title=f"[bold bright_cyan]{title}[/]",
        box=box.ROUNDED,
        border_style="bright_cyan",
        header_style="bold bright_white",
        row_styles=["dim", ""],
    )

    # Add index column
    table.add_column("№", style="dim", width=4, justify="right")

    # Add columns for each key
    keys = list(items[0].keys())
    for key in keys:
        table.add_column(str(key), style="bright_cyan")

    # Add rows
    for idx, item in enumerate(items, 1):
        row_values = [str(idx)]
        for key in keys:
            value = item.get(key, "")
            row_values.append(format_value(value, max_length=30))
        table.add_row(*row_values)

    console.print(table)


def print_nested_structure(
    data: Dict, title: str = "Data Structure", console: Console = None
) -> None:
    """
    Display nested data structure as a tree for better visualization.

    Args:
        data: Nested dictionary or structure
        title: Title for the tree
        console: Rich console instance (optional)
    """
    if console is None:
        console = Console()

    tree = Tree(f"[bold bright_cyan]{title}[/]")
    _build_tree(tree, data)

    panel = Panel(
        tree,
        border_style="bright_cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )

    console.print(panel)


def _build_tree(tree: Tree, data: Any) -> None:
    """Recursively build tree structure."""

    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                branch = tree.add(f"[bright_yellow]{k}[/] [dim]({type(v).__name__})[/]")
                _build_tree(branch, v)
            else:
                value_str = format_value(v, max_length=60)
                tree.add(f"[bright_cyan]{k}[/]: [white]{value_str}[/]")

    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if isinstance(item, (dict, list)):
                branch = tree.add(f"[dim][{idx}][/] [dim]({type(item).__name__})[/]")
                _build_tree(branch, item)
            else:
                value_str = format_value(item, max_length=60)
                tree.add(f"[dim][{idx}][/]: [white]{value_str}[/]")

    else:
        tree.add(f"[white]{format_value(data, max_length=60)}[/]")


def print_json_pretty(
    data: Union[Dict, List], title: str = "JSON Data", console: Console = None
) -> None:
    """
    Display JSON data with syntax highlighting.

    Args:
        data: Dictionary or list to display as JSON
        title: Title for the display
        console: Rich console instance (optional)
    """
    if console is None:
        console = Console()

    json_str = json.dumps(data, indent=2, ensure_ascii=False)

    syntax = Syntax(
        json_str,
        "json",
        theme="monokai",
        line_numbers=False,
        word_wrap=True,
    )

    panel = Panel(
        syntax,
        title=f"[bold bright_cyan]{title}[/]",
        border_style="bright_cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )

    console.print(panel)


def parse_spark_schema_line(line: str) -> Optional[Tuple[str, str, str, int]]:
    """
    Parse a single line from Spark's printSchema() output.

    Returns: (field_name, data_type, nullable, indent_level) or None
    """
    # Match pattern like: " |-- Field: type (nullable = true)"
    pattern = r"^(\s*\|--\s+)([^:]+):\s+([^(]+)\(nullable\s*=\s*(true|false)\)"
    match = re.match(pattern, line)

    if match:
        indent = match.group(1)
        field_name = match.group(2).strip()
        data_type = match.group(3).strip()
        nullable = match.group(4)
        indent_level = (len(indent) - 4) // 4  # Calculate nesting level
        return (field_name, data_type, nullable, indent_level)

    return None


def print_spark_schema(
    schema_text: str, title: str = "DataFrame Schema", console: Console = None
) -> None:
    """
    Display Spark DataFrame schema in an elegant table format.

    Args:
        schema_text: String output from df.printSchema() or similar
        title: Title for the schema display
        console: Rich console instance (optional)

    Example:
        >>> schema = '''root
        ...  |-- Country: string (nullable = true)
        ...  |-- City: string (nullable = true)
        ...  |-- Population: integer (nullable = true)'''
        >>> print_spark_schema(schema)
    """
    if console is None:
        console = Console()

    lines = schema_text.strip().split("\n")
    fields = []

    for line in lines:
        if line.strip().startswith("|--"):
            parsed = parse_spark_schema_line(line)
            if parsed:
                fields.append(parsed)

    if not fields:
        console.print("[yellow]No schema fields found to display[/]")
        return

    # Create elegant table
    table = Table(
        title=f"[bold bright_cyan]📊 {title}[/]",
        box=box.ROUNDED,
        border_style="bright_cyan",
        header_style=HEADER_STYLE_ON_BLUE,
        show_lines=False,
        padding=(0, 1),
    )

    table.add_column("№", style="dim", width=4, justify="right")
    table.add_column("Field Name", style="bright_cyan", no_wrap=True, min_width=20)
    table.add_column("Data Type", style="bright_yellow", justify="center", width=15)
    table.add_column("Nullable", style="bright_green", justify="center", width=10)

    # Add rows
    for idx, (field_name, data_type, nullable, indent_level) in enumerate(fields, 1):
        # Add indentation for nested fields
        display_name = "  " * indent_level + field_name

        # Format nullable with icon
        nullable_icon = "✓" if nullable == "true" else "✗"
        nullable_style = "bright_green" if nullable == "true" else "bright_red"
        nullable_display = f"[{nullable_style}]{nullable_icon}[/]"

        # Color code data types
        type_style = _get_type_color(data_type)
        type_display = f"[{type_style}]{data_type}[/]"

        table.add_row(str(idx), display_name, type_display, nullable_display)

    # Summary footer
    footer = Text()
    footer.append("Total Fields: ", style="dim")
    footer.append(f"{len(fields)}", style=BOLD_BRIGHT_WHITE)

    console.print()
    console.print(table)
    console.print("  ", footer)
    console.print()


def _get_type_color(data_type: str) -> str:
    """Get color for data type."""
    data_type_lower = data_type.lower()

    # Numeric types
    if any(
        t in data_type_lower for t in ["int", "long", "short", "byte", "double", "float", "decimal"]
    ):
        return "bright_yellow"

    # String types
    elif "string" in data_type_lower or "char" in data_type_lower:
        return "bright_cyan"

    # Date/Time types
    elif any(t in data_type_lower for t in ["date", "time", "timestamp"]):
        return "bright_magenta"

    # Boolean
    elif "bool" in data_type_lower:
        return "bright_green"

    # Complex types
    elif any(t in data_type_lower for t in ["array", "map", "struct"]):
        return "bright_blue"

    # Default
    else:
        return "white"


def print_pandas_schema(
    df, title: str = "Pandas DataFrame Schema", console: Console = None
) -> None:
    """
    Display Pandas DataFrame schema in an elegant table format.

    Args:
        df: Pandas DataFrame
        title: Title for the schema display
        console: Rich console instance (optional)
    """
    if console is None:
        console = Console()

    try:
        import pandas as pd

        if not isinstance(df, pd.DataFrame):
            console.print("[yellow]Input is not a Pandas DataFrame[/]")
            return

        # Get schema info
        columns = df.columns.tolist()
        dtypes = df.dtypes.to_dict()
        shape = df.shape

        # Header with shape info
        header = Text()
        header.append("📊 ", style="bright_cyan")
        header.append(title, style="bold bright_cyan")
        header.append(f"  ({shape[0]:,} rows × {shape[1]} columns)", style="dim")

        console.print()
        console.print(header)
        console.print()

        # Create table
        table = Table(
            box=box.ROUNDED,
            border_style="bright_cyan",
            header_style=HEADER_STYLE_ON_BLUE,
            show_lines=False,
            padding=(0, 1),
        )

        table.add_column("№", style="dim", width=4, justify="right")
        table.add_column("Column", style="bright_cyan", no_wrap=True, min_width=20)
        table.add_column("Data Type", style="bright_yellow", justify="center", width=15)
        table.add_column("Non-Null", style="bright_green", justify="right", width=12)
        table.add_column("Null Count", style="bright_red", justify="right", width=12)

        # Add rows
        for idx, col in enumerate(columns, 1):
            dtype = str(dtypes[col])
            non_null = df[col].count()
            null_count = df[col].isna().sum()

            # Format type with color
            type_style = _get_type_color(dtype)
            type_display = f"[{type_style}]{dtype}[/]"

            table.add_row(
                str(idx),
                col,
                type_display,
                f"{non_null:,}",
                f"[dim]{null_count:,}[/]" if null_count == 0 else f"[bright_red]{null_count:,}[/]",
            )

        console.print(table)

        # Memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        memory_mb = memory_usage / (1024 * 1024)

        footer = Text()
        footer.append("Memory Usage: ", style="dim")
        footer.append(f"{memory_mb:.2f} MB", style=BOLD_BRIGHT_WHITE)

        console.print("  ", footer)
        console.print()

    except ImportError:
        console.print("[yellow]Pandas is not installed[/]")
    except Exception as e:
        console.print(f"[red]Error displaying schema: {e}[/]")


class SchemaCapture:
    """
    Context manager to capture and beautify Spark schema output.

    Usage:
        with SchemaCapture():
            df.printSchema()
    """

    def __init__(self, title: str = "DataFrame Schema", console: Console = None):
        self.title = title
        self.console = console or Console()
        self.captured_output = []

    def __enter__(self):
        import sys
        from io import StringIO

        self.old_stdout = sys.stdout
        self.buffer = StringIO()
        sys.stdout = self.buffer
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import sys

        sys.stdout = self.old_stdout
        schema_text = self.buffer.getvalue()

        if schema_text.strip():
            print_spark_schema(schema_text, title=self.title, console=self.console)

        return False
