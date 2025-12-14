"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Error analyzer for elegant and developer-friendly error reporting.
"""
import re
from typing import Dict, List, Optional, Any, Tuple
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.syntax import Syntax
from rich.table import Table
from rich import box


class SparkErrorAnalyzer:
    """Analiza y formatea errores de Spark y Python para una mejor comprensión del desarrollador."""

    # Style constants
    STYLE_CYAN_BOLD = "bold bright_cyan"
    STYLE_BOLD_BRIGHT_RED = "bold bright_red"
    STYLE_BOLD_BRIGHT_WHITE_ON_YELLOW = "bold bright_white on bright_yellow"
    STYLE_RED = "bright_red"

    # Patrones de errores comunes de Spark, Python y PySpark
    ERROR_PATTERNS = {
        "UNRESOLVED_COLUMN": {
            "pattern": r"\[UNRESOLVED_COLUMN\.WITH_SUGGESTION\] A column.*?`([^`]+)`.*?Did you mean.*?\[([^\]]+)\]",
            "type": "Column Not Found",
            "emoji": "🔍",
            "color": "bright_yellow",
        },
        "COLUMN_NOT_FOUND_ALT": {
            "pattern": r"cannot resolve '([^']+)'.*?given input columns: \[([^\]]+)\]",
            "type": "Column Not Found",
            "emoji": "❌",
            "color": "bright_red",
        },
        "TYPE_MISMATCH": {
            "pattern": r"cannot resolve.*?due to data type mismatch|incompatible.*?type",
            "type": "Type Mismatch",
            "emoji": "⚠️",
            "color": "bright_yellow",
        },
        "PARSE_EXCEPTION": {
            "pattern": r"ParseException",
            "type": "SQL Parse Error",
            "emoji": "📝",
            "color": "bright_red",
        },
        "FILE_NOT_FOUND": {
            "pattern": r"FileNotFoundError|Path does not exist|No such file or directory",
            "type": "File Not Found",
            "emoji": "📁",
            "color": "bright_red",
        },
        "KEY_ERROR": {
            "pattern": r"KeyError:\s*['\"]([^'\"]+)['\"]",
            "type": "Key Error",
            "emoji": "🔑",
            "color": "bright_yellow",
        },
        "ATTRIBUTE_ERROR": {
            "pattern": r"AttributeError.*?'([^']+)'.*?has no attribute\s*'([^']+)'",
            "type": "Attribute Error",
            "emoji": "🔧",
            "color": "bright_yellow",
        },
        "VALUE_ERROR": {
            "pattern": r"ValueError",
            "type": "Value Error",
            "emoji": "💢",
            "color": "bright_yellow",
        },
        "INDEX_ERROR": {
            "pattern": r"IndexError",
            "type": "Index Error",
            "emoji": "📍",
            "color": "bright_yellow",
        },
        "TYPE_ERROR": {
            "pattern": r"TypeError",
            "type": "Type Error",
            "emoji": "🔤",
            "color": "bright_yellow",
        },
        "IMPORT_ERROR": {
            "pattern": r"ImportError|ModuleNotFoundError|No module named",
            "type": "Import Error",
            "emoji": "📦",
            "color": "bright_red",
        },
        "MEMORY_ERROR": {
            "pattern": r"OutOfMemoryError|MemoryError",
            "type": "Memory Error",
            "emoji": "💾",
            "color": "bright_red",
        },
        "NULL_POINTER": {
            "pattern": r"NullPointerException|'NoneType' object",
            "type": "Null Reference Error",
            "emoji": "⭕",
            "color": "bright_yellow",
        },
        "DIVISION_BY_ZERO": {
            "pattern": r"ZeroDivisionError|division by zero",
            "type": "Division by Zero",
            "emoji": "➗",
            "color": "bright_red",
        },
        "SCHEMA_MISMATCH": {
            "pattern": r"Schema mismatch|expected.*?but got",
            "type": "Schema Mismatch",
            "emoji": "📋",
            "color": "bright_yellow",
        },
        "TIMEOUT": {
            "pattern": r"TimeoutError|timeout|timed out",
            "type": "Timeout Error",
            "emoji": "⏱️",
            "color": "bright_red",
        },
        "CONNECTION_ERROR": {
            "pattern": r"ConnectionError|Connection refused|unable to connect",
            "type": "Connection Error",
            "emoji": "🔌",
            "color": "bright_red",
        },
    }

    @staticmethod
    def _extract_json_stacktrace(error_msg: str) -> Optional[List[Dict[str, str]]]:
        """Extrae stacktrace en formato JSON."""
        if '"stacktrace"' not in error_msg:
            return None

        match = re.search(r'"stacktrace":\s*\[([^\]]+)\]', error_msg)
        if not match:
            return None

        frames = []
        file_matches = re.findall(r'"file":\s*"([^"]+)".*?"line":\s*"?(\d+)"?', match.group(1))
        for file_path, line_num in file_matches:
            if file_path and not file_path.startswith("java.base") and ".py" in file_path:
                frames.append({"file": file_path, "line": line_num})

        return frames if frames else None

    @staticmethod
    def _extract_plaintext_traceback(error_msg: str) -> Optional[List[Dict[str, str]]]:
        """Extrae stacktrace en formato texto plano."""
        frames = []
        file_matches = re.findall(r'File\s+"([^"]+\.py)",\s+line\s+(\d+)', error_msg)
        for file_path, line_num in file_matches:
            if not any(exclude in file_path for exclude in ["site-packages", "lib/python"]):
                frames.append({"file": file_path, "line": line_num})

        return frames if frames else None

    @staticmethod
    def extract_python_traceback(error_msg: str) -> Optional[List[Dict[str, str]]]:
        """Extrae solo las líneas del traceback de Python (no Java/Scala)."""
        # Intentar extraer del formato JSON primero
        python_frames = SparkErrorAnalyzer._extract_json_stacktrace(error_msg)

        # Si no hay frames en JSON, buscar en formato texto plano
        if not python_frames:
            python_frames = SparkErrorAnalyzer._extract_plaintext_traceback(error_msg)

        return python_frames

    @staticmethod
    def extract_error_message(error_msg: str) -> str:
        """Extrae el mensaje de error principal, limpiando el ruido de Java/Scala."""
        # Intentar extraer mensaje principal de diferentes formatos

        # Formato JSON con "msg"
        json_match = re.search(r'"msg":\s*"([^"]+)"', error_msg)
        if json_match:
            return json_match.group(1)

        # Buscar mensaje después de exception class
        exception_match = re.search(r"(?:Exception|Error):\s*(.+?)(?:\n|\\n)", error_msg)
        if exception_match:
            msg = exception_match.group(1).strip()
            # Limpiar referencias a Java
            msg = re.sub(r"\\[rn]", " ", msg)
            return msg

        # Tomar primeras líneas si no encontramos nada específico
        lines = error_msg.split("\n")
        for line in lines[:5]:
            line = line.strip()
            if line and len(line) > 20 and not line.startswith("{"):
                return line

        return error_msg[:200] + "..." if len(error_msg) > 200 else error_msg

    @staticmethod
    def _handle_error_pattern(
        error_key: str,
        match: Any,
        error_msg: str,
        result: Dict[str, Any],
    ) -> None:
        """Handle specific error patterns and update result dict."""
        if error_key == "UNRESOLVED_COLUMN":
            wrong_column = match.group(1)
            available_columns = match.group(2).replace("`", "").split(", ")

            result["message"] = f"Column '{wrong_column}' does not exist in DataFrame"
            result["suggestions"] = available_columns
            result["context"] = {
                "wrong_column": wrong_column,
                "available_columns": available_columns,
            }
            result["severity"] = "medium"

        elif error_key == "COLUMN_NOT_FOUND_ALT":
            wrong_column = match.group(1)
            available_columns = match.group(2).split(", ")

            result["message"] = f"Cannot resolve column '{wrong_column}'"
            result["suggestions"] = available_columns
            result["context"] = {
                "wrong_column": wrong_column,
                "available_columns": available_columns,
            }
            result["severity"] = "medium"

        elif error_key == "KEY_ERROR":
            missing_key = match.group(1)
            result["message"] = f"Key '{missing_key}' does not exist in dictionary"
            result["context"]["missing_key"] = missing_key
            result["severity"] = "medium"

        elif error_key == "ATTRIBUTE_ERROR":
            object_type = match.group(1)
            attribute = match.group(2)
            result["message"] = f"Object '{object_type}' has no attribute '{attribute}'"
            result["context"]["object_type"] = object_type
            result["context"]["attribute"] = attribute
            result["severity"] = "medium"

        elif error_key == "FILE_NOT_FOUND":
            result["message"] = "File or directory not found"
            result["severity"] = "high"

        elif error_key == "IMPORT_ERROR":
            module_match = re.search(r"No module named ['\"]([^'\"]+)['\"]", error_msg)
            if module_match:
                module = module_match.group(1)
                result["message"] = f"Cannot import module '{module}'"
                result["context"]["module"] = module
                result["suggestions"] = [f"pip install {module}"]
            result["severity"] = "high"

        elif error_key == "MEMORY_ERROR":
            result["message"] = "Insufficient memory to complete operation"
            result["suggestions"] = [
                "Reduce dataset size",
                "Increase available memory",
                "Process in smaller batches",
            ]
            result["severity"] = "critical"

        elif error_key == "NULL_POINTER":
            result["message"] = "Attempt to access None/null object"
            result["severity"] = "medium"

        elif error_key == "DIVISION_BY_ZERO":
            result["message"] = "Division by zero detected"
            result["severity"] = "high"

        elif error_key == "TIMEOUT":
            result["message"] = "Operation exceeded time limit"
            result["suggestions"] = [
                "Increase timeout value",
                "Optimize query performance",
                "Check network connections",
            ]
            result["severity"] = "high"

    @staticmethod
    def analyze_error(error_msg: str, exception: Optional[Exception] = None) -> Dict[str, Any]:
        """
        Analiza un error y extrae información clave para el desarrollador.

        Returns:
            Dict con: error_type, message, suggestions, python_location, severity
        """
        result = {
            "error_type": "Unknown Error",
            "emoji": "❌",
            "color": "bright_red",
            "message": SparkErrorAnalyzer.extract_error_message(error_msg),
            "suggestions": [],
            "python_location": None,
            "severity": "high",
            "context": {},
            "raw_error": error_msg,
        }

        # Detectar tipo de error
        for error_key, config in SparkErrorAnalyzer.ERROR_PATTERNS.items():
            match = re.search(config["pattern"], error_msg, re.IGNORECASE | re.DOTALL)
            if match:
                result["error_type"] = config["type"]
                result["emoji"] = config["emoji"]
                result["color"] = config["color"]

                # Extraer contexto específico según el tipo de error
                SparkErrorAnalyzer._handle_error_pattern(error_key, match, error_msg, result)

                break

        # Extraer ubicación en código Python
        python_frames = SparkErrorAnalyzer.extract_python_traceback(error_msg)
        if python_frames:
            # Tomar el último frame de Python (más cercano al código del usuario)
            result["python_location"] = python_frames[-1]
            # Guardar todos los frames para debugging
            result["all_frames"] = python_frames

        # Si tenemos el objeto exception, extraer info adicional
        if exception:
            result["exception_type"] = type(exception).__name__
            if hasattr(exception, "__traceback__"):
                import traceback

                result["full_traceback"] = "".join(traceback.format_tb(exception.__traceback__))

        return result

    @staticmethod
    def _print_error_header(
        console: Console,
        error_type: str,
        node_name: str,
        emoji: str,
        color: str,
        severity: str,
    ) -> None:
        """Print the error header with title and severity."""
        console.print()
        console.print("━" * console.width, style=f"bold {color}")

        severity_emoji = {"critical": "🚨", "high": "❌", "medium": "⚠️", "low": "ℹ️"}
        severity_icon = severity_emoji.get(severity, "❌")

        title = Text()
        title.append(f"{severity_icon} ", style=f"bold {color}")
        title.append(f"{error_type} ", style=f"bold {color}")
        title.append(f"in node '{node_name}'", style=f"dim {color}")

        console.print(title)
        console.print("━" * console.width, style=f"bold {color}")
        console.print()

    @staticmethod
    def _shorten_file_path(file_path: str, max_length: int) -> str:
        """Shorten file path for display."""
        if len(file_path) <= max_length:
            return file_path

        parts = file_path.split("\\")
        if max_length == 60:
            return "...\\" + "\\".join(parts[-3:]) if len(parts) > 3 else file_path
        else:  # max_length == 50
            return "...\\" + "\\".join(parts[-2:]) if len(parts) > 2 else file_path

    @staticmethod
    def _build_error_content(
        color: str,
        message: str,
        python_location: Optional[Dict[str, str]],
        all_frames: List[Dict[str, str]],
    ) -> Text:
        """Build the error content panel."""
        error_content = Text()
        error_content.append("ERROR: ", style=f"bold {color}")
        error_content.append(message, style="bold bright_white")

        if python_location:
            error_content.append("\n\n")
            error_content.append("📍 Location:\n", style=SparkErrorAnalyzer.STYLE_CYAN_BOLD)
            error_content.append("  File: ", style="dim")
            file_path = SparkErrorAnalyzer._shorten_file_path(python_location["file"], 60)
            error_content.append(f"{file_path}\n", style="bright_white")
            error_content.append("  Line: ", style="dim")
            error_content.append(f"{python_location['line']}", style="bright_yellow")

        if len(all_frames) > 1:
            error_content.append("\n\n")
            error_content.append("📚 Call Stack:\n", style=SparkErrorAnalyzer.STYLE_CYAN_BOLD)
            for i, frame in enumerate(all_frames[-3:], 1):
                file_path = SparkErrorAnalyzer._shorten_file_path(frame["file"], 50)
                error_content.append(f"  {i}. ", style="dim")
                error_content.append(f"{file_path}:{frame['line']}\n", style="bright_white")

        return error_content

    @staticmethod
    def _render_context_panel(console: Console, context: Dict[str, Any]) -> None:
        """Render context-specific error panels."""
        if not context:
            return

        if "wrong_column" in context:
            SparkErrorAnalyzer._render_column_error(console, context)
        elif "missing_key" in context:
            SparkErrorAnalyzer._render_key_error(console, context)
        elif "object_type" in context and "attribute" in context:
            SparkErrorAnalyzer._render_attribute_error(console, context)
        elif "module" in context:
            SparkErrorAnalyzer._render_module_error(console, context)

    @staticmethod
    def _render_column_error(console: Console, context: Dict[str, Any]) -> None:
        """Render column not found error."""
        wrong_col = context["wrong_column"]
        available = context.get("available_columns", [])

        table = Table(
            box=box.ROUNDED,
            border_style="bright_yellow",
            header_style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_WHITE_ON_YELLOW,
            show_lines=True,
            title="[bold bright_yellow]🔍 Column Analysis[/]",
        )

        table.add_column("❌ You used", style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED, no_wrap=True)
        table.add_column("✓ Available columns", style="bright_green")

        similar_cols = [
            col
            for col in available
            if col.lower().replace("_", "") in wrong_col.lower().replace("_", "")
        ]
        available_display = "\n".join(
            [
                f"[{SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED}]{col}[/]"
                if col in similar_cols
                else f"[dim]{col}[/]"
                for col in available[:10]
            ]
        )

        if len(available) > 10:
            available_display += f"\n[dim]... and {len(available) - 10} more[/]"

        table.add_row(
            f"[{SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED}]{wrong_col}[/]", available_display
        )
        console.print(table)
        console.print()

    @staticmethod
    def _render_key_error(console: Console, context: Dict[str, Any]) -> None:
        """Render key error."""
        key_info = Text()
        key_info.append("🔑 Missing Key: ", style="bold bright_yellow")
        key_info.append(context["missing_key"], style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED)
        console.print(Panel(key_info, border_style="bright_yellow", box=box.ROUNDED))
        console.print()

    @staticmethod
    def _render_attribute_error(console: Console, context: Dict[str, Any]) -> None:
        """Render attribute error."""
        attr_info = Text()
        attr_info.append("Type: ", style="dim")
        attr_info.append(f"{context['object_type']}\n", style="bright_cyan")
        attr_info.append("Missing attribute: ", style="dim")
        attr_info.append(f"{context['attribute']}", style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED)
        console.print(
            Panel(
                attr_info,
                border_style="bright_yellow",
                box=box.ROUNDED,
                title="[bold]🔧 Attribute Details[/]",
            )
        )
        console.print()

    @staticmethod
    def _render_module_error(console: Console, context: Dict[str, Any]) -> None:
        """Render module import error."""
        module_info = Text()
        module_info.append("📦 Module: ", style="bold bright_yellow")
        module_info.append(context["module"], style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED)
        console.print(Panel(module_info, border_style="bright_yellow", box=box.ROUNDED))
        console.print()

    @staticmethod
    def _build_resolution_guide(
        context: Dict[str, Any], severity: str, suggestions: List[str]
    ) -> Text:
        """Build personalized resolution guide."""
        how_to_fix = Text()
        how_to_fix.append("🔧 How to fix:\n\n", style="bold bright_green")

        if "wrong_column" in context:
            wrong_col = context["wrong_column"]
            how_to_fix.append("1. ", style="dim")
            how_to_fix.append(f"Verify '{wrong_col}' spelling in your code\n", style="bright_white")
            how_to_fix.append("2. ", style="dim")
            how_to_fix.append(
                "Check available columns using df.printSchema() or df.columns\n",
                style="bright_white",
            )
            how_to_fix.append("3. ", style="dim")
            how_to_fix.append(
                "Ensure your input data has the expected columns\n", style="bright_white"
            )
        elif "module" in context:
            how_to_fix.append("1. ", style="dim")
            how_to_fix.append(
                f"Install the missing module: {suggestions[0] if suggestions else 'pip install <module>'}\n",
                style="bright_white",
            )
            how_to_fix.append("2. ", style="dim")
            how_to_fix.append("Verify the module name is correct\n", style="bright_white")
        elif severity == "critical":
            how_to_fix.append("⚠️  ", style=SparkErrorAnalyzer.STYLE_BOLD_BRIGHT_RED)
            how_to_fix.append(
                "This is a critical error that requires immediate attention\n\n", style="bright_red"
            )
            for i, sug in enumerate(suggestions or ["Review logs and system resources"], 1):
                how_to_fix.append(f"{i}. ", style="dim")
                how_to_fix.append(f"{sug}\n", style="bright_white")
        else:
            how_to_fix.append("1. ", style="dim")
            how_to_fix.append("Review the error message and location\n", style="bright_white")
            how_to_fix.append("2. ", style="dim")
            how_to_fix.append("Check the Python file at the indicated line\n", style="bright_white")
            how_to_fix.append("3. ", style="dim")
            how_to_fix.append("Verify your data types and values\n", style="bright_white")

        return how_to_fix

    @staticmethod
    def print_error_report(
        error_analysis: Dict[str, Any],
        node_name: str,
        console: Optional[Console] = None,
    ) -> None:
        """
        Print an elegant error report for the developer.
        """
        if console is None:
            console = Console()

        emoji = error_analysis["emoji"]
        error_type = error_analysis["error_type"]
        color = error_analysis["color"]
        message = error_analysis["message"]
        suggestions = error_analysis.get("suggestions", [])
        python_location = error_analysis.get("python_location")
        context = error_analysis.get("context", {})
        severity = error_analysis.get("severity", "high")
        all_frames = error_analysis.get("all_frames", [])

        # Print header
        SparkErrorAnalyzer._print_error_header(
            console, error_type, node_name, emoji, color, severity
        )

        # Build and print error content panel
        error_content = SparkErrorAnalyzer._build_error_content(
            color, message, python_location, all_frames
        )
        panel = Panel(
            error_content,
            border_style=color,
            box=box.HEAVY,
            padding=(1, 2),
            title=f"[bold]{emoji}  Error Details[/]",
            title_align="left",
        )
        console.print(panel)
        console.print()

        # Render context-specific panels
        SparkErrorAnalyzer._render_context_panel(console, context)

        # Render suggestions
        if suggestions:
            suggestions_text = Text()
            suggestions_text.append("💡 Suggestions:\n\n", style=SparkErrorAnalyzer.STYLE_CYAN_BOLD)

            for i, suggestion in enumerate(suggestions[:5], 1):
                suggestions_text.append(f"  {i}. ", style="dim")
                suggestions_text.append(f"{suggestion}\n", style="bold bright_green")

            suggestion_panel = Panel(
                suggestions_text,
                border_style="bright_cyan",
                box=box.ROUNDED,
                padding=(1, 2),
                title="[bold bright_cyan]💡 Quick Fixes[/]",
                title_align="left",
            )
            console.print(suggestion_panel)
            console.print()

        # Render resolution guide
        how_to_fix = SparkErrorAnalyzer._build_resolution_guide(context, severity, suggestions)
        fix_panel = Panel(
            how_to_fix,
            border_style="bright_green",
            box=box.ROUNDED,
            padding=(1, 2),
            title="[bold bright_green]🔧 Resolution Guide[/]",
            title_align="left",
        )
        console.print(fix_panel)
        console.print()

        console.print("━" * console.width, style=f"bold {color}")
        console.print()


def format_error_for_developer(
    error: Exception,
    node_name: str,
    console: Optional[Console] = None,
) -> None:
    """
    Punto de entrada principal para formatear errores de forma elegante.

    Args:
        error: La excepción capturada
        node_name: Nombre del nodo donde ocurrió el error
        console: Instancia de Console (opcional)
    """
    error_msg = str(error)

    # Analizar el error
    analysis = SparkErrorAnalyzer.analyze_error(error_msg, error)

    # Imprimir reporte elegante
    SparkErrorAnalyzer.print_error_report(analysis, node_name, console)
