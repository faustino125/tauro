from core.cli.cli import UnifiedCLI, main

# Backwards compatibility: older code may import TauroCLI
TauroCLI = UnifiedCLI
from core.cli.config import ConfigDiscovery, ConfigManager
from core.cli.core import (
    CLIConfig,
    ConfigCache,
    ConfigFormat,
    ConfigurationError,
    ExecutionError,
    ExitCode,
    LoggerManager,
    LogLevel,
    PathManager,
    SecurityError,
    SecurityValidator,
    TauroError,
    ValidationError,
)
from core.cli.execution import ContextInitializer
from core.cli.execution import PipelineExecutor as CLIPipelineExecutor
from core.cli.template import (
    TemplateCommand,
    TemplateGenerator,
    TemplateType,
)

__all__ = [
    "ConfigFormat",
    "LogLevel",
    "ExitCode",
    "TauroError",
    "ConfigurationError",
    "ValidationError",
    "ExecutionError",
    "SecurityError",
    "CLIConfig",
    "SecurityValidator",
    "LoggerManager",
    "PathManager",
    "ConfigCache",
    "ConfigDiscovery",
    "ConfigManager",
    "ContextInitializer",
    "CLIPipelineExecutor",
    "UnifiedCLI",
    "TauroCLI",  # legacy alias
    "main",
    "TemplateCommand",
    "TemplateGenerator",
    "TemplateType",
]
