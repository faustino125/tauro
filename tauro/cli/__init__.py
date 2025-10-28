from .cli import UnifiedCLI, main

# Backwards compatibility: older code may import TauroCLI
TauroCLI = UnifiedCLI
from .config import ConfigDiscovery, ConfigManager
from .core import (
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
from .execution import ContextInitializer
from .execution import PipelineExecutor as CLIPipelineExecutor
from .template import (
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
