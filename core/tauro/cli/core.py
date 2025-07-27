import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from loguru import logger  # type: ignore


class ConfigFormat(Enum):
    """Supported configuration file formats."""

    YAML = "yaml"
    JSON = "json"
    DSL = "dsl"


class LogLevel(Enum):
    """Available logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ExitCode(Enum):
    """Standard application exit codes."""

    SUCCESS = 0
    GENERAL_ERROR = 1
    CONFIGURATION_ERROR = 2
    VALIDATION_ERROR = 3
    EXECUTION_ERROR = 4
    DEPENDENCY_ERROR = 5
    SECURITY_ERROR = 6


class TauroError(Exception):
    """Base exception for all Tauro-related errors."""

    def __init__(self, message: str, exit_code: ExitCode = ExitCode.GENERAL_ERROR):
        super().__init__(message)
        self.exit_code = exit_code


class ConfigurationError(TauroError):
    """Raised when configuration is invalid or missing."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.CONFIGURATION_ERROR)


class ValidationError(TauroError):
    """Raised when input validation fails."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.VALIDATION_ERROR)


class ExecutionError(TauroError):
    """Raised when pipeline execution fails."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.EXECUTION_ERROR)


class SecurityError(TauroError):
    """Raised when security validation fails."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.SECURITY_ERROR)


@dataclass
class CLIConfig:
    """Configuration object for CLI arguments."""

    env: str
    pipeline: str
    node: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    base_path: Optional[str] = None
    layer_name: Optional[str] = None
    use_case_name: Optional[str] = None
    config_type: Optional[str] = None
    interactive: bool = False
    list_configs: bool = False
    log_level: str = "INFO"
    log_file: Optional[str] = None
    validate_only: bool = False
    dry_run: bool = False
    verbose: bool = False
    quiet: bool = False
    template: Optional[str] = None
    project_name: Optional[str] = None
    output_path: Optional[str] = None
    format: str = "yaml"
    no_sample_code: bool = False
    list_templates: bool = False
    template_interactive: bool = False


class ConfigLoaderProtocol(Protocol):
    """Protocol for configuration loaders."""

    def load_config(self, file_path: str) -> Dict[str, Any]:
        """Load configuration from file."""
        ...

    def get_format_name(self) -> str:
        """Get the format name."""
        ...


class SecurityValidator:
    """Validates file paths and prevents directory traversal attacks."""

    @staticmethod
    def validate_path(base_path: Path, target_path: Path) -> Path:
        """Ensure target path is within base path boundaries."""
        try:
            resolved_base = base_path.resolve().absolute()
            resolved_target = target_path.resolve().absolute()

            try:
                resolved_target.relative_to(resolved_base)
            except ValueError:
                raise SecurityError(
                    f"Path traversal attempt blocked: {resolved_target}"
                )

            return resolved_target
        except Exception as e:
            if isinstance(e, SecurityError):
                raise
            raise SecurityError(f"Path validation failed: {e}") from e


class ConfigCache:
    """Thread-safe cache for discovered configurations."""

    _EXPIRATION_SECONDS = 300  # 5 minutes
    _cache: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def get(cls, key: str) -> Optional[List[Tuple[Path, str]]]:
        """Get cached configurations if not expired."""
        if key in cls._cache:
            cached = cls._cache[key]
            if time.time() - cached["timestamp"] < cls._EXPIRATION_SECONDS:
                return cached["configs"]
        return None

    @classmethod
    def set(cls, key: str, configs: List[Tuple[Path, str]]) -> None:
        """Cache configurations with timestamp."""
        cls._cache[key] = {"configs": configs, "timestamp": time.time()}

    @classmethod
    def clear(cls) -> None:
        """Clear all cached data."""
        cls._cache.clear()


class LoggerManager:
    """Centralized logger configuration."""

    @staticmethod
    def setup(
        level: str = "INFO",
        log_file: Optional[str] = None,
        verbose: bool = False,
        quiet: bool = False,
    ) -> None:
        """Configure application logging."""
        logger.remove()

        console_level = "ERROR" if quiet else "DEBUG" if verbose else level.upper()

        logger.add(
            sys.stderr,
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>tauro</cyan> | <level>{message}</level>",
            colorize=True,
            level=console_level,
        )

        log_path = Path(log_file) if log_file else Path("logs/tauro.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            log_path,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            rotation="10 MB",
            retention="7 days",
            level="DEBUG",
        )


class PathManager:
    """Manages Python import paths for pipeline execution."""

    def __init__(self, config_dir: Optional[Path] = None):
        self.config_dir = config_dir
        self.added_paths: List[str] = []
        self.original_cwd = Path.cwd()

    def setup_import_paths(self) -> None:
        """Add necessary directories to Python path."""
        if not self.config_dir:
            return

        try:
            validated_dir = SecurityValidator.validate_path(
                self.original_cwd, self.config_dir
            )

            # Add common Python source directories
            paths_to_add = [
                validated_dir,
                validated_dir / "src",
                validated_dir / "lib",
                validated_dir.parent,
                validated_dir.parent / "src",
            ]

            for path in paths_to_add:
                if path.exists() and path.is_dir():
                    self._add_to_path(str(path))

        except SecurityError as e:
            logger.error(f"Security violation in path setup: {e}")

    def _add_to_path(self, path_str: str) -> None:
        """Safely add path to sys.path."""
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
            self.added_paths.append(path_str)
            logger.debug(f"Added to Python path: {path_str}")

    def cleanup(self) -> None:
        """Remove added paths from sys.path."""
        for path in self.added_paths:
            while path in sys.path:
                sys.path.remove(path)
        self.added_paths.clear()

    def diagnose_import_error(self, error_msg: str) -> None:
        """Provide helpful diagnostics for import errors."""
        logger.info("Import error diagnostics:")
        logger.info(f"Error: {error_msg}")
        logger.info("Suggestions:")
        logger.info("- Check if all required modules are installed")
        logger.info("- Verify __init__.py files exist in package directories")
        logger.info("- Ensure working directory is correct")
