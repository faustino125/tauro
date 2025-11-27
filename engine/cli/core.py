"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import os
import stat
import sys
import time
from dataclasses import dataclass
from datetime import datetime  # added
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from loguru import logger  # type: ignore


# ===== Environment Constants =====
# Default environments that every Tauro project should support
DEFAULT_ENVIRONMENTS = ["base", "dev", "sandbox", "staging", "prod"]

# User-friendly aliases that map to canonical names
ENV_ALIASES = {
    "development": "dev",
    "prod": "prod",
    "production": "prod",
    "staging": "staging",
    "test": "sandbox",
    "testing": "sandbox",
}


class CanonicalEnvironment(str, Enum):
    """Canonical environments supported by Tauro."""

    BASE = "base"
    DEV = "dev"
    SANDBOX = "sandbox"
    STAGING = "staging"
    PROD = "prod"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def is_local(cls, env: str) -> bool:
        """Check if environment is local (development-friendly)"""
        normalized = str(env).lower()
        return normalized in [cls.BASE.value, cls.DEV.value, cls.SANDBOX.value]

    @classmethod
    def is_remote(cls, env: str) -> bool:
        """Check if environment is remote (deployment)"""
        normalized = str(env).lower()
        return normalized in [cls.STAGING.value, cls.PROD.value]

    @classmethod
    def is_testing(cls, env: str) -> bool:
        """Check if environment is for testing"""
        normalized = str(env).lower()
        return normalized in [cls.DEV.value, cls.SANDBOX.value]

    @classmethod
    def is_safe(cls, env: str) -> bool:
        """Check if environment is safe (non-production)"""
        normalized = str(env).lower()
        return normalized != cls.PROD.value


# Fallback chains for each environment
FALLBACK_CHAINS: Dict[str, List[str]] = {
    CanonicalEnvironment.BASE.value: [CanonicalEnvironment.BASE.value],
    CanonicalEnvironment.DEV.value: [
        CanonicalEnvironment.DEV.value,
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.SANDBOX.value: [
        CanonicalEnvironment.SANDBOX.value,
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.STAGING.value: [
        CanonicalEnvironment.STAGING.value,
        CanonicalEnvironment.PROD.value,
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.PROD.value: [
        CanonicalEnvironment.PROD.value,
        CanonicalEnvironment.BASE.value,
    ],
}


def get_fallback_chain(env: str) -> List[str]:
    """
    Get the fallback chain for an environment.

    Args:
        env: Environment name (should be normalized)

    Returns:
        List of environments to try in order (fallback priority)

    Raises:
        ValueError: If environment is not recognized

    Examples:
        >>> get_fallback_chain("dev")
        ["dev", "base"]
        >>> get_fallback_chain("sandbox_alice")
        ["sandbox_alice", "sandbox", "base"]
    """
    normalized = str(env).lower()

    # Handle sandbox_<developer> pattern
    if normalized.startswith("sandbox_") and normalized != "sandbox":
        return [
            normalized,  # sandbox_alice
            CanonicalEnvironment.SANDBOX.value,  # sandbox
            CanonicalEnvironment.BASE.value,  # base
        ]

    # Check if it's in predefined chains
    if normalized in FALLBACK_CHAINS:
        return FALLBACK_CHAINS[normalized]

    # Unknown environment - raise error
    raise ValueError(
        f"Unknown environment '{env}'. Valid environments: {', '.join(DEFAULT_ENVIRONMENTS)}"
    )


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
    base_path: Optional[Path] = None
    layer_name: Optional[str] = None
    use_case_name: Optional[str] = None
    config_type: Optional[str] = None
    interactive: bool = False
    list_configs: bool = False
    list_pipelines: bool = False
    pipeline_info: Optional[str] = None
    clear_cache: bool = False
    log_level: str = "INFO"
    log_file: Optional[Path] = None
    validate_only: bool = False
    dry_run: bool = False
    verbose: bool = False
    quiet: bool = False
    template: Optional[str] = None
    project_name: Optional[str] = None
    output_path: Optional[Path] = None
    format: str = "yaml"
    no_sample_code: bool = False
    list_templates: bool = False
    streaming: bool = False
    streaming_command: Optional[str] = None
    streaming_config: Optional[Path] = None
    streaming_pipeline: Optional[str] = None
    execution_id: Optional[str] = None
    streaming_mode: str = "async"
    model_version: Optional[str] = None
    hyperparams: Optional[str] = None


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

    if os.name == "posix":
        _sensitive_list = [
            Path("/etc"),
            Path("/proc"),
            Path("/sys"),
            Path("/var/run"),
            Path("/root"),
            Path.home() / ".ssh",
        ]
    elif os.name == "nt":
        _sensitive_list = [
            Path("C:/Windows"),
            Path("C:/Program Files"),
            Path("C:/Program Files (x86)"),
            Path("C:/Users/Administrator"),
        ]
    else:
        _sensitive_list = []

    SENSITIVE_DIRS: Tuple[Path, ...] = tuple(_sensitive_list)

    @staticmethod
    def _is_permissive() -> bool:
        val = os.getenv("TAURO_PERMISSIVE_PATH_VALIDATION", "0")
        return val in ("1", "true", "True", "yes", "YES")

    @staticmethod
    def validate_path(base_path: Path, target_path: Path) -> Path:
        """Ensure target path is within base path boundaries and safe."""
        permissive = SecurityValidator._is_permissive()
        try:
            resolved_base = base_path.resolve()
            resolved_target = target_path.resolve()

            # If permissive and target is absolute and not relative to base, allow after checks
            if permissive and resolved_target.is_absolute():
                SecurityValidator._check_sensitive_dirs(resolved_target)
                SecurityValidator._check_file_permissions(resolved_target, permissive=permissive)
                return resolved_target

            # Strict behavior: ensure target is inside base
            SecurityValidator._check_relative_to_base(resolved_base, resolved_target)
            SecurityValidator._check_relative_parts(resolved_base, resolved_target)
            SecurityValidator._check_sensitive_dirs(resolved_target)
            SecurityValidator._check_file_permissions(resolved_target, permissive=permissive)

            return resolved_target
        except Exception as e:
            if isinstance(e, SecurityError):
                raise
            raise SecurityError(f"Path validation failed: {e}") from e

    @staticmethod
    def _check_relative_to_base(resolved_base: Path, resolved_target: Path) -> None:
        """Check if target is within base path."""
        try:
            resolved_target.relative_to(resolved_base)
        except ValueError:
            raise SecurityError(f"Path traversal attempt blocked: {resolved_target}")

    @staticmethod
    def _check_relative_parts(resolved_base: Path, resolved_target: Path) -> None:
        """Check for '..' and hidden files in relative path."""
        relative = resolved_target.relative_to(resolved_base)
        if ".." in relative.parts:
            raise SecurityError(f"Path contains '..': {resolved_target}")
        # Hidden files check can be disabled in permissive mode by caller (we check permissive in validate_path)
        if any(part.startswith(".") for part in relative.parts):
            raise SecurityError(f"Hidden file/directory access denied: {resolved_target}")

    @staticmethod
    def _check_sensitive_dirs(resolved_target: Path) -> None:
        """Check if target is inside sensitive directories."""
        for sensitive_dir in SecurityValidator.SENSITIVE_DIRS:
            try:
                # is_relative_to available in py>=3.9
                if resolved_target.is_relative_to(sensitive_dir):
                    raise SecurityError(f"Access to sensitive directory '{sensitive_dir}' denied")
            except AttributeError:
                try:
                    resolved_target.relative_to(sensitive_dir)
                    raise SecurityError(f"Access to sensitive directory '{sensitive_dir}' denied")
                except Exception:
                    pass

    @staticmethod
    def _check_file_permissions(resolved_target: Path, permissive: bool = False) -> None:
        """Check file permissions and ownership.

        In permissive mode ownership and some permission checks are skipped.
        """
        if not resolved_target.exists():
            return
        try:
            stat_info = os.stat(resolved_target)
            if os.name == "posix":
                SecurityValidator._check_posix_permissions(resolved_target, stat_info, permissive)
        except OSError:
            pass

    @staticmethod
    def _check_posix_permissions(
        resolved_target: Path, stat_info: os.stat_result, permissive: bool = False
    ) -> None:
        """Check POSIX-specific file permissions and ownership."""
        if hasattr(stat, "S_IWOTH") and (stat_info.st_mode & stat.S_IWOTH):
            # world-writable is suspicious even in permissive mode -> raise
            raise SecurityError(f"World-writable file detected: {resolved_target}")
        if not permissive and hasattr(os, "getuid") and stat_info.st_uid != os.getuid():
            # Ownership mismatch is strict-only check
            raise SecurityError(f"File not owned by current user: {resolved_target}")


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

        if quiet:
            console_level = "ERROR"
        elif verbose:
            console_level = "DEBUG"
        else:
            console_level = level.upper()

        logger.add(
            sys.stderr,
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>tauro</cyan> | <level>{message}</level>",
            colorize=True,
            level=console_level,
        )

        log_path = Path(log_file) if log_file else Path("logs/log")
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
            validated_dir = SecurityValidator.validate_path(self.original_cwd, self.config_dir)

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


def parse_iso_date(date_str: Optional[str]) -> Optional[str]:
    """Validate and normalize a date string to ISO format YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValidationError(f"Invalid date format '{date_str}'. Use YYYY-MM-DD") from e


def validate_date_range(start_date: Optional[str], end_date: Optional[str]) -> None:
    """Ensure start_date <= end_date when both are provided."""
    if start_date and end_date:
        try:
            sd = datetime.strptime(start_date, "%Y-%m-%d")
            ed = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValidationError("Dates must be in YYYY-MM-DD format")
        if sd > ed:
            raise ValidationError(f"Start date {start_date} must be <= end date {end_date}")


def is_sandbox_environment(env_name: str) -> bool:
    """Check if environment name is a sandbox environment (sandbox or sandbox_*)."""
    if not env_name:
        return False
    return env_name == "sandbox" or env_name.startswith("sandbox_")


def get_sandbox_developer(env_name: str) -> Optional[str]:
    """Extract developer name from sandbox environment name."""
    if not is_sandbox_environment(env_name):
        return None

    if env_name == "sandbox":
        return None  # Generic sandbox

    # Extract developer name from sandbox_<developer>
    parts = env_name.split("_", 1)
    if len(parts) == 2 and parts[0] == "sandbox":
        return parts[1]

    return None


def get_base_environment(env_name: str) -> str:
    """Get base environment name (e.g., sandbox_juan -> sandbox)."""
    if is_sandbox_environment(env_name):
        return "sandbox"
    return env_name


def is_valid_environment(env: str) -> bool:
    """
    Check if an environment name is valid (canonical or alias).

    Args:
        env: Environment name to check

    Returns:
        True if valid (canonical or alias), False otherwise
    """
    if not isinstance(env, str):
        return False

    normalized = env.strip().lower()

    # Check canonical environments
    if normalized in DEFAULT_ENVIRONMENTS:
        return True

    # Check aliases
    if normalized in ENV_ALIASES:
        return True

    # Check sandbox pattern: sandbox_<developer>
    if normalized == "sandbox" or normalized.startswith("sandbox_"):
        return True

    return False


def validate_environment_name(env_name: str) -> bool:
    """Validate environment name format."""
    from typing import Optional

    # Use normalization and allowed list
    norm = normalize_environment(env_name) if env_name else None
    if not norm:
        return False
    return is_allowed_environment(norm)


# ----- New helpers for scalable env handling -----
# Now uses canonical definitions from common
# These are kept for backward compatibility but reference the canonical source


def normalize_environment(env_name: Optional[str]) -> Optional[str]:
    """Normalize environment name: lower-case, map aliases, and trim."""
    if not env_name:
        return None
    env = env_name.strip().lower()

    # Return None if empty after stripping
    if not env:
        return None

    # Map aliases for exact matches
    if env in ENV_ALIASES:
        return ENV_ALIASES[env]

    # If sandbox developer pattern, normalize prefix
    if env == "sandbox":
        return "sandbox"
    if env.startswith("sandbox_"):
        # sanitize developer part: allow alnum and underscore only
        dev = env.split("_", 1)[1]
        safe_dev = "".join(c for c in dev if c.isalnum() or c == "_")
        return f"sandbox_{safe_dev}" if safe_dev else "sandbox"

    return env


def allowed_environments() -> List[str]:
    """Return the list of allowed canonical environments."""
    extra = os.getenv("TAURO_ALLOWED_ENVS", "")
    extras = [e.strip().lower() for e in extra.split(",") if e.strip()] if extra else []
    # Normalize extras as well
    normalized_extras = [normalize_environment(e) for e in extras]
    uniq = []
    for e in DEFAULT_ENVIRONMENTS + normalized_extras:
        if e and e not in uniq:
            uniq.append(e)
    return uniq


def is_allowed_environment(env_name: str) -> bool:
    """Check whether an environment is allowed (after normalization)."""
    if not is_valid_environment(env_name):
        return False

    norm = normalize_environment(env_name)
    if not norm:
        return False

    # sandbox variants should be accepted as long as prefix is sandbox
    if norm == "sandbox" or norm.startswith("sandbox_"):
        return True

    return norm in allowed_environments()
