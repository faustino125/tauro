from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from tauro.io.validators import ConfigValidator
from tauro.io.exceptions import ConfigurationError
from tauro.io.context_manager import ContextManager
from tauro.io.sql import SQLSanitizer


class BaseIO:
    """Base class for input/output operations with enhanced validation and error handling."""

    def __init__(self, context: Any):
        """Initialize BaseIO with application context (dict or object)."""
        self.config_validator = ConfigValidator()
        self._context: Any = None
        self.context_manager: ContextManager
        self.context = context
        logger.debug("BaseIO initialized with context")

    @property
    def context(self) -> Any:
        """Current application context (dict or object)."""
        return self._context

    @context.setter
    def context(self, value: Any) -> None:
        """Set context and keep the internal ContextManager in sync."""
        if value is None:
            raise ConfigurationError("Context cannot be None") from None
        self._context = value
        self.context_manager = ContextManager(value)

    def _ctx_get(self, key: str, default: Optional[Any] = None) -> Any:
        """Safe get from context for both dict and object."""
        return self.context_manager.get(key, default)

    def _ctx_has(self, key: str) -> bool:
        """Safe hasattr/contains for context."""
        return self.context_manager.has(key)

    def _ctx_spark(self) -> Optional[Any]:
        """Get SparkSession if present, else None."""
        return self.context_manager.get_spark()

    def _ctx_mode(self) -> Optional[str]:
        """Get normalized execution mode."""
        return self.context_manager.get_execution_mode()

    def _is_local(self) -> bool:
        """Check if execution mode is local."""
        return self.context_manager.is_local_mode()

    def _validate_config(
        self, config: Dict[str, Any], required_fields: List[str], config_type: str
    ) -> None:
        """Validate configuration using validator."""
        try:
            self.config_validator.validate(config, required_fields, config_type)
        except Exception as e:
            raise ConfigurationError(
                f"Configuration validation failed for {config_type}: {e}"
            ) from e

    def _prepare_local_directory(self, path: str) -> None:
        """Create local directories if necessary."""
        if self._is_local():
            if "://" in path or path.startswith("dbfs:/"):
                logger.debug(
                    f"Skipping local directory creation for non-local path: {path}"
                )
                return
            try:
                dir_path = Path(path).parent
                if dir_path and not dir_path.exists():
                    logger.debug(f"Creating directory: {dir_path}")
                    dir_path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Directory created: {dir_path}")
            except OSError as e:
                logger.exception(f"Error creating local directory: {dir_path}")
                raise IOError(f"Failed to create directory {dir_path}") from e

    def _spark_available(self) -> bool:
        """Check if Spark context is available."""
        return self.context_manager.is_spark_available()

    def _is_spark_connect(self) -> bool:
        """Detect if the active SparkSession is a Spark Connect session."""
        try:
            return self.context_manager.is_spark_connect()
        except Exception:
            return False

    def _parse_output_key(self, out_key: str) -> Dict[str, str]:
        """Parse output key using validator."""
        try:
            return self.config_validator.validate_output_key(out_key)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to parse output key '{out_key}': {e}"
            ) from e

    @staticmethod
    def sanitize_sql_query(query: str) -> str:
        """Safe sanitization of SQL queries using the specialized class."""
        try:
            return SQLSanitizer.sanitize_query(query)
        except Exception as e:
            raise ConfigurationError(f"SQL query sanitization failed: {e}") from e
