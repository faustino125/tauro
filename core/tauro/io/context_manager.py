"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Optional
from loguru import logger  # type: ignore

from tauro.io.exceptions import ConfigurationError


class ContextManager:
    """Centralized context manager for unified access to configuration and resources."""

    def __init__(self, context: Any):
        """Initialize context manager with either dict or object context."""
        if context is None:
            raise ConfigurationError("Context cannot be None") from None
        self._context = context
        self._is_dict_context = isinstance(context, dict)
        logger.debug(
            f"ContextManager initialized with {'dict' if self._is_dict_context else 'object'} context"
        )

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Safe get from context for both dict and object."""
        try:
            if self._is_dict_context:
                return self._context.get(key, default)
            return getattr(self._context, key, default)
        except Exception as e:
            logger.debug(f"Error accessing context key '{key}': {e}")
            return default

    def has(self, key: str) -> bool:
        """Safe check for key existence in context."""
        try:
            if self._is_dict_context:
                return key in self._context
            return hasattr(self._context, key)
        except Exception as e:
            logger.debug(f"Error checking context key '{key}': {e}")
            return False

    def get_spark(self) -> Optional[Any]:
        """Get SparkSession if present, else None."""
        return self.get("spark")

    def get_execution_mode(self) -> Optional[str]:
        """Get normalized execution mode."""
        mode = self.get("execution_mode")
        if not mode:
            return None

        # Soporta strings y enums (u otros objetos con atributo 'value')
        try:
            if not isinstance(mode, str) and hasattr(mode, "value"):
                mode = str(mode.value)
        except Exception:
            pass

        mode = str(mode).lower()
        if mode == "databricks":
            return "distributed"
        return mode

    def is_local_mode(self) -> bool:
        """Check if execution mode is local."""
        return self.get_execution_mode() == "local"

    def is_spark_available(self) -> bool:
        """Check if Spark context is available."""
        spark = self.get_spark()
        is_available = spark is not None
        logger.debug(f"Spark availability: {is_available}")
        return is_available

    def is_spark_connect(self) -> bool:
        """Detect if the active SparkSession is a Spark Connect session."""
        spark = self.get_spark()
        try:
            return spark is not None and "pyspark.sql.connect" in type(spark).__module__
        except Exception:
            return False

    def get_input_config(self) -> dict:
        """Get input configuration dictionary."""
        return self.get("input_config", {}) or {}

    def get_output_config(self) -> dict:
        """Get output configuration dictionary."""
        return self.get("output_config", {}) or {}

    def get_global_settings(self) -> dict:
        """Get global settings dictionary."""
        return self.get("global_settings", {}) or {}

    def get_output_path(self) -> Optional[str]:
        """Get base output path."""
        return self.get("output_path")

    def validate_required_keys(self, required_keys: list) -> None:
        """Validate that required keys exist in context."""
        missing_keys = [key for key in required_keys if not self.has(key)]
        if missing_keys:
            raise ConfigurationError(
                f"Missing required context keys: {', '.join(missing_keys)}"
            ) from None

    def get_context_info(self) -> dict:
        """Get context information for debugging."""
        info = {
            "context_type": "dict" if self._is_dict_context else "object",
            "spark_available": self.is_spark_available(),
            "execution_mode": self.get_execution_mode(),
            "output_path_configured": bool(self.get_output_path()),
        }

        if self._is_dict_context:
            info["available_keys"] = list(self._context.keys())
        else:
            info["available_attributes"] = [
                attr for attr in dir(self._context) if not attr.startswith("_")
            ]

        return info
