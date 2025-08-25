import os
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from tauro.io.validators import ConfigValidator


class BaseIO:
    """Base class for input/output operations with enhanced validation and error handling."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize BaseIO with application context (dict u objeto Context)."""
        self.context = context
        self.config_validator = ConfigValidator()
        logger.debug("BaseIO initialized with context")

    def _ctx_get(self, key: str, default: Optional[Any] = None) -> Any:
        """Safe get from context for both dict and object."""
        if isinstance(self.context, dict):
            return self.context.get(key, default)
        return getattr(self.context, key, default)

    def _ctx_has(self, key: str) -> bool:
        """Safe hasattr/contains for context."""
        if isinstance(self.context, dict):
            return key in self.context
        return hasattr(self.context, key)

    def _ctx_spark(self) -> Optional[Any]:
        """Get SparkSession if present, else None."""
        return self._ctx_get("spark")

    def _ctx_mode(self) -> Optional[str]:
        """Get normalized execution mode."""
        mode = self._ctx_get("execution_mode")
        if not mode:
            return None
        mode = str(mode).lower()
        if mode == "databricks":
            return "distributed"
        return mode

    def _is_local(self) -> bool:
        return self._ctx_mode() == "local"

    def _validate_config(
        self, config: Dict[str, Any], required_fields: List[str], config_type: str
    ) -> None:
        """Validate configuration using validator."""
        self.config_validator.validate(config, required_fields, config_type)

    def _prepare_local_directory(self, path: str) -> None:
        """Create local directories if necessary."""
        if self._is_local():
            try:
                dir_path = os.path.dirname(path)
                if dir_path and not os.path.isdir(dir_path):
                    logger.debug(f"Creating directory: {dir_path}")
                    os.makedirs(dir_path, exist_ok=True)
                    logger.info(f"Directory created: {dir_path}")
            except OSError as e:
                logger.exception(f"Error creating local directory: {dir_path}")
                raise IOError(f"Failed to create directory {dir_path}") from e

    def _spark_available(self) -> bool:
        """Check if Spark context is available."""
        spark = self._ctx_spark()
        is_available = spark is not None
        logger.debug(f"Spark availability: {is_available}")
        return is_available

    def _is_spark_connect(self) -> bool:
        """Detect if the active SparkSession is a Spark Connect session."""
        spark = self._ctx_spark()
        try:
            return spark is not None and "pyspark.sql.connect" in type(spark).__module__
        except Exception:
            return False

    def _parse_output_key(self, out_key: str) -> Dict[str, str]:
        """Parse output key using validator."""
        return self.config_validator.validate_output_key(out_key)
