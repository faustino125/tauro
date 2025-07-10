import os
from typing import Any, Dict, List

from loguru import logger  # type: ignore

from tauro.io.validators import ConfigValidator


class BaseIO:
    """Base class for input/output operations with enhanced validation and error handling."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize BaseIO with application context."""
        self.context = context
        self.config_validator = ConfigValidator()
        logger.debug("BaseIO initialized with context")

    def _validate_config(
        self, config: Dict[str, Any], required_fields: List[str], config_type: str
    ) -> None:
        """Validate configuration using validator."""
        self.config_validator.validate(config, required_fields, config_type)

    def _prepare_local_directory(self, path: str) -> None:
        """Create local directories if necessary."""
        if getattr(self.context, "execution_mode", None) == "local":
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
        is_available = hasattr(self.context, "spark") and self.context.spark is not None
        logger.debug(f"Spark availability: {is_available}")
        return is_available

    def _parse_output_key(self, out_key: str) -> Dict[str, str]:
        """Parse output key using validator."""
        return self.config_validator.validate_output_key(out_key)
