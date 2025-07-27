class ConfigurationError(Exception):
    """Base exception for configuration-related errors."""

    pass


class ConfigLoadError(ConfigurationError):
    """Exception raised when configuration loading fails."""

    pass


class ConfigValidationError(ConfigurationError):
    """Exception raised when configuration validation fails."""

    pass


class PipelineValidationError(ConfigurationError):
    """Exception raised when pipeline validation fails."""

    pass
