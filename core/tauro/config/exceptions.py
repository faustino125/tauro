"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""


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


class ConfigRepositoryError(ConfigurationError):
    """Exception raised when configuration repository operations fail."""

    pass


class ActiveConfigNotFound(ConfigurationError):
    """Exception raised when active configuration version is not found."""

    pass
