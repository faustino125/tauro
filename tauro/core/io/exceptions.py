"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""


class IOManagerError(Exception):
    """Base exception for all IO Manager operations."""

    pass


class ConfigurationError(IOManagerError):
    """Raised when configuration is invalid or missing."""

    pass


class DataValidationError(IOManagerError):
    """Raised when data validation fails."""

    pass


class FormatNotSupportedError(IOManagerError):
    """Raised when a data format is not supported."""

    pass


class WriteOperationError(IOManagerError):
    """Raised when write operations fail."""

    pass


class ReadOperationError(IOManagerError):
    """Raised when read operations fail."""

    pass
