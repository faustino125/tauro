"""Tauro IO public API.
This module re-exports the most commonly used IO components for convenience.
"""

from engine.io.constants import (
    SupportedFormats,
    WriteMode,
    ExecutionMode,
    DEFAULT_ENCODING,
    DEFAULT_CSV_OPTIONS,
    DEFAULT_VACUUM_RETENTION_HOURS,
    MIN_VACUUM_RETENTION_HOURS,
    CLOUD_URI_PREFIXES,
)
from engine.io.exceptions import (
    IOManagerError,
    ConfigurationError,
    DataValidationError,
    FormatNotSupportedError,
    WriteOperationError,
    ReadOperationError,
)
from engine.io.validators import ConfigValidator, DataValidator
from engine.io.factories import ReaderFactory, WriterFactory
from engine.io.base import BaseIO
from engine.io.context_manager import ContextManager
from engine.io.sql import SQLSanitizer
from engine.io.input import InputLoader, InputLoadingStrategy, SequentialLoadingStrategy
from engine.io.output import (
    DataFrameManager,
    PathManager,
    SqlSafetyMixin,
    UnityCatalogManager,
    DataOutputManager,
    PathComponents,
    UnityCatalogConfig,
    is_cloud_path,
    join_cloud_path,
    parse_iso_datetime,
    validate_date_range,
)
from engine.io.readers import (
    ParquetReader,
    JSONReader,
    CSVReader,
    DeltaReader,
    PickleReader,
    AvroReader,
    ORCReader,
    XMLReader,
    QueryReader,
)
from engine.io.writers import (
    DeltaWriter,
    ParquetWriter,
    CSVWriter,
    JSONWriter,
    ORCWriter,
)

__all__ = [
    # Constants
    "SupportedFormats",
    "WriteMode",
    "ExecutionMode",
    "DEFAULT_ENCODING",
    "DEFAULT_CSV_OPTIONS",
    "DEFAULT_VACUUM_RETENTION_HOURS",
    "MIN_VACUUM_RETENTION_HOURS",
    "CLOUD_URI_PREFIXES",
    # Exceptions
    "IOManagerError",
    "ConfigurationError",
    "DataValidationError",
    "FormatNotSupportedError",
    "WriteOperationError",
    "ReadOperationError",
    # Validators
    "ConfigValidator",
    "DataValidator",
    # Factories
    "ReaderFactory",
    "WriterFactory",
    # Base and Core
    "BaseIO",
    "ContextManager",
    "SQLSanitizer",
    # Input
    "InputLoader",
    "InputLoadingStrategy",
    "SequentialLoadingStrategy",
    # Output - Managers
    "DataFrameManager",
    "PathManager",
    "SqlSafetyMixin",
    "UnityCatalogManager",
    "DataOutputManager",
    # Output - Data classes and utilities
    "PathComponents",
    "UnityCatalogConfig",
    "is_cloud_path",
    "join_cloud_path",
    "parse_iso_datetime",
    "validate_date_range",
    # Readers
    "ParquetReader",
    "JSONReader",
    "CSVReader",
    "DeltaReader",
    "PickleReader",
    "AvroReader",
    "ORCReader",
    "XMLReader",
    "QueryReader",
    # Writers
    "DeltaWriter",
    "ParquetWriter",
    "CSVWriter",
    "JSONWriter",
    "ORCWriter",
]
