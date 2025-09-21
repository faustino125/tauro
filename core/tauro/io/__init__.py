"""Tauro IO public API.

This module re-exports the most commonly used IO components for convenience.
"""

from .constants import SupportedFormats, WriteMode
from .exceptions import (
    IOManagerError,
    ConfigurationError,
    DataValidationError,
    FormatNotSupportedError,
    WriteOperationError,
    ReadOperationError,
)
from .validators import ConfigValidator, DataValidator
from .factories import ReaderFactory, WriterFactory
from .input import InputLoader
from .output import (
    OutputManager,
    DataWriter,
    DataFrameConverter,
    PathResolver,
    ModelArtifactManager,
    UnityCatalogManager,
)
from .readers import (
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
from .writers import (
    DeltaWriter,
    ParquetWriter,
    CSVWriter,
    JSONWriter,
    ORCWriter,
)

__all__ = [
    # Enums
    "SupportedFormats",
    "WriteMode",
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
    # Input/Output managers
    "InputLoader",
    "OutputManager",
    # Helpers
    "DataWriter",
    "DataFrameConverter",
    "PathResolver",
    "ModelArtifactManager",
    "UnityCatalogManager",
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
