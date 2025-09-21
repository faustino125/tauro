"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from enum import Enum


class SupportedFormats(Enum):
    """Supported data formats."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"
    DELTA = "delta"
    PICKLE = "pickle"
    AVRO = "avro"
    ORC = "orc"
    XML = "xml"
    QUERY = "query"
    UNITY_CATALOG = "unity_catalog"


class WriteMode(Enum):
    """Supported write modes."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"


class ExecutionMode(Enum):
    """Execution modes."""

    LOCAL = "local"
    DISTRIBUTED = "distributed"


DEFAULT_CSV_OPTIONS = {"header": "true"}
DEFAULT_VACUUM_RETENTION_HOURS = 168  # 7 days
MIN_VACUUM_RETENTION_HOURS = 168

# Centralized prefixes for cloud/distributed filesystems
CLOUD_URI_PREFIXES = ("s3://", "abfss://", "gs://", "dbfs:/")
