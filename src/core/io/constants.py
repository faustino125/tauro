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
# Vacuum retention: Delta Lake enforces minimum 7 days (168 hours) to prevent
# accidental deletion of files that might be read by concurrent queries.
# Lower values require setting spark.databricks.delta.retentionDurationCheck.enabled=false
DEFAULT_VACUUM_RETENTION_HOURS = 168  # 7 days
MIN_VACUUM_RETENTION_HOURS = 168  # Delta Lake enforced minimum
DEFAULT_ENCODING = "UTF-8"  # Default encoding for text file operations

CLOUD_URI_PREFIXES = ("s3://", "abfss://", "gs://", "dbfs:/")
