"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from loguru import logger  # type: ignore

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import col, from_json  # type: ignore
from pyspark.sql.types import StructType  # type: ignore

from core.streaming.constants import STREAMING_FORMAT_CONFIGS, StreamingFormat
from core.streaming.exceptions import (
    StreamingError,
    StreamingFormatNotSupportedError,
)


class _StreamingSparkMixin:
    """Helper mixin to access SparkSession from dict or object context."""

    @property
    def spark(self):
        ctx = getattr(self, "context", None)
        if ctx is None:
            raise StreamingError("Context is not set in streaming reader")
        # context can be a dict-like or an object with .spark
        if isinstance(ctx, dict):
            spark = ctx.get("spark")
        else:
            spark = getattr(ctx, "spark", None)
        if spark is None:
            raise StreamingError("Spark session is not available in context")
        return spark

    def _spark_read_stream(self):
        return self.spark.readStream


class BaseStreamingReader(ABC, _StreamingSparkMixin):
    """Base class for streaming data readers."""

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read streaming data and return a streaming DataFrame."""
        raise NotImplementedError

    def _validate_options(self, config: Dict[str, Any], format_name: str) -> None:
        """Validate required options for the streaming format."""
        try:
            format_config = STREAMING_FORMAT_CONFIGS.get(format_name, {}) or {}
            required_options = format_config.get("required_options", []) or []
            provided_options = config.get("options", {}) or {}

            missing_options = [opt for opt in required_options if provided_options.get(opt) is None]
            if missing_options:
                raise StreamingError(
                    f"Missing required options for {format_name}: {missing_options}"
                )
        except Exception as e:
            logger.error(f"Error validating options for {format_name}: {str(e)}")
            raise

    def _apply_options(self, reader, options: Dict[str, Any]):
        """Safely apply reader.options, skipping None values and casting to str."""
        try:
            for key, value in (options or {}).items():
                if value is None:
                    continue
                reader = reader.option(str(key), str(value))
            return reader
        except Exception as e:
            logger.error(f"Error applying options {list((options or {}).keys())}: {str(e)}")
            raise StreamingError(f"Failed to apply reader options: {str(e)}")

    def _ensure_schema(self, schema_cfg: Any) -> Optional["StructType"]:
        """Accept StructType or DDL string; return StructType or None."""
        if schema_cfg is None:
            return None
        try:
            # If already a StructType, return it
            if isinstance(schema_cfg, StructType):
                return schema_cfg
            # If it's a string, try to parse as DDL
            if isinstance(schema_cfg, str) and schema_cfg.strip():
                from pyspark.sql.types import StructType as _StructType  # type: ignore

                return _StructType.fromDDL(schema_cfg)
            logger.warning("json_schema provided but not a StructType or DDL string; ignoring")
            return None
        except Exception as e:
            logger.error(f"Error parsing json_schema: {str(e)}")
            raise StreamingError(f"Failed to parse json_schema: {str(e)}")


class KafkaStreamingReader(BaseStreamingReader):
    """Streaming reader for Apache Kafka."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from Kafka stream."""
        try:
            self._validate_options(config, StreamingFormat.KAFKA.value)
            options = config.get("options", {}) or {}

            bs = options.get("kafka.bootstrap.servers")
            logger.info(f"Creating Kafka streaming reader (bootstrap servers: {bs})")

            subscription_options = ["subscribe", "subscribePattern", "assign"]
            provided_subscriptions = [opt for opt in subscription_options if opt in options]
            if len(provided_subscriptions) != 1:
                raise StreamingError(
                    f"Exactly one of {subscription_options} must be provided for Kafka stream"
                )

            reader = self._spark_read_stream().format("kafka")
            reader = self._apply_options(reader, options)

            streaming_df = reader.load()

            if config.get("parse_json", False):
                schema = self._ensure_schema(config.get("json_schema"))
                if schema:
                    streaming_df = streaming_df.select(
                        from_json(col("value").cast("string"), schema).alias("data"),
                        col("timestamp"),
                        col("topic"),
                        col("partition"),
                        col("offset"),
                    ).select("data.*", "timestamp", "topic", "partition", "offset")
                else:
                    logger.warning("parse_json=True but no valid json_schema provided")

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating Kafka streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Kafka stream: {str(e)}")


class DeltaStreamingReader(BaseStreamingReader):
    """Streaming reader for Delta Lake change data feed."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        try:
            self._validate_options(config, StreamingFormat.DELTA_STREAM.value)
            options = config.get("options", {}) or {}

            logger.info(f"Creating Delta CDF streaming reader with options: {list(options.keys())}")

            reader = self._spark_read_stream().format("delta")
            reader = self._apply_options(reader, options)

            path = config.get("path")
            if not path:
                raise StreamingError("Delta streaming requires 'path' in config")

            return reader.load(str(path))

        except Exception as e:
            logger.error(f"Error creating Delta streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Delta stream: {str(e)}")


class RateStreamingReader(BaseStreamingReader):
    """Streaming reader for Spark built-in rate source (for testing)."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        try:
            self._validate_options(config, "rate")
            options = config.get("options", {}) or {}

            logger.info(f"Creating Rate streaming reader with options: {list(options.keys())}")

            reader = self._spark_read_stream().format("rate")
            reader = self._apply_options(reader, options)

            return reader.load()

        except Exception as e:
            logger.error(f"Error creating Rate streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Rate stream: {str(e)}")


class StreamingReaderFactory:
    """Factory for creating streaming data readers."""

    def __init__(self, context):
        self.context = context
        self._readers: Dict[str, BaseStreamingReader] = {}
        self._initialize_readers()

    def _initialize_readers(self):
        """Initialize all available readers."""
        try:
            self._readers = {
                StreamingFormat.KAFKA.value: KafkaStreamingReader(self.context),
                StreamingFormat.DELTA_STREAM.value: DeltaStreamingReader(self.context),
                "rate": RateStreamingReader(self.context),
                # Add more readers here as they are implemented (e.g., socket, memory)
            }
            logger.info(
                f"Initialized {len(self._readers)} streaming readers: {list(self._readers.keys())}"
            )
        except Exception as e:
            logger.error(f"Error initializing streaming readers: {str(e)}")
            raise StreamingError(f"Failed to initialize streaming readers: {str(e)}")

    def get_reader(self, format_name: str) -> BaseStreamingReader:
        """Get streaming reader for specified format."""
        try:
            if not format_name or not isinstance(format_name, str):
                raise StreamingError("Format name must be a non-empty string")

            format_key = format_name.lower()
            if format_key not in self._readers:
                supported = list(self._readers.keys())
                raise StreamingFormatNotSupportedError(
                    f"Streaming format '{format_name}' not supported. Supported formats: {supported}"
                )
            return self._readers[format_key]
        except Exception as e:
            logger.error(f"Error getting reader for format '{format_name}': {str(e)}")
            raise

    def list_supported_formats(self) -> list:
        """List all supported streaming input formats."""
        return list(self._readers.keys())

    def validate_format_support(self, format_name: str) -> bool:
        """Check if a format is supported."""
        return isinstance(format_name, str) and format_name.lower() in self._readers

    def register_custom_reader(self, format_name: str, reader_class, *args, **kwargs):
        """Register a custom streaming reader."""
        try:
            if not issubclass(reader_class, BaseStreamingReader):
                raise StreamingError("Custom reader must inherit from BaseStreamingReader")
            reader_instance = reader_class(self.context, *args, **kwargs)
            self._readers[format_name.lower()] = reader_instance
            logger.info(f"Registered custom reader '{format_name}'")
        except Exception as e:
            logger.error(f"Error registering custom reader '{format_name}': {str(e)}")
            raise StreamingError(f"Failed to register custom reader: {str(e)}")
