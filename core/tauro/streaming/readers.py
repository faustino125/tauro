from abc import ABC, abstractmethod
from typing import Any, Dict

from loguru import logger  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import col, from_json  # type: ignore

from tauro.streaming.constants import STREAMING_FORMAT_CONFIGS, StreamingFormat
from tauro.streaming.exceptions import StreamingError, StreamingFormatNotSupportedError


class _StreamingSparkMixin:
    """Helper mixin to access SparkSession from dict or object context."""

    def _get_spark(self) -> Any:
        ctx = getattr(self, "context", None)
        if isinstance(ctx, dict):
            return ctx.get("spark")
        return getattr(ctx, "spark", None)

    def _spark_read_stream(self) -> Any:
        spark = self._get_spark()
        if spark is None:
            raise StreamingError("Spark session is required for streaming operations")
        return spark.readStream


class BaseStreamingReader(ABC, _StreamingSparkMixin):
    """Base class for streaming data readers."""

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read streaming data and return a streaming DataFrame."""
        pass

    def _validate_options(self, config: Dict[str, Any], format_name: str) -> None:
        """Validate required options for the streaming format."""
        try:
            format_config = STREAMING_FORMAT_CONFIGS.get(format_name, {})
            required_options = format_config.get("required_options", [])
            provided_options = config.get("options", {})

            missing_options = [
                opt for opt in required_options if opt not in provided_options
            ]
            if missing_options:
                raise StreamingError(
                    f"Missing required options for {format_name}: {missing_options}"
                )
        except Exception as e:
            logger.error(f"Error validating options for {format_name}: {str(e)}")
            raise


class KafkaStreamingReader(BaseStreamingReader):
    """Streaming reader for Apache Kafka."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from Kafka stream."""
        try:
            self._validate_options(config, StreamingFormat.KAFKA.value)

            options = config.get("options", {}) or {}

            logger.info(
                f"Creating Kafka streaming reader with bootstrap servers: {options.get('kafka.bootstrap.servers')}"
            )

            subscription_options = ["subscribe", "subscribePattern", "assign"]
            provided_subscriptions = [
                opt for opt in subscription_options if opt in options
            ]

            if len(provided_subscriptions) != 1:
                raise StreamingError(
                    f"Exactly one of {subscription_options} must be provided for Kafka stream"
                )

            reader = self._spark_read_stream().format("kafka")

            for key, value in options.items():
                reader = reader.option(key, str(value))

            streaming_df = reader.load()

            if config.get("parse_json", False):
                schema = config.get("json_schema")
                if schema:
                    streaming_df = streaming_df.select(
                        from_json(col("value").cast("string"), schema).alias("data"),
                        col("timestamp"),
                        col("topic"),
                        col("partition"),
                        col("offset"),
                    ).select("data.*", "timestamp", "topic", "partition", "offset")
                else:
                    logger.warning("parse_json=True but no json_schema provided")

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating Kafka streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Kafka stream: {str(e)}")


class DeltaStreamingReader(BaseStreamingReader):
    """Streaming reader for Delta Lake change data feed."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from Delta table as a stream."""
        try:
            self._validate_options(config, StreamingFormat.DELTA_STREAM.value)

            table_path = config.get("path")
            if not table_path:
                raise StreamingError("Delta stream requires 'path' to be set")

            options = config.get("options", {}) or {}

            logger.info(f"Creating Delta streaming reader from path: {table_path}")

            reader = self._spark_read_stream().format("delta")

            for key, value in options.items():
                reader = reader.option(key, str(value))

            return reader.load(table_path)

        except Exception as e:
            logger.error(f"Error creating Delta streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Delta stream: {str(e)}")
