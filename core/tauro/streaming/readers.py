from abc import ABC, abstractmethod
from typing import Any, Dict

from loguru import logger  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import from_json, col  # type: ignore

from tauro.streaming.constants import STREAMING_FORMAT_CONFIGS, StreamingFormat
from tauro.streaming.exceptions import StreamingFormatNotSupportedError, StreamingError


class BaseStreamingReader(ABC):
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

            options = config.get("options", {})

            logger.info(
                f"Creating Kafka streaming reader with bootstrap servers: {options.get('kafka.bootstrap.servers')}"
            )

            # Validate mutually exclusive options
            subscription_options = ["subscribe", "subscribePattern", "assign"]
            provided_subscriptions = [
                opt for opt in subscription_options if opt in options
            ]

            if len(provided_subscriptions) != 1:
                raise StreamingError(
                    f"Exactly one of {subscription_options} must be provided for Kafka stream"
                )

            # Create streaming DataFrame
            reader = self.context.spark.readStream.format("kafka")

            # Apply all options
            for key, value in options.items():
                reader = reader.option(key, str(value))

            streaming_df = reader.load()

            # Add common Kafka transformations if needed
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
                raise StreamingError("Delta streaming requires 'path' configuration")

            options = config.get("options", {})

            logger.info(f"Creating Delta streaming reader for path: {table_path}")

            # Create streaming DataFrame from Delta table
            reader = self.context.spark.readStream.format("delta")

            # Apply Delta-specific options
            for key, value in options.items():
                reader = reader.option(key, str(value))

            streaming_df = reader.load(table_path)

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating Delta streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Delta stream: {str(e)}")


class FileStreamingReader(BaseStreamingReader):
    """Streaming reader for file-based sources."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from file stream."""
        try:
            self._validate_options(config, StreamingFormat.FILE_STREAM.value)

            options = config.get("options", {})
            path = options.get("path")
            file_format = config.get("file_format", "json")

            if not path:
                raise StreamingError("File streaming requires 'path' in options")

            logger.info(
                f"Creating file streaming reader for path: {path}, format: {file_format}"
            )

            # Create streaming DataFrame
            reader = self.context.spark.readStream.format(file_format)

            # Apply options
            for key, value in options.items():
                if key != "path":  # path is handled separately
                    reader = reader.option(key, str(value))

            streaming_df = reader.load(path)

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating file streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create file stream: {str(e)}")


class KinesisStreamingReader(BaseStreamingReader):
    """Streaming reader for Amazon Kinesis."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from Kinesis stream."""
        try:
            self._validate_options(config, StreamingFormat.KINESIS.value)

            options = config.get("options", {})
            stream_name = options.get("streamName")
            region = options.get("region")

            if not stream_name or not region:
                raise StreamingError(
                    "Kinesis streaming requires 'streamName' and 'region'"
                )

            logger.info(
                f"Creating Kinesis streaming reader for stream: {stream_name}, region: {region}"
            )

            # Create streaming DataFrame
            reader = self.context.spark.readStream.format("kinesis")

            # Apply all options
            for key, value in options.items():
                reader = reader.option(key, str(value))

            streaming_df = reader.load()

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating Kinesis streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create Kinesis stream: {str(e)}")


class SocketStreamingReader(BaseStreamingReader):
    """Streaming reader for socket sources (mainly for testing)."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from socket stream."""
        try:
            options = config.get("options", {})
            host = options.get("host", "localhost")
            port = options.get("port", 9999)

            # Validate port is a number
            try:
                port = int(port)
            except (ValueError, TypeError):
                raise StreamingError(f"Invalid port number: {port}")

            logger.info(f"Creating socket streaming reader for {host}:{port}")

            streaming_df = (
                self.context.spark.readStream.format("socket")
                .option("host", str(host))
                .option("port", port)
                .load()
            )

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating socket streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create socket stream: {str(e)}")


class RateStreamingReader(BaseStreamingReader):
    """Streaming reader for rate source (testing and benchmarking)."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from rate stream."""
        try:
            options = config.get("options", {})

            logger.info("Creating rate streaming reader for testing")

            reader = self.context.spark.readStream.format("rate")

            # Apply rate-specific options with validation
            valid_rate_options = ["rowsPerSecond", "rampUpTime", "numPartitions"]
            for key, value in options.items():
                if key in valid_rate_options:
                    reader = reader.option(key, str(value))
                else:
                    logger.warning(f"Unknown rate option '{key}' ignored")

            streaming_df = reader.load()

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating rate streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create rate stream: {str(e)}")


class MemoryStreamingReader(BaseStreamingReader):
    """Streaming reader for memory source (testing)."""

    def read_stream(self, config: Dict[str, Any]) -> DataFrame:
        """Read from memory stream."""
        try:
            options = config.get("options", {})
            table_name = options.get("tableName")

            if not table_name:
                raise StreamingError("Memory streaming requires 'tableName' in options")

            logger.info(f"Creating memory streaming reader for table: {table_name}")

            streaming_df = (
                self.context.spark.readStream.format("memory")
                .option("tableName", table_name)
                .load()
            )

            return streaming_df

        except Exception as e:
            logger.error(f"Error creating memory streaming reader: {str(e)}")
            raise StreamingError(f"Failed to create memory stream: {str(e)}")


class StreamingReaderFactory:
    """Factory for creating streaming data readers."""

    def __init__(self, context):
        self.context = context
        self._readers = {}
        self._initialize_readers()

    def _initialize_readers(self):
        """Initialize all available readers."""
        try:
            self._readers = {
                StreamingFormat.KAFKA.value: KafkaStreamingReader(self.context),
                StreamingFormat.DELTA_STREAM.value: DeltaStreamingReader(self.context),
                StreamingFormat.FILE_STREAM.value: FileStreamingReader(self.context),
                StreamingFormat.KINESIS.value: KinesisStreamingReader(self.context),
                StreamingFormat.SOCKET.value: SocketStreamingReader(self.context),
                StreamingFormat.RATE.value: RateStreamingReader(self.context),
                StreamingFormat.MEMORY.value: MemoryStreamingReader(self.context),
            }
            logger.info(f"Initialized {len(self._readers)} streaming readers")
        except Exception as e:
            logger.error(f"Error initializing streaming readers: {str(e)}")
            raise StreamingError(f"Failed to initialize streaming readers: {str(e)}")

    def get_reader(self, format_name: str) -> BaseStreamingReader:
        """Get streaming reader for specified format."""
        try:
            if not format_name:
                raise StreamingError("Format name cannot be empty")

            format_key = format_name.lower()

            if format_key not in self._readers:
                supported_formats = list(self._readers.keys())
                raise StreamingFormatNotSupportedError(
                    f"Streaming format '{format_name}' not supported. "
                    f"Supported formats: {supported_formats}"
                )

            return self._readers[format_key]

        except Exception as e:
            logger.error(f"Error getting reader for format '{format_name}': {str(e)}")
            raise

    def list_supported_formats(self) -> list:
        """List all supported streaming formats."""
        return list(self._readers.keys())

    def validate_format_support(self, format_name: str) -> bool:
        """Check if a format is supported."""
        return format_name.lower() in self._readers
