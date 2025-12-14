"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TYPE_CHECKING

from loguru import logger  # type: ignore

from pyspark.sql.streaming import DataStreamWriter, StreamingQuery  # type: ignore


from core.streaming.exceptions import StreamingError, StreamingFormatNotSupportedError


class BaseStreamingWriter(ABC):
    """Base class for streaming data writers."""

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write streaming data and return the streaming query."""
        pass

    def _validate_config(
        self, config: Dict[str, Any], required_fields: Optional[list] = None
    ) -> None:
        """Validate basic configuration requirements."""
        if not isinstance(config, dict):
            raise StreamingError("Configuration must be a dictionary")

        req = required_fields or []
        missing_fields = [field for field in req if field not in config]
        if missing_fields:
            raise StreamingError(f"Missing required fields: {missing_fields}")

    def _apply_options(self, writer: DataStreamWriter, options: Dict[str, Any]) -> DataStreamWriter:
        """Apply options to writer with error handling."""
        try:
            for key, value in options.items():
                if value is None:
                    continue
                # Spark expects string values for writer.option
                writer = writer.option(str(key), str(value))
            return writer
        except Exception as e:
            logger.error(f"Error applying options {options}: {str(e)}")
            raise StreamingError(f"Failed to apply writer options: {str(e)}")

    def _apply_common(self, writer: DataStreamWriter, config: Dict[str, Any]) -> DataStreamWriter:
        """
        Apply common streaming settings:
        - outputMode: append|complete|update
        - trigger: {"once": true} | {"processingTime": "10 seconds"} | {"availableNow": true}
        - checkpointLocation: path string
        - queryName: name string
        """
        try:
            options = config.get("options", {}) or {}
            writer = self._apply_options(writer, options)

            output_mode = config.get("outputMode")
            if output_mode:
                writer = writer.outputMode(str(output_mode))

            trigger_cfg = config.get("trigger")
            if trigger_cfg:
                # Pass through supported trigger configs
                # Example: {"once": True} | {"availableNow": True} | {"processingTime": "10 seconds"}
                if isinstance(trigger_cfg, dict):
                    writer = writer.trigger(**trigger_cfg)
                else:
                    # Fallback: if string, assume processing time
                    writer = writer.trigger(processingTime=str(trigger_cfg))

            checkpoint = config.get("checkpointLocation")
            if checkpoint:
                writer = writer.option("checkpointLocation", str(checkpoint))

            query_name = config.get("queryName")
            if query_name:
                writer = writer.queryName(str(query_name))

            return writer
        except Exception as e:
            logger.error(f"Error applying common streaming settings: {str(e)}")
            raise StreamingError(f"Failed to apply common streaming settings: {str(e)}")

    def _apply_partitioning(
        self, writer: DataStreamWriter, config: Dict[str, Any]
    ) -> DataStreamWriter:
        """Apply partitionBy when present."""
        partition_by = config.get("partitionBy")
        if partition_by:
            if isinstance(partition_by, list):
                writer = writer.partitionBy(*partition_by)
            else:
                writer = writer.partitionBy(str(partition_by))
        return writer


class ConsoleStreamingWriter(BaseStreamingWriter):
    """Streaming writer for console output (debugging/testing)."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to console."""
        try:
            self._validate_config(config)
            options = config.get("options", {}) or {}

            logger.info("Starting console streaming writer")

            writer = write_stream.format("console")
            writer = self._apply_common(writer, config)

            if "numRows" not in options:
                writer = writer.option("numRows", "20")
            if "truncate" not in options:
                writer = writer.option("truncate", "false")

            return writer.start()

        except Exception as e:
            logger.error(f"Error starting console streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start console writer: {str(e)}")


class DeltaStreamingWriter(BaseStreamingWriter):
    """Streaming writer for Delta Lake."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to Delta table."""
        try:
            self._validate_config(config, required_fields=["path"])

            path = config.get("path")
            options = config.get("options", {}) or {}

            logger.info(f"Starting Delta streaming writer to path: {path}")

            writer = write_stream.format("delta")
            writer = self._apply_common(writer, config)

            # Delta-specific defaults
            if "mergeSchema" not in options:
                writer = writer.option("mergeSchema", "true")

            writer = self._apply_partitioning(writer, config)

            return writer.start(str(path))

        except Exception as e:
            logger.error(f"Error starting Delta streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start Delta writer: {str(e)}")


class ParquetStreamingWriter(BaseStreamingWriter):
    """Streaming writer for Parquet files."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to Parquet files."""
        try:
            self._validate_config(config, required_fields=["path"])

            path = config.get("path")
            logger.info(f"Starting Parquet streaming writer to path: {path}")

            writer = write_stream.format("parquet")
            writer = self._apply_common(writer, config)
            writer = self._apply_partitioning(writer, config)

            return writer.start(str(path))

        except Exception as e:
            logger.error(f"Error starting Parquet streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start Parquet writer: {str(e)}")


class KafkaStreamingWriter(BaseStreamingWriter):
    """Streaming writer for Apache Kafka."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to Kafka."""
        try:
            self._validate_config(config)
            options = config.get("options", {}) or {}

            required_kafka_options = ["kafka.bootstrap.servers", "topic"]
            missing_options = [opt for opt in required_kafka_options if not options.get(opt)]
            if missing_options:
                raise StreamingError(f"Missing required Kafka options: {missing_options}")

            topic = options.get("topic")
            logger.info(f"Starting Kafka streaming writer to topic: {topic}")

            writer = write_stream.format("kafka")
            writer = self._apply_common(writer, config)

            # Kafka sink does not use partitionBy. Options already applied in _apply_common.
            return writer.start()

        except Exception as e:
            logger.error(f"Error starting Kafka streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start Kafka writer: {str(e)}")


class MemoryStreamingWriter(BaseStreamingWriter):
    """Streaming writer for memory sink (testing)."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to memory sink."""
        try:
            self._validate_config(config)
            options = config.get("options", {}) or {}
            query_name = options.get("queryName") or config.get("queryName") or "memory_query"

            logger.info(f"Starting memory streaming writer with query name: {query_name}")

            writer = write_stream.format("memory").queryName(str(query_name))
            writer = self._apply_common(writer, config)

            return writer.start()

        except Exception as e:
            logger.error(f"Error starting memory streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start memory writer: {str(e)}")


class ForeachBatchStreamingWriter(BaseStreamingWriter):
    """Streaming writer using foreachBatch for custom processing."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write using foreachBatch."""
        try:
            self._validate_config(config, required_fields=["batch_function"])

            batch_function = config.get("batch_function")
            logger.info("Starting foreachBatch streaming writer")

            func = self._load_batch_function(batch_function)

            writer = write_stream.foreachBatch(func)
            writer = self._apply_common(writer, config)

            return writer.start()

        except Exception as e:
            logger.error(f"Error starting foreachBatch streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start foreachBatch writer: {str(e)}")

    def _load_batch_function(self, batch_function):
        """Load and validate batch processing function."""
        try:
            if isinstance(batch_function, dict):
                module_path = batch_function.get("module")
                function_name = batch_function.get("function")

                if not module_path or not function_name:
                    raise StreamingError("batch_function must specify 'module' and 'function'")

                import importlib

                module = importlib.import_module(module_path)

                if not hasattr(module, function_name):
                    raise StreamingError(
                        f"Function '{function_name}' not found in module '{module_path}'"
                    )

                func = getattr(module, function_name)

                if not callable(func):
                    raise StreamingError(f"'{function_name}' is not callable")

                return func

            elif callable(batch_function):
                return batch_function
            else:
                raise StreamingError("batch_function must be callable or dict with module/function")

        except Exception as e:
            logger.error(f"Error loading batch function: {str(e)}")
            raise StreamingError(f"Failed to load batch function: {str(e)}")


class JSONStreamingWriter(BaseStreamingWriter):
    """Streaming writer for JSON files."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to JSON files."""
        try:
            self._validate_config(config, required_fields=["path"])

            path = config.get("path")
            logger.info(f"Starting JSON streaming writer to path: {path}")

            writer = write_stream.format("json")
            writer = self._apply_common(writer, config)
            writer = self._apply_partitioning(writer, config)

            return writer.start(str(path))

        except Exception as e:
            logger.error(f"Error starting JSON streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start JSON writer: {str(e)}")


class CSVStreamingWriter(BaseStreamingWriter):
    """Streaming writer for CSV files."""

    def write_stream(
        self, write_stream: DataStreamWriter, config: Dict[str, Any]
    ) -> StreamingQuery:
        """Write to CSV files."""
        try:
            self._validate_config(config, required_fields=["path"])

            path = config.get("path")
            options = config.get("options", {}) or {}

            logger.info(f"Starting CSV streaming writer to path: {path}")

            writer = write_stream.format("csv")
            writer = self._apply_common(writer, config)

            if "header" not in options:
                writer = writer.option("header", "true")

            writer = self._apply_partitioning(writer, config)

            return writer.start(str(path))

        except Exception as e:
            logger.error(f"Error starting CSV streaming writer: {str(e)}")
            raise StreamingError(f"Failed to start CSV writer: {str(e)}")


class StreamingWriterFactory:
    """Factory for creating streaming data writers."""

    def __init__(self, context):
        self.context = context
        self._writers: Dict[str, BaseStreamingWriter] = {}
        self._initialize_writers()

    def _initialize_writers(self):
        """Initialize all available writers."""
        try:
            self._writers = {
                "console": ConsoleStreamingWriter(self.context),
                "delta": DeltaStreamingWriter(self.context),
                "parquet": ParquetStreamingWriter(self.context),
                "kafka": KafkaStreamingWriter(self.context),
                "memory": MemoryStreamingWriter(self.context),
                "foreachbatch": ForeachBatchStreamingWriter(self.context),
                "json": JSONStreamingWriter(self.context),
                "csv": CSVStreamingWriter(self.context),
            }
            logger.info(
                f"Initialized {len(self._writers)} streaming writers: {list(self._writers.keys())}"
            )
        except Exception as e:
            logger.error(f"Error initializing streaming writers: {str(e)}")
            raise StreamingError(f"Failed to initialize streaming writers: {str(e)}")

    def get_writer(self, format_name: str) -> BaseStreamingWriter:
        """Get streaming writer for specified format."""
        try:
            if not format_name or not isinstance(format_name, str):
                raise StreamingError("Format name must be a non-empty string")

            format_key = format_name.lower()

            if format_key not in self._writers:
                supported_formats = list(self._writers.keys())
                raise StreamingFormatNotSupportedError(
                    f"Streaming format '{format_name}' not supported. "
                    f"Supported formats: {supported_formats}"
                )

            return self._writers[format_key]

        except Exception as e:
            logger.error(f"Error getting writer for format '{format_name}': {str(e)}")
            raise

    def list_supported_formats(self) -> list:
        """List all supported streaming output formats."""
        return list(self._writers.keys())

    def validate_format_support(self, format_name: str) -> bool:
        """Check if a format is supported."""
        return isinstance(format_name, str) and format_name.lower() in self._writers

    def register_custom_writer(self, format_name: str, writer_class, *args, **kwargs):
        """Register a custom streaming writer."""
        try:
            if not issubclass(writer_class, BaseStreamingWriter):
                raise StreamingError("Custom writer must inherit from BaseStreamingWriter")

            writer_instance = writer_class(self.context, *args, **kwargs)
            self._writers[format_name.lower()] = writer_instance

            logger.info(f"Registered custom writer '{format_name}'")

        except Exception as e:
            logger.error(f"Error registering custom writer '{format_name}': {str(e)}")
            raise StreamingError(f"Failed to register custom writer: {str(e)}")
