"""Tauro Streaming public API.
This module re-exports the most commonly used Streaming components for convenience.
"""

# Constants and Enums
from engine.streaming.constants import (
    PipelineType,
    StreamingMode,
    StreamingTrigger,
    StreamingFormat,
    StreamingOutputMode,
    DEFAULT_STREAMING_CONFIG,
    STREAMING_FORMAT_CONFIGS,
    STREAMING_VALIDATIONS,
)

# Exceptions
from engine.streaming.exceptions import (
    StreamingError,
    StreamingValidationError,
    StreamingFormatNotSupportedError,
    StreamingQueryError,
    StreamingPipelineError,
    StreamingConnectionError,
    StreamingConfigurationError,
    StreamingTimeoutError,
    StreamingResourceError,
    handle_streaming_error,
    create_error_context,
)

# Validators
from engine.streaming.validators import StreamingValidator

# Readers
from engine.streaming.readers import (
    BaseStreamingReader,
    KafkaStreamingReader,
    DeltaStreamingReader,
    RateStreamingReader,
    StreamingReaderFactory,
)

# Writers
from engine.streaming.writers import (
    BaseStreamingWriter,
    ConsoleStreamingWriter,
    DeltaStreamingWriter,
    ParquetStreamingWriter,
    KafkaStreamingWriter,
    MemoryStreamingWriter,
    ForeachBatchStreamingWriter,
    JSONStreamingWriter,
    CSVStreamingWriter,
    StreamingWriterFactory,
)

# Managers
from engine.streaming.query_manager import StreamingQueryManager
from engine.streaming.pipeline_manager import StreamingPipelineManager

__all__ = [
    # Constants and Enums
    "PipelineType",
    "StreamingMode",
    "StreamingTrigger",
    "StreamingFormat",
    "StreamingOutputMode",
    "DEFAULT_STREAMING_CONFIG",
    "STREAMING_FORMAT_CONFIGS",
    "STREAMING_VALIDATIONS",
    # Exceptions
    "StreamingError",
    "StreamingValidationError",
    "StreamingFormatNotSupportedError",
    "StreamingQueryError",
    "StreamingPipelineError",
    "StreamingConnectionError",
    "StreamingConfigurationError",
    "StreamingTimeoutError",
    "StreamingResourceError",
    "handle_streaming_error",
    "create_error_context",
    # Validators
    "StreamingValidator",
    # Readers - Base and Factory
    "BaseStreamingReader",
    "StreamingReaderFactory",
    # Readers - Implementations
    "KafkaStreamingReader",
    "DeltaStreamingReader",
    "RateStreamingReader",
    # Writers - Base and Factory
    "BaseStreamingWriter",
    "StreamingWriterFactory",
    # Writers - Implementations
    "ConsoleStreamingWriter",
    "DeltaStreamingWriter",
    "ParquetStreamingWriter",
    "KafkaStreamingWriter",
    "MemoryStreamingWriter",
    "ForeachBatchStreamingWriter",
    "JSONStreamingWriter",
    "CSVStreamingWriter",
    # Managers
    "StreamingQueryManager",
    "StreamingPipelineManager",
]
