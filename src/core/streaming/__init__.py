"""Tauro Streaming public API.
This module re-exports the most commonly used Streaming components for convenience.
"""

# Constants and Enums
from core.streaming.constants import (
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
from core.streaming.exceptions import (
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
from core.streaming.validators import StreamingValidator

# Readers
from core.streaming.readers import (
    BaseStreamingReader,
    KafkaStreamingReader,
    DeltaStreamingReader,
    RateStreamingReader,
    StreamingReaderFactory,
)

# Writers
from core.streaming.writers import (
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
from core.streaming.query_manager import StreamingQueryManager, TransformationRegistry
from core.streaming.pipeline_manager import StreamingPipelineManager, QueryHealthMonitor

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
    # Security and Monitoring
    "TransformationRegistry",
    "QueryHealthMonitor",
]
