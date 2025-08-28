"""Tauro Streaming public API."""

from .constants import (
    PipelineType,
    StreamingMode,
    StreamingTrigger,
    StreamingFormat,
    StreamingOutputMode,
    DEFAULT_STREAMING_CONFIG,
    STREAMING_FORMAT_CONFIGS,
    STREAMING_VALIDATIONS,
)
from .exceptions import (
    StreamingError,
    StreamingValidationError,
    StreamingFormatNotSupportedError,
    StreamingQueryError,
    StreamingPipelineError,
    StreamingConnectionError,
    StreamingConfigurationError,
    StreamingTimeoutError,
    StreamingResourceError,
)
from .validators import StreamingValidator
from .readers import (
    StreamingReaderFactory,
    KafkaStreamingReader,
    DeltaStreamingReader,
)
from .writers import (
    StreamingWriterFactory,
    ConsoleStreamingWriter,
    DeltaStreamingWriter,
    ParquetStreamingWriter,
    KafkaStreamingWriter,
    MemoryStreamingWriter,
    ForeachBatchStreamingWriter,
    JSONStreamingWriter,
    CSVStreamingWriter,
)
from .query_manager import StreamingQueryManager
from .pipeline_manager import StreamingPipelineManager

__all__ = [
    # Constants and enums
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
    # Validator
    "StreamingValidator",
    # Readers
    "StreamingReaderFactory",
    "KafkaStreamingReader",
    "DeltaStreamingReader",
    # Writers
    "StreamingWriterFactory",
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
