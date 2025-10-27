"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from enum import Enum
from typing import Dict, Any


class PipelineType(Enum):
    """Types of pipelines supported by the framework."""

    BATCH = "batch"
    STREAMING = "streaming"
    HYBRID = "hybrid"  # Para futuros casos de uso mixtos


class StreamingMode(Enum):
    """Streaming execution modes."""

    CONTINUOUS = "continuous"
    MICRO_BATCH = "micro_batch"
    TRIGGER_ONCE = "trigger_once"


class StreamingTrigger(Enum):
    """Streaming trigger types."""

    PROCESSING_TIME = "processing_time"
    ONCE = "once"
    CONTINUOUS = "continuous"
    AVAILABLE_NOW = "available_now"


class StreamingFormat(Enum):
    """Supported streaming data formats."""

    KAFKA = "kafka"
    KINESIS = "kinesis"
    DELTA_STREAM = "delta_stream"
    FILE_STREAM = "file_stream"
    SOCKET = "socket"
    RATE = "rate"  # Para testing
    MEMORY = "memory"  # Para testing


class StreamingOutputMode(Enum):
    """Streaming output modes."""

    APPEND = "append"
    UPDATE = "update"
    COMPLETE = "complete"


# Configuraciones por defecto para streaming
DEFAULT_STREAMING_CONFIG = {
    "trigger": {
        "type": StreamingTrigger.PROCESSING_TIME.value,
        "interval": "10 seconds",
    },
    "output_mode": StreamingOutputMode.APPEND.value,
    "checkpoint_location": "/tmp/checkpoints",
    "query_name": None,
    "watermark": {"column": None, "delay": "10 seconds"},
    "options": {},
}

# Format-specific configurations
STREAMING_FORMAT_CONFIGS: Dict[str, Dict[str, Any]] = {
    StreamingFormat.KAFKA.value: {
        "required_options": ["kafka.bootstrap.servers"],
        "optional_options": {
            "subscribe": None,
            "subscribePattern": None,
            "assign": None,
            "startingOffsets": "latest",
            "endingOffsets": "latest",
            "failOnDataLoss": "true",
            "kafkaConsumer.pollTimeoutMs": "120000",
            "includeHeaders": "false",
        },
    },
    StreamingFormat.DELTA_STREAM.value: {
        "required_options": [],
        "optional_options": {
            "readChangeFeed": "true",
            "startingVersion": "latest",
            "endingVersion": None,
            "skipChangeCommits": "false",
        },
    },
    StreamingFormat.FILE_STREAM.value: {
        "required_options": ["path"],
        "optional_options": {
            "maxFilesPerTrigger": "1000",
            "latestFirst": "false",
            "fileNameOnly": "false",
            "maxFileAge": "7d",
        },
    },
    StreamingFormat.KINESIS.value: {
        "required_options": ["streamName", "region"],
        "optional_options": {
            "initialPosition": "latest",
            "kinesis.executor.maxFetchRecordsPerShard": "100000",
            "kinesis.executor.maxFetchTimeInMs": "1000",
        },
    },
}

# Format-specific validations
STREAMING_VALIDATIONS = {
    "max_checkpoint_age_hours": 24,
    "min_trigger_interval_seconds": 1,
    "max_watermark_delay_minutes": 60,
    "required_streaming_fields": ["format", "trigger"],
    "mutually_exclusive_kafka_options": [["subscribe", "subscribePattern", "assign"]],
}
