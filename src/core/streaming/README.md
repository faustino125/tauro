# tauro.streaming

# tauro.streaming

A comprehensive, production-grade streaming module for Apache Spark Structured Streaming. Provides declarative configuration, robust validation, safe error handling, and a complete lifecycle management for reading from streaming sources, applying transformations, and writing results with exactly-once semantics and proper checkpointing.

This module is designed to work seamlessly with:
- A Context object (e.g., from `tauro.config.Context`)
- Context-like dictionaries (e.g., for tests or standalone scripts)
- Optional format policies (`context.format_policy`) for batch/streaming consistency in hybrid pipelines

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture Overview](#architecture-overview)
- [Supported Formats](#supported-streaming-formats)
- [Installation and Requirements](#installation-and-requirements)
- [Configuration Model](#configuration-model)
- [Components Reference](#components-reference)
- [Getting Started](#getting-started)
- [Readers and Sources](#readers-and-sources)
- [Writers and Sinks](#writers-and-sinks)
- [Query Management](#query-management)
- [Pipeline Management](#pipeline-management)
- [Validation System](#validation-system)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [Testing and Development](#testing-and-development)
- [Troubleshooting](#troubleshooting)
- [Extending the Module](#extending-the-module)
- [End-to-End Example](#end-to-end-example)

---

## Key Features

- **Declarative Configuration**: Define streaming pipelines using simple YAML or Python dictionaries
- **Multiple Input Sources**: Kafka, Delta Change Data Feed, File Streams, Kinesis, and testing sources (rate, memory, socket)
- **Flexible Output Sinks**: Delta Lake, Console output, and extensible writer interface
- **Robust Validation**: Comprehensive, actionable validation errors with rich context
- **Lifecycle Management**: `StreamingQueryManager` and `StreamingPipelineManager` handle complex orchestration
- **Smart Defaults**: Sensible defaults for trigger, output mode, checkpoint handling, and watermarking
- **Context Integration**: Works with `context.spark` provided either as an attribute or dictionary key
- **Format Policy Support**: Optional format policy (`context.format_policy`) maintains consistency across batch and streaming
- **Thread-Safe**: Built-in support for concurrent pipeline execution with configurable thread pool
- **Error Enrichment**: Decorated methods automatically wrap exceptions with operation context and component details
- **Production Ready**: Exactly-once semantics, fault tolerance via checkpointing, and graceful shutdown

---

## Architecture Overview

The streaming module is organized around several interconnected layers:

### Core Components

1. **Constants Layer** (`constants.py`)
   - Enum definitions for formats, triggers, output modes
   - Default configurations and format-specific settings
   - Validation constraints and invariants

2. **Exception Layer** (`exceptions.py`)
   - Hierarchical exception classes with rich context
   - Utility decorators for automatic error wrapping
   - Context builders for operation-specific metadata

3. **Reader Layer** (`readers.py`)
   - Abstract base class `BaseStreamingReader`
   - Format-specific implementations (Kafka, Delta CDF, File, Kinesis, Rate, Memory, Socket)
   - Factory pattern for dynamic reader instantiation
   - Automatic schema handling and JSON parsing

4. **Writer Layer** (`writers.py`)
   - Abstract base class `BaseStreamingWriter`
   - Format-specific implementations (Delta, Console)
   - Trigger and output mode configuration
   - Checkpoint management

5. **Validation Layer** (`validators.py`)
   - `StreamingValidator` class for pipeline and node validation
   - Policy-aware format validation
   - Time interval and trigger configuration validation
   - Format-specific option validation

6. **Query Management Layer** (`query_manager.py`)
   - `StreamingQueryManager` for single-node query lifecycle
   - Creates, configures, and starts queries
   - Manages state and query handles
   - Integration with Spark Structured Streaming API

7. **Pipeline Orchestration Layer** (`pipeline_manager.py`)
   - `StreamingPipelineManager` for multi-node workflows
   - Thread pool execution with configurable concurrency
   - Dependency tracking and status monitoring
   - Graceful shutdown with timeout support

### Data Flow

```
Configuration Input
        ↓
    Validation
        ↓
Reader Factory Creates Reader Instance
        ↓
Reader Loads Data from Source
        ↓
Optional Transformations Applied
        ↓
Writer Configures Output
        ↓
Query Started via Spark Structured Streaming
        ↓
Query Status Monitored & Results Written to Sink
```

---

## Installation and Requirements

### Dependencies

```bash
pip install pyspark>=3.2.0
pip install loguru>=0.6.0
pip install pyyaml>=6.0
```

### Optional Dependencies

- For Kafka support: Spark with Kafka connector (included in standard distributions)
- For Delta Lake: `databricks-labs-delta-kernel` or Spark with Delta connector
- For advanced monitoring: Additional Spark metrics configurations

### Spark Session Configuration

Depending on your deployment mode, configure Spark appropriately:

```python
from pyspark.sql import SparkSession

# Local mode (development)
spark = SparkSession.builder \
    .appName("streaming-app") \
    .master("local[*]") \
    .getOrCreate()

# Cluster mode (production)
spark = SparkSession.builder \
    .appName("streaming-app") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .config("spark.sql.streaming.checkpointLocation", "/path/to/checkpoints") \
    .getOrCreate()
```

---

## Supported Streaming Formats

### Input Formats (Readers)

| Format | Source | Status | Common Use Cases |
|--------|--------|--------|------------------|
| `kafka` | Apache Kafka | Stable | Event streaming, log aggregation, real-time data feeds |
| `delta_stream` | Delta Change Data Feed | Stable | CDC pipelines, maintaining audit trails |
| `file_stream` | Cloud or local filesystem | Stable | Batch file ingestion, auto-loaded data |
| `kinesis` | AWS Kinesis | Stable | AWS real-time analytics |
| `rate` | In-memory test source | Stable | Testing, development, performance testing |
| `memory` | In-memory test sink | Stable | Unit testing without external dependencies |
| `socket` | TCP socket | Dev/Test | Network debugging, prototype scenarios |

### Output Formats (Writers)

| Format | Sink | Status | Characteristics |
|--------|------|--------|-----------------|
| `delta` | Delta Lake table | Stable | ACID compliant, schema evolution, time travel |
| `console` | Console output | Stable | Debugging, development, validation |

Note: Additional formats can be implemented by extending `BaseStreamingReader` and `BaseStreamingWriter` and registering them in the respective factories.

---

## Configuration Model

### Streaming Node Structure

A streaming node configuration typically contains these sections:

```yaml
name: Logical identifier for the node
input:
  format: Source format (kafka, delta_stream, etc.)
  path: [Optional] Path for file-based sources
  options: Source-specific options (dict)
  parse_json: [Optional] Parse Kafka value as JSON
  json_schema: [Optional] PySpark StructType for JSON parsing
  watermark: [Optional] Watermarking configuration

transforms:
  - [Optional] List of transformation callables or references

output:
  format: Sink format (delta, console, etc.)
  path: [Optional] Path for Delta Lake sink
  table_name: [Optional] Alternative to path for Delta table
  options: Sink-specific options (dict)

streaming:
  trigger:
    type: Trigger type (processing_time, once, continuous, available_now)
    interval: [Optional] Interval for processing_time triggers
  output_mode: Output mode (append, complete, update)
  checkpoint_location: Path for checkpoint data (required for production)
  query_name: Human-readable query identifier
  watermark: Watermarking configuration
  options: Additional streaming options
```

### Complete Configuration Examples

#### Example 1: Kafka → Delta (Production)

```yaml
name: "events_to_delta"
input:
  format: "kafka"
  options:
    kafka.bootstrap.servers: "broker1:9092,broker2:9092"
    subscribe: "events-topic"
    startingOffsets: "latest"
    failOnDataLoss: "false"
  parse_json: true
  json_schema: ${EVENTS_SCHEMA}  # Provided via Python
  
output:
  format: "delta"
  path: "/mnt/delta/events"
  options:
    mergeSchema: "true"
    
streaming:
  trigger:
    type: "processing_time"
    interval: "30 seconds"
  output_mode: "append"
  checkpoint_location: "/mnt/checkpoints/events_to_delta"
  query_name: "events_to_delta_stream"
```

#### Example 2: Delta CDC → Console (Development/Debug)

```yaml
name: "delta_cdf_debug"
input:
  format: "delta_stream"
  path: "/mnt/delta/source_table"
  options:
    readChangeFeed: "true"
    startingVersion: "latest"
    
output:
  format: "console"
  options:
    numRows: 20
    truncate: false
    
streaming:
  trigger:
    type: "once"  # Bounded query
  output_mode: "append"
  checkpoint_location: "/tmp/checkpoints/debug"
```

#### Example 3: Rate Source → Memory (Testing)

```yaml
name: "rate_test"
input:
  format: "rate"
  options:
    rowsPerSecond: "100"
    rampUpTime: "5s"
    numPartitions: "4"
    
output:
  format: "console"
  options:
    numRows: 5
    
streaming:
  trigger:
    type: "processing_time"
    interval: "10 seconds"
  output_mode: "append"
  checkpoint_location: "/tmp/rate_test"
```

---

## Components Reference

### Constants

Define supported formats, trigger types, and default configurations:

```python
from tauro.streaming.constants import (
    StreamingFormat,
    StreamingTrigger,
    StreamingOutputMode,
    DEFAULT_STREAMING_CONFIG,
    STREAMING_FORMAT_CONFIGS,
)

# Available formats
StreamingFormat.KAFKA
StreamingFormat.DELTA_STREAM
StreamingFormat.FILE_STREAM
StreamingFormat.KINESIS

# Available trigger types
StreamingTrigger.PROCESSING_TIME
StreamingTrigger.ONCE
StreamingTrigger.CONTINUOUS
StreamingTrigger.AVAILABLE_NOW

# Available output modes
StreamingOutputMode.APPEND
StreamingOutputMode.COMPLETE
StreamingOutputMode.UPDATE
```

### Exceptions

Comprehensive exception hierarchy with rich context:

```python
from tauro.streaming.exceptions import (
    StreamingError,           # Base exception
    StreamingValidationError, # Validation failures
    StreamingFormatNotSupportedError,  # Unknown format
    StreamingQueryError,      # Query execution failures
    StreamingPipelineError,   # Pipeline orchestration failures
    handle_streaming_error,   # Decorator for automatic error wrapping
    create_error_context,     # Builder for context metadata
)
```

All exceptions include `.context` dictionary with operation details, component information, and original exception cause.

---

## Getting Started

### Basic Setup with Context

```python
from tauro.config import Context
from tauro.streaming.query_manager import StreamingQueryManager

# 1. Create or load a Context
context = Context.load_from_file("config.yaml")

# 2. Define a streaming node
node_config = {
    "name": "kafka_to_delta",
    "input": {
        "format": "kafka",
        "options": {
            "kafka.bootstrap.servers": "localhost:9092",
            "subscribe": "my-topic",
            "startingOffsets": "latest",
        }
    },
    "output": {
        "format": "delta",
        "path": "/data/delta/my_table",
    },
    "streaming": {
        "trigger": {"type": "processing_time", "interval": "15 seconds"},
        "output_mode": "append",
        "checkpoint_location": "/checkpoints/kafka_to_delta",
        "query_name": "kafka_ingestion",
    }
}

# 3. Create and start query
sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(
    node_config=node_config,
    execution_id="exec-20250101-001",
    pipeline_name="data_ingestion",
)

# 4. Monitor query
print(f"Query {query.id} started: {query.status}")
```

### Basic Setup with Dictionary Context

```python
from tauro.streaming.query_manager import StreamingQueryManager
from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder.appName("streaming").getOrCreate()

# 2. Create dict-based context
context = {
    "spark": spark,
    "output_path": "/data/delta",
}

# 3. Define streaming node (same as above)
node_config = {...}

# 4. Create query manager and start
sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(node_config, execution_id="exec-001")
```

---

## Readers and Sources

### KafkaStreamingReader

Reads from Apache Kafka topics with optional JSON parsing.

**Required Options:**
- `kafka.bootstrap.servers`: Broker addresses (comma-separated)
- Exactly one of: `subscribe`, `subscribePattern`, or `assign`

**Optional Options:**
- `startingOffsets`: `earliest`, `latest` (default: `latest`)
- `endingOffsets`: `latest` (default: `latest`)
- `failOnDataLoss`: `true`/`false` (default: `true`)
- `includeHeaders`: Include Kafka headers in output
- `kafkaConsumer.pollTimeoutMs`: Poll timeout in milliseconds

**JSON Parsing:**

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType()),
])

kafka_config = {
    "name": "kafka_json",
    "input": {
        "format": "kafka",
        "options": {
            "kafka.bootstrap.servers": "broker:9092",
            "subscribe": "events",
        },
        "parse_json": True,
        "json_schema": schema,
    },
    "output": {"format": "console"},
}
```

### DeltaStreamingReader (Change Data Feed)

Reads changes from Delta Lake tables using the Change Data Feed feature.

**Prerequisites:**
- Source table must have CDC enabled: `ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`
- User must have read permissions on the table

**Configuration:**

```python
delta_cdc_config = {
    "name": "delta_cdc",
    "input": {
        "format": "delta_stream",
        "path": "/mnt/delta/source_table",
        "options": {
            "readChangeFeed": "true",
            "startingVersion": "0",  # or "latest"
            "skipChangeCommits": "false",
        },
    },
    "output": {"format": "console"},
}
```

### Rate Source (Testing)

Generates test data at a controlled rate without external dependencies.

**Common Options:**
- `rowsPerSecond`: Rows per second (default: 1)
- `rampUpTime`: Time to reach target rate
- `numPartitions`: Number of partitions

```python
rate_config = {
    "name": "rate_test",
    "input": {
        "format": "rate",
        "options": {
            "rowsPerSecond": "100",
            "numPartitions": "4",
        },
    },
    "output": {"format": "console"},
}
```

### Other Readers

- **File Stream** (`file_stream`): Watch directory for new files
- **Kinesis** (`kinesis`): AWS Kinesis streams
- **Memory** (`memory`): In-memory table (for testing)
- **Socket** (`socket`): TCP socket input (development only)

---

## Writers and Sinks

### DeltaStreamingWriter

Writes query results to Delta Lake with ACID compliance.

**Configuration:**

```python
delta_output = {
    "format": "delta",
    "path": "/mnt/delta/output_table",
    "options": {
        "mergeSchema": "true",
        "checkpointLocation": "/checkpoints/delta_out",
    },
}
```

**Schema Evolution:**
- Set `mergeSchema: true` to automatically handle schema additions
- Delta will track schema changes and migrate existing data

### ConsoleStreamingWriter

Writes to console output for debugging and development.

**Configuration:**

```python
console_output = {
    "format": "console",
    "options": {
        "numRows": 20,
        "truncate": False,
        "mode": "append",  # or "complete", "update"
    },
}
```

---

## Query Management

### StreamingQueryManager

Manages the lifecycle of individual streaming queries.

**Key Methods:**

```python
from tauro.streaming.query_manager import StreamingQueryManager

sqm = StreamingQueryManager(context)

# Create and start a query
query = sqm.create_and_start_query(
    node_config=node_config,
    execution_id="exec-001",
    pipeline_name="ingestion",
)

# Access query info
print(query.id)           # Query ID
print(query.status)       # Status: INITIALIZED, ACTIVE, TERMINATED
print(query.name)         # Query name from config
print(query.lastProgress) # Last progress update
```

**Query Lifecycle:**

1. **Creation**: Validates configuration, instantiates reader and writer
2. **Transformation**: Optional user-defined transformations applied
3. **Configuration**: Trigger, output mode, checkpoint set
4. **Start**: Query begun via `query.start()`
5. **Running**: Query processes data and emits results
6. **Stop** (optional): Query stopped with optional timeout
7. **Cleanup**: Checkpoint and state resources managed

**Checkpoint Location Best Practices:**

```python
# ✅ DO: Use dedicated checkpoint per query
{
    "streaming": {
        "checkpoint_location": "/mnt/checkpoints/events_to_delta"
    }
}

# ❌ DON'T: Share checkpoint between queries
{
    "streaming": {
        "checkpoint_location": "/mnt/checkpoints/shared"  # Wrong!
    }
}
```

---

## Pipeline Management

### StreamingPipelineManager

Orchestrates multi-node streaming pipelines with concurrent execution.

**Key Features:**

- Parallel query execution with configurable thread pool
- Automatic dependency tracking
- Graceful shutdown with timeout
- Status monitoring across all queries

**Usage:**

```python
from tauro.streaming.pipeline_manager import StreamingPipelineManager

# 1. Create manager with max concurrent pipelines
spm = StreamingPipelineManager(
    context=context,
    max_concurrent_pipelines=5,  # Thread pool size
)

# 2. Start pipeline
execution_id = spm.start_pipeline(
    pipeline_name="data_ingestion",
    pipeline_config={
        "type": "streaming",
        "nodes": [
            kafka_to_delta_config,
            delta_to_console_config,
        ],
        "streaming": {
            "trigger": {"type": "processing_time", "interval": "10 seconds"},
        },
    },
)

# 3. Monitor pipeline
status = spm.status(execution_id)
print(status)

# 4. Stop pipeline (if needed)
spm.stop_pipeline(execution_id, timeout=60)

# 5. Get results
results = spm.get_results(execution_id)
```

**Pipeline Configuration Structure:**

```yaml
type: "streaming"  # or "hybrid" for mixed batch/streaming

nodes:
  - name: "node1"
    input: {...}
    output: {...}
    streaming: {...}
    
  - name: "node2"
    input: {...}
    output: {...}
    streaming: {...}

streaming:  # Pipeline-level defaults
  trigger:
    type: "processing_time"
    interval: "10 seconds"
  output_mode: "append"
```

**Concurrent Execution:**

```python
# Max 5 pipelines run concurrently
spm = StreamingPipelineManager(context, max_concurrent_pipelines=5)

# Submit multiple pipelines (will queue if > 5)
exec_ids = []
for i in range(10):
    eid = spm.start_pipeline(f"pipeline_{i}", config)
    exec_ids.append(eid)

# Monitor and retrieve
for eid in exec_ids:
    results = spm.get_results(eid)
```

---

## Validation System

### StreamingValidator

Comprehensive validation at pipeline and node levels.

**Pipeline-Level Validation:**

```python
from tauro.streaming.validators import StreamingValidator

validator = StreamingValidator(format_policy=context.format_policy)

# Validate entire pipeline
pipeline_config = {
    "type": "streaming",
    "nodes": [node1, node2],
}
validator.validate_streaming_pipeline_config(pipeline_config)
```

**Node-Level Validation:**

```python
# Validate individual nodes
for node in pipeline_config["nodes"]:
    validator.validate_streaming_node_config(node)
```

**Format-Specific Validation:**

```python
# Kafka: Must have exactly one subscription method
kafka_config = {
    "format": "kafka",
    "options": {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "topic1",
        # ❌ Don't also use subscribePattern or assign
    }
}

# Delta CDF: Path required
delta_config = {
    "format": "delta_stream",
    "path": "/path/to/table",  # Required
}

# File Stream: Path required
file_config = {
    "format": "file_stream",
    "path": "/path/to/files",  # Required
}
```

**Trigger Validation:**

```python
# Time intervals must be valid and positive
valid_triggers = [
    {"type": "processing_time", "interval": "10 seconds"},
    {"type": "processing_time", "interval": "5 minutes"},
    {"type": "once"},
    {"type": "available_now"},
]

# Invalid intervals
invalid_triggers = [
    {"type": "processing_time", "interval": "0 seconds"},  # ❌ Zero not allowed
    {"type": "processing_time", "interval": "-5 seconds"}, # ❌ Negative not allowed
    {"type": "processing_time", "interval": "400 days"},   # ❌ > 365 days
]
```

---

## Error Handling

### Exception Hierarchy

```
StreamingError (base)
├── StreamingValidationError
├── StreamingFormatNotSupportedError
├── StreamingQueryError
└── StreamingPipelineError
```

### Rich Error Context

All exceptions include a `.context` dictionary:

```python
try:
    sqm.create_and_start_query(bad_config)
except StreamingValidationError as e:
    print(e.context)
    # {
    #     "operation": "validate_streaming_node_config",
    #     "component": "StreamingValidator",
    #     "field": "input.format",
    #     "expected": "str (kafka, delta_stream, ...)",
    #     "actual": "None",
    #     "node_name": "my_node",
    #     "details": "Input format is required"
    # }
```

### Automatic Error Wrapping

Many public methods are decorated with `@handle_streaming_error`:

```python
@handle_streaming_error
def create_and_start_query(self, node_config, ...):
    # Any raised exception automatically wrapped
    # with operation context and component info
```

### Common Error Scenarios

| Error | Cause | Solution |
|-------|-------|----------|
| `StreamingValidationError` | Invalid configuration | Check field types and required options |
| `StreamingFormatNotSupportedError` | Unknown format | Use supported formats from `StreamingFormat` enum |
| `StreamingQueryError` | Query startup fails | Verify Spark config, source connectivity |
| `StreamingPipelineError` | Pipeline orchestration fails | Check node dependencies, checkpoint paths |

---

## Best Practices

### 1. Checkpoint Management

Always use dedicated, durable checkpoint locations:

```python
# ✅ Good: Isolated, persistent checkpoint per query
"checkpoint_location": "/mnt/checkpoints/prod/kafka_to_delta_v2"

# ❌ Bad: Shared checkpoint
"checkpoint_location": "/tmp/checkpoints"

# ❌ Bad: Temporary directory
"checkpoint_location": "/tmp/query_checkpoint"
```

### 2. Trigger Configuration

Match trigger to your data characteristics:

```python
# For continuously arriving data (Kafka)
{"type": "processing_time", "interval": "30 seconds"}

# For bounded backlogs (files, finite datasets)
{"type": "once"}  # Process all available data once

# For near-real-time requirements
{"type": "available_now"}
```

### 3. Kafka Configuration

Use explicit, minimal Kafka options:

```python
# ✅ Good: Explicit configuration
{
    "kafka.bootstrap.servers": "broker1:9092,broker2:9092",
    "subscribe": "events-topic",
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
}

# ❌ Bad: Ambiguous or conflicting
{
    "kafka.bootstrap.servers": "broker1:9092",
    "subscribe": "topic1",
    "subscribePattern": "topic.*",  # Conflicts with subscribe!
    "assign": "{0:[0,1]}",          # Also conflicts!
}
```

### 4. Delta Lake CDC Configuration

Enable CDC on source table before reading:

```sql
-- Enable CDC on source table
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

```python
# Then configure reader
{
    "format": "delta_stream",
    "path": "/path/to/my_table",
    "options": {
        "readChangeFeed": "true",
        "startingVersion": "0",  # Or "latest"
    }
}
```

### 5. Format Policy Integration

Maintain consistency with batch pipelines:

```python
from tauro.config import Context

context = Context(...)  # Includes format_policy

# Create validator with policy
validator = StreamingValidator(format_policy=context.format_policy)

# Validation now checks batch/streaming compatibility
validator.validate_streaming_pipeline_config(pipeline_config)
```

### 6. Error Handling in Production

Implement comprehensive error handling:

```python
from tauro.streaming.exceptions import (
    StreamingError,
    StreamingValidationError,
    StreamingQueryError,
)

try:
    spm = StreamingPipelineManager(context)
    execution_id = spm.start_pipeline(pipeline_name, config)
except StreamingValidationError as e:
    logger.error(f"Validation failed: {e.context}")
    raise
except StreamingQueryError as e:
    logger.error(f"Query failed: {e.context}")
    # Implement retry logic or alerting
except StreamingError as e:
    logger.error(f"Streaming error: {e.context}")
    raise
```

### 7. Watermarking for Late Data

Configure watermarks to handle late-arriving data:

```python
node_config = {
    "input": {
        "format": "kafka",
        "watermark": {
            "column": "event_time",
            "delay": "10 seconds",  # Allow 10 seconds of lateness
        }
    },
    "output": {"format": "delta"},
}
```

---

## Testing and Development

### Using Rate Source for Testing

```python
from pyspark.sql import SparkSession
from tauro.streaming.query_manager import StreamingQueryManager

spark = SparkSession.builder.appName("test").getOrCreate()
context = {"spark": spark}

# Generate test data at 100 rows/second
test_config = {
    "name": "rate_test",
    "input": {
        "format": "rate",
        "options": {
            "rowsPerSecond": "100",
            "numPartitions": "4",
        }
    },
    "output": {
        "format": "console",
        "options": {"numRows": 5},
    },
    "streaming": {
        "trigger": {"type": "processing_time", "interval": "5 seconds"},
        "checkpoint_location": "/tmp/test_checkpoint",
    }
}

sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(test_config)

# Let it run for testing
import time
time.sleep(30)  # Run for 30 seconds
query.stop()
```

### Unit Testing with Mocking

```python
import pytest
from unittest.mock import Mock, patch
from tauro.streaming.validators import StreamingValidator

@pytest.fixture
def validator():
    return StreamingValidator()

def test_valid_kafka_config(validator):
    config = {
        "name": "test",
        "input": {
            "format": "kafka",
            "options": {
                "kafka.bootstrap.servers": "broker:9092",
                "subscribe": "topic",
            }
        }
    }
    # Should not raise
    validator.validate_streaming_node_config(config)

def test_invalid_kafka_missing_server(validator):
    config = {
        "name": "test",
        "input": {
            "format": "kafka",
            "options": {}  # Missing kafka.bootstrap.servers
        }
    }
    with pytest.raises(StreamingValidationError):
        validator.validate_streaming_node_config(config)
```

---

## Troubleshooting

### Common Issues and Solutions

| Issue | Symptoms | Cause | Solution |
|-------|----------|-------|----------|
| Spark is None | `AttributeError: 'NoneType' object` | Context missing `spark` attribute | Ensure context has `spark` attribute or dict key |
| Kafka connection fails | `KafkaConsumer timeout` | Broker unreachable | Verify `kafka.bootstrap.servers` and network connectivity |
| Kafka subscription error | `NotSerializableException` | Multiple subscription methods | Use only one of: `subscribe`, `subscribePattern`, `assign` |
| Delta CDF not available | `AnalysisException: ... readChangeFeed` | CDC not enabled | Run `ALTER TABLE tbl SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` |
| Checkpoint locked error | `FileAlreadyExistsException` | Query already running | Verify only one query uses each checkpoint location |
| Memory issues | `OutOfMemory` on Executor | High batch sizes | Reduce `maxRatePerPartition` or `rowsPerSecond` |
| Slow query startup | Long initialization time | Complex parsing or large schemas | Simplify JSON schema or reduce initial data volume |

### Debug Mode

Enable verbose logging:

```python
import logging
from loguru import logger

logger.enable("tauro.streaming")
logger.add(sys.stderr, level="DEBUG")
```

### Monitoring Query Progress

```python
from tauro.streaming.query_manager import StreamingQueryManager

sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(config)

# Monitor in a loop
import time
while query.isActive:
    progress = query.lastProgress
    print(f"Processed: {progress['numInputRows']}, "
          f"Batch: {progress['batchId']}, "
          f"Duration: {progress['durationMs']}ms")
    time.sleep(10)
```

---

## Extending the Module

### Adding a Custom Reader

```python
from tauro.streaming.readers import BaseStreamingReader

class CustomReader(BaseStreamingReader):
    """Custom reader for MyDataSource."""
    
    def __init__(self, context):
        super().__init__(context)
    
    def read(self, **options):
        """Return a streaming DataFrame from custom source."""
        spark = self.context.spark if hasattr(self.context, 'spark') else self.context['spark']
        
        # Your implementation
        df = spark.readStream \
            .format("custom-format") \
            .options(**options) \
            .load()
        
        return df

# Register in StreamingReaderFactory
from tauro.streaming.readers import StreamingReaderFactory
StreamingReaderFactory.register("custom_format", CustomReader)
```

### Adding a Custom Writer

```python
from tauro.streaming.writers import BaseStreamingWriter

class CustomWriter(BaseStreamingWriter):
    """Custom writer for MyDataSink."""
    
    def write(self, df, **options):
        """Write streaming DataFrame to custom sink."""
        query = df.writeStream \
            .format("custom-sink") \
            .options(**options) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        return query

# Register in StreamingWriterFactory
from tauro.streaming.writers import StreamingWriterFactory
StreamingWriterFactory.register("custom_sink", CustomWriter)
```

### Adding Format-Specific Validation

Update `STREAMING_FORMAT_CONFIGS` in `constants.py`:

```python
STREAMING_FORMAT_CONFIGS["custom_format"] = {
    "required_options": ["required_option_1"],
    "optional_options": {
        "optional_option_1": "default_value",
        "optional_option_2": None,
    },
}
```

Then enhance `StreamingValidator`:

```python
def _validate_custom_format_options(self, options):
    # Your custom validation logic
    pass
```

---

## End-to-End Example

Complete example: Reading from Kafka, transforming, writing to Delta Lake.

```python
from tauro.config import Context
from tauro.streaming.pipeline_manager import StreamingPipelineManager
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Setup Context
context = Context(...)  # Loaded from config

# 2. Define schema for JSON parsing
event_schema = StructType([
    StructField("user_id", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", LongType()),
])

# 3. Define transformation function
def parse_and_enrich(df):
    """Parse JSON and add computed columns."""
    df = df.select(
        from_json(col("value"), event_schema).alias("data")
    ).select("data.*")
    
    df = df.withColumn(
        "processed_at",
        current_timestamp()
    )
    
    return df

# 4. Configure pipeline
pipeline_config = {
    "type": "streaming",
    "nodes": [
        {
            "name": "kafka_input",
            "input": {
                "format": "kafka",
                "options": {
                    "kafka.bootstrap.servers": "localhost:9092",
                    "subscribe": "events",
                    "startingOffsets": "latest",
                },
                "parse_json": True,
                "json_schema": event_schema,
                "watermark": {
                    "column": "timestamp",
                    "delay": "10 seconds",
                }
            },
            "transforms": [parse_and_enrich],
            "output": {
                "format": "delta",
                "path": "/mnt/delta/events",
            },
            "streaming": {
                "trigger": {"type": "processing_time", "interval": "30 seconds"},
                "output_mode": "append",
                "checkpoint_location": "/mnt/checkpoints/kafka_events",
                "query_name": "kafka_events_stream",
            }
        }
    ]
}

# 5. Start pipeline
spm = StreamingPipelineManager(context, max_concurrent_pipelines=2)
execution_id = spm.start_pipeline(
    pipeline_name="event_ingestion",
    pipeline_config=pipeline_config,
)

# 6. Monitor results
import time
while True:
    status = spm.status(execution_id)
    print(f"Pipeline status: {status}")
    time.sleep(60)
```

---

## Architecture Improvements

Recent enhancements to the streaming module:

- **Enhanced Error Context**: All exceptions now include rich metadata about operation, component, field, and expected/actual values
- **Thread-Safe Managers**: Pipeline manager uses thread pools for safe concurrent execution
- **Format Policy Integration**: Seamless integration with batch format policies for hybrid pipelines
- **Comprehensive Validation**: Multi-layer validation (schema, options, format-specific rules)
- **Watermark Support**: Built-in watermarking for handling late-arriving data
- **Production Defaults**: Sensible defaults aligned with production best practices

---

## API Quick Reference

### Key Classes

```python
# Query Management
from tauro.streaming.query_manager import StreamingQueryManager
sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(node_config, execution_id, pipeline_name)

# Pipeline Management
from tauro.streaming.pipeline_manager import StreamingPipelineManager
spm = StreamingPipelineManager(context, max_concurrent_pipelines=5)
exec_id = spm.start_pipeline(pipeline_name, pipeline_config)
status = spm.status(exec_id)

# Validation
from tauro.streaming.validators import StreamingValidator
validator = StreamingValidator(format_policy=context.format_policy)
validator.validate_streaming_pipeline_config(pipeline_config)

# Readers
from tauro.streaming.readers import StreamingReaderFactory
reader = StreamingReaderFactory.create(format, context)
df = reader.read(**options)

# Writers
from tauro.streaming.writers import StreamingWriterFactory
writer = StreamingWriterFactory.create(sink_format, context)
query = writer.write(df, **options)
```

### Common Methods

| Method | Purpose | Returns |
|--------|---------|---------|
| `create_and_start_query()` | Create and start single query | Query object |
| `start_pipeline()` | Start multi-node pipeline | Execution ID |
| `status()` | Get pipeline status | Status dict |
| `stop_pipeline()` | Stop running pipeline | Boolean |
| `get_results()` | Get pipeline results | Results dict |
| `validate_streaming_pipeline_config()` | Validate pipeline | None (raises on error) |
| `validate_streaming_node_config()` | Validate node | None (raises on error) |

---

## License

Copyright (c) 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.

---

## Notes

- The streaming module does not manage cross-dependencies with batch nodes. When using hybrid pipelines, ensure orchestrators (in `tauro.exec`) coordinate sequencing appropriately.
- Checkpoint locations must be accessible from all Spark executors. Use distributed storage (HDFS, S3, ADLS) for production deployments.
- Use a centralized `format_policy` via `Context` to maintain batch/streaming compatibility throughout your project.
- For advanced Spark tuning, consult the [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).

````

This module is designed to work with both:
- A Context object (e.g., from `tauro.config.Context`)
- Context-like dicts (e.g., for tests or simple scripts)

It also integrates with an optional format policy (`context.format_policy`) to keep batch/streaming compatibility in check when used alongside hybrid pipelines.

---

## Key Features

- Declarative streaming configuration (inputs, transformations, outputs)
- Readers for Kafka and Delta Change Data Feed (plus testing readers like rate/memory)
- Writers for Delta and Console (extensible to new sinks)
- Robust validation with actionable error messages
- Managed lifecycle via StreamingQueryManager and StreamingPipelineManager
- Sensible defaults (trigger, output mode, checkpoint handling)
- Context-aware: works with `context.spark` provided as attribute or key

---

## Components Overview

- Constants
  - StreamingFormat, StreamingTrigger, StreamingOutputMode, PipelineType
  - DEFAULT_STREAMING_CONFIG
  - STREAMING_FORMAT_CONFIGS (required/optional options per source)
  - STREAMING_VALIDATIONS (limits and invariants)
- Exceptions
  - StreamingError (base, with enhanced context)
  - StreamingValidationError
  - StreamingFormatNotSupportedError
  - StreamingQueryError
  - StreamingPipelineError
  - Utilities: `handle_streaming_error` decorator, `create_error_context` builder
- Readers
  - KafkaStreamingReader
  - DeltaStreamingReader
  - Factory: StreamingReaderFactory (maps format -> reader)
- Writers
  - ConsoleStreamingWriter
  - DeltaStreamingWriter
  - Factory: StreamingWriterFactory (maps sink -> writer)
- Validators
  - StreamingValidator (pipeline- and node-level validation)
- Managers
  - StreamingQueryManager (single-node query lifecycle)
  - StreamingPipelineManager (orchestrates multi-node streaming pipelines)

---

## Supported Streaming Formats

Inputs (via Readers):
- kafka
- delta_stream (Delta Change Data Feed)
- file_stream (planned/optional)
- kinesis (planned/optional)
- socket (testing/dev)
- rate (testing)
- memory (testing)

Outputs (via Writers):
- delta
- console

Note: Additional formats/sinks can be implemented by adding new Reader/Writer classes and updating the respective factories.

---

## Configuration Model

A streaming node configuration generally contains:
- name: Logical node name.
- input: Input source config (format, options, optional watermark).
- transforms: Optional list of transformation callables or references (pipeline-specific).
- output: Sink config (format, path/schema/table_name, options).
- streaming: Node-level streaming parameters (trigger, output mode, checkpoint location, etc.).

Example (Kafka -> Delta):

```yaml
name: "events_to_delta"
input:
  format: "kafka"
  options:
    kafka.bootstrap.servers: "broker:9092"
    subscribe: "events"
    startingOffsets: "latest"
  parse_json: true
  json_schema: ${PYTHON_SCHEMA_OBJECT}  # provided via code (not YAML)
output:
  format: "delta"
  path: "/mnt/delta/events_cdf"
  options:
    mergeSchema: "true"
streaming:
  trigger:
    type: "processing_time"
    interval: "30 seconds"
  output_mode: "append"
  checkpoint_location: "/mnt/checkpoints/events_to_delta"
  query_name: "events_to_delta_query"
```

Example (Delta CDF -> Console):

```yaml
name: "delta_cdf_debug"
input:
  format: "delta_stream"
  path: "/mnt/delta/events"
  options:
    readChangeFeed: "true"
    startingVersion: "latest"
output:
  format: "console"
  options:
    numRows: 20
    truncate: false
streaming:
  trigger:
    type: "once"
  output_mode: "append"
```

---

## Readers

### KafkaStreamingReader

- Required options:
  - `kafka.bootstrap.servers`
  - Exactly one of: `subscribe`, `subscribePattern`, `assign`
- Optional options (common examples):
  - `startingOffsets`, `endingOffsets`, `failOnDataLoss`, `includeHeaders`
- Parsing:
  - If `parse_json: true` and `json_schema` supplied, the reader extracts `value` as JSON into columns.

Example:

```python
node_config = {
    "name": "events",
    "input": {
        "format": "kafka",
        "options": {
            "kafka.bootstrap.servers": "broker:9092",
            "subscribe": "events",
            "startingOffsets": "latest",
        },
        "parse_json": True,
        "json_schema": my_structtype_schema,
    },
    "output": {"format": "console"},
}
```

### DeltaStreamingReader (Delta CDF)

- Requires a `path` (Delta table location)
- Common options:
  - `readChangeFeed: "true"`
  - `startingVersion`: `"latest"` or a numeric version
  - `endingVersion`: optional

Example:

```python
node_config = {
    "name": "cdf_insights",
    "input": {
        "format": "delta_stream",
        "path": "/mnt/delta/my_table",
        "options": {
            "readChangeFeed": "true",
            "startingVersion": "latest",
        },
    },
    "output": {"format": "console"},
}
```

---

## Writers

### DeltaStreamingWriter

- Requires `path` for the sink
- Applies options and defaults
  - Default: `mergeSchema: "true"` if not provided
- Uses Spark Structured Streaming `start(path)` to begin the query

### ConsoleStreamingWriter

- Debug/testing sink
- Options:
  - `numRows` (default 20)
  - `truncate` (default False)

---

## StreamingQueryManager

Creates and starts a streaming query from a node configuration:
1. Validates node config with `StreamingValidator.validate_streaming_node_config()`
2. Loads input with the appropriate reader
3. Applies optional transformations (if defined in the node config)
4. Configures trigger, output mode, query name, checkpoint location
5. Creates writer and starts the query

Basic usage:

```python
from tauro.streaming.query_manager import StreamingQueryManager

sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(
    node_config=my_node_config,
    execution_id="exec-123",
    pipeline_name="streaming_pipeline",
)

# You can await or monitor the query
print(query.id, query.status)
```

Checkpoint Location:
- Provide `streaming.checkpoint_location` in the node config for a deterministic location.
- If not provided, you can derive one from context (e.g., `context.output_path`) before starting.

Watermarking:
- Supported via `input.watermark` within the node config.
- Ensure the watermark column exists and matches event-time semantics.

---

## StreamingPipelineManager

Coordinates multi-node streaming pipelines, handling:
- Validation of pipeline-level config
- Creating and starting each node’s streaming query in a managed thread pool
- Tracking status, errors, query handles, and execution metadata
- Graceful shutdown via stop semantics

Basic usage:

```python
from tauro.streaming.pipeline_manager import StreamingPipelineManager

spm = StreamingPipelineManager(context, max_concurrent_pipelines=3)

execution_id = spm.start_pipeline(
    pipeline_name="events_ingestion",
    pipeline_config={
        "type": "streaming",
        "nodes": [ node_config1, node_config2 ],
        "streaming": {
            "trigger": {"type": "processing_time", "interval": "10 seconds"},
            "output_mode": "append",
        },
    },
)

# Inspect running pipelines
print(spm.status(execution_id))
# Stop if needed
spm.stop_pipeline(execution_id, timeout=60)
```

Notes:
- `StreamingPipelineManager` uses a `ThreadPoolExecutor` under the hood with `max_concurrent_pipelines`.
- Validates pipeline shape and node configs using `StreamingValidator`.

---

## Validation

`StreamingValidator` performs comprehensive checks:
- Pipeline-level:
  - type must be one of `streaming` or `hybrid` (when streaming present)
  - `nodes` must be a non-empty list
- Node-level:
  - `input.format` must be supported
  - Required options per format (e.g., Kafka, Delta CDF)
  - Kafka: exactly one of `subscribe`, `subscribePattern`, `assign`
  - Optional policy-based checks (if `context.format_policy` is available)

Usage:

```python
from tauro.streaming.validators import StreamingValidator

validator = StreamingValidator(format_policy=getattr(context, "format_policy", None))
validator.validate_streaming_pipeline_config(pipeline_config)
for node in pipeline_config["nodes"]:
    if isinstance(node, dict):
        validator.validate_streaming_node_config(node)
```

Validation errors raise `StreamingValidationError` with enriched context (field, expected, actual).

---

## Error Handling

The module uses structured exceptions with rich context:
- StreamingError (base)
- StreamingValidationError
- StreamingFormatNotSupportedError
- StreamingQueryError (query lifecycle failures)
- StreamingPipelineError (pipeline lifecycle failures)

Patterns:
- Many public methods are decorated with `@handle_streaming_error`, converting unexpected exceptions into the appropriate streaming exception with contextual data (operation, component, node/pipeline).
- `create_error_context` is used to attach operation-specific metadata (e.g., `pipeline_name`, `execution_id`, `node_name`).

Troubleshooting:
- Inspect the exception message and `.context` dict for details.
- Use `.get_full_traceback()` where available to dump cause chains.

---

## Context Integration

Readers access Spark via `context.spark`, where:
- `context` can be an object with a `spark` attribute (e.g., from `tauro.config.Context`)
- or a dict-style context with a `"spark"` key (commonly used in tests)

The managers also try to use `context.format_policy` (if available) to harmonize with batch/streaming rules.

---

## Default Settings and Triggers

From `DEFAULT_STREAMING_CONFIG`:
- trigger:
  - type: `processing_time`
  - interval: `10 seconds`
- output_mode: `append`
- checkpoint_location: `/tmp/checkpoints` (override in production)
- watermark: `{"column": None, "delay": "10 seconds"}`
- options: `{}` (writer options)

Triggers:
- processing_time (interval)
- once
- continuous
- available_now (where supported)

Example (available now):

```yaml
streaming:
  trigger:
    type: "available_now"
  output_mode: "append"
```

---

## Testing and Development

Recommended approaches:
- Use `rate` input format to generate test data at a controlled rate.
- Use `memory` or `console` writers for quick feedback loops.
- Provide small schemas and simple transformations to validate the end-to-end setup.

Example (rate -> console):

```python
node_config = {
    "name": "rate_debug",
    "input": {
        "format": "rate",
        "options": {"rowsPerSecond": 5},
    },
    "output": {"format": "console", "options": {"numRows": 5, "truncate": False}},
    "streaming": {
        "trigger": {"type": "processing_time", "interval": "5 seconds"},
        "output_mode": "append",
        "query_name": "rate_debug_query",
        "checkpoint_location": "/tmp/checkpoints/rate_debug",
    },
}
```

---

## Best Practices

- Always set a dedicated `checkpoint_location` per node for exactly-once semantics and fault tolerance.
- Keep Kafka options minimal and explicit; ensure only one subscription method is used.
- For Delta CDF, ensure the table has change data feed enabled and permissions set.
- Use `available_now` or `once` triggers for ingesting bounded backlogs where appropriate.
- Propagate a coherent format policy across Context and streaming validators for consistency with batch pipelines.

---

## Minimal End-to-End Example

```python
from tauro.config import Context
from tauro.streaming.query_manager import StreamingQueryManager

# 1) Build a context (could also be a dict with 'spark')
context = Context(...)

# 2) Define a streaming node (Kafka -> Delta)
node = {
    "name": "events_to_delta",
    "input": {
        "format": "kafka",
        "options": {
            "kafka.bootstrap.servers": "broker:9092",
            "subscribe": "events",
            "startingOffsets": "latest",
        },
        "parse_json": True,
        "json_schema": schema,  # Provide a pyspark.sql.types.StructType
    },
    "output": {
        "format": "delta",
        "path": "/mnt/delta/events",
        "options": {"mergeSchema": "true"},
    },
    "streaming": {
        "trigger": {"type": "processing_time", "interval": "15 seconds"},
        "output_mode": "append",
        "checkpoint_location": "/mnt/checkpoints/events_to_delta",
        "query_name": "events_to_delta_q",
    },
}

# 3) Start the query
sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(node, execution_id="exec-001", pipeline_name="ingestion")
print(f"Started query id={query.id}, name={query.name}")
```

---

## Extending the Module

- Add a new Reader
  - Implement a subclass of `BaseStreamingReader`
  - Register it in `StreamingReaderFactory` mapped from a new `StreamingFormat` or string format
- Add a new Writer
  - Implement a subclass of `BaseStreamingWriter`
  - Register it in `StreamingWriterFactory`

Make sure to:
- Update `STREAMING_FORMAT_CONFIGS` with required/optional options (for readers)
- Enhance `StreamingValidator` to recognize format-specific rules as needed
- Add targeted tests covering normal and invalid configurations

---

## Troubleshooting

- Spark is None
  - Ensure your context exposes `spark` (either as an attribute or dict key).
- Kafka subscription error
  - Provide exactly one of `subscribe`, `subscribePattern`, or `assign`.
- Delta CDF read fails
  - Verify `readChangeFeed` is enabled on the table and that `path` is correct and accessible.
- Writer fails to start
  - Check `checkpoint_location` and path permissions. Ensure the sink path exists or is creatable.
- Validation errors
  - Review exception context (field, expected, actual). Many methods are decorated to include operation and node/pipeline names.

---

## Notes

- The streaming module does not directly manage cross-dependencies with batch nodes; when using hybrid pipelines, ensure orchestrators (in `tauro.exec`) coordinate sequencing appropriately.
- Use a centralized `format_policy` via `Context` to keep batch/streaming compatibility consistent throughout your project.

---
