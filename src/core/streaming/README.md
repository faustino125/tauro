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

## Query Health Monitoring and Failure Detection

### Understanding Query States

Streaming queries can be in several states:

| State | Meaning | Action |
|-------|---------|--------|
| **ACTIVE** | Query running, processing data | Normal operation |
| **STALLED** | Running but no progress for timeout | May need investigation |
| **FAILED** | Query exception or error | Immediate attention required |
| **COMPLETED** | Query gracefully stopped | Normal for bounded queries |

### QueryHealthMonitor: Automatic Health Checks

The `QueryHealthMonitor` continuously checks query health:

```python
from tauro.streaming import QueryHealthMonitor
from pyspark.sql.streaming import StreamingQuery

# Create a health monitor
monitor = QueryHealthMonitor(
    query=spark_query_object,
    query_name="my_query",
    timeout_seconds=300  # Detect stalls after 5 minutes
)

# Check health
is_healthy, error_message = monitor.check_health()

if not is_healthy:
    print(f"Query problem: {error_message}")
    # Examples:
    # "Query failed with exception: org.apache.spark.sql..."
    # "Query stalled: no progress for 325.1s (timeout: 300s)"
```

### What QueryHealthMonitor Detects

```python
# 1. Query Exceptions
# Detects if query encountered an exception and stopped

# 2. Query Stopped
# Detects if query is no longer active (isActive=false)

# 3. Stalled Queries
# Detects if query hasn't processed data in the specified timeout
# (Uses lastProgress.batchId to track activity)

# 4. Distinguishes graceful stops from errors
# - No progress but still active: stalled (timeout issue)
# - No progress and inactive: may be gracefully stopped (OK)
# - Exception available: definitely failed (error)
```

### Monitoring in Pipelines

Health monitoring happens automatically in `StreamingPipelineManager`:

```python
spm = StreamingPipelineManager(context)
exec_id = spm.start_pipeline("pipeline", config)

# Background monitor thread checks health every 5 seconds
# Failed queries are automatically detected and reported

# Get status with health information
while True:
    status = spm.get_pipeline_status(exec_id)
    
    if status["status"] == "error":
        print(f"Pipeline error: {status['error']}")
        break
    
    if status["status"] == "running":
        active = status.get("active_queries", 0)
        total = status.get("total_queries", 0)
        print(f"Progress: {active}/{total} queries active")
    
    import time
    time.sleep(10)
```

### Example: Custom Health Checking

```python
from tauro.streaming import QueryHealthMonitor

def monitor_query_with_alerts(query, query_name, timeout=300, alert_callback=None):
    """Monitor a query and trigger alerts on failures."""
    monitor = QueryHealthMonitor(query, query_name, timeout_seconds=timeout)
    
    import time
    while query.isActive:
        is_healthy, error_message = monitor.check_health()
        
        if not is_healthy:
            if error_message and alert_callback:
                alert_callback(query_name, error_message)
                break
        
        print(f"[{query_name}] Status: {'✓ Healthy' if is_healthy else '✗ Problem'}")
        time.sleep(30)

# Usage with alerting
def send_alert(query_name, error):
    print(f"🚨 ALERT: {query_name} - {error}")
    # Send to Slack, PagerDuty, etc.

query = sqm.create_and_start_query(config)
monitor_query_with_alerts(
    query,
    "events_sink",
    timeout=300,
    alert_callback=send_alert
)
```

### Log Analysis for Failed Queries

```python
# Pipeline manager logs detected failures:
# [ERROR] Pipeline 'exec_123' has failed queries: [('transform', 'org.apache.spark.sql.SparkException: ...')]
# [WARN] Query 'source' stalled: no progress for 325.3s (timeout: 300s)

# In production, monitor logs for these patterns:
import logging
logger = logging.getLogger("tauro.streaming")
logger.setLevel(logging.DEBUG)

# Look for:
# - "has failed queries"
# - "stalled: no progress"
# - "Error monitoring"
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

## Pipeline Dependencies and Node Synchronization

### Why Node Dependencies Matter

In multi-node pipelines, you often need to ensure execution order:
- Node B reads output from Node A → A must complete first
- Node C aggregates from Nodes A and B → both must be running
- Node D depends on transformed output from C → C must be active

The `depends_on` field ensures correct execution order and prevents race conditions.

### Defining Dependencies

Each node can specify `depends_on` to list its dependencies:

```yaml
type: streaming
nodes:
  - name: "source"
    input: { format: "kafka", ... }
    output: { format: "delta", ... }
    # No dependencies - starts first
  
  - name: "transform"
    depends_on: ["source"]  # ← Waits for "source" to be running
    input: { format: "delta", path: "/output/source", ... }
    output: { format: "delta", ... }
  
  - name: "aggregate"
    depends_on: ["transform"]  # ← Waits for "transform"
    input: { format: "delta", path: "/output/transform", ... }
    output: { format: "delta", ... }
  
  - name: "parallel_sink"
    depends_on: ["source"]  # ← Waits for "source", runs in parallel with "transform"
    input: { format: "delta", path: "/output/source", ... }
    output: { format: "delta", ... }
```

### Execution Order Visualization

```
Timeline of execution with dependencies:

Time →
|
├─ [source]════════════════════════════════════════════► (depends on: none)
│     │
│     └─► [transform]════════════════════════════════► (depends on: source)
│              │
│              └─► [aggregate]════════════════════► (depends on: transform)
│
└─► [parallel_sink]════════════════════════════════► (depends on: source, runs in parallel)
```

### Dependency Validation

The `StreamingPipelineManager` automatically validates:

```python
# ✅ Valid: Linear dependency chain
{
    "nodes": [
        {"name": "a", "depends_on": []},
        {"name": "b", "depends_on": ["a"]},
        {"name": "c", "depends_on": ["b"]},
    ]
}

# ✅ Valid: Diamond dependency (multiple parents, single child)
{
    "nodes": [
        {"name": "a", "depends_on": []},
        {"name": "b", "depends_on": []},
        {"name": "c", "depends_on": ["a", "b"]},  # Waits for both
    ]
}

# ❌ Invalid: Circular dependency
{
    "nodes": [
        {"name": "a", "depends_on": ["b"]},
        {"name": "b", "depends_on": ["a"]},  # Circular!
    ]
}
# Raises: StreamingPipelineError - Circular dependency detected

# ❌ Invalid: Non-existent dependency
{
    "nodes": [
        {"name": "a", "depends_on": ["nonexistent"]},  # 'nonexistent' not defined
    ]
}
# Raises: StreamingPipelineError - Dependency not found
```

### Advanced Example: Multi-Source Pipeline

```python
pipeline_config = {
    "type": "streaming",
    "nodes": [
        # Source 1: Kafka events
        {
            "name": "events_kafka",
            "input": {
                "format": "kafka",
                "options": {"subscribe": "events-topic", ...}
            },
            "output": {"format": "delta", "path": "/staging/events", ...},
            "streaming": {"trigger": {"type": "processing_time", "interval": "10s"}, ...}
        },
        
        # Source 2: User master data (Delta)
        {
            "name": "users_delta",
            "input": {
                "format": "delta_stream",
                "path": "/data/users",
            },
            "output": {"format": "delta", "path": "/staging/users", ...},
            "streaming": {"trigger": {"type": "processing_time", "interval": "10s"}, ...}
        },
        
        # Join: Combine events + users
        {
            "name": "enriched_events",
            "depends_on": ["events_kafka", "users_delta"],  # ← Wait for both
            "input": {"format": "delta", "path": "/staging/events", ...},
            "function": {
                "key": "join_with_users"  # Reads /staging/users in transformation
            },
            "output": {"format": "delta", "path": "/processed/enriched", ...},
            "streaming": {"trigger": {"type": "processing_time", "interval": "15s"}, ...}
        },
        
        # Aggregate: Run after join
        {
            "name": "aggregates",
            "depends_on": ["enriched_events"],  # ← Wait for join
            "input": {"format": "delta", "path": "/processed/enriched", ...},
            "output": {"format": "delta", "path": "/analytics/aggregates", ...},
            "streaming": {"trigger": {"type": "processing_time", "interval": "30s"}, ...}
        },
    ]
}

spm = StreamingPipelineManager(context)
exec_id = spm.start_pipeline("analytics", pipeline_config)
```

### Handling Dependency Failures

If a dependency fails, dependent nodes won't start:

```python
status = spm.get_pipeline_status(exec_id)

if status["status"] == "error":
    print(f"Pipeline failed: {status['error']}")
    
    # Check which queries failed
    for query_name, query_status in status.get("query_statuses", {}).items():
        if not query_status.get("isActive"):
            print(f"  - Query '{query_name}' is not running")
            if query_status.get("exception"):
                print(f"    Exception: {query_status['exception']}")
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

## Security and Transformation Management

### TransformationRegistry: Safe Transformation Execution

The `TransformationRegistry` provides a secure, whitelist-based approach to transformation functions, preventing code injection vulnerabilities.

#### Why Whitelist Registration?

Dynamic module loading can expose your system to code injection attacks. The registry ensures:
- ✅ Only pre-registered transformations can be executed
- ✅ No arbitrary code execution from configuration
- ✅ Thread-safe concurrent access
- ✅ Clear audit trail of allowed transformations

#### Registering Transformations

```python
from tauro.streaming import TransformationRegistry

# Define your transformation function
def clean_and_enrich(df, config):
    """Clean and enrich streaming data."""
    from pyspark.sql.functions import col, current_timestamp
    
    df = df.filter(col("value").isNotNull())
    df = df.withColumn("processed_at", current_timestamp())
    
    return df

# Register at startup (before pipelines run)
TransformationRegistry.register("clean_and_enrich", clean_and_enrich)

# List all registered transformations
available = TransformationRegistry.list_transformations()
print(f"Available: {available}")
```

#### Using Registered Transformations

```python
# In your node configuration:
node_config = {
    "name": "transform_events",
    "input": {"format": "kafka", ...},
    "function": {
        "key": "clean_and_enrich"  # ← Reference to registered function
    },
    "output": {"format": "delta", ...},
}
```

#### Legacy Mode (Unsafe, Not Recommended)

For backward compatibility, dynamic module loading is still supported but **disabled by default**:

```python
# To enable dynamic imports (not recommended for production):
import os
os.environ["STREAMING_UNSAFE_IMPORT"] = "true"

# Then use module/function syntax:
node_config = {
    "function": {
        "module": "mymodule",
        "function": "my_transform"
    }
}
```

**⚠️ WARNING**: Dynamic module loading is disabled by default for security. Only enable if absolutely necessary and in controlled environments.

#### Best Practices for Transformations

```python
# ✅ DO: Register transformations at application startup
def startup():
    TransformationRegistry.register("extract_fields", extract_fields)
    TransformationRegistry.register("filter_nulls", filter_nulls)
    TransformationRegistry.register("add_timestamp", add_timestamp)

# ✅ DO: Keep transformations pure (deterministic, no side effects)
def filter_valid_events(df, config):
    return df.filter(df.value > 0)  # Deterministic

# ❌ DON'T: Use random or time-dependent operations
def add_random_id(df, config):
    import random
    # Don't do this - non-deterministic!
    random_id = random.randint(1, 100)
    return df.withColumn("id", random_id)

# ✅ DO: Use config parameter for parameterization
def filter_by_threshold(df, config):
    threshold = config.get("threshold", 0)
    return df.filter(df.value > threshold)

# Usage:
TransformationRegistry.register("filter_by_threshold", filter_by_threshold)
node_config = {
    "function": {
        "key": "filter_by_threshold",
        "params": {"threshold": 100}  # Passed to config
    }
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
| Query fails silently | No error, just stops | Exception in transformation | Enable `DEBUG` logging, check `lastProgress` |
| Stalled query (no progress) | `isActive=true` but no data processed | Timeout, blocking operation, or source empty | Check `QueryHealthMonitor` logs for stall detection |
| Dependency not found | `StreamingPipelineError: ... not defined` | `depends_on` references missing node | Verify all node names in `depends_on` list exist |
| Circular dependency | `StreamingPipelineError: Circular dependency` | Node A depends on B, B depends on A | Audit pipeline DAG, visualize dependencies |
| Query state inconsistent | Pipeline reports running but queries inactive | Race condition in monitoring | Ensure only one pipeline manager per context |
| Transformation fails | `TRANSFORMATION_FAILURE` error | Code error in transformation function | Test transformation separately with sample data |
| JSON parsing fails | `from_json error` | Schema mismatch or malformed JSON | Validate JSON structure, test with `parse_json: false` first |
| Watermark not working | Late data still dropped | Column name mismatch | Ensure watermark column matches actual event time column |

### Query Failure Scenarios

#### Scenario 1: Query Fails During Startup

```python
# Symptoms: Query created but throws exception immediately
try:
    query = sqm.create_and_start_query(config)
except StreamingQueryError as e:
    print(f"Failed to start: {e.context}")
    # Common causes:
    # - Source connection failed (Kafka broker down, permissions)
    # - Schema mismatch or invalid JSON
    # - Writer configuration invalid

# Solution:
# 1. Verify source connectivity
#    kafka: telnet broker 9092
#    delta: ls /path/to/table/_delta_log
#    file: ls /path/to/files

# 2. Test with simpler config (e.g., console writer)
# 3. Check permissions (read source, write checkpoint)
```

#### Scenario 2: Query Starts But Stalls

```python
# Symptoms: Query isActive=true, but lastProgress.batchId not changing
# Detected by: QueryHealthMonitor timeout

# Causes:
# 1. Source has no data → normal, waiting
# 2. Blocking operation in transformation → deadlock
# 3. Executor crash without exception → lost worker
# 4. Network partition → queries continue but can't write

# Solution:
# 1. Check source for data
#    kafka: ./bin/kafka-consumer-groups.sh --describe
#    rate: should always generate data

# 2. Test transformation standalone:
def test_transform():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test").getOrCreate()
    df = spark.range(10).toDF("value")
    result = my_transformation(df, {})
    result.show()

# 3. Check logs for executor failures
# 4. Reduce batch size if memory issues

# Enable QueryHealthMonitor timeout detection:
# Default: 300 seconds, can be configured
monitor = QueryHealthMonitor(query, query_name, timeout_seconds=120)
```

#### Scenario 3: Query Fails With Exception

```python
# Symptoms: lastProgress shows successful batches, then exception
# detected by: QueryHealthMonitor exception detection

# Causes:
# 1. Source connection lost mid-stream
# 2. Malformed data in stream
# 3. Sink (Delta) lock or permission issue
# 4. Executor out of memory

# Solution:
# 1. Check exception message:
exception = query.exception()
if exception:
    print(f"Error: {exception}")
    # Examples:
    # - "org.apache.spark.sql.SparkException: Job aborted"
    # - "java.io.IOException: Cannot write to checkpoint"
    # - "OutOfMemoryError: Java heap space"

# 2. Address root cause:
# - Connection lost: improve network resilience
# - Malformed data: add validation in transformation
# - Lock error: ensure unique checkpoint per query
# - OOM: reduce batch size or memory per executor

# 3. Implement retry logic:
def create_query_with_retry(sqm, config, max_retries=3):
    for attempt in range(max_retries):
        try:
            return sqm.create_and_start_query(config)
        except StreamingQueryError as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt+1} failed, retrying...")
                import time
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            else:
                raise
```

#### Scenario 4: Dependency Failure Cascades

```python
# Scenario: Node A fails, Node B (depends_on A) never starts

# Symptoms:
# - Node A: error status
# - Node B: waiting status (never transitions to running)

status = spm.get_pipeline_status(exec_id)
for node_name, query_status in status.get("query_statuses", {}).items():
    if query_status.get("exception"):
        print(f"{node_name} failed: {query_status['exception']}")
        # Find dependent nodes
        dependent = [
            n for n in config["nodes"]
            if exec_id in n.get("depends_on", [])
        ]
        print(f"  Blocked: {dependent}")

# Solution:
# 1. Fix the failing node (A)
# 2. Restart pipeline
# 3. Or isolate Node B in separate pipeline if independent
```

### Debug Mode

Enable verbose logging:

```python
import logging
from loguru import logger

# Set logging level
logger.enable("tauro.streaming")
logger.add(sys.stderr, level="DEBUG")

# Or use standard Python logging
logging.basicConfig(level=logging.DEBUG)

# Check specific loggers:
logging.getLogger("tauro.streaming.pipeline_manager").setLevel(logging.DEBUG)
logging.getLogger("tauro.streaming.query_manager").setLevel(logging.DEBUG)
```

### Monitoring Query Progress

```python
from tauro.streaming.query_manager import StreamingQueryManager
from tauro.streaming import QueryHealthMonitor
import time

sqm = StreamingQueryManager(context)
query = sqm.create_and_start_query(config)

# Monitor with health checking
monitor = QueryHealthMonitor(query, "my_query", timeout_seconds=300)

while query.isActive:
    is_healthy, error = monitor.check_health()
    
    if not is_healthy:
        print(f"Query problem: {error}")
        break
    
    progress = query.lastProgress
    if progress:
        print(f"Batch {progress['batchId']}: "
              f"Processed {progress['numInputRows']} rows in "
              f"{progress['durationMs'].get('total', 0)}ms")
    
    time.sleep(10)
```

### Pipeline Diagnostics

```python
from tauro.streaming import StreamingPipelineManager

spm = StreamingPipelineManager(context)
exec_id = spm.start_pipeline("test", config)

# Monitor with detailed diagnostics
import time
import json

for _ in range(5):
    status = spm.get_pipeline_status(exec_id)
    
    print(f"Pipeline Status: {status['status']}")
    print(f"Active Queries: {status.get('active_queries', 0)}")
    print(f"Failed Queries: {status.get('failed_queries', 0)}")
    
    # Check individual query statuses
    for query_name, query_status in status.get("query_statuses", {}).items():
        is_active = query_status.get("isActive", False)
        has_error = "exception" in query_status
        print(f"  - {query_name}: {'✓' if is_active else '✗'} "
              f"{'[ERROR]' if has_error else ''}")
        
        if has_error:
            print(f"      {query_status['exception']}")
    
    time.sleep(30)
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
