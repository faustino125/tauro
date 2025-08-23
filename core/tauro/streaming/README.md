# tauro.streaming

Tauro’s streaming module provides a structured way to define, validate, and run streaming pipelines on Spark Structured Streaming. It focuses on clear configuration, safe error handling, and a consistent lifecycle for reading from streaming sources, applying transformations, and writing results with proper checkpointing.

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
