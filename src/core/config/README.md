# tauro.config

A cohesive, production-ready configuration layer for Tauro that loads, validates, and orchestrates pipeline configuration and Spark session lifecycle in a consistent, extensible manner. It is designed to work seamlessly with:

- **File-based configs:** YAML, JSON, Python modules for environment-specific configurations
- **In-memory dict configs:** Ideal for testing, prototyping, and quick setups
- **Unified loading interface:** Consistent API regardless of source type

## Overview

The `tauro.config` module provides a comprehensive configuration management solution that centralizes:

- **Configuration Loading & Merging:** Unified loading from multiple sources with factory pattern
- **Variable Interpolation:** Dynamic path and configuration value replacement with environment-first precedence
- **Validation Framework:** Comprehensive configuration and pipeline structure validation
- **Format Policy Management:** Streaming-aware format compatibility and validation rules
- **Spark Session Lifecycle:** Robust singleton Spark session creation for local, Databricks, and distributed modes
- **IO/Execution Integration:** Convenience attributes bridging configuration to downstream layers

## Key Components

- **Context:** Central orchestrator for the entire configuration lifecycle. Manages all configuration sources, Spark session, and exposes convenience attributes for IO and execution layers.
- **PipelineManager:** Validates pipeline definitions and expands node references into full, validated node configurations with dependency resolution.
- **ConfigLoaderFactory:** Unified factory for loading configurations from YAML, JSON, Python modules, or in-memory dicts with automatic source detection.
- **VariableInterpolator:** Advanced variable replacement in strings and file paths with environment variable precedence and fallback to variables dict.
- **Validators:** Specialized validators for configuration structure, pipeline integrity, and format policy compliance.
- **SparkSessionFactory:** Robust, singleton Spark session creation and management for local, Databricks, and distributed execution modes.
- **Exceptions:** Clear, hierarchical exception types for configuration and validation failures with detailed error context.

---

## Installation and Requirements

- **Python:** 3.8 or higher
- **Dependencies:** pyyaml (for YAML loading)
- **Optional:** pyspark for Spark session creation
- **Optional:** Databricks Connect (when using Databricks or distributed mode)

---

## Global Settings (Required & Optional)

Context requires the following keys in `global_settings`:

**Required Keys:**
- `input_path` (str): Base directory path for input data sources
- `output_path` (str): Base directory path for output data destinations
- `mode` (str): Execution mode - must be one of: "local", "databricks", or "distributed" (alias for "databricks")

**Optional Keys:**
- `layer` (str): Free-form string to categorize contexts (e.g., "ml", "streaming", "batch", "etl")
- `format_policy` (dict): Override default supported formats and compatibility rules
- `spark_config` or `ml_config` (dict): Custom Spark configurations passed via builder.config(k, v)
- Environment-specific overrides via variable interpolation (${ENV_VAR})

## Creating a Context

The Context constructor accepts five configuration sources. Each source can be either a file path (str) or an in-memory dict. All five sources are required.

- `global_settings`: str (file path) | dict (configuration object)
- `pipelines_config`: str (file path) | dict (configuration object)
- `nodes_config`: str (file path) | dict (configuration object)
- `input_config`: str (file path) | dict (configuration object)
- `output_config`: str (file path) | dict (configuration object)

### Creating from File Paths

```python
from tauro.config import Context

context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)
```

### Creating from In-Memory Dictionaries

```python
from tauro.config import Context

context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "layer": "batch",
    },
    pipelines_config={
        "daily_etl": {
            "type": "batch",
            "nodes": ["extract", "transform", "load"],
        }
    },
    nodes_config={
        "extract": {"input": ["src_parquet"], "function": "myapp.pipeline.extract"},
        "transform": {"dependencies": ["extract"], "function": "myapp.pipeline.transform"},
        "load": {"dependencies": ["transform"], "output": ["out_delta:curated/sales"], "function": "myapp.pipeline.load"},
    },
    input_config={
        "src_parquet": {"format": "parquet", "filepath": "/data/in/source.parquet"}
    },
    output_config={
        "out_delta:curated/sales": {"format": "delta", "schema": "curated", "table_name": "sales"}
    },
)
```

---

## What Context Exposes

The Context object provides the following attributes and methods for use by downstream layers:

**Configuration Sources:**
- `global_settings`: Dictionary containing global settings (input_path, output_path, mode, etc.)
- `pipelines_config`: Dictionary with pipeline definitions
- `nodes_config`: Dictionary with node configurations
- `input_config`: Dictionary with input source definitions
- `output_config`: Dictionary with output destination definitions

**Computed Attributes:**
- `spark`: Active SparkSession (singleton, lazily initialized)
- `execution_mode`: String value from global_settings["mode"]
- `input_path`: Convenience attribute from global_settings["input_path"]
- `output_path`: Convenience attribute from global_settings["output_path"]
- `format_policy`: FormatPolicy instance with stream/batch compatibility rules
- `_pipeline_manager`: Internal PipelineManager for pipeline expansion (cached)

These top-level attributes ensure seamless integration with the IO layer and custom executors that expect context-like objects.

---

## PipelineManager

PipelineManager validates pipeline definitions and expands node references into complete, executable configurations:

- **Validation:** Ensures all nodes referenced by a pipeline exist in nodes_config
- **Expansion:** Transforms node names into full node configurations including all dependencies
- **Dependency Resolution:** Maintains dependency order for execution
- **Convenience Helpers:** Methods to list, fetch, and expand pipelines

### Usage Example

```python
# Access the pipeline manager through the context
pm = context._pipeline_manager

# List all available pipelines
pipeline_names = pm.list_pipeline_names()  # ['daily_etl']

# Get expanded pipeline configuration
pipeline = pm.get_pipeline('daily_etl')
# Returns:
# {
#   "type": "batch",
#   "nodes": [
#     {"name": "extract", "input": ["src_parquet"], "function": "..."},
#     {"name": "transform", "dependencies": ["extract"], "function": "..."},
#     {"name": "load", "dependencies": ["transform"], "output": [...], "function": "..."}
#   ],
#   "inputs": [],
#   "outputs": [],
#   "spark_config": {}
# }

# Access all expanded pipelines
all_pipelines = pm.pipelines  # Returns dict of all expanded pipelines
```

**Note:** Context caches validated pipelines via a cached_property decorator for efficiency.

---

## Loading and Validation Flow

The configuration loading and validation process follows these steps:

1. **Detection:** ConfigLoaderFactory detects source type (file path vs. dict)
2. **Loading:** 
   - YAML/JSON: Files are parsed using safe_load (YAML) or json.load (JSON), returns empty dict if empty
   - Python: Module is executed and top-level variable named `config` is extracted
   - Dict: Direct pass-through, no parsing needed
3. **Validation:**
   - ConfigValidator checks basic structure (must be dict) and required keys
   - PipelineValidator verifies all pipeline node references exist
   - FormatPolicy validates format compatibility

### Example: Using ConfigLoaderFactory

```python
from tauro.config.loaders import ConfigLoaderFactory

loader = ConfigLoaderFactory()

# Load from YAML file
cfg = loader.load_config("config/global.yml")  # Returns dict

# Load from JSON file
cfg = loader.load_config("config/pipelines.json")  # Returns dict

# Load from Python module
cfg = loader.load_config("config.settings")  # Module must have top-level 'config' variable

# Direct dict passthrough
cfg = loader.load_config({"key": "value"})  # Returns dict as-is
```

---

## Variable Interpolation

VariableInterpolator provides sophisticated placeholder resolution in configuration values:

**Supported Placeholder Formats:**
- `${ENV_STAGE}` - Replaces with environment variable (highest precedence)
- `${date}` - Replaces with value from variables dict
- `${default_value}` - Replaces with fallback value from variables dict

**Precedence (highest to lowest):**
1. Environment variables (os.environ)
2. Variables dict provided to interpolate method
3. Default values in configuration

### Usage Example

```python
from tauro.config.interpolator import VariableInterpolator

# Raw configuration with placeholders
raw_path = "s3://bucket/${ENV_STAGE}/data/table=${date}"

# Interpolation with environment variables and variables dict
interpolated = VariableInterpolator.interpolate(
    raw_path,
    variables={"date": "2025-01-15"}
)

# If ENV_STAGE=prod in environment:
# Result: "s3://bucket/prod/data/table=2025-01-15"

# Context automatically applies interpolation to all filepaths
# using global_settings as the variables source
```

**Automatic Interpolation in Context:**

```python
# When Context is created, it automatically interpolates:
# - All filepaths in input_config
# - All relevant paths in output_config
# - Using global_settings as the variables source and environment variables as overrides
```

---

## Format Policy (Streaming-Aware Compatibility)

FormatPolicy provides comprehensive format compatibility management for batch and streaming pipelines:

**Supported Streaming Input Formats:**
- kafka, kinesis, delta_stream, file_stream, socket, rate, memory

**Supported Batch Output Formats:**
- kafka, memory, console, delta, parquet, json, csv, avro, orc, xml

**Compatibility Features:**
- Maps batch outputs to compatible streaming inputs
- Specifies which streaming inputs require checkpoints
- Validates format compatibility between pipeline stages
- Customizable via global_settings.format_policy

### Usage Example

```python
from tauro.config import Context

context = Context(...)
policy = context.format_policy

# Query format support
is_kafka_input_supported = policy.is_supported_input("kafka")      # True
is_delta_output_supported = policy.is_supported_output("delta")    # True
is_format_compatible = policy.can_output_to_streaming("parquet")   # True/False
```

**Configuration Override:**

```python
context_with_custom_policy = Context(
    global_settings={
        ...,
        "format_policy": {
            "supported_streaming_inputs": ["kafka", "kinesis"],
            "supported_batch_outputs": ["delta", "parquet", "csv"],
            # ... additional overrides
        }
    },
    ...
)
```

**Notes:**
- Unity Catalog is handled by IO/UC managers, not part of format policy
- ORC is supported in IO writers but not typically used in streaming format policy
- Format policy is streaming-focused and independent of ML configurations

---

## Spark Session Lifecycle

SparkSessionFactory provides robust, singleton-based Spark session management:

**Supported Execution Modes:**
- `local`: Single-node local Spark cluster (requires pyspark)
- `databricks`: Databricks Connect mode with cluster execution (requires databricks-connect package)
- `distributed`: Alias for "databricks" mode

**Session Management:**
- Creates session lazily on first request (get_session)
- Maintains singleton pattern to prevent multiple sessions
- Provides reset_session() for testing and cleanup
- Accepts custom Spark configurations via ml_config parameter

### Usage Example

```python
from tauro.config import SparkSessionFactory, Context

# Automatic session creation via Context
context = Context(...)
spark = context.spark  # Lazily creates and caches session

# Direct factory usage
spark = SparkSessionFactory.get_session(mode="local")

# With custom Spark configurations
spark = SparkSessionFactory.get_session(
    mode="databricks",
    ml_config={
        "spark.sql.shuffle.partitions": "200",
        "spark.executor.memory": "8g"
    }
)

# Reset session (useful between tests)
SparkSessionFactory.reset_session()
```

**Databricks/Distributed Mode Requirements:**

When using "databricks" or "distributed" mode, ensure:
1. Databricks Connect package is installed: `pip install databricks-connect`
2. Environment variables are configured:
   - `DATABRICKS_HOST`: Databricks workspace URL
   - `DATABRICKS_TOKEN`: Personal access token
   - `DATABRICKS_CLUSTER_ID`: Target cluster ID
3. ML and tuning configurations can be passed via ml_config parameter

---

## Exception Handling

The module provides clear, hierarchical exception types for error handling:

**Exception Hierarchy:**

- `ConfigError`: Base exception for all configuration-related errors
  - `ConfigLoadError`: Raised when loading or parsing configuration fails
    - File not found, invalid YAML/JSON, Python module execution errors
  - `ConfigValidationError`: Raised when configuration validation fails
    - Missing required keys, incorrect types, invalid structure
  - `PipelineValidationError`: Raised when pipeline definition is invalid
    - Missing node references, circular dependencies, invalid configurations

**Error Context:**

All exceptions are logged with contextual details before being raised:
- Source path or type
- Specific validation failures
- Suggestions for remediation

### Error Handling Example

```python
from tauro.config import Context
from tauro.config.exceptions import ConfigValidationError, PipelineValidationError

try:
    context = Context(
        global_settings="config/global.yml",
        pipelines_config="config/pipelines.yml",
        nodes_config="config/nodes.yml",
        input_config="config/input.yml",
        output_config="config/output.yml",
    )
except ConfigValidationError as e:
    print(f"Configuration validation failed: {e}")
except PipelineValidationError as e:
    print(f"Pipeline validation failed: {e}")
```

---

## Minimal End-to-End Example

```python
from tauro.config import Context

# 1) Create the context from YAML files
context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)

# 2) Access Spark for distributed operations
spark = context.spark
df = spark.read.parquet(context.input_path)

# 3) Use pipelines for orchestration
pm = context._pipeline_manager
pipeline = pm.get_pipeline("daily_etl")

for node in pipeline["nodes"]:
    print(f"Executing node: {node['name']}")
    print(f"  Function: {node.get('function')}")
    print(f"  Dependencies: {node.get('dependencies', [])}")

# 4) Use convenience attributes for IO and execution
print(f"Execution Mode: {context.execution_mode}")
print(f"Input Path: {context.input_path}")
print(f"Output Path: {context.output_path}")

# 5) Access configuration sources
print(f"Available inputs: {list(context.input_config.keys())}")
print(f"Available outputs: {list(context.output_config.keys())}")
```

---

## Best Practices & Tips

1. **Global Settings:** Keep global_settings minimal but explicit. Only include input_path, output_path, and mode as required; use environment variables for sensitive data via interpolation.

2. **Environment-Specific Configs:** Leverage ${ENV_VAR} placeholders in paths to make configurations portable across environments without duplicating files.

3. **Format Policy:** Centralize streaming format policies in global_settings.format_policy instead of sprinkling custom format checks throughout your code.

4. **Spark Session Lifecycle:**
   - For Databricks/Distributed mode, ensure Databricks Connect is installed and environment variables are configured
   - Use SparkSessionFactory.reset_session() between tests to avoid session leakage

5. **Testing:** Use dict-based configurations for unit tests and leverage SparkSessionFactory.reset_session() for cleanup between test cases.

6. **Validation:** All validation errors are logged with context. Check logs when configuration fails to understand what went wrong.

7. **Variable Interpolation:** Remember that environment variables have precedence over the variables dict, making them suitable for secrets and environment-specific overrides.

---

## Troubleshooting

**Missing Required Keys (input_path, output_path, mode)**
- **Symptom:** ConfigValidationError on Context creation
- **Solution:** Ensure your global_settings dict/YAML contains all three required keys

**Invalid YAML/JSON Files**
- **Symptom:** ConfigLoadError with parse error details
- **Solution:** Validate files with `python -m yaml` or online JSON validators; ensure root element is a dict/object

**Python Loader Module Not Found**
- **Symptom:** ConfigLoadError when loading Python modules
- **Solution:** Ensure the module path is in PYTHONPATH and module defines a top-level `config` variable

**Spark Session Errors**
- **Local mode:** Verify pyspark is installed - `pip install pyspark`
- **Databricks mode:** Install Databricks Connect and set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID

**Variable Interpolation Not Working**
- **Symptom:** Placeholders remain unreplaced in loaded configuration
- **Solution:** Confirm placeholders use format ${VAR}; remember environment variables take precedence

**Pipeline Validation Errors**
- **Symptom:** PipelineValidationError with missing node references
- **Solution:** Ensure all nodes referenced in pipelines_config exist in nodes_config; check spelling

---

## Architecture Improvements

Recent enhancements to `tauro.config`:

- **Top-Level Attributes:** Context now exposes execution_mode, input_path, and output_path as direct attributes for better compatibility with IO and execution layers
- **"distributed" Mode Alias:** SparkSessionFactory now accepts "distributed" as an alias for "databricks"
- **Unified ConfigLoaderFactory:** Single factory interface for loading dicts, YAML, JSON, and Python modules
- **Advanced Variable Interpolation:** Environment variables take precedence, enabling secure configuration management
- **Streaming-Aware Format Policy:** Comprehensive format compatibility validation for batch and streaming pipelines

---

## API Quick Reference

**Context Class**
```python
Context(
    global_settings: str | dict,
    pipelines_config: str | dict,
    nodes_config: str | dict,
    input_config: str | dict,
    output_config: str | dict
) -> Context
```

Attributes: `spark`, `execution_mode`, `input_path`, `output_path`, `format_policy`, `global_settings`, `pipelines_config`, `nodes_config`, `input_config`, `output_config`

**PipelineManager (via context._pipeline_manager)**
```python
list_pipeline_names() -> List[str]
get_pipeline(name: str) -> Dict[str, Any]
pipelines -> Dict[str, Dict[str, Any]]  # All expanded pipelines
```

**ConfigLoaderFactory**
```python
load_config(source: str | dict) -> Dict[str, Any]
get_loader(source: str | dict) -> Loader
```

**VariableInterpolator**
```python
interpolate(string: str, variables: Dict[str, str]) -> str
interpolate_config_paths(config: Dict, variables: Dict[str, str]) -> Dict
```

**Validators**
```python
ConfigValidator.validate(config, required_keys)
PipelineValidator.validate(pipelines_config, nodes_config)
FormatPolicy.is_supported_input(format: str) -> bool
FormatPolicy.is_supported_output(format: str) -> bool
```

**SparkSessionFactory**
```python
get_session(mode: str, ml_config: Dict = None) -> SparkSession
reset_session() -> None
set_protected_configs(configs: List[str]) -> None
```

**Exceptions**
```python
ConfigLoadError(message: str)
ConfigValidationError(message: str)
PipelineValidationError(message: str)
