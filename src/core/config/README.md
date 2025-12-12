# tauro.config

> A production-ready configuration layer for data pipelines with support for ML, streaming, and batch workloads.

**tauro.config** provides a comprehensive, extensible configuration management solution that handles:
- 🔄 **Multi-source loading**: YAML, JSON, Python, DSL files + in-memory dicts
- ✅ **Comprehensive validation**: Structure, pipeline integrity, format compatibility
- 🔗 **Variable interpolation**: Environment-first precedence with smart defaults
- ⚡ **Spark lifecycle management**: Thread-safe singleton for local/Databricks modes
- 🎯 **ML-aware configuration**: Hyperparameters, model versions, experiment tracking
- 🌊 **Streaming support**: Format policies, checkpoint management, compatibility rules

## Quick Start

```python
from tauro.config import Context

# Create context from files (or dicts)
context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)

# Access Spark and configuration
spark = context.spark
pipeline = context.get_pipeline("daily_etl")

# Iterate over nodes
for node in pipeline["nodes"]:
    print(f"Execute: {node['name']} -> {node['function']}")
```

---

## Overview

The `tauro.config` module centralizes:

| Feature | Benefit |
|---------|---------|
| **Configuration Loading** | Unified API for YAML, JSON, Python, DSL, and dict sources |
| **Variable Interpolation** | Dynamic path/value replacement with environment variable precedence |
| **Validation Framework** | Multi-layer validation: structure → pipeline → format → ML-specific |
| **Format Policy** | Streaming-aware compatibility rules for batch/streaming pipelines |
| **Spark Lifecycle** | Thread-safe singleton with caching, validation, and cleanup |
| **Context Specialization** | Auto-detection of ML/Streaming/Hybrid pipeline types |

## Key Components

| Component | Purpose |
|-----------|---------|
| **Context** | Central orchestrator; loads configs, manages Spark, exposes convenience attributes |
| **PipelineManager** | Validates pipelines and expands node references into full configurations |
| **ConfigLoaderFactory** | Auto-detects and loads YAML, JSON, Python, DSL, or dict configs |
| **VariableInterpolator** | Resolves `${ENV_VAR}` and `${var_name}` placeholders with proper precedence |
| **Validators** | ConfigValidator, PipelineValidator, FormatPolicy, MLValidator, StreamingValidator |
| **SparkSessionManager** | Thread-safe session caching with validation, timeout, and cleanup |
| **ContextFactory** | Auto-selects MLContext, StreamingContext, or HybridContext based on pipelines |
| **Exceptions** | Hierarchical exception types: ConfigLoadError, ConfigValidationError, PipelineValidationError |

---

## Installation & Requirements

### Dependencies
- **Python**: 3.8+
- **Required**: `pyyaml` (for YAML loading)
- **Optional**: `pyspark` (for local mode)
- **Optional**: `databricks-connect` (for Databricks/distributed mode)

### Install

```bash
# Core dependencies
pip install pyyaml

# For Spark support
pip install pyspark

# For Databricks support
pip install databricks-connect
```

---

## Configuration Structure

Every Context requires **5 configuration sources**. Each can be a file path (string) or dict.

### Global Settings (Required)

```yaml
# config/global.yml
input_path: /data/in                    # ✅ Required
output_path: /data/out                  # ✅ Required
mode: local                             # ✅ Required: "local", "databricks", "distributed"

# Optional
layer: batch                            # "batch", "ml", "streaming", "hybrid"
project_name: my_project                # For MLOps tracking
default_model_version: v1.0             # Default ML model version

# Custom Spark configs
spark_config:
  spark.sql.shuffle.partitions: "200"
  spark.executor.memory: "4g"

# Custom format policy
format_policy:
  supported_inputs: [kafka, kinesis]
  supported_outputs: [delta, parquet]
```

### Pipelines Config

```yaml
# config/pipelines.yml
daily_etl:
  type: batch                           # batch, ml, streaming, hybrid
  nodes: [extract, transform, load]     # References to nodes_config
  spark_config:
    spark.sql.shuffle.partitions: "300"

ml_pipeline:
  type: ml
  nodes: [preprocess, train, evaluate]
  model_version: v2.0                   # Override global default
```

### Nodes Config

```yaml
# config/nodes.yml
extract:
  input: [raw_source]
  function: etl.extract

transform:
  dependencies: [extract]               # Execution order
  function: etl.transform

load:
  dependencies: [transform]
  output: [target_table]
  function: etl.load
  
train:
  input: [training_data]
  function: ml.train
  model:
    type: spark_ml
  hyperparams:                          # ML-specific
    max_iter: 100
    learning_rate: 0.01
```

### Input Config

```yaml
# config/input.yml
raw_source:
  format: parquet
  filepath: /data/in/raw.parquet

training_data:
  format: csv
  filepath: s3://bucket/${ENV_STAGE}/train.csv  # Variables supported
```

### Output Config

```yaml
# config/output.yml
target_table:
  format: delta
  schema: curated
  table_name: sales

model_artifact:
  format: pickle
  filepath: /models/model-${model_version}.pkl
```

---

## Creating a Context

### Option 1: From Files

```python
from tauro.config import Context

context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)

print(context.input_path)           # "/data/in"
print(context.execution_mode)       # "local"
print(context.spark)                # SparkSession (lazy-initialized)
```

### Option 2: From Dicts (Testing/Prototyping)

```python
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
    },
    pipelines_config={
        "simple_etl": {
            "type": "batch",
            "nodes": ["process"]
        }
    },
    nodes_config={
        "process": {
            "input": ["source"],
            "function": "pipeline.process"
        }
    },
    input_config={
        "source": {"format": "csv", "filepath": "/data/in/data.csv"}
    },
    output_config={
        "result": {"format": "parquet", "filepath": "/data/out/result.parquet"}
    }
)
```

### Option 3: Mixed (Files + Dicts)

```python
context = Context(
    global_settings="config/global.yml",           # File
    pipelines_config={...},                        # Dict
    nodes_config="config/nodes.yml",               # File
    input_config="config/input.yml",               # File
    output_config="config/output.yml"              # File
)
```

### Option 4: With ML Info

```python
context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
    ml_info={
        "model_name": "sales_predictor",
        "model_version": "v2.1",
        "hyperparams": {
            "max_depth": 10,
            "n_estimators": 100,
        },
        "metrics": ["accuracy", "precision", "recall"]
    }
)

# Access consolidated ML info
ml_info = context.get_pipeline_ml_info("ml_pipeline")
print(ml_info["model_name"])       # "sales_predictor"
print(ml_info["hyperparams"])      # {...}
```

### Option 5: With Custom Spark Session

```python
from pyspark.sql import SparkSession

# Create custom session
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("custom") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# Reuse in Context
context = Context(
    global_settings="config/global.yml",
    ...,
    spark_session=spark  # Reuse instead of creating new one
)
```

---

## What Context Exposes

### Key Attributes

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `input_path` | str | Base directory for inputs | `/data/in` |
| `output_path` | str | Base directory for outputs | `/data/out` |
| `execution_mode` | str | Execution context | `local` \| `databricks` |
| `spark` | SparkSession | Lazy-initialized Spark session | `context.spark.sql(...)` |
| `pipelines` | dict | Validated & expanded pipelines | `context.pipelines["daily_etl"]` |
| `layers` | list | Loaded context layers | `["batch", "ml"]` |
| `global_vars` | dict | Merged global variables | `context.global_vars["key"]` |

### Getting Spark Session

```python
# Lazy initialization - only created when first accessed
spark = context.spark
df = spark.read.parquet("/data/in/file.parquet")
df.show()

# Check session validity without creating
from tauro.config.session import SparkSessionManager
manager = SparkSessionManager()
if manager.get_session_info():
    print("Session is valid")
```

### Accessing Pipelines

```python
# Get all pipelines (with validation & expansion)
all_pipelines = context.pipelines
print(list(all_pipelines.keys()))  # ["daily_etl", "ml_pipeline"]

# Get single pipeline
pipeline = context.get_pipeline("daily_etl")
print(pipeline["nodes"])           # [{"name": "extract", "function": "...", ...}, ...]

# Get ML info for pipeline
ml_info = context.get_pipeline_ml_info("ml_pipeline")
print(ml_info["model_version"])    # "v2.1"
print(ml_info["hyperparams"])      # {"max_depth": 10, ...}
```

### Accessing Node Info

```python
# Get all nodes from context
nodes = context.pipelines  # Contains all expanded nodes

# Useful for iteration
for pipeline_name, pipeline in context.pipelines.items():
    for node in pipeline["nodes"]:
        print(f"{pipeline_name} -> {node['name']}: {node['function']}")
```

---

## PipelineManager

### Automatic Pipeline Expansion

PipelineManager validates and expands pipeline definitions by:
1. ✅ **Validating** all referenced nodes exist in nodes_config
2. ✅ **Resolving** dependencies to determine execution order
3. ✅ **Expanding** node definitions with full configuration
4. ✅ **Merging** inputs/outputs from nodes into pipeline

### Usage

```python
# Access PipelineManager
manager = context.pipeline_manager
print(f"Total pipelines: {len(context.pipelines)}")

# Get expanded pipeline with all node details
pipeline = context.get_pipeline("daily_etl")

for node in pipeline["nodes"]:
    print(f"Node: {node['name']}")
    print(f"  Function: {node['function']}")
    print(f"  Dependencies: {node.get('dependencies', [])}")
    print(f"  Inputs: {node.get('input', [])}")
    print(f"  Outputs: {node.get('output', [])}")
```

### Example Expanded Pipeline

```python
# Original config
pipelines_config = {
    "daily_etl": {
        "type": "batch",
        "nodes": ["extract", "transform", "load"]
    }
}

nodes_config = {
    "extract": {
        "input": ["raw_data"],
        "function": "etl.extract"
    },
    "transform": {
        "dependencies": ["extract"],
        "function": "etl.transform"
    },
    "load": {
        "dependencies": ["transform"],
        "output": ["target"],
        "function": "etl.load"
    }
}

# After expansion via PipelineManager
context.get_pipeline("daily_etl") 
# Returns:
# {
#     "name": "daily_etl",
#     "type": "batch",
#     "nodes": [
#         {
#             "name": "extract",
#             "input": ["raw_data"],
#             "function": "etl.extract",
#             "order": 0  # Execution order determined by dependencies
#         },
#         {
#             "name": "transform",
#             "dependencies": ["extract"],
#             "function": "etl.transform",
#             "order": 1
#         },
#         {
#             "name": "load",
#             "dependencies": ["transform"],
#             "output": ["target"],
#             "function": "etl.load",
#             "order": 2
#         }
#     ]
# }
```

---

## Loading and Validation Flow

### Automatic Multi-Step Validation

When creating a Context, the following happens in order:

```
1. Format Detection
   ├─ Detect file type (YAML/JSON/DSL) or use dict as-is
   └─ Load/parse all 5 configuration sources

2. Basic Structure Validation
   ├─ Check required keys (input_path, output_path, mode)
   ├─ Validate value types
   └─ Check for syntax errors

3. Variable Interpolation
   ├─ Replace ${ENV_VAR} from environment
   ├─ Replace ${var} from context variables
   ├─ Protect against circular references
   └─ Interpolate file paths

4. Pipeline Validation
   ├─ Verify all referenced nodes exist
   ├─ Resolve node dependencies
   ├─ Check for circular node dependencies
   └─ Expand pipeline definitions

5. Format Policy Validation
   ├─ Check input format compatibility
   ├─ Check output format compatibility
   ├─ Validate against configured supported formats
   └─ Check streaming vs batch compatibility

6. ML-Specific Validation (if needed)
   ├─ Validate model_type field
   ├─ Validate hyperparameters structure
   └─ Validate metrics list

7. Spark Session Initialization (lazy)
   ├─ Create session on first access
   ├─ Apply spark_config settings
   └─ Cache and reuse session
```

### Example: Validation Error Scenarios

```python
# ❌ Error: Missing required key
try:
    context = Context(
        global_settings={"input_path": "/data/in"},  # Missing output_path, mode
        ...
    )
except ConfigValidationError as e:
    print(f"Validation error: {e}")

# ❌ Error: Node not found
try:
    context = Context(
        ...,
        pipelines_config={"etl": {"nodes": ["extract", "transform"]}},
        nodes_config={"extract": {...}}  # Missing "transform" node
    )
except PipelineValidationError as e:
    print(f"Pipeline error: {e}")

# ❌ Error: Circular dependencies
try:
    context = Context(
        ...,
        nodes_config={
            "node_a": {"dependencies": ["node_b"], ...},
            "node_b": {"dependencies": ["node_a"], ...}
        }
    )
except PipelineValidationError as e:
    print(f"Circular dependency detected: {e}")

# ❌ Error: Unsupported input format
try:
    context = Context(
        global_settings={..., "format_policy": {"supported_inputs": ["parquet"]}},
        ...,
        input_config={"data": {"format": "avro", ...}}
    )
except ConfigValidationError as e:
    print(f"Format not supported: {e}")
```

### Using ConfigLoaderFactory

```python
from tauro.config.loaders import ConfigLoaderFactory

loader = ConfigLoaderFactory()

# Auto-detect and load from file
cfg_yaml = loader.load_config("config/global.yml")      # Detects YAML
cfg_json = loader.load_config("config/pipelines.json")  # Detects JSON
cfg_python = loader.load_config("config.settings")      # Loads Python module

# Direct dict passthrough
cfg_dict = loader.load_config({"key": "value"})         # Returns as-is
```

---

## Variable Interpolation

Context automatically replaces `${VARIABLE}` placeholders in all configuration values. Variables are resolved with this precedence:

### 1. Environment Variables (Highest Priority)

```yaml
# config/global.yml
input_path: /data/${ENV_STAGE}/in        # Uses $ENV_STAGE from environment
output_path: ${DATA_ROOT}/out

spark_config:
  spark.executor.memory: "${EXECUTOR_MEM:4g}"  # Default value: 4g if not set
```

```python
import os
os.environ["ENV_STAGE"] = "production"
os.environ["DATA_ROOT"] = "/mnt/data"

context = Context(
    global_settings="config/global.yml",
    ...
)

print(context.input_path)    # "/data/production/in"
print(context.output_path)   # "/mnt/data/out"
```

### 2. Context Variables (Fallback)

```python
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "variables": {
            "env_stage": "test",
            "model_version": "v2.0"
        }
    },
    pipelines_config={
        "pipeline_a": {
            "model_version": "${model_version}"  # Resolves from context variables
        }
    },
    ...
)

# Access resolved values
pipeline = context.get_pipeline("pipeline_a")
print(pipeline["model_version"])  # "v2.0"
```

### 3. File Path Interpolation

```yaml
# config/input.yml
training_data:
  format: csv
  filepath: s3://bucket/${ENV_STAGE}/train-${model_version}.csv
  # Results in: s3://bucket/test/train-v2.0.csv (if ENV_STAGE=test, model_version=v2.0)

model_artifact:
  format: pickle
  filepath: /models/model-${model_version}.pkl
  # Results in: /models/model-v2.0.pkl
```

### Protection Against Infinite Loops

```python
# ✅ Safe - circular references are detected
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "variables": {
            "a": "${b}",
            "b": "${a}"      # Circular! Will raise ConfigurationError
        }
    },
    ...
)
```

---

## Format Policy

### Supported Formats

FormatPolicy manages format compatibility for batch and streaming pipelines:

#### Batch/General Formats
- **Inputs**: parquet, csv, json, xml, orc, avro, delta
- **Outputs**: parquet, csv, json, xml, orc, avro, delta, kafka, memory, console

#### Streaming Formats
- **Inputs**: kafka, kinesis, delta_stream, file_stream, socket, rate, memory
- **Outputs**: kafka, memory, console, delta

### Usage

```python
# Access format policy from context
policy = context.format_policy

# Query support
is_kafka_input = policy.is_supported_input("kafka")         # True
is_delta_output = policy.is_supported_output("delta")       # True
can_stream = policy.can_output_to_streaming("parquet")      # True/False

# Check format compatibility
is_compatible = policy.is_format_compatible("parquet", "delta")  # True
```

### Custom Format Policy

```python
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "format_policy": {
            "supported_inputs": ["kafka", "kinesis", "parquet"],
            "supported_outputs": ["delta", "parquet", "kafka"],
            # Optionally disable Unity Catalog support
            "use_unity_catalog": False
        }
    },
    ...
)

# Use custom policy
pipeline = context.get_pipeline("my_pipeline")
```

### Streaming-Specific Rules

```python
# Streaming pipelines have format constraints
streaming_config = {
    "type": "streaming",
    "nodes": ["source", "process", "sink"],
    "input_format": "kafka",          # Must be streaming-compatible
    "output_format": "delta",         # Must be streaming-compatible
    "checkpoint_dir": "/checkpoints"  # Required for stateful operations
}
```

---

## Spark Session Lifecycle

### Two Approaches to Session Management

#### 1. SparkSessionManager (Recommended) ⭐

Thread-safe, production-ready session management with validation and caching:

```python
from tauro.config.session import SparkSessionManager

manager = SparkSessionManager()

# Get or create session
spark = manager.get_or_create_session(mode="local")

# Check if session is valid
info = manager.get_session_info()
if info:
    print(f"Session age: {info['age_seconds']}s")
    print(f"Is valid: {info['is_valid']}")

# Cleanup on app shutdown
manager.cleanup_all()  # Called automatically via atexit
```

#### 2. SparkSessionFactory (Deprecated)

Simple singleton pattern without validation:

```python
from tauro.config.session import SparkSessionFactory

# Not recommended for new code
spark = SparkSessionFactory.get_session(mode="local")
SparkSessionFactory.reset_session()
```

### Via Context (Recommended)

```python
# Spark session created lazily on first access
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "spark_config": {
            "spark.sql.shuffle.partitions": "200",
            "spark.executor.memory": "4g"
        }
    },
    ...
)

spark = context.spark  # Lazy initialization
df = spark.read.parquet("/data/in/file.parquet")
```

### Execution Modes

| Mode | Description | Requirements |
|------|-------------|--------------|
| `local` | Single-node Spark | `pip install pyspark` |
| `databricks` | Databricks cluster via Connect | `pip install databricks-connect` + env vars |
| `distributed` | Alias for "databricks" | Same as databricks |

### Databricks Configuration

```bash
# Set environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef
export DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh
```

```python
context = Context(
    global_settings={
        "input_path": "/mnt/data/in",
        "output_path": "/mnt/data/out",
        "mode": "databricks",
        "spark_config": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.sql.shuffle.partitions": "1"
        }
    },
    ...
)

spark = context.spark
```

---

## Exception Handling

### Exception Hierarchy

All configuration errors inherit from `ConfigurationError`:

```
ConfigurationError (base)
├── ConfigLoadError
│   ├── File not found
│   ├── Invalid YAML/JSON syntax
│   └── Python module execution error
├── ConfigValidationError
│   ├── Missing required keys
│   ├── Invalid types
│   └── Invalid format
└── PipelineValidationError
    ├── Missing node references
    ├── Circular dependencies
    └── Invalid node configuration
```

### Error Handling Patterns

```python
from tauro.config import Context
from tauro.config.exceptions import (
    ConfigLoadError,
    ConfigValidationError,
    PipelineValidationError
)

# Pattern 1: Catch specific errors
try:
    context = Context(
        global_settings="config/global.yml",
        pipelines_config="config/pipelines.yml",
        nodes_config="config/nodes.yml",
        input_config="config/input.yml",
        output_config="config/output.yml",
    )
except ConfigLoadError as e:
    print(f"❌ Failed to load configuration: {e}")
except ConfigValidationError as e:
    print(f"❌ Configuration validation failed: {e}")
except PipelineValidationError as e:
    print(f"❌ Pipeline validation failed: {e}")

# Pattern 2: Catch all config errors
from tauro.config.exceptions import ConfigurationError

try:
    context = Context(...)
except ConfigurationError as e:
    print(f"❌ Configuration error: {type(e).__name__}")
    print(f"   Message: {e}")
    # Log for debugging
    import logging
    logger = logging.getLogger(__name__)
    logger.error(f"Configuration failed", exc_info=True)
```

### Common Error Scenarios

```python
# ❌ Missing required keys
try:
    Context(
        global_settings={"input_path": "/data/in"},  # Missing output_path, mode
        pipelines_config={...},
        nodes_config={...},
        input_config={...},
        output_config={...}
    )
except ConfigValidationError as e:
    # Message: "Missing required key: output_path"
    pass

# ❌ Node not found in nodes_config
try:
    Context(
        global_settings={...},
        pipelines_config={"etl": {"nodes": ["extract", "transform"]}},
        nodes_config={"extract": {...}},  # Missing "transform"
        input_config={...},
        output_config={...}
    )
except PipelineValidationError as e:
    # Message: "Node 'transform' referenced but not defined in nodes_config"
    pass

# ❌ File not found
try:
    Context(
        global_settings="config/nonexistent.yml",  # File doesn't exist
        pipelines_config={...},
        nodes_config={...},
        input_config={...},
        output_config={...}
    )
except ConfigLoadError as e:
    # Message: "Failed to load config/nonexistent.yml: No such file or directory"
    pass
```

---

## Complete End-to-End Example

Here's a minimal but complete example from configuration to execution:

### Step 1: Create Configuration Files

```yaml
# config/global.yml
input_path: /data/${ENV_STAGE}/in
output_path: /data/${ENV_STAGE}/out
mode: local

spark_config:
  spark.sql.shuffle.partitions: "200"
```

```yaml
# config/pipelines.yml
daily_etl:
  type: batch
  nodes: [extract, transform, load]
```

```yaml
# config/nodes.yml
extract:
  input: [raw_source]
  function: pipeline.extract

transform:
  dependencies: [extract]
  function: pipeline.transform

load:
  dependencies: [transform]
  output: [target]
  function: pipeline.load
```

```yaml
# config/input.yml
raw_source:
  format: parquet
  filepath: /raw/data.parquet
```

```yaml
# config/output.yml
target:
  format: delta
  schema: curated
  table_name: daily_metrics
```

### Step 2: Create Application

```python
# app.py
from tauro.config import Context
from tauro.config.exceptions import ConfigurationError

def main():
    try:
        # 1. Create context from configurations
        context = Context(
            global_settings="config/global.yml",
            pipelines_config="config/pipelines.yml",
            nodes_config="config/nodes.yml",
            input_config="config/input.yml",
            output_config="config/output.yml",
        )
        
        # 2. Access Spark for data operations
        spark = context.spark
        
        # 3. Get expanded pipeline
        pipeline = context.get_pipeline("daily_etl")
        
        # 4. Execute each node in order
        for node in pipeline["nodes"]:
            print(f"⏳ Executing: {node['name']}")
            print(f"   Function: {node['function']}")
            print(f"   Dependencies: {node.get('dependencies', [])}")
        
        print(f"\n✅ Pipeline complete!")
        print(f"   Input from: {context.input_path}")
        print(f"   Output to: {context.output_path}")
        print(f"   Mode: {context.execution_mode}")
        
    except ConfigurationError as e:
        print(f"❌ Configuration error: {e}")
        raise

if __name__ == "__main__":
    main()
```

### Step 3: Run Application

```bash
# Set environment for stage
export ENV_STAGE=production

# Run the app
python app.py

# Output:
# ⏳ Executing: extract
#    Function: pipeline.extract
#    Dependencies: []
# ⏳ Executing: transform
#    Function: pipeline.transform
#    Dependencies: ['extract']
# ⏳ Executing: load
#    Function: pipeline.load
#    Dependencies: ['transform']
#
# ✅ Pipeline complete!
#    Input from: /data/production/in
#    Output to: /data/production/out
#    Mode: local
```

---

## Best Practices & Tips

### 1. Configuration Organization

```
config/
├── global.yml          # Always use this for global settings
├── pipelines.yml       # Pipeline definitions
├── nodes.yml           # Node configurations
├── input.yml           # Input source definitions
└── output.yml          # Output destination definitions
```

### 2. Environment-Specific Configuration

✅ **Good: Use environment variables for environment-specific values**

```yaml
# config/global.yml
input_path: /data/${ENV_STAGE}/in
output_path: /data/${ENV_STAGE}/out
mode: ${EXECUTION_MODE:local}

spark_config:
  spark.executor.memory: ${EXECUTOR_MEMORY:4g}
  spark.sql.shuffle.partitions: ${SHUFFLE_PARTITIONS:200}
```

```bash
# Set before running
export ENV_STAGE=production
export EXECUTION_MODE=databricks
export EXECUTOR_MEMORY=8g
python app.py
```

❌ **Avoid: Duplicating config files for each environment**

### 3. Secure Credential Handling

✅ **Good: Use environment variables for secrets**

```python
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "database": {
            "host": "${DB_HOST}",
            "user": "${DB_USER}",
            "password": "${DB_PASSWORD}"  # From environment
        }
    },
    ...
)
```

```bash
export DB_HOST=db.example.com
export DB_USER=etl_user
export DB_PASSWORD=secret_password
python app.py
```

❌ **Avoid: Hardcoding credentials in YAML files**

### 4. Pipeline Definition Best Practices

✅ **Good: Clear node dependencies**

```yaml
# nodes.yml
extract:
  input: [raw_source]
  function: etl.extract

transform:
  dependencies: [extract]    # Explicit dependency
  function: etl.transform

load:
  dependencies: [transform]  # Clear execution order
  output: [target_table]
  function: etl.load
```

### 5. Testing Configuration

✅ **Good: Use dicts for testing, no file dependencies**

```python
def test_pipeline():
    context = Context(
        global_settings={
            "input_path": "/tmp/test_in",
            "output_path": "/tmp/test_out",
            "mode": "local"
        },
        pipelines_config={"test_pipeline": {"nodes": ["process"]}},
        nodes_config={"process": {"function": "test.process"}},
        input_config={"test_data": {"format": "parquet", "filepath": "/tmp/test.parquet"}},
        output_config={"result": {"format": "parquet", "filepath": "/tmp/result.parquet"}}
    )
    
    assert context.input_path == "/tmp/test_in"
    # More assertions...
```

### 6. Session Management

✅ **Good: Use SparkSessionManager for thread-safe operations**

```python
from tauro.config.session import SparkSessionManager

manager = SparkSessionManager()
spark = manager.get_or_create_session(mode="local")

# Use spark...

# Cleanup on shutdown (automatic via atexit)
manager.cleanup_all()
```

### 7. Variable Interpolation Safety

Remember the precedence order:

1. 🔴 **Environment variables** (highest - overrides all)
2. 🟡 **Context variables dict**
3. 🟢 **Defaults in config** (lowest)

This means environment variables always win, making them suitable for:
- Secrets (passwords, tokens)
- Environment-specific settings
- Runtime overrides

---

## Troubleshooting

### Configuration Loading Issues

**Problem: "No such file or directory" when creating Context**

```python
# ❌ Error
context = Context(
    global_settings="config/global.yml",  # File not found
    ...
)
```

**Solution:**
- Verify file paths are relative to current working directory or use absolute paths
- Use `pathlib` for portable paths:

```python
from pathlib import Path

config_dir = Path(__file__).parent / "config"
context = Context(
    global_settings=str(config_dir / "global.yml"),
    pipelines_config=str(config_dir / "pipelines.yml"),
    nodes_config=str(config_dir / "nodes.yml"),
    input_config=str(config_dir / "input.yml"),
    output_config=str(config_dir / "output.yml")
)
```

### Validation Issues

**Problem: "Missing required key: output_path"**

**Solution:** Ensure all 3 required keys are in global_settings:

```yaml
# ✅ Correct
input_path: /data/in
output_path: /data/out
mode: local

# ❌ Incorrect - missing output_path
input_path: /data/in
mode: local
```

**Problem: "Node 'transform' referenced but not defined"**

**Solution:** Verify all nodes referenced in pipelines exist in nodes_config:

```python
# ✅ Correct
pipelines_config = {
    "my_pipeline": {
        "nodes": ["extract", "transform", "load"]  # All 3 exist below
    }
}

nodes_config = {
    "extract": {...},
    "transform": {...},
    "load": {...}
}

# ❌ Incorrect - "load" missing from nodes_config
nodes_config = {
    "extract": {...},
    "transform": {...}
}
```

### Variable Interpolation Issues

**Problem: Variable not being replaced, placeholder remains in output**

**Solution:** Check variable format and precedence:

```python
import os

# ✅ Correct - environment variable
os.environ["ENV_STAGE"] = "prod"
path = "/data/${ENV_STAGE}/in"  # Becomes "/data/prod/in"

# ❌ Incorrect - variable name doesn't match
os.environ["STAGE"] = "prod"
path = "/data/${ENV_STAGE}/in"  # Still has placeholder (STAGE != ENV_STAGE)

# ✅ Use context variables as fallback
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "variables": {
            "stage": "test"
        }
    },
    pipelines_config={"pipe": {"stage": "${stage}"}},  # Resolved from variables
    ...
)
```

### Spark Session Issues

**Problem: "No module named 'pyspark'" in local mode**

**Solution:** Install pyspark:

```bash
pip install pyspark
```

**Problem: "DATABRICKS_HOST not set" in databricks mode**

**Solution:** Set environment variables:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890...
export DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh

python app.py
```

**Problem: Spark session already exists**

**Solution:** Reset between tests:

```python
from tauro.config.session import SparkSessionManager

def test_case_1():
    context1 = Context(...)
    # ...
    SparkSessionManager().cleanup_all()  # Reset for next test

def test_case_2():
    context2 = Context(...)
    # ...
```

### Format Compatibility Issues

**Problem: "Format 'avro' not supported in streaming pipelines"**

**Solution:** Check format policy and use compatible formats:

```python
context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "format_policy": {
            "supported_inputs": ["kafka", "kinesis", "parquet"],
            "supported_outputs": ["delta", "parquet", "kafka"]
        }
    },
    ...
)

# Use supported formats
input_config = {
    "source": {"format": "kafka", "..."}  # ✅ Supported
}

output_config = {
    "sink": {"format": "avro", "..."}  # ❌ Not in supported_outputs
}
```

---

## Architecture Improvements

---

## Architecture Overview

### Module Organization

```
tauro.core.config/
├── contexts.py           # Main Context class & orchestration
├── loaders.py           # Configuration file loading & parsing
├── validators.py        # Multi-layer validation (structure, pipeline, format)
├── session.py           # Spark session lifecycle management
├── interpolator.py      # Variable substitution & path handling
├── providers.py         # Config repository abstraction
├── context_loader.py    # High-level context creation
├── exceptions.py        # Hierarchical exception types
└── __init__.py          # Public API exports
```

### Key Design Patterns

| Pattern | Implementation | Purpose |
|---------|----------------|---------|
| **Factory** | ConfigLoaderFactory, ContextFactory | Auto-detect & create appropriate handler |
| **Singleton** | SparkSessionManager | Single cached session per mode |
| **Template Method** | BaseSpecializedContext | Extensible context types |
| **Strategy** | Multiple Loaders (YAML/JSON/Python) | Pluggable format support |
| **Visitor** | VariableInterpolator | Recursive structure traversal |
| **Lazy Init** | @cached_property on pipelines & spark | Deferred computation |

### Configuration Flow

```
┌─────────────────────┐
│  Input Sources      │
│  (File/Dict/Dict)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐      ┌──────────────────┐
│  Format Detection   │─────▶│ ConfigLoader     │
│  (YAML/JSON/etc)    │      │ implementations  │
└──────────┬──────────┘      └──────────────────┘
           │
           ▼
┌─────────────────────┐
│  Structure Validate │
│  (required keys)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Interpolate Vars   │
│  (${ENV_VAR})       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Pipeline Validate  │
│  (node refs)        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Format Policy      │
│  (stream/batch)     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Context Ready ✅    │
└─────────────────────┘
```

### Key Components

#### **Context** (Main Orchestrator)
- Loads and validates all 5 configuration sources
- Manages Spark session lifecycle
- Exposes pipelines & convenience attributes
- Thread-safe access to shared resources

#### **PipelineManager** (Validator & Expander)
- Validates nodes exist
- Resolves dependencies
- Expands node definitions
- Cached for performance

#### **ConfigLoaderFactory** (Format Auto-Detector)
- Detects file type (YAML/JSON/DSL/dict)
- Selects appropriate loader
- Handles parsing & validation
- Security: Prevents path traversal

#### **VariableInterpolator** (Variable Resolver)
- Replaces ${VAR} placeholders
- Environment variables win (highest precedence)
- Protects against infinite loops
- Recursive structure traversal

#### **SparkSessionManager** (Session Lifecycle)
- Thread-safe singleton pattern
- Validates session health
- Automatic cleanup on exit
- Supports local & Databricks modes

#### **FormatPolicy** (Format Compatibility)
- Validates format support
- Streaming vs batch rules
- Customizable via global_settings
- Checkpoint requirements

---

## API Quick Reference

### Context

```python
from tauro.config import Context

# Create context
context = Context(
    global_settings="config/global.yml" | dict,
    pipelines_config="config/pipelines.yml" | dict,
    nodes_config="config/nodes.yml" | dict,
    input_config="config/input.yml" | dict,
    output_config="config/output.yml" | dict,
    ml_info: dict = None,
    spark_session: SparkSession = None
) -> Context

# Attributes
context.input_path                # str - from global_settings
context.output_path               # str - from global_settings
context.execution_mode            # str - "local"|"databricks"
context.spark                     # SparkSession (lazy)
context.pipelines                 # dict - all validated pipelines
context.layers                    # list - loaded layers
context.format_policy             # FormatPolicy instance

# Methods
context.get_pipeline(name: str) -> dict
context.get_pipeline_ml_info(name: str) -> dict
```

### PipelineManager

```python
# Access via context
manager = context.pipeline_manager

# Methods
manager.list_pipeline_names() -> List[str]
manager.get_pipeline(name: str) -> Dict
manager.pipelines -> Dict[str, Dict]  # All expanded pipelines
```

### ConfigLoaderFactory

```python
from tauro.config.loaders import ConfigLoaderFactory

loader = ConfigLoaderFactory()

# Methods
loader.load_config(source: str | dict) -> Dict
loader.get_loader(source: str | dict) -> Loader
```

### VariableInterpolator

```python
from tauro.config.interpolator import VariableInterpolator

# Methods
VariableInterpolator.interpolate(
    value: str,
    variables: Dict[str, str]
) -> str

VariableInterpolator.interpolate_structure(
    config: Dict,
    variables: Dict[str, str]
) -> Dict
```

### SparkSessionManager

```python
from tauro.config.session import SparkSessionManager

manager = SparkSessionManager()

# Methods
manager.get_or_create_session(
    mode: str = "local",
    config: Dict = None
) -> SparkSession

manager.get_session_info() -> Dict
manager.cleanup_all() -> None
manager.cleanup_stale_sessions(timeout_seconds: int = 3600) -> None
```

### Exceptions

```python
from tauro.config.exceptions import (
    ConfigurationError,     # Base exception
    ConfigLoadError,        # File/parse errors
    ConfigValidationError,  # Structure errors
    PipelineValidationError # Node/dependency errors
)

try:
    context = Context(...)
except ConfigLoadError as e:
    pass
except ConfigValidationError as e:
    pass
except PipelineValidationError as e:
    pass
```

---

## Further Reading

- **For ML Integration**: See `../mlops/config.py` for ML-specific configuration patterns
- **For Streaming**: Refer to `../streaming/` for streaming-specific pipeline configurations
- **For Execution**: Check `../exec/executor.py` for how configs are used in node execution
- **For Analysis**: See `CONFIG_ANALYSIS.md` in project root for detailed technical breakdown
- **For Recommendations**: See `CONFIG_RECOMMENDATIONS.md` for improvement roadmap
