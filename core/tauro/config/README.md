# tauro.config

A cohesive configuration layer for Tauro that loads, validates, and prepares pipeline configuration and the Spark session in a consistent way. It is designed to work equally well with:
- File-based configs (YAML, JSON, Python module)
- In-memory dict configs (for tests or quick setups)

This module centralizes:
- Loading and merging of configuration sources
- Variable interpolation in paths (env-first precedence)
- Validation of configuration structure and pipeline integrity
- Format policy for batch/streaming compatibility
- Creation and lifecycle of the Spark session (local or Databricks/Distributed)
- Exposing convenience attributes for IO and Execution layers

---

## Key Components

- Context: Orchestrates the entire configuration lifecycle for Tauro, exposes the Spark session and convenience attributes.
- PipelineManager: Expands pipeline definitions to full, validated node configs.
- Loaders: YAML, JSON, and Python module loaders, with a factory that also accepts dicts directly.
- Interpolator: Variable replacement in strings and file paths with environment variable precedence.
- Validators: Configuration and pipeline validators, plus a format policy for streaming compatibility.
- SparkSessionFactory: Robust, singleton Spark session creation for local and Databricks/Distributed modes.
- Exceptions: Clear error types for load and validation failures.

---

## Installation and Requirements

- Python 3.8+
- Optional: pyspark for Spark session creation
- Optional: Databricks Connect (when using Databricks/Distributed mode)

---

## Global Settings (Required)

Context requires these keys in global_settings:
- input_path: Base path for inputs.
- output_path: Base path for outputs.
- mode: "local", "databricks", or "distributed" (alias of "databricks").

Optional keys commonly used:
- layer: Free-form string used to categorize contexts (e.g., "ml", "streaming", "batch").
- format_policy: Dict to override supported formats and compatibility.
- spark ML configs: Arbitrary Spark configs to pass via ml_config (see Spark section).

---

## Creating a Context

You can build a Context from file paths or dicts. All five sources are required (file path or dict per source).

- global_settings: str | dict
- pipelines_config: str | dict
- nodes_config: str | dict
- input_config: str | dict
- output_config: str | dict

### From file paths

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

### From in-memory dicts

```python
from tauro.config import Context

context = Context(
    global_settings={
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "format_policy": {
            # optional overrides
        },
    },
    pipelines_config={
        "daily_pipeline": {
            "type": "batch",
            "nodes": ["extract", "transform", "load"],
        }
    },
    nodes_config={
        "extract": {"input": ["src_parquet"], "function": "pkg.module.extract"},
        "transform": {"dependencies": ["extract"], "function": "pkg.module.transform"},
        "load": {"dependencies": ["transform"], "output": ["out_delta:demo/curated/tbl"], "function": "pkg.module.load"},
    },
    input_config={
        "src_parquet": {"format": "parquet", "filepath": "/data/in/source.parquet"}
    },
    output_config={
        "out_delta:demo/curated/tbl": {"format": "delta", "schema": "demo", "table_name": "tbl"}
    },
)
```

---

## What Context Exposes

- global_settings: The loaded global settings dict.
- pipelines_config, nodes_config, input_config, output_config: The loaded configuration sources as dicts.
- format_policy: An instance of FormatPolicy (configurable via global_settings.format_policy).
- spark: The active SparkSession (singleton).
- execution_mode: Copied from global_settings["mode"].
- input_path, output_path: Convenience attributes copied from global_settings.

These top-level attributes ensure compatibility with the IO layer and custom executors that expect context-like objects or dicts.

---

## PipelineManager

PipelineManager turns your pipelines_config and nodes_config into usable pipeline definitions:
- Validates that all nodes referenced by a pipeline exist in nodes_config.
- Expands node names into full node configs.
- Provides helpers to fetch, list, and expand pipelines.

Example:

```python
# Access the pipeline manager through the context
pm = context._pipeline_manager

print(pm.list_pipeline_names())  # ['daily_pipeline']

p = pm.get_pipeline('daily_pipeline')  # returns expanded config:
# {
#   "nodes": [
#     {"name": "extract", "input": [...], "function": "..."},
#     {"name": "transform", "dependencies": ["extract"], "function": "..."},
#     {"name": "load", "dependencies": ["transform"], "output": [...], "function": "..."}
#   ],
#   "inputs": [],
#   "outputs": [],
#   "type": "batch",
#   "spark_config": {}
# }
```

Note: Context caches the validated pipelines via a cached property for efficiency.

---

## Loading and Validation

The loading flow:
1) ConfigLoaderFactory.load_config(...) loads dicts directly or uses the appropriate loader for YAML, JSON, or Python module.
2) ConfigValidator validates basic shape (type dict) and required global_settings keys.
3) PipelineValidator checks node references within pipelines.

Loaders:
- YAML: safe_load, returns {} if empty.
- JSON: json.load, returns {} if empty.
- Python: Executes a module and reads a top-level variable named config.

Example:

```python
from tauro.config.loaders import ConfigLoaderFactory

loader = ConfigLoaderFactory()
cfg = loader.load_config("config/global.yml")  # dict
```

---

## Variable Interpolation

VariableInterpolator supports placeholders in strings using:
- Environment variables (highest precedence)
- A variables dict (fallback)

Example path in config:
- "filepath": "s3://bucket/${ENV_STAGE}/data/${date}"

Interpolation behavior:
- ${ENV_STAGE} is replaced by os.environ["ENV_STAGE"] if set.
- ${date} is replaced by variables["date"] if provided.

Context automatically interpolates filepaths in input_config and output_config using global_settings as a variables source.

```python
from tauro.config.interpolator import VariableInterpolator

raw = "abfss://cont@acct.dfs.core.windows.net/${ENV_STAGE}/tbl=${date}"
out = VariableInterpolator.interpolate(raw, {"date": "2025-01-01"})
# If ENV_STAGE=dev in environment -> abfss://.../dev/tbl=2025-01-01
```

---

## Format Policy (Streaming-Aware)

FormatPolicy provides:
- Supported streaming inputs (e.g., kafka, kinesis, delta_stream, file_stream, socket, rate, memory)
- Supported batch outputs (e.g., kafka, memory, console, delta, parquet, json, csv)
- Compatibility mapping between batch outputs and streaming inputs
- Which streaming inputs require checkpoints

You can override defaults via global_settings.format_policy.

Example:

```python
from tauro.config import FormatPolicy

policy = context.format_policy
policy.is_supported_input("kafka")         # True
policy.is_supported_output("delta")        # True
```

Notes:
- Unity Catalog is handled by IO/UC managers and typically not part of streaming format policy.
- ORC is supported in IO writers, but format policy for streaming focuses on streaming-oriented formats.

---

## Spark Session Lifecycle

SparkSessionFactory creates and manages a singleton Spark session:
- Modes: "local", "databricks", and "distributed" (an alias for "databricks").
- Databricks mode uses Databricks Connect and requires host, token, and cluster_id to be configured for the active environment.
- You can reset the session (for tests) with reset_session().

Example:

```python
from tauro.config import SparkSessionFactory

spark = SparkSessionFactory.get_session(mode=context.execution_mode)
# ... use spark ...
SparkSessionFactory.reset_session()  # useful in unit tests
```

ML and tuning-related Spark configs can be passed as ml_config when Context constructs the session. Internally, these are applied via builder.config(k, v).

---

## Exceptions

- ConfigLoadError: Raised when the system cannot load or parse a config source.
- ConfigValidationError: Raised when required keys or types are missing/incorrect.
- PipelineValidationError: Raised when a pipeline references missing nodes.

All errors are logged with contextual details (source path/type) before being raised.

---

## Minimal End-to-End Example

```python
from tauro.config import Context

# 1) Create the context (from files or dicts)
context = Context(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)

# 2) Access Spark
spark = context.spark

# 3) Use pipelines
pm = context._pipeline_manager
pipeline = pm.get_pipeline("daily_pipeline")
for node in pipeline["nodes"]:
    print(node["name"], "->", node.get("function"))

# 4) Use convenience attributes for IO/Exec
print(context.execution_mode)  # local/databricks/distributed
print(context.input_path)      # from global_settings
print(context.output_path)     # from global_settings
```

---

## Tips and Best Practices

- Keep global_settings minimal but explicit: input_path, output_path, mode are required.
- Use environment variables for secrets and environment-specific parts of paths; let VariableInterpolator resolve them.
- Centralize streaming format policies in global_settings.format_policy instead of sprinkling custom checks elsewhere.
- For Databricks/Distributed mode, ensure Databricks Connect is installed and configured (host, token, cluster_id).
- In tests, use dict-based configs and SparkSessionFactory.reset_session() between test cases to avoid session leakage.

---

## Troubleshooting

- Missing required keys (input_path, output_path, mode)
  - Ensure your global_settings contains all three keys.
- Invalid YAML/JSON
  - Check that files parse and contain a mapping (dict) at the root.
- Python loader fails
  - Ensure the module defines a top-level variable named config (a dict).
- Spark session errors
  - local: Verify pyspark is installed and available.
  - databricks/distributed: Ensure Databricks Connect packages are installed and environment variables are set for host/token/cluster_id.
- Interpolation not applied
  - Confirm placeholders are wrapped like ${VAR}; environment has priority over variables dict.

---

## Recent Improvements

- Context now exposes execution_mode, input_path, and output_path as top-level attributes for better IO/Exec compatibility.
- SparkSessionFactory accepts "distributed" as an alias for "databricks".
- ConfigLoaderFactory gained load_config to load dicts and files uniformly.
- VariableInterpolator clarifies precedence: environment variables first, then variables dict.

---

## API at a Glance

- Context(global_settings, pipelines_config, nodes_config, input_config, output_config)
  - Attributes: spark, execution_mode, input_path, output_path, format_policy, ...
- PipelineManager (internal via context._pipeline_manager)
  - list_pipeline_names(), get_pipeline(name), pipelines (expanded)
- ConfigLoaderFactory
  - load_config(source), get_loader(source)
- VariableInterpolator
  - interpolate(string, variables), interpolate_config_paths(config, variables)
- Validators
  - ConfigValidator, PipelineValidator, FormatPolicy
- SparkSessionFactory
  - get_session(mode, ml_config=None), reset_session(), set_protected_configs(list)

---
