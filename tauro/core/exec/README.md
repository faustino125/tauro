# tauro.exec

A sophisticated execution engine for Apache Spark-based data pipelines. Orchestrates the execution of batch and hybrid (batch + streaming) pipelines by transforming configuration into dependency-aware execution plans, executing nodes in parallel while respecting dependencies, and providing comprehensive state tracking and error recovery capabilities.

This module integrates tightly with:
- `tauro.config.Context` (configuration, Spark session, format policy, and global settings)
- `tauro.io.input.InputLoader` and `tauro.io.output.DataOutputManager` (reading inputs and persisting outputs)
- `tauro.streaming.StreamingPipelineManager` (orchestrating streaming nodes in hybrid pipelines)

This module provides:
- A command pattern for node execution (standard and ML-enhanced nodes)
- Intelligent dependency resolution with topological sorting
- Safe parallel execution with retry and circuit-breaker support
- Comprehensive validation for pipeline shape and format compatibility
- Unified execution state tracking for monitoring, recovery, and debugging
- Resource cleanup and memory management
- ML-aware execution with hyperparameter injection and experiment optimization
- Utilities to normalize and extract dependencies from various formats

---

## Table of Contents

- [Key Concepts](#key-concepts)
- [Architecture Overview](#architecture-overview)
- [Components Reference](#components-reference)
- [Node Function Signatures](#node-function-signatures)
- [Dependencies Format and Normalization](#dependencies-format-and-normalization)
- [Dependency Resolution and Ordering](#dependency-resolution-and-ordering)
- [Commands](#commands)
- [Node Executor](#node-executor)
- [Pipeline Validation](#pipeline-validation)
- [Unified Pipeline State](#unified-pipeline-state)
- [Base Executor](#base-executor)
- [Execution Modes](#execution-modes)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Getting Started](#getting-started)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [API Quick Reference](#api-quick-reference)

---

## Key Concepts

### Node
A unit of work representing a data transformation or processing step. Each node:
- Has an identifier (name) and a corresponding function
- Accepts 0 to N input DataFrames
- Receives date boundaries (start_date, end_date) for filtering and parameterization
- Returns a single output DataFrame or artifact
- May have dependencies on other nodes
- Can be annotated with ML metadata (hyperparameters, metrics, etc.)

### Pipeline
An ordered set of nodes with explicit inter-node dependencies. Pipelines can be:
- **Batch pipelines**: All nodes are batch operations
- **Streaming pipelines**: All nodes are streaming operations
- **Hybrid pipelines**: Mix of batch and streaming nodes with coordinated execution

### Executor
The orchestration engine that:
- Loads inputs via InputLoader
- Creates execution commands for nodes
- Validates configurations
- Executes nodes according to dependency ordering
- Saves outputs via DataOutputManager
- Tracks state and provides monitoring capabilities

### Command
An encapsulation of a single node invocation with:
- Function reference
- Input DataFrames
- Date boundaries
- Optional ML-specific metadata

### DAG (Directed Acyclic Graph)
A graph representation of dependencies where:
- Nodes are pipeline tasks
- Edges represent dependencies
- Topological sorting determines valid execution orders
- Cycles are detected and raise clear errors

### Dependency
References between nodes that establish:
- Execution order constraints
- Data flow patterns
- Node relationships for scheduling

---

## Architecture Overview

The execution module is built in layers:

### Layer 1: Configuration & Parsing
- Reads pipeline and node configurations from Context
- Normalizes dependency formats (string, dict, list)
- Validates configuration integrity

### Layer 2: Dependency Analysis
- Builds dependency graph from configuration
- Performs topological sort for execution ordering
- Detects circular dependencies
- Identifies independent nodes for parallel execution

### Layer 3: Command Construction
- Creates appropriate command objects (NodeCommand, MLNodeCommand, etc.)
- Injected with function references, inputs, and parameters
- Configures ML-specific parameters when applicable

### Layer 4: Execution Engine
- Uses ThreadPoolExecutor for parallel execution
- Respects dependency constraints
- Handles retries and circuit-breaker logic
- Manages resource lifecycle (caching, unpersisting)

### Layer 5: State Management
- Tracks per-node execution state (pending, running, completed, failed, retrying)
- Records execution metadata (start time, duration, status)
- Provides progress monitoring and recovery points

### Layer 6: Output Management
- Validates result schemas
- Persists outputs via DataOutputManager
- Coordinates resource cleanup

---

## Components Reference

### Commands (`commands.py`)

**Command Protocol**
- Base interface for node execution
- Requires `execute()` method returning the result
- Enables testability and extensibility

**NodeCommand**
- Minimal execution for standard nodes
- Loads function and input DataFrames
- Invokes with dates as keyword arguments
- Returns result DataFrame

**MLNodeCommand**
- Extends NodeCommand for machine learning
- Merges hyperparameters: defaults → pipeline-level → explicit overrides
- Configures Spark properties under `tauro.ml.*` namespace
- Records execution metadata (duration, status)
- Optionally passes `ml_context` dict to function if supported

**ExperimentCommand**
- Lazy-imports `skopt` at execution time
- Runs Bayesian optimization with `gp_minimize`
- Uses configured search space and objective function
- Returns optimized parameters

### NodeExecutor (`node_executor.py`)

Responsibilities:
- Resolves node configuration and imports node functions
- Loads input DataFrames via InputLoader
- Builds appropriate Command object based on context
- Executes command and captures result
- Validates output schema
- Persists results via DataOutputManager
- Releases resources explicitly (unpersist, close, clear)

Key methods:
```python
execute_single_node(node_name, start_date, end_date, ml_info)
execute_nodes_parallel(execution_order, node_configs, dag, start_date, end_date, ml_info)
```

Parallel execution features:
- ThreadPoolExecutor with configurable max_workers
- Dynamic scheduling based on completed dependencies
- Retry support with exponential backoff
- Circuit breaker for cascading failures

### DependencyResolver (`dependency_resolver.py`)

Responsibilities:
- Builds dependency graph from configuration
- Performs topological sorting
- Normalizes dependency formats
- Detects circular dependencies

Key methods:
```python
build_dependency_graph(pipeline_nodes, node_configs) -> Dict[str, Set[str]]
topological_sort(dag) -> List[str]
get_node_dependencies(node_config) -> List[str]
normalize_dependencies(deps) -> List[str]
```

### PipelineValidator (`pipeline_validator.py`)

Focuses on batch/hybrid pipeline validation:
- Required parameters (pipeline name, date window)
- Pipeline configuration integrity
- Node configuration completeness
- Format compatibility (leveraging FormatPolicy)
- Supported batch outputs: parquet, delta, json, csv, kafka, orc
- Batch-streaming compatibility in hybrid pipelines

Key methods:
```python
validate_required_params(pipeline_name, start_date, end_date, context_start_date, context_end_date)
validate_pipeline_config(pipeline)
validate_node_configs(pipeline_nodes, node_configs)
validate_hybrid_pipeline(pipeline, node_configs, format_policy=None)
```

### Pipeline State (`pipeline_state.py`)

**UnifiedPipelineState** tracks:
- Per-node execution status (pending, running, completed, failed, retrying, cancelled)
- Failure counts and circuit breaker threshold
- Dependencies and reverse dependents
- Batch output DataFrame handles
- Streaming query execution IDs
- Execution metadata (timestamps, durations)

Used by orchestrators to:
- Coordinate batch and streaming execution
- React to failures with retry or skip logic
- Provide monitoring and debugging information
- Support recovery from partial failures

### Base Executor (`executor.py`)

Wires all components together:
- Manages Context, IO managers, and NodeExecutor
- Prepares ML metadata and resolves hyperparameters
- Orchestrates batch, streaming, and hybrid execution
- Validates using PipelineValidator
- Resolves dependencies using DependencyResolver
- Tracks state using UnifiedPipelineState
- Provides high-level API for pipeline execution

### Utils (`utils.py`)

Helper functions:
- `normalize_dependencies()`: Convert various dependency formats to lists
- `extract_dependency_name()`: Extract node name from strings or dicts
- `extract_pipeline_nodes()`: Get node list from pipeline config
- `get_node_dependencies()`: Retrieve dependencies for a node

---

## Node Function Signatures

### Recommended Signature

```python
def my_node(*dfs, start_date: str, end_date: str, ml_context: dict | None = None):
    # dfs: 0..N input DataFrames in configured order
    # start_date/end_date: ISO strings (YYYY-MM-DD) passed as keyword arguments
    # ml_context (optional for ML nodes): dict with hyperparams, spark, execution metadata
    ...
    return output_df
```

### Important Notes

- Always use keyword-only arguments for `start_date` and `end_date` (after `*dfs`)
- Functions must be importable via dotted path (e.g., `package.module.function`)
- NodeCommand automatically passes dates as keyword arguments
- MLNodeCommand inspects signature; passes `ml_context` only if function accepts it
- Functions should be pure when possible (avoid global state mutations)

### Example Node Functions

```python
# Minimal node (no inputs)
def generate_data(*, start_date: str, end_date: str):
    return spark.range(0, 100).toDF()

# Single input
def filter_events(events_df, *, start_date: str, end_date: str):
    return events_df.filter(
        (col("date") >= start_date) & (col("date") <= end_date)
    )

# Multiple inputs
def join_datasets(customers, orders, *, start_date: str, end_date: str):
    return customers.join(orders, "customer_id")

# ML node with hyperparameters
def train_model(training_df, *, start_date: str, end_date: str, ml_context=None):
    if ml_context:
        hyperparams = ml_context.get("hyperparams", {})
        lr = hyperparams.get("learning_rate", 0.01)
    else:
        lr = 0.01
    
    model = GBTClassifier(learningRate=lr)
    return model.fit(training_df)
```

---

## Node Configuration

Node configurations define execution parameters:

```yaml
extract:
  function: "my_pkg.etl.extract"
  input: ["src_raw"]
  output: ["bronze.events"]
  
transform:
  function: "my_pkg.etl.transform"
  input:
    - bronze.events  # Automatic dependency from input
  output: ["silver.events"]
  dependencies: ["extract"]  # Explicit dependency
  
train_model:
  function: "my_pkg.ml.train"
  input: ["silver.events"]
  dependencies:
    - transform
  output: ["model"]
  hyperparams:
    learning_rate: 0.01
    max_depth: 6
  metrics:
    - accuracy
    - precision
    - recall
```

**Configuration Fields:**
- `function`: Dotted path to Python function
- `input`: List of input identifiers
- `output`: Output identifier or list
- `dependencies`: Explicit dependencies (string, dict, or list)
- `hyperparams`: ML-related hyperparameters (optional)
- `metrics`: Metrics to track (optional)
- `description`: Human-readable description (optional)

---

## Dependencies Format and Normalization

The exec module supports multiple dependency specification formats:

### Format Examples

```yaml
# Format 1: String (single dependency)
dependencies: "extract"

# Format 2: Dictionary (single key)
dependencies:
  extract: {}

# Format 3: List of strings
dependencies:
  - extract
  - transform

# Format 4: List of dictionaries (single key each)
dependencies:
  - extract: {}
  - transform: {}

# Format 5: Mixed list
dependencies:
  - extract
  - transform: {}
```

### Normalization Process

All formats normalize to a list of node names:
- `"extract"` → `["extract"]`
- `{"extract": {}}` → `["extract"]`
- `["extract", "transform"]` → `["extract", "transform"]`
- `[{"extract": {}}, "transform"]` → `["extract", "transform"]`

Normalization happens automatically in:
- `DependencyResolver.normalize_dependencies()`
- `PipelineValidator._get_node_dependencies()`

---

## Dependency Resolution and Ordering

### Graph Construction

`DependencyResolver.build_dependency_graph()` creates a mapping:
- **Key**: Node name
- **Value**: Set of nodes that depend on it (reverse dependencies)

Example:
```python
# Pipeline: extract → transform → model
# Dependencies: transform depends on extract, model depends on transform
dag = {
    "extract": {"transform"},
    "transform": {"model"},
    "model": set(),
}
```

### Topological Sorting

`DependencyResolver.topological_sort()` returns valid execution order:
```python
order = DependencyResolver.topological_sort(dag)
# Result: ["extract", "transform", "model"]
```

### Circular Dependency Detection

Circular dependencies raise clear error:
```python
# Invalid: node2 depends on node1, node1 depends on node2
raise ValueError("Circular dependency detected: node1 <-> node2")
```

### Execution Planning

From topological order, the executor:
1. Identifies independent nodes (no unmet dependencies)
2. Schedules them in parallel
3. Waits for dependencies to complete
4. Schedules next batch of ready nodes
5. Continues until all nodes complete or failure occurs

---

## Commands

### NodeCommand

Standard node execution without ML enhancements.

```python
command = NodeCommand(
    function=my_node_function,
    input_dfs=[df1, df2],
    start_date="2025-01-01",
    end_date="2025-01-31",
    node_name="transform",
)

result = command.execute()
```

### MLNodeCommand

Machine learning node with hyperparameter support.

```python
command = MLNodeCommand(
    function=train_model_function,
    input_dfs=[training_df],
    start_date="2025-01-01",
    end_date="2025-01-31",
    node_name="train_model",
    model_version="v1.0.0",
    hyperparams={"learning_rate": 0.05, "max_depth": 8},
    node_config={"metrics": ["auc", "f1"]},
    pipeline_config={"model_name": "gbt_classifier"},
    spark=spark_session,
)

model = command.execute()
```

**Hyperparameter Merging:**
1. Start with defaults from `MLNodeCommand`
2. Merge pipeline-level hyperparams
3. Apply explicit overrides
4. Pass final dict to function via `ml_context`

### ExperimentCommand

Bayesian optimization for hyperparameter tuning.

```python
from skopt import Real, Integer

def objective(params):
    # Train model with params, return validation loss
    ...

space = [
    Real(0.001, 0.1, name="learning_rate"),
    Integer(3, 10, name="max_depth"),
]

command = ExperimentCommand(
    objective_func=objective,
    space=space,
    n_calls=20,
    random_state=42,
)

best_params = command.execute()
```

---

## Node Executor

### Responsibilities

1. **Resolution**: Find and load node function
2. **Input Loading**: Retrieve input DataFrames via InputLoader
3. **Command Building**: Create appropriate Command object
4. **Execution**: Run command and capture result
5. **Validation**: Check output schema
6. **Persistence**: Save results via DataOutputManager
7. **Cleanup**: Release resources explicitly

### Single Node Execution

```python
executor = NodeExecutor(
    context=context,
    input_loader=input_loader,
    output_manager=output_manager,
    max_workers=4,
)

executor.execute_single_node(
    node_name="transform",
    start_date="2025-01-01",
    end_date="2025-01-31",
    ml_info={"hyperparams": {"lr": 0.01}},
)
```

### Parallel Execution

```python
# Execute nodes respecting dependencies
executor.execute_nodes_parallel(
    execution_order=["extract", "transform", "model"],
    node_configs=node_configs,
    dag=dependency_graph,
    start_date="2025-01-01",
    end_date="2025-01-31",
    ml_info={},
)
```

**Parallel Execution Features:**
- ThreadPoolExecutor with configurable worker count
- Dynamic scheduling based on dependency satisfaction
- Automatic retry with exponential backoff
- Circuit breaker to prevent cascading failures
- Resource cleanup per node

### Resource Management

```python
# Explicit cleanup
if hasattr(df, 'unpersist'):
    df.unpersist()

# Close connections
if hasattr(resource, 'close'):
    resource.close()

# Clear caches
if hasattr(cache, 'clear'):
    cache.clear()
```

---

## Pipeline Validation

### Validation Scope (Batch/Hybrid)

The exec module validates:
- Required parameters present and valid
- Pipeline configuration has required structure
- All referenced nodes have configurations
- Format compatibility with batch/streaming
- Supported output formats (parquet, delta, json, csv, kafka, orc)
- Hybrid pipeline structure

### Streaming Validation

For streaming-specific validation, see `tauro.streaming.validators.StreamingValidator`.

### Validation Example

```python
from tauro.exec.pipeline_validator import PipelineValidator

# Validate required parameters
PipelineValidator.validate_required_params(
    pipeline_name="daily_pipeline",
    start_date="2025-01-01",
    end_date="2025-01-31",
    context_start_date="2025-01-01",
    context_end_date="2025-01-31",
)

# Validate pipeline configuration
PipelineValidator.validate_pipeline_config(pipeline)

# Validate node configurations
PipelineValidator.validate_node_configs(pipeline_nodes, node_configs)

# Validate hybrid pipeline
result = PipelineValidator.validate_hybrid_pipeline(
    pipeline, node_configs, context.format_policy
)
print(result["batch_nodes"])      # List of batch nodes
print(result["streaming_nodes"])  # List of streaming nodes
```

---

## Unified Pipeline State

Tracks comprehensive execution state:

```python
state = UnifiedPipelineState(circuit_breaker_threshold=3)

# Register nodes
state.register_node_with_dependencies("transform", ["extract"])

# Update status
state.set_node_status("extract", "completed")
state.set_node_status("transform", "running")

# Retrieve status
status = state.get_node_status("transform")
dependencies = state.get_node_dependencies("transform")

# Handle failures
if state.get_node_failure_count("node") > 3:
    state.set_node_status("node", "cancelled")
```

---

## Base Executor

Highest-level orchestration API:

```python
executor = BaseExecutor(
    context=context,
    input_loader=input_loader,
    output_manager=output_manager,
    streaming_manager=streaming_manager,
)

# Execute batch pipeline
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
    max_workers=4,
)

# Execute hybrid pipeline
executor.execute_hybrid_pipeline(
    pipeline_name="streaming_etl",
    model_version="v1.0.0",
    hyperparams={"lr": 0.01},
)

# Execute streaming pipeline
executor.execute_streaming_pipeline(
    pipeline_name="realtime_ingestion",
    execution_mode="async",  # or "sync"
)
```

---

## Execution Modes

### Batch Pipeline Execution

Traditional sequential or parallel execution:
- Validates all inputs upfront
- Builds complete dependency graph
- Executes according to topological order
- Persists all outputs
- Returns execution summary

### Streaming Pipeline Execution

Streaming-optimized execution:
- Delegates to StreamingPipelineManager
- Manages long-running query lifecycle
- Supports both synchronous (wait) and asynchronous (background) modes
- Handles resource conflicts
- Provides query monitoring

### Hybrid Pipeline Execution

Coordinated batch and streaming:
- Validates both batch and streaming nodes
- Executes batch nodes first
- Transitions to streaming phase
- Manages data flow between phases
- Provides unified state tracking
- Graceful shutdown of streaming on completion

---

## Best Practices

### 1. Node Function Design

```python
# ✅ DO: Use keyword-only date arguments
def my_node(df1, df2, *, start_date: str, end_date: str):
    return df1.filter(...)

# ❌ DON'T: Use positional date arguments
def bad_node(df1, df2, start_date, end_date):
    ...
```

### 2. Dependency Management

```yaml
# ✅ DO: Explicit, minimal dependencies
transform:
  dependencies: ["extract"]
  input: ["bronze.events"]

# ❌ DON'T: Rely only on implicit input dependencies
transform:
  input: ["bronze.events"]  # Circular if input is output of another node
```

### 3. Resource Management

```python
# ✅ DO: Cache when reused, unpersist when done
df.cache()
result = df.count()
df.unpersist()

# ❌ DON'T: Leave large DataFrames in memory
large_df.cache()
# ... never unpersist
```

### 4. Worker Configuration

```python
# ✅ DO: Match workers to cluster resources
max_workers = min(spark_partition_count // 2, available_cores // 2)

# ❌ DON'T: Use excessive workers
max_workers = 100  # Causes thread pool overhead
```

### 5. Error Handling

```python
# ✅ DO: Let exceptions propagate with context
try:
    execute_nodes_parallel(...)
except Exception as e:
    logger.error(f"Pipeline failed: {e}")
    # Clean up resources
    raise

# ❌ DON'T: Silently catch all exceptions
try:
    execute_nodes_parallel(...)
except:
    pass  # Critical failures hidden!
```

### 6. Hyperparameter Management

```python
# ✅ DO: Define defaults, allow overrides
ml_info = {
    "hyperparams": {
        "learning_rate": 0.01,  # Default
        "max_depth": 6,
    }
}

# ❌ DON'T: Hardcode hyperparameters
model = GBTClassifier(learningRate=0.01, maxDepth=6)
```

---

## Error Handling

### Exception Hierarchy

- `ExecutionError`: Base execution exception
- `NodeExecutionError`: Node-specific failures
- `DependencyError`: Dependency resolution issues
- `ValidationError`: Configuration validation failures
- `ResourceError`: Resource/memory issues

### Common Error Scenarios

| Error | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError` | Function not importable | Check dotted path in `function` config |
| `TypeError` | Function signature mismatch | Ensure keyword-only date arguments |
| `ValidationError` | Invalid configuration | Run validation separately to diagnose |
| `CircularDependencyError` | Dependency cycle | Review dependency graph |
| `OutOfMemoryError` | Too many nodes in parallel | Reduce `max_workers` |
| `TimeoutError` | Node execution too slow | Increase timeout or optimize function |

### Logging and Debugging

```python
from loguru import logger

logger.enable("tauro.exec")
logger.add(sys.stderr, level="DEBUG")

# Trace execution
executor.execute_pipeline(...)
```

---

## Getting Started

### Setup

```python
from tauro.config import Context
from tauro.io.input import InputLoader
from tauro.io.output import DataOutputManager
from tauro.exec.executor import BaseExecutor

# 1. Load context
context = Context.load_from_file("config.yaml")

# 2. Create managers
input_loader = InputLoader(context)
output_manager = DataOutputManager(context)

# 3. Create executor
executor = BaseExecutor(
    context=context,
    input_loader=input_loader,
    output_manager=output_manager,
)

# 4. Execute pipeline
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
    max_workers=4,
)
```

---

## Examples

### Example 1: Simple ETL Pipeline

```yaml
# nodes_config.yml
extract:
  function: "my_pkg.etl.extract_raw"
  output: ["bronze.raw_events"]

transform:
  function: "my_pkg.etl.transform"
  dependencies: ["extract"]
  output: ["silver.events"]

aggregate:
  function: "my_pkg.etl.aggregate"
  dependencies: ["transform"]
  output: ["gold.event_summary"]
```

```python
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
)
# Execution order: extract → transform → aggregate
```

### Example 2: ML Pipeline with Hyperparameters

```yaml
# nodes_config.yml
prepare_data:
  function: "my_pkg.ml.prepare_data"
  output: ["training_data"]

train_model:
  function: "my_pkg.ml.train"
  dependencies: ["prepare_data"]
  output: ["model"]
  hyperparams:
    learning_rate: 0.01
    max_depth: 6

evaluate_model:
  function: "my_pkg.ml.evaluate"
  dependencies: ["train_model"]
  output: ["metrics"]
  metrics:
    - accuracy
    - precision
```

```python
executor.execute_pipeline(
    pipeline_name="daily_ml",
    start_date="2025-01-01",
    end_date="2025-01-31",
    model_version="v1.0.0",
    hyperparams={"learning_rate": 0.05},  # Override
)
```

### Example 3: Hybrid Pipeline

```yaml
# nodes_config.yml
type: "hybrid"

extract_batch:
  function: "my_pkg.batch.extract"
  output: ["bronze.events"]

stream_events:
  function: "my_pkg.streaming.stream_kafka"
  output: ["stream.events"]

join_sources:
  function: "my_pkg.ml.join"
  dependencies:
    - extract_batch
    - stream_events
  output: ["gold.unified"]
```

```python
executor.execute_hybrid_pipeline(
    pipeline_name="hybrid_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
)
```

---

## Troubleshooting

### Circular Dependency Error

```
ValueError: Circular dependency detected: node1 <-> node2
```

**Solution:**
1. Review node dependencies in configuration
2. Draw dependency graph manually
3. Remove or restructure circular references

### Function Not Importable

```
ModuleNotFoundError: No module named 'my_pkg.etl'
```

**Solution:**
1. Verify function path in `nodes_config`
2. Ensure package is in Python path
3. Check for typos in dotted path

### Signature Mismatch

```
TypeError: my_node() got an unexpected keyword argument 'start_date'
```

**Solution:**
1. Modify function to accept keyword arguments
2. Use `*, start_date: str, end_date: str` syntax

### Out of Memory

```
java.lang.OutOfMemoryError: Java heap space
```

**Solution:**
1. Reduce `max_workers`
2. Add explicit `.unpersist()` calls
3. Increase Spark executor memory

### Validation Errors

```
ValidationError: Pipeline validation failed
```

**Solution:**
1. Run validation separately: `PipelineValidator.validate_pipeline_config()`
2. Check error message for specific field
3. Review configuration against schema

---

## API Quick Reference

### BaseExecutor

```python
executor = BaseExecutor(context, input_loader, output_manager, streaming_manager)

# Batch execution
executor.execute_pipeline(pipeline_name, start_date, end_date, max_workers=4)

# Hybrid execution
executor.execute_hybrid_pipeline(pipeline_name, model_version, hyperparams)

# Streaming execution
executor.execute_streaming_pipeline(pipeline_name, execution_mode="async")
```

### NodeExecutor

```python
executor = NodeExecutor(context, input_loader, output_manager, max_workers=4)

# Execute single node
executor.execute_single_node(node_name, start_date, end_date, ml_info)

# Execute multiple nodes in parallel
executor.execute_nodes_parallel(execution_order, node_configs, dag, start_date, end_date, ml_info)
```

### DependencyResolver

```python
# Build dependency graph
dag = DependencyResolver.build_dependency_graph(pipeline_nodes, node_configs)

# Get execution order
order = DependencyResolver.topological_sort(dag)

# Get node dependencies
deps = DependencyResolver.get_node_dependencies(node_config)

# Normalize dependencies
normalized = DependencyResolver.normalize_dependencies(dependencies)
```

### PipelineValidator

```python
# Validate required parameters
PipelineValidator.validate_required_params(pipeline_name, start_date, end_date, ctx_start, ctx_end)

# Validate configuration
PipelineValidator.validate_pipeline_config(pipeline)
PipelineValidator.validate_node_configs(pipeline_nodes, node_configs)

# Validate hybrid pipeline
result = PipelineValidator.validate_hybrid_pipeline(pipeline, node_configs, format_policy)
```

### Commands

```python
# Standard node execution
command = NodeCommand(function, input_dfs, start_date, end_date, node_name)
result = command.execute()

# ML node execution
command = MLNodeCommand(function, input_dfs, start_date, end_date, node_name,
                        model_version, hyperparams, node_config, pipeline_config, spark)
result = command.execute()

# Experiment execution
command = ExperimentCommand(objective_func, space, n_calls, random_state)
best_params = command.execute()
```

---

## Architecture Improvements

Recent enhancements to the execution module:

- **Enhanced Error Context**: Detailed exception messages with operation, component, and field information
- **Unified State Management**: Comprehensive tracking of execution across batch and streaming
- **Smart Parallelization**: Automatic detection of independent nodes for optimal throughput
- **ML Integration**: First-class support for hyperparameter injection and experiment management
- **Resource Lifecycle**: Explicit cleanup of DataFrames and connections
- **Format Policy Integration**: Batch/streaming format compatibility via context policy
- **Comprehensive Validation**: Multi-layer validation for configuration integrity

---

## Notes

- The exec module coordinates batch and hybrid pipelines; streaming is delegated to `tauro.streaming`
- Use `DependencyResolver` for complex pipeline analysis and visualization
- Keep node functions pure; use `ml_context` for parameterization
- Always set appropriate `max_workers` based on cluster resources
- For very large pipelines, consider breaking into stages with intermediate persistence
- Integrate with a model registry via `context` for production ML workflows

---

## License

Copyright (c) 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.

---
