# tauro.exec

The `tauro.exec` module orchestrates the execution of Tauro data pipelines (batch and hybrid), turning configuration into a dependency-aware execution plan and running nodes in parallel while respecting dependencies. It integrates tightly with:

- `tauro.config.Context` (for configuration, Spark session, and format policy)
- `tauro.io.input.InputLoader` and `tauro.io.output.DataOutputManager` (for reading inputs and persisting outputs)
- `tauro.streaming` (for streaming-specific execution, coordinated elsewhere)

This module provides:
- A command pattern for node execution (standard and ML nodes)
- Dependency resolution and topological sorting
- Safe parallel execution with retry/circuit-breaker support
- Validation utilities for pipeline shape and format compatibility
- Execution state tracking for monitoring and recovery
- Utilities to normalize and extract dependencies

---

## Key Concepts

- Node: A unit of work (function) that takes 0..N input DataFrames and date boundaries, and returns a DataFrame (or another artifact).
- Pipeline: An ordered set of nodes with explicit dependencies.
- Executor: Loads inputs, executes node functions, validates and saves outputs.
- Command: Encapsulates a node invocation (standard or ML-enhanced).
- DAG (Directed Acyclic Graph): Constructed from node dependencies; determines order/parallelism.

---

## Components Overview

- Commands (`commands.py`)
  - Command: Base protocol.
  - NodeCommand: Executes a node function.
  - MLNodeCommand: Adds ML concerns (hyperparameters, metrics, spark confs).
  - ExperimentCommand: Optional hyperparameter optimization (lazy dependency on `skopt`).
- NodeExecutor (`node_executor.py`)
  - Loads node function and inputs; executes a node using a Command; validates and persists results.
  - Parallel execution of multiple nodes respecting DAG.
- DependencyResolver (`dependency_resolver.py`)
  - Builds a dependency graph and performs topological sort.
  - Normalizes various dependency formats.
- PipelineValidator (`pipeline_validator.py`)
  - Validates basic pipeline shape and supported formats for batch/hybrid flows.
- Pipeline State (`pipeline_state.py`)
  - Tracks node status, dependencies, retries, metrics, and circuit breaker.
- Base Executor (`executor.py`)
  - Wires Context, IO, NodeExecutor, and validation; prepares ML metadata and pipeline execution.
- Utils (`utils.py`)
  - Helpers to normalize and extract dependencies, and to extract node names from mixed representations.

---

## Node Function Signature

Tauro supports flexible node function signatures. Recommended signature:

```python
def my_node(*dfs, start_date: str, end_date: str, ml_context: dict | None = None):
    # dfs: 0..N input DataFrames in configured order
    # start_date/end_date: ISO strings passed by the executor (keyword args)
    # ml_context (optional for ML nodes): hyperparams, spark, execution metadata, etc.
    ...
    return output_df
```

Notes:
- `NodeCommand` invokes the function passing `start_date` and `end_date` as keyword arguments for robustness.
- `MLNodeCommand` inspects the function signature; if it supports `ml_context`, it passes it, otherwise falls back to standard invocation.
- Functions must be importable via a dotted path (e.g., `package.module.function`) as defined in `nodes_config`.

---

## Node Configuration (from config layer)

Typical node configuration fields:
- name: Node name (required).
- function: Dotted path to the Python function to execute.
- input: List of input identifiers (resolved by `InputLoader`) or per-source descriptors (depending on IO config).
- output: List or descriptor of outputs (handled by `DataOutputManager`).
- dependencies: Node dependencies (string | dict | list; see below).
- hyperparams/metrics/description: Optional ML-related metadata.

Example (nodes_config.yml):

```yaml
extract:
  function: "my_pkg.etl.extract"
  input: ["src_raw"]
  output: ["bronze.events"]

transform:
  function: "my_pkg.etl.transform"
  dependencies: ["extract"]
  output: ["silver.events"]

train_model:
  function: "my_pkg.ml.train"
  dependencies:
    - transform
  hyperparams:
    learning_rate: 0.01
    max_depth: 6
```

---

## Dependencies: Formats and Normalization

`tauro.exec` accepts multiple dependency shapes:

- String: `"extract"`
- Dict: `{"extract": {}}` (only one key allowed)
- List: `["extract", "transform"]`

Normalization logic (in `utils.py` / `dependency_resolver.py`):
- `normalize_dependencies` converts any supported form into a list of dependency entries.
- `extract_dependency_name` extracts the name from strings or single-key dicts; raises for invalid shapes.

---

## Dependency Graph and Ordering

- `DependencyResolver.build_dependency_graph(pipeline_nodes, node_configs)` builds a DAG mapping each node to its dependents.
- `DependencyResolver.topological_sort(dag)` returns an execution order honoring dependencies.
- Circular dependencies raise a clear error.

---

## Commands

- NodeCommand
  - Minimal execution: load function and input DataFrames, call with dates as keyword args, return result.
- MLNodeCommand
  - Merges hyperparameters: defaults -> pipeline-level -> explicit overrides
  - Optionally configures Spark conf under the `tauro.ml.*` namespace for downstream context
  - Records execution metadata (start/end time, duration, status)
- ExperimentCommand
  - Lazy-imports `skopt` at execution time
  - Runs Bayesian optimization with `gp_minimize` on a provided objective and space

---

## NodeExecutor

Responsibilities:
- Resolve node config and import the node function
- Load all necessary input DataFrames via `InputLoader`
- Build a `Command` (NodeCommand or MLNodeCommand) depending on layer/context
- Execute and capture the result
- Validate and persist results via `DataOutputManager`
- Release resources explicitly (unpersist/close/clear) to encourage GC

Parallel execution:
- `execute_nodes_parallel(...)` uses `ThreadPoolExecutor` with `max_workers`
- Tracks running/completed/failed nodes; submits nodes whose dependencies are satisfied
- Retries and circuit breaker can be coordinated with `UnifiedPipelineState` (where used)

Resource cleanup:
- DataFrames are unpersisted if applicable; general objects may be closed/cleared when supported.

---

## Pipeline Validation (exec scope)

`PipelineValidator` (in `exec`) focuses on:
- Required params (pipeline name and date window) presence
- Pipeline config integrity (nodes existence, non-empty lists)
- Format compatibility hints for batch/hybrid (leveraging a `FormatPolicy` where applicable)
- Supported batch outputs include: `parquet`, `delta`, `json`, `csv`, `kafka`, `orc`

For streaming-specific validation, see `tauro.streaming.validators`.

---

## Unified Pipeline State

`UnifiedPipelineState` tracks:
- Per-node execution status (pending/running/completed/failed/retrying/cancelled)
- Dependencies and reverse dependents
- Failure counts and a circuit breaker threshold
- Batch outputs and streaming query handles for hybrid cases

Used by orchestrators to coordinate execution and react to failures.

---

## BaseExecutor

`BaseExecutor` wires everything together:
- `context`: sourced from `tauro.config.Context` or compatible object
- IO managers: `InputLoader` and `DataOutputManager`
- `NodeExecutor`: executes nodes with ML-aware behavior when applicable
- ML prep:
  - Merge hyperparams and resolve model version from context or pipeline-level config
  - Optionally integrate with a model registry provided by the context
  - Provide an `ml_info` dict to `NodeExecutor` for ML nodes

It also uses:
- `tauro.exec.pipeline_validator.PipelineValidator`
- `tauro.exec.dependency_resolver.DependencyResolver`
- `tauro.exec.utils` helpers

---

## Logging and Error Handling

- Uses `loguru` for structured logging.
- Catches and logs exceptions with context (node name, pipeline, durations).
- Raises explicit errors for:
  - Missing/invalid dependencies
  - Function import failures
  - Invalid node results
  - IO errors when saving outputs
- Streaming-specific errors are handled by `tauro.streaming` exceptions and decorators.

---

## Examples

### 1) Writing a Node Function

```python
# my_pkg/etl.py
from pyspark.sql import functions as F

def transform(df_events, *, start_date: str, end_date: str, ml_context=None):
    # df_events is first input
    out = df_events.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
    if ml_context:
        # optional: leverage hyperparams or metadata
        hp = ml_context.get("hyperparams", {})
        out = out.withColumn("hp_max_depth", F.lit(hp.get("max_depth", None)))
    return out
```

Note the signature uses keyword-only `start_date` and `end_date`. This is recommended.

### 2) Executing a Single Node

```python
from tauro.config import Context
from tauro.exec.node_executor import NodeExecutor
from tauro.io.input import InputLoader
from tauro.io.output import DataOutputManager

context = Context(...)

input_loader = InputLoader(context)
output_manager = DataOutputManager(context)

executor = NodeExecutor(context, input_loader, output_manager, max_workers=4)

ml_info = {
    "model_version": "1.0.0",
    "hyperparams": {"max_depth": 6},
    "pipeline_config": {},
}

executor.execute_single_node(
    node_name="transform",
    start_date="2025-01-01",
    end_date="2025-01-31",
    ml_info=ml_info,
)
```

### 3) Building and Executing a Small Pipeline

```python
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.node_executor import NodeExecutor

# Suppose you have a pipeline dict and nodes_config already loaded via context
pipeline = context._pipeline_manager.get_pipeline("daily_pipeline")
node_configs = context.nodes_config

# Extract node names
from tauro.exec.utils import extract_pipeline_nodes
pipeline_nodes = extract_pipeline_nodes(pipeline)

# Build DAG and order
dag = DependencyResolver.build_dependency_graph(pipeline_nodes, node_configs)
order = DependencyResolver.topological_sort(dag)

# Execute nodes in order (simple version; see NodeExecutor.execute_nodes_parallel for parallel execution)
for node_name in order:
    executor.execute_single_node(
        node_name=node_name,
        start_date=context.global_settings.get("start_date", "2025-01-01"),
        end_date=context.global_settings.get("end_date", "2025-01-31"),
        ml_info={},  # or computed via BaseExecutor._prepare_ml_info(...)
    )
```

### 4) ML Node with Hyperparameters

```python
ml_info = {
    "model_version": "2.1.0",
    "hyperparams": {
        "learning_rate": 0.05,
        "max_depth": 8,
    },
    "pipeline_config": {
        "model_name": "gbt_classifier",
        "metrics": ["auc", "f1"]
    },
}

executor.execute_single_node(
    "train_model", "2025-01-01", "2025-01-31", ml_info
)
```

---

## Best Practices

- Keep node functions pure with clear inputs/outputs; avoid side effects where possible.
- Always use keyword arguments for date bounds in your function signatures.
- Use `ml_context` if you need hyperparams, Spark session, or execution metadata.
- Keep dependencies explicit and minimal; avoid hidden couplings between nodes.
- Choose appropriate `max_workers` based on cluster resources and I/O characteristics.
- Release resources aggressively in long pipelines (e.g., cache/unpersist appropriately).

---



## API Quick Reference

- Commands
  - `Command.execute() -> Any`
  - `NodeCommand(function, input_dfs, start_date, end_date, node_name)`
  - `MLNodeCommand(..., model_version, hyperparams=None, node_config=None, pipeline_config=None, spark=None)`
  - `ExperimentCommand(objective_func, space, n_calls=20, random_state=None)`
- NodeExecutor
  - `execute_single_node(node_name, start_date, end_date, ml_info) -> None`
  - `execute_nodes_parallel(execution_order, node_configs, dag, start_date, end_date, ml_info) -> None`
- DependencyResolver
  - `build_dependency_graph(pipeline_nodes, node_configs) -> Dict[str, Set[str]]`
  - `topological_sort(dag) -> List[str]`
  - `get_node_dependencies(node_config) -> List[str]`
- PipelineValidator (exec)
  - `validate_required_params(pipeline_name, start_date, end_date, context_start_date, context_end_date)`
  - `validate_pipeline_config(pipeline)`
  - `validate_node_configs(pipeline_nodes, node_configs)`
  - `validate_hybrid_pipeline(pipeline, node_configs, format_policy=None) -> Dict[str, Any]`
- Pipeline State
  - `UnifiedPipelineState(circuit_breaker_threshold=3)`

---
