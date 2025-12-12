# 🚀 tauro.exec - Execution Engine

**Advanced orchestration engine for Apache Spark data pipelines** with intelligent dependency resolution, parallel execution, and automatic MLOps integration.

## ✨ Key Features

| Feature | Benefit |
|---------|---------|
| **Smart Dependency Resolution** | Automatic topological sorting → parallel execution |
| **Unified State Tracking** | Real-time monitoring across batch/streaming/hybrid |
| **ML-Ready** | Auto-detect ML nodes, automatic hyperparameter injection |
| **Zero-Config MLOps** | Experiment tracking auto-enabled when needed |
| **Resilient Execution** | Retry logic, circuit breaker, timeout management |
| **Format Validation** | Catch incompatibilities at validation time |
| **Resource Management** | Explicit cleanup, memory optimization |

## 🔗 Integrations

**Core Dependencies:**
- `tauro.config.Context` → Configuration, Spark session, format policies
- `tauro.io` → InputLoader, DataOutputManager
- `tauro.streaming` → Hybrid pipeline orchestration
- `tauro.mlops` → Auto-detected experiment tracking & model registry

## 📋 What This Module Does

```
Your config.yaml
        ↓
  [DependencyResolver]  ← Builds DAG, detects cycles
        ↓
[PipelineValidator]    ← Format compatibility, schema validation
        ↓
 [NodeExecutor]        ← Parallel execution with retries
        ↓
[UnifiedPipelineState] ← Track progress, failures, recovery
        ↓
   Outputs saved
```

---

## 📚 Table of Contents

**Getting Started**
- [Quick Start (5 min)](#-quick-start) 
- [Examples by Use Case](#-use-case-examples)

**Core Concepts**
- [Key Concepts](#-key-concepts)
- [Node Functions](#node-function-signatures)
- [Dependencies](#dependencies-format-and-normalization)

**Architecture**
- [Architecture Overview](#architecture-overview)
- [Execution Modes](#execution-modes)
- [Components Deep Dive](#components-reference)

**MLOps Integration**
- [MLOps Guide](#mlops-zero-config-by-default)
- [ML Configuration](#ml-node-configuration)
- [Auto-Detection](#auto-detection-internals)

**Advanced Topics**
- [Error Handling & Recovery](#error-handling)
- [Performance & Tuning](#best-practices)
- [API Reference](#api-quick-reference)
- [Troubleshooting](#troubleshooting)

---

## ⚡ Quick Start

### 1. Install & Setup (2 min)

```python
from tauro.config import Context
from tauro.io.input import InputLoader
from tauro.io.output import DataOutputManager
from tauro.exec import BaseExecutor

# Load configuration
context = Context.load("config.yaml")

# Create executor
executor = BaseExecutor(
    context=context,
    input_loader=InputLoader(context),
    output_manager=DataOutputManager(context),
)
```

### 2. Define Pipeline (in config.yaml)

```yaml
pipelines:
  daily_etl:
    nodes: [extract, transform, load]

nodes:
  extract:
    function: "my_pkg.etl.extract"
    output: ["bronze.raw"]
  
  transform:
    function: "my_pkg.etl.transform"
    dependencies: ["extract"]
    output: ["silver.events"]
  
  load:
    function: "my_pkg.etl.load"
    dependencies: ["transform"]
    output: ["gold.dashboard"]
```

### 3. Execute (1 line!)

```python
# Automatic: dependency resolution → parallel execution → output persistence
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
    max_workers=4,  # Auto-parallelize independent nodes
)
```

**That's it!** 🎉 The executor handles:
- ✅ Resolving dependencies  
- ✅ Validating formats  
- ✅ Loading inputs  
- ✅ Running nodes in parallel  
- ✅ Saving outputs  
- ✅ Tracking state  

---

## 🎯 Use Case Examples

### Use Case 1: Simple ETL (Recommended Starting Point)

**Scenario:** Extract raw data → Transform → Load

```yaml
nodes:
  extract:
    function: "etl.extract_raw"
    output: ["bronze.events"]
  
  transform:
    function: "etl.clean_and_normalize"
    dependencies: ["extract"]
    output: ["silver.events"]
  
  load:
    function: "etl.aggregate"
    dependencies: ["transform"]
    output: ["gold.summary"]
```

```python
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
)
# Execution: extract → transform → load (sequential dependencies)
```

---

### Use Case 2: ML Pipeline (Auto MLOps)

**Scenario:** Feature engineering → Train → Evaluate

```yaml
nodes:
  prepare_features:  # Auto-detected as ML
    function: "ml.prepare_features"
    output: ["features"]
  
  train_model:  # Auto-detected as ML (name starts with "train_")
    function: "ml.train_xgboost"
    dependencies: ["prepare_features"]
    output: ["model"]
    hyperparams:
      learning_rate: 0.01
      max_depth: 6
  
  evaluate:  # Auto-detected as ML
    function: "ml.evaluate"
    dependencies: ["train_model"]
    output: ["metrics"]
    metrics: [accuracy, precision, auc]
```

```python
# ✨ MLOps auto-activated! Experiment tracking included
executor.execute_pipeline(
    pipeline_name="daily_ml",
    start_date="2025-01-01",
    end_date="2025-01-31",
    hyperparams={"learning_rate": 0.05},  # Override defaults
)

# Check: MLflow experiment "daily_ml" created automatically
# Check: Model artifacts logged
# Check: Metrics tracked
```

---

### Use Case 3: Hybrid Pipeline (Batch + Streaming)

**Scenario:** Batch data + streaming events → unified processing

```yaml
nodes:
  load_batch:
    function: "batch.load_historical"
    output: ["historical_data"]
  
  stream_events:
    function: "streaming.kafka_stream"
    output: ["live_events"]
  
  join_sources:  # Depends on both batch and streaming
    function: "hybrid.join_and_enrich"
    dependencies:
      - load_batch
      - stream_events
    output: ["unified_stream"]
```

```python
executor.execute_hybrid_pipeline(
    pipeline_name="realtime_ingestion",
    start_date="2025-01-01",
    end_date="2025-01-31",
)

# Execution: 
# 1. Run batch nodes in parallel (load_batch)
# 2. Start streaming (stream_events)
# 3. Join when both ready (join_sources)
# 4. Graceful shutdown
```

---

### Use Case 4: Parallel Independent Nodes

**Scenario:** Load 3 different data sources in parallel

```yaml
nodes:
  load_customers:
    function: "sources.load_customers"
    output: ["raw.customers"]
  
  load_orders:
    function: "sources.load_orders"
    output: ["raw.orders"]
  
  load_products:
    function: "sources.load_products"
    output: ["raw.products"]
  
  join_all:  # Depends on all three
    function: "transform.combine"
    dependencies:
      - load_customers
      - load_orders
      - load_products
    output: ["unified.data"]
```

```python
executor.execute_pipeline(pipeline_name="multi_source", ...)

# Execution:
# ┌─→ load_customers ──┐
# ├─→ load_orders     ├──→ join_all
# └─→ load_products ──┘
# (load_* run in parallel, then join_all)
```

---
## 🏗️ Key Concepts

### Node
**A unit of work** (function + inputs + configuration)

```
Node: "transform"
  ├─ Function: my_pkg.etl.transform
  ├─ Inputs: [bronze.events]
  ├─ Outputs: [silver.events]  
  ├─ Dependencies: [extract]
  └─ Dates: start_date, end_date
```

**Characteristics:**
- Accepts 0 to N input DataFrames
- Receives date boundaries for filtering
- Returns single output DataFrame
- Can have dependencies on other nodes
- Can include ML metadata (hyperparams, metrics)

---

### Pipeline
**Ordered set of nodes with explicit dependencies**

```yaml
# Example: ETL Pipeline
extract → transform → load
         ↓         ↓
    dependencies
```

**Types:**
- **Batch**: All nodes are batch operations (traditional ETL)
- **Streaming**: All nodes are streaming (Kafka, etc.)
- **Hybrid**: Mix of batch and streaming (modern data architecture)

---

### Executor
**The orchestration engine**

```
Pipeline Config → Validation → Dependency Graph → Parallel Execution → Output Persistence
```

**Responsibilities:**
1. Load inputs via InputLoader
2. Validate configuration (format, schema, dependencies)
3. Build execution plan (DAG + topological sort)
4. Execute nodes respecting dependencies
5. Save outputs via DataOutputManager
6. Track state and provide monitoring

---

### Command (Design Pattern)
**Encapsulates a single node invocation**

```python
# NodeCommand: Standard execution
command = NodeCommand(
    function=my_transform_func,
    input_dfs=[events_df],
    start_date="2025-01-01",
    end_date="2025-01-31",
)
result = command.execute()

# MLNodeCommand: ML-enhanced execution
command = MLNodeCommand(
    function=train_model_func,
    input_dfs=[features_df],
    hyperparams={"lr": 0.01, "max_depth": 6},
    ml_context={"model_version": "v1.0"}
)
model = command.execute()
```

---

### DAG (Directed Acyclic Graph)
**Dependency visualization**

```
Valid DAG (no cycles):
  extract → transform → load ✅

Invalid DAG (circular):
  A ↔ B (cycle detected) ❌
```

**Used for:**
- Detecting circular dependencies
- Determining execution order
- Identifying parallelizable nodes

---



## 🤖 MLOps: Zero-Config by Default

**Philosophy:** Auto-enable ML features when needed, zero overhead otherwise.

| Pipeline Type | MLOps Status | Config Needed |
|---|---|---|
| Pure ETL (no ML nodes) | ❌ Auto-disabled | None |
| ML prototype (few nodes) | ✅ Auto-enabled | None (uses defaults) |
| Production ML | ✅ Auto-enabled | `ml_info.yaml` (optional) |

---

### Auto-Detection: How It Works

The executor detects ML nodes automatically by pattern:

```python
# Detected as ML nodes (auto-enabled experiment tracking):
train_model()        # Name matches "train_*"
predict_churn()      # Name matches "predict_*"
ml_feature_engineering()  # Name matches "ml_*"

# NOT detected (standard execution):
extract_raw()        # Regular ETL
transform()          # Regular ETL
```

**Detection Patterns:**
- Node/function names: `train_*`, `predict_*`, `ml_*`, `model_*`, `fit_*`
- Artifacts: `*.pkl`, `*.joblib`, `*.h5`, `*.pt` outputs

---

### Quick Start: ML Pipeline (3 options)

#### Option 1: Zero-Config (Simplest) ⭐ Recommended

```yaml
# config/nodes.yaml - NO extra config needed!
nodes:
  prepare_data:
    function: "ml.prepare_data"
    output: ["features"]
  
  train_model:  # ← Auto-detected as ML
    function: "ml.train_xgboost"
    dependencies: ["prepare_data"]
    output: ["model"]
    hyperparams:
      learning_rate: 0.01
      max_depth: 6
```

```python
# Python - no MLOps config needed!
executor.execute_pipeline(
    pipeline_name="daily_ml",
    start_date="2025-01-01",
    end_date="2025-01-31",
)

# ✅ Result:
# - Experiment "daily_ml" created in MLflow
# - Run created with hyperparameters logged
# - train_model metrics automatically tracked
# - Model artifacts auto-registered
```

---

#### Option 2: With Hyperparameter Overrides

```python
executor.execute_pipeline(
    pipeline_name="daily_ml",
    start_date="2025-01-01",
    end_date="2025-01-31",
    hyperparams={
        "learning_rate": 0.05,  # Override default 0.01
        "max_depth": 8,         # Override default 6
    },
)

# ✅ Overrides applied:
# - train_model receives lr=0.05, max_depth=8
# - Tracked in MLflow under "hyperparams"
```

---

#### Option 3: With ml_info.yaml (Production) 

**For complex ML projects with centralized config:**

```yaml
# config/ml_info.yaml
mlops:
  backend: "databricks"          # or "local"
  experiment:
    name: "customer_churn"
    description: "Churn prediction model"
  
hyperparameters:
  learning_rate: 0.01
  max_depth: 6
  n_estimators: 100

tags:
  team: "data_science"
  environment: "production"

metrics:
  - accuracy
  - precision
  - auc

model_registry:
  register_model: true
  model_name: "churn_classifier"
```

```python
# Python code - unchanged!
executor.execute_pipeline(
    pipeline_name="daily_ml",
    start_date="2025-01-01",
    end_date="2025-01-31",
    hyperparams={"learning_rate": 0.05},  # Can still override
)

# ✅ Uses ml_info.yaml automatically
```

---

### Configuration Precedence

When multiple configs exist, this is the merge order:

```
Node config (nodes.yaml)
    ↓ (highest priority)
Pipeline config (pipelines.yaml)
    ↓
ml_info.yaml
    ↓
Global settings (global_settings.yaml)
    ↓
Auto-defaults
    ↓ (lowest priority)
```

**Example:**
```yaml
# global_settings.yaml
mlops:
  backend: "local"  # ← Used if nothing else specifies

# ml_info.yaml
mlops:
  backend: "databricks"  # ← Overrides global_settings

# nodes.yaml > train_model
mlops:
  backend: "databricks_prod"  # ← Final value used
```

---

### Your Node Function Receives ML Context

If your node function accepts `ml_context`, it receives hyperparameters:

```python
def train_model(training_df, *, start_date: str, end_date: str, ml_context=None):
    """
    ML node with hyperparameter access.
    
    Args:
        training_df: Input training data
        start_date/end_date: Period boundaries
        ml_context: Optional dict with hyperparams and metadata
    """
    
    # Extract hyperparameters
    if ml_context:
        hyperparams = ml_context.get("hyperparams", {})
        lr = hyperparams.get("learning_rate", 0.01)
        max_depth = hyperparams.get("max_depth", 6)
    else:
        lr = 0.01
        max_depth = 6
    
    # Train with resolved hyperparameters
    model = XGBClassifier(learning_rate=lr, max_depth=max_depth)
    model.fit(training_df.select("features"), training_df.select("label"))
    
    return model

# ✅ Called as:
# train_model(df, start_date="2025-01-01", end_date="2025-01-31", 
#             ml_context={"hyperparams": {"learning_rate": 0.05, ...}})
```

---

### MLOps Classes (Advanced)

**If you need manual control**, use these directly:

```python
from tauro.core.exec import MLOpsExecutorIntegration
from tauro.core.mlops.experiment_tracking import RunStatus

integration = MLOpsExecutorIntegration()

# Create experiment
exp_id = integration.create_pipeline_experiment(
    pipeline_name="daily_ml",
    pipeline_type="BATCH",
    tags={"team": "data_science"}
)

# Start run
run_id = integration.start_pipeline_run(
    experiment_id=exp_id,
    pipeline_name="daily_ml",
    hyperparams={"learning_rate": 0.01}
)

# Log execution
integration.log_node_execution(
    run_id=run_id,
    node_name="train_model",
    status="completed",
    duration_seconds=45.2,
    metrics={"accuracy": 0.95, "auc": 0.92}
)

# Register model
integration.register_model_from_run(
    run_id=run_id,
    model_name="churn_classifier",
    artifact_path="/path/to/model.pkl",
    metrics={"auc": 0.92}
)

# End run
integration.end_pipeline_run(run_id, status=RunStatus.COMPLETED)
```

---

### Environment Variables

Configure MLOps via environment:

```bash
# Backend selection
export TAURO_MLOPS_BACKEND=databricks  # or "local"

# Databricks configuration
export DATABRICKS_HOST=https://workspace.databricks.com
export DATABRICKS_TOKEN=your-token

# Auto-initialization
export TAURO_MLOPS_AUTO_INIT=true
```

---



---

## 🏛️ Architecture Overview

### Execution Pipeline (How It Works)

```
1. Configuration Loading
   ├─ Pipeline config (structure, nodes)
   ├─ Node configs (functions, inputs, outputs, dependencies)
   └─ ML info (if ML nodes detected)
        ↓
2. Validation
   ├─ Check required parameters
   ├─ Validate pipeline structure
   ├─ Validate format compatibility
   └─ Detect circular dependencies
        ↓
3. Dependency Resolution
   ├─ Build directed acyclic graph (DAG)
   ├─ Topological sort
   └─ Identify parallel-safe nodes
        ↓
4. Command Construction
   ├─ Load node functions
   ├─ Create appropriate Command object
   │  ├─ NodeCommand (standard)
   │  └─ MLNodeCommand (ML nodes with hyperparams)
   └─ Inject inputs and configuration
        ↓
5. Parallel Execution
   ├─ Schedule independent nodes to ThreadPoolExecutor
   ├─ Wait for dependencies to complete
   ├─ Handle retries and failures
   ├─ Track state and progress
   └─ Log metrics and errors
        ↓
6. Output Persistence
   ├─ Validate output schema
   ├─ Persist via DataOutputManager
   ├─ Register artifacts (for ML nodes)
   └─ Cleanup resources
        ↓
7. State Tracking
   └─ Return execution summary (nodes completed, failed, duration, etc.)
```

---

### 6 Execution Layers

| Layer | Component | Responsibility |
|-------|-----------|-----------------|
| **1. Config** | Context, DependencyResolver | Parse config, normalize dependencies |
| **2. Validation** | PipelineValidator | Check integrity, format compatibility |
| **3. Planning** | DependencyResolver | Build DAG, detect cycles, sort topologically |
| **4. Commands** | NodeCommand, MLNodeCommand | Encapsulate execution (function + inputs + params) |
| **5. Execution** | NodeExecutor, ThreadPoolExecutor | Run nodes, retry logic, circuit breaker |
| **6. State** | UnifiedPipelineState | Track progress, failures, recovery |

---

### Component Interaction Diagram

```
BaseExecutor (facade)
  ├─ DependencyResolver
  │   └─ Builds DAG, validates acyclic
  ├─ PipelineValidator
  │   └─ Checks format, schema, required params
  ├─ NodeExecutor
  │   ├─ ThreadPoolExecutor (workers)
  │   ├─ Commands (NodeCommand, MLNodeCommand)
  │   └─ InputLoader + DataOutputManager
  ├─ UnifiedPipelineState
  │   └─ Tracks node status, failures, metrics
  └─ MLOpsExecutorIntegration (if ML detected)
      └─ Automatic experiment tracking, metrics logging
```

---



### MLOps Components

## 🔧 Components Reference

### BaseExecutor
**Main entry point.** Orchestrates all execution layers.

```python
executor = BaseExecutor(context, input_loader, output_manager)

# Simple batch pipeline
executor.execute_pipeline(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
    max_workers=4,
)

# ML pipeline with hyperparameters
executor.execute_pipeline(
    pipeline_name="daily_ml",
    hyperparams={"learning_rate": 0.05},
    max_workers=4,
)

# Hybrid (batch + streaming)
executor.execute_hybrid_pipeline(
    pipeline_name="realtime_pipeline",
    start_date="2025-01-01",
    end_date="2025-01-31",
)

# Streaming only
executor.execute_streaming_pipeline(
    pipeline_name="kafka_consumer",
    execution_mode="async",  # or "sync"
)
```

---

### NodeExecutor
**Executes individual nodes and coordinates parallel execution.**

```python
executor = NodeExecutor(
    context=context,
    input_loader=input_loader,
    output_manager=output_manager,
    max_workers=4,
)

# Execute single node
executor.execute_single_node(
    node_name="transform",
    start_date="2025-01-01",
    end_date="2025-01-31",
)

# Execute multiple nodes respecting dependencies
executor.execute_nodes_parallel(
    execution_order=["extract", "transform", "load"],
    node_configs=node_configs,
    dag=dependency_graph,
    start_date="2025-01-01",
    end_date="2025-01-31",
)
```

**Features:**
- ThreadPoolExecutor for parallel execution
- Automatic retry with exponential backoff
- Circuit breaker to prevent cascading failures
- Resource cleanup (unpersist, close, clear)
- Timeout management for long-running nodes

---

### DependencyResolver
**Builds execution plan from configuration.**

```python
from tauro.core.exec import DependencyResolver

# Build dependency graph
dag = DependencyResolver.build_dependency_graph(
    pipeline_nodes=["extract", "transform", "load"],
    node_configs={
        "extract": {"output": ["raw"]},
        "transform": {"dependencies": ["extract"], "output": ["clean"]},
        "load": {"dependencies": ["transform"], "output": ["final"]},
    }
)
# Result: {"extract": {"transform"}, "transform": {"load"}, "load": set()}

# Get execution order
execution_order = DependencyResolver.topological_sort(dag)
# Result: ["extract", "transform", "load"]

# Get node dependencies
deps = DependencyResolver.get_node_dependencies(
    node_config={"dependencies": ["extract", "transform"]}
)
# Result: ["extract", "transform"]

# Normalize various dependency formats to list
normalized = DependencyResolver.normalize_dependencies(
    dependencies="extract"  # or {"extract": {}} or ["extract"]
)
# Result: ["extract"]
```

---

### PipelineValidator  
**Validates configuration before execution.**

```python
from tauro.core.exec import PipelineValidator

# Validate required parameters
PipelineValidator.validate_required_params(
    pipeline_name="daily_etl",
    start_date="2025-01-01",
    end_date="2025-01-31",
    context_start_date="2025-01-01",
    context_end_date="2025-01-31",
)

# Validate pipeline structure
PipelineValidator.validate_pipeline_config(
    pipeline={"nodes": ["extract", "transform", "load"]}
)

# Validate node configurations
PipelineValidator.validate_node_configs(
    pipeline_nodes=["extract", "transform"],
    node_configs={
        "extract": {"function": "etl.extract", "output": ["raw"]},
        "transform": {"function": "etl.transform", "dependencies": ["extract"]},
    }
)

# Validate hybrid pipeline (batch + streaming compatibility)
result = PipelineValidator.validate_hybrid_pipeline(
    pipeline=pipeline_config,
    node_configs=node_configs,
    format_policy=context.format_policy,
)
# Result: {"batch_nodes": [...], "streaming_nodes": [...], "valid": True}
```

---

### UnifiedPipelineState
**Tracks execution progress and failures.**

```python
from tauro.core.exec import UnifiedPipelineState

state = UnifiedPipelineState(circuit_breaker_threshold=3)

# Register nodes
state.register_node_with_dependencies("transform", ["extract"])

# Update status
state.set_node_status("extract", "completed")
state.set_node_status("transform", "running")

# Query status
status = state.get_node_status("transform")  # "running"
failed_count = state.get_node_failure_count("transform")  # 0
dependencies = state.get_node_dependencies("transform")  # ["extract"]

# Check if can execute (dependencies completed)
can_run = state.can_execute("load")  # True if all deps completed

# Handle failures
if state.get_node_failure_count("transform") > 3:
    state.set_node_status("transform", "cancelled")
```

---

### Commands (NodeCommand, MLNodeCommand)

**Pattern for encapsulating node execution.**

```python
from tauro.core.exec.commands import NodeCommand, MLNodeCommand

# Standard node
cmd = NodeCommand(
    function=my_transform_func,
    input_dfs=[events_df],
    start_date="2025-01-01",
    end_date="2025-01-31",
    node_name="transform",
)
result = cmd.execute()  # Calls: my_transform_func(events_df, start_date="...", end_date="...")

# ML node with hyperparameters
cmd = MLNodeCommand(
    function=train_model_func,
    input_dfs=[features_df],
    start_date="2025-01-01",
    end_date="2025-01-31",
    node_name="train_model",
    model_version="v1.0",
    hyperparams={"learning_rate": 0.01, "max_depth": 6},
    node_config={"metrics": ["auc"]},
    pipeline_config={"model_name": "churn"},
    spark=spark_session,
)
# Merges hyperparams: node → pipeline → ml_info → global → auto-defaults
model = cmd.execute()  # Calls: train_model_func(features_df, start_date="...", end_date="...", 
                       #                           ml_context={"hyperparams": {...}, ...})
```

---

## 📝 Node Function Signatures

### Recommended Signature

```python
def my_node(*dfs, start_date: str, end_date: str, ml_context: dict | None = None):
    """
    Standard node signature.
    
    Args:
        *dfs: 0 to N input DataFrames in configuration order
        start_date: ISO date string (YYYY-MM-DD) as keyword argument
        end_date: ISO date string (YYYY-MM-DD) as keyword argument
        ml_context: (Optional) Dict with hyperparams and metadata for ML nodes
    
    Returns:
        Output DataFrame or artifact
    """
    return output_df
```

**Rules:**
- ✅ Use `*dfs` to accept variable inputs
- ✅ Use `*, start_date, end_date` (keyword-only) after inputs
- ✅ Functions must be importable via dotted path
- ✅ Return single output (DataFrame or object)
- ❌ Don't use positional date arguments
- ❌ Don't mutate global state

### Examples

```python
# No inputs
def generate_data(*, start_date: str, end_date: str):
    return spark.range(0, 1000).toDF()

# Single input
def filter_events(events_df, *, start_date: str, end_date: str):
    return events_df.filter(col("date") between(start_date, end_date))

# Multiple inputs
def join_data(customers, orders, *, start_date: str, end_date: str):
    return customers.join(orders, "customer_id")

# ML node with hyperparameters
def train_model(features_df, *, start_date: str, end_date: str, ml_context=None):
    if ml_context:
        lr = ml_context.get("hyperparams", {}).get("learning_rate", 0.01)
    else:
        lr = 0.01
    model = GBTClassifier(learningRate=lr)
    return model.fit(features_df)
```

---

## 📋 Dependencies: Format & Normalization

The exec module supports multiple dependency formats (all normalize automatically):

```yaml
# Format 1: String (simplest)
dependencies: "extract"

# Format 2: Dictionary
dependencies:
  extract: {}

# Format 3: List of strings  
dependencies:
  - extract
  - transform

# Format 4: Mixed
dependencies:
  - extract
  - transform: {}
```

All normalize to: `["extract"]`, `["extract", "transform"]`, etc.

---

## ⚙️ Execution Modes

**Batch** → Sequential/parallel with date window  
**Streaming** → Long-running queries (async/sync)  
**Hybrid** → Batch nodes + streaming nodes coordinated  

---

## 💡 Best Practices





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

## 🚀 Quick Reference

**Setup:**
```python
executor = BaseExecutor(context, input_loader, output_manager)
```

**Execute:**
```python
# Batch
executor.execute_pipeline(pipeline_name, start_date, end_date, max_workers=4)

# Hybrid (batch + streaming)
executor.execute_hybrid_pipeline(pipeline_name, start_date, end_date)

# Streaming
executor.execute_streaming_pipeline(pipeline_name, execution_mode="async")
```

---

## 📚 Related Documentation

- **[tauro/core/mlops](../mlops/README.md)** - MLOps engine (experiment tracking, model registry)
- **[tauro/core/config](../config/README.md)** - Configuration and Context
- **[tauro/core/io](../io/README.md)** - Input/Output management
- **[tauro/streaming](../streaming/README.md)** - Streaming pipelines

---

## 📝 License

Copyright (c) 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.

---

