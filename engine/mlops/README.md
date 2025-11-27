# MLOps Layer

Integrated MLOps layer for Tauro providing **Model Registry** and **Experiment Tracking**, with dual support for local storage (Parquet) and Databricks Unity Catalog.

## 🎯 Philosophy: Invisible Until Needed

MLOps in Tauro is designed to be:
- **Zero-config for ETL**: Does not interfere with data-only pipelines
- **Auto-activated for ML**: Automatically detects ML nodes
- **Progressively complex**: Simple configuration by default, fine-grained control when needed

---

## ✨ Features

### Model Registry
- ✅ Automatic model versioning
- ✅ Structured metadata (framework, hyperparameters, metrics)
- ✅ Artifact storage (sklearn, XGBoost, PyTorch, etc.)
- ✅ Lifecycle management (Staging → Production → Archived)
- ✅ Search by name, version, and stage
- ✅ Tags and annotations

### Experiment Tracking
- ✅ Experiment and run creation
- ✅ Metric logging (with timestamps and steps)
- ✅ Hyperparameter logging
- ✅ Artifact storage per run
- ✅ Run comparison (DataFrame)
- ✅ Run search by metrics
- ✅ Nested run support (parent-child)

### Backends
- ✅ **Local**: Parquet storage (no external dependencies)
- ✅ **Databricks**: Unity Catalog (with databricks-sql-connector)

### 🆕 Event System and Observability
- ✅ **EventEmitter**: Pub/sub event system with history
- ✅ **MetricsCollector**: Metrics collection (counters, gauges, timers)
- ✅ **HooksManager**: Pre/post hooks for operations
- ✅ **AuditLogger**: Audit logging with queries

### 🆕 Cache Layer
- ✅ **LRUCache**: Thread-safe LRU cache with TTL
- ✅ **TwoLevelCache**: Two-level cache (L1 memory / L2 storage)
- ✅ **BatchProcessor**: Batch operation processing
- ✅ **CachedStorage**: Cache wrapper for storage backends

### 🆕 Health Checks and Diagnostics
- ✅ **HealthMonitor**: Central system health monitor
- ✅ **StorageHealthCheck**: Storage status verification
- ✅ **MemoryHealthCheck**: Memory usage monitoring
- ✅ **DiskHealthCheck**: Disk space verification
- ✅ **Liveness** and **readiness** probes (Kubernetes-style)

### 🆕 Improved Architecture
- ✅ **Protocols**: Abstract interfaces for all components
- ✅ **Base Classes**: Base classes with lifecycle management
- ✅ **Enhanced Exceptions**: Exceptions with error codes and context
- ✅ **Resilience**: Retry policies and circuit breakers

---

## 🚀 Quick Start

### Installation

```bash
pip install pandas loguru pyarrow

# For Databricks (optional)
pip install databricks-sql-connector
```

### Basic Usage

```python
from engine.mlops import (
    MLOpsContext,
    init_mlops,
    get_mlops_context,
    ModelStage,
    RunStatus,
)

# Initialize MLOps context
ctx = init_mlops(backend_type="local", storage_path="./mlops_data")

# Or using the global context
mlops = get_mlops_context()
```

### Model Registry

```python
registry = ctx.model_registry

# Register model
model_v1 = registry.register_model(
    name="credit_risk_model",
    artifact_path="/path/to/model.pkl",
    artifact_type="sklearn",
    framework="scikit-learn",
    hyperparameters={"n_estimators": 100, "max_depth": 10},
    metrics={"accuracy": 0.92, "auc": 0.95},
    tags={"team": "ds", "project": "credit"}
)

# Promote to production
registry.promote_model("credit_risk_model", 1, ModelStage.PRODUCTION)

# Get production model
prod_model = registry.get_model_by_stage("credit_risk_model", ModelStage.PRODUCTION)
```

### Experiment Tracking

```python
tracker = ctx.experiment_tracker

# Create experiment
exp = tracker.create_experiment(
    name="model_tuning_v1",
    description="Hyperparameter tuning",
    tags={"team": "ds"}
)

# Start run with context manager
with tracker.run_context(exp.experiment_id, name="trial_1") as run:
    for epoch in range(10):
        tracker.log_metric(run.run_id, "loss", 0.5 - epoch * 0.05, step=epoch)
        tracker.log_metric(run.run_id, "accuracy", 0.7 + epoch * 0.03, step=epoch)
    tracker.log_artifact(run.run_id, "/path/to/model.pkl")
# Run is automatically finalized
```

---

## 🆕 Event System

```python
from engine.mlops import (
    EventEmitter, 
    EventType, 
    get_event_emitter,
    get_metrics_collector,
)

# Get global event emitter
emitter = get_event_emitter()

# Subscribe to events
def on_model_registered(event):
    print(f"Model registered: {event.data}")

emitter.subscribe(EventType.MODEL_REGISTERED, on_model_registered)

# Events are automatically emitted by components
# You can also emit events manually:
emitter.emit(EventType.MODEL_REGISTERED, {"name": "my_model", "version": 1})
```

### Metrics

```python
metrics = get_metrics_collector()

# Counters
metrics.increment("models_registered")
metrics.increment("api_requests", tags={"endpoint": "/models"})

# Gauges
metrics.gauge("active_runs", 5)

# Timers
with metrics.timer("training_duration"):
    train_model()

# Get summary
summary = metrics.get_summary()
print(summary)
```

### Hooks

```python
from engine.mlops import HooksManager, HookType, get_hooks_manager

hooks = get_hooks_manager()

# Register pre-operation hook
@hooks.register(HookType.PRE_MODEL_REGISTER)
def validate_model(data):
    if data.get("metrics", {}).get("accuracy", 0) < 0.5:
        raise ValueError("Model accuracy too low")
    return data

# Register post-operation hook
@hooks.register(HookType.POST_MODEL_REGISTER)
def notify_slack(data):
    send_slack_notification(f"New model: {data['name']}")
    return data
```

---

## 🆕 Cache Layer

```python
from engine.mlops import LRUCache, TwoLevelCache, CachedStorage

# Simple LRU cache
cache = LRUCache(max_size=1000, default_ttl=300)  # 5 min TTL
cache.set("model:v1", model_metadata)
cached = cache.get("model:v1")

# Two-level cache
l1_cache = LRUCache(max_size=100, default_ttl=60)   # Fast, small
l2_cache = LRUCache(max_size=10000, default_ttl=3600)  # Large, slow
two_level = TwoLevelCache(l1=l1_cache, l2=l2_cache)

# Storage wrapper with cache
cached_storage = CachedStorage(storage=storage_backend, cache=cache)
# Reads are automatically cached
data = cached_storage.read_json("path/to/config.json")
```

### Batch Processing

```python
from engine.mlops import BatchProcessor, BatchOperation

def process_batch(operations):
    for op in operations:
        storage.write(op.key, op.value)

processor = BatchProcessor(
    process_func=process_batch,
    batch_size=100,
    flush_interval=5.0  # seconds
)

# Operations accumulate and are processed in batches
processor.add(BatchOperation(key="k1", value="v1", operation_type="write"))
processor.add(BatchOperation(key="k2", value="v2", operation_type="write"))
# Manual flush if needed
processor.flush()
```

---

## 🆕 Health Checks

```python
from engine.mlops import (
    HealthMonitor,
    StorageHealthCheck,
    MemoryHealthCheck,
    DiskHealthCheck,
    get_health_monitor,
    check_health,
    is_healthy,
    is_ready,
)

# Get global monitor
monitor = get_health_monitor()

# Register health checks
monitor.register(StorageHealthCheck("storage", storage_backend))
monitor.register(MemoryHealthCheck("memory", warning_threshold=0.8))
monitor.register(DiskHealthCheck("disk", path="/data", warning_threshold=0.9))

# Check health
report = check_health()
print(f"Status: {report.overall_status}")
for check in report.checks:
    print(f"  {check.name}: {check.status} - {check.message}")

# Kubernetes-style probes
if is_healthy():  # Liveness
    print("System is alive")

if is_ready():  # Readiness
    print("System is ready to accept traffic")
```

---

## 🆕 Enhanced Exceptions

```python
from engine.mlops import (
    ErrorCode,
    ErrorContext,
    MLOpsException,
    ModelNotFoundError,
    create_error_response,
    wrap_exception,
)

# Exceptions with error codes
try:
    model = registry.get_model_version("nonexistent")
except ModelNotFoundError as e:
    print(f"Error code: {e.error_code}")  # ErrorCode.MODEL_NOT_FOUND
    print(f"Context: {e.context}")

# Create error response for APIs
response = create_error_response(
    error_code=ErrorCode.VALIDATION_ERROR,
    message="Invalid model name",
    details={"field": "name", "reason": "Must be alphanumeric"}
)

# Wrap external exceptions
try:
    external_operation()
except Exception as e:
    raise wrap_exception(e, ErrorCode.STORAGE_ERROR, "Failed to save model")
```

---

## 🆕 Protocols (Interfaces)

The system defines clear interfaces for all components:

```python
from engine.mlops import (
    StorageBackendProtocol,
    ExperimentTrackerProtocol,
    ModelRegistryProtocol,
    LockProtocol,
    EventEmitterProtocol,
)

# Create custom implementation
class MyCustomStorage:
    """Implements StorageBackendProtocol."""
    
    def write_dataframe(self, df, path, mode="overwrite"):
        ...
    
    def read_dataframe(self, path):
        ...
    
    # ... remaining methods

# Type checking works automatically
def process_data(storage: StorageBackendProtocol):
    df = storage.read_dataframe("data.parquet")
    ...
```

---

## 📦 Architecture

```
engine/mlops/
├── __init__.py              # Public API exports
├── config.py                # MLOpsContext, configuration, and factories
├── storage.py               # Storage backends (Local, Databricks)
├── model_registry.py        # Model Registry implementation
├── experiment_tracking.py   # Experiment Tracking implementation
│
├── protocols.py             # Abstract interfaces (Protocols)
├── events.py                # Event system, metrics, hooks, audit
├── cache.py                 # Caching layer (LRU, TwoLevel, Batch)
├── base.py                  # Base classes and mixins
├── health.py                # Health checks and diagnostics
├── exceptions.py            # Enhanced exceptions with error codes
│
├── concurrency.py           # 🆕 Consolidated: locks, transactions
├── mlflow.py                # 🆕 Consolidated: MLflow integration
├── resilience.py            # Retry policies, circuit breakers
├── validators.py            # Input validation
│
└── test/                    # Unit tests
    ├── test_protocols.py
    ├── test_events.py
    ├── test_cache.py
    ├── test_base.py
    ├── test_health.py
    ├── test_locking.py
    ├── test_transaction.py
    └── test_factory.py
```

### Consolidated Modules (v2.0)

| Module | Contains | Replaces |
|--------|----------|----------|
| `concurrency.py` | FileLock, OptimisticLock, ReadWriteLock, Transaction, SafeTransaction | `locking.py`, `transaction.py` |
| `mlflow.py` | MLflowPipelineTracker, mlflow_track decorator, MLflowHelper | `mlflow_adapter.py`, `mlflow_decorators.py`, `mlflow_utils.py` |
| `config.py` | MLOpsContext, factories (StorageBackendFactory, etc.) | Original `config.py` + `factory.py` |

### Main Components

| Component | Description |
|-----------|-------------|
| `StorageBackend` | Abstraction for local (Parquet) and Databricks (Unity Catalog) |
| `ModelRegistry` | Model versioning, lifecycle, artifacts |
| `ExperimentTracker` | Experiments, runs, metrics, parameters |
| `MLOpsContext` | Factory and centralized configuration |
| `EventEmitter` | Pub/sub system for events |
| `MetricsCollector` | Operational metrics collection |
| `HooksManager` | Pre/post hooks for extensibility |
| `LRUCache` | In-memory cache with TTL |
| `HealthMonitor` | Health checks and diagnostics |

---

## 🔧 Configuration

### Environment Variables

```bash
# Local backend
TAURO_MLOPS_BACKEND=local
TAURO_MLOPS_PATH=/path/to/mlops/data

# Databricks backend
TAURO_MLOPS_BACKEND=databricks
TAURO_MLOPS_CATALOG=my_catalog
TAURO_MLOPS_SCHEMA=mlops
DATABRICKS_HOST=https://workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
```

### Configuration with ml_info.yaml

```yaml
# config/ml_info.yaml
mlops:
  enabled: true
  backend: "databricks"
  experiment:
    name: "customer-churn-prediction"
    description: "Customer churn prediction model"
  model_registry:
    catalog: "main"
    schema: "ml_models"
  tracking:
    catalog: "main"
    schema: "ml_experiments"
  auto_log: true
  
  # 🆕 Cache configuration
  cache:
    enabled: true
    max_size: 1000
    default_ttl: 300
  
  # 🆕 Health checks configuration
  health:
    enabled: true
    memory_threshold: 0.85
    disk_threshold: 0.90
```

---

## 📊 Data Structure

### Model Registry

```
model_registry/
├── models/
│   └── index.parquet              # Model index
├── metadata/
│   └── {model_id}/
│       ├── v1.json                # Metadata v1
│       └── v2.json                # Metadata v2
└── artifacts/
    └── {model_id}/
        ├── v1/                    # Artifacts v1
        └── v2/                    # Artifacts v2
```

### Experiment Tracking

```
experiment_tracking/
├── experiments/
│   ├── index.parquet              # Experiment index
│   └── {exp_id}.json              # Experiment metadata
├── runs/
│   └── {exp_id}/
│       ├── index.parquet          # Run index
│       └── {run_id}.json          # Run metadata
└── artifacts/
    └── {run_id}/                  # Run artifacts
```

---

## 🧪 Testing

```bash
# Run all mlops module tests
pytest engine/mlops/test/ -v

# Specific tests
pytest engine/mlops/test/test_protocols.py -v
pytest engine/mlops/test/test_events.py -v
pytest engine/mlops/test/test_cache.py -v
pytest engine/mlops/test/test_health.py -v
```

---

## 📚 API Reference

### Main Exports

```python
from engine.mlops import (
    # Context and Config
    MLOpsContext, MLOpsConfig, init_mlops, get_mlops_context,
    
    # Protocols
    StorageBackendProtocol, ExperimentTrackerProtocol, ModelRegistryProtocol,
    
    # Events
    EventType, Event, EventEmitter, MetricsCollector, HooksManager, AuditLogger,
    get_event_emitter, get_metrics_collector, get_hooks_manager,
    
    # Cache
    LRUCache, TwoLevelCache, BatchProcessor, CachedStorage, CacheKeyBuilder,
    
    # Health
    HealthMonitor, HealthStatus, StorageHealthCheck, MemoryHealthCheck,
    get_health_monitor, check_health, is_healthy, is_ready,
    
    # Base
    BaseMLOpsComponent, ComponentState, ValidationMixin, PathManager,
    
    # Model Registry
    ModelRegistry, ModelMetadata, ModelVersion, ModelStage,
    
    # Experiment Tracking
    ExperimentTracker, Experiment, Run, Metric, RunStatus,
    
    # Storage
    LocalStorageBackend, DatabricksStorageBackend,
    
    # Exceptions
    ErrorCode, MLOpsException, ModelNotFoundError, ExperimentNotFoundError,
    
    # Resilience
    RetryConfig, with_retry, CircuitBreaker,
)
```

---

## 🎓 Usage Examples

### 1. ETL Pipeline (No MLOps)

```yaml
nodes:
  load_data:
    function: "etl.load_csv"
  transform:
    function: "etl.clean_data"
# ✅ MLOps auto-disabled → No overhead
```

### 2. ML Pipeline with Full Tracking

```python
from engine.mlops import (
    init_mlops, ModelStage, RunStatus,
    get_event_emitter, get_metrics_collector,
)

# Initialize
ctx = init_mlops(backend_type="local", storage_path="./mlops")
tracker = ctx.experiment_tracker
registry = ctx.model_registry

# Operational metrics
metrics = get_metrics_collector()

# Create experiment
exp = tracker.create_experiment("xgboost_tuning")
metrics.increment("experiments_created")

# Train with tracking
with tracker.run_context(exp.experiment_id, name="trial_1") as run:
    with metrics.timer("training_time"):
        model = train_model(params)
    
    # Log metrics
    tracker.log_metric(run.run_id, "accuracy", 0.95)
    tracker.log_metric(run.run_id, "auc", 0.98)
    
    # Log artifact
    tracker.log_artifact(run.run_id, "model.pkl")
    metrics.increment("models_trained")

# Register best model
version = registry.register_model(
    name="xgboost_classifier",
    artifact_path="model.pkl",
    artifact_type="xgboost",
    framework="xgboost",
    metrics={"accuracy": 0.95, "auc": 0.98},
)
metrics.increment("models_registered")

# Promote to production
registry.promote_model("xgboost_classifier", version.version, ModelStage.PRODUCTION)
```

### 3. Health Monitoring in Production

```python
from engine.mlops import (
    get_health_monitor, StorageHealthCheck, MemoryHealthCheck,
    DiskHealthCheck, ComponentHealthCheck,
)

# Configure health checks
monitor = get_health_monitor()
monitor.register(StorageHealthCheck("storage", ctx.storage))
monitor.register(MemoryHealthCheck("memory", warning_threshold=0.8))
monitor.register(DiskHealthCheck("disk", path="./mlops", warning_threshold=0.9))
monitor.register(ComponentHealthCheck("registry", ctx.model_registry))

# Health check endpoint (Flask example)
@app.route("/health")
def health():
    report = monitor.check_all()
    status_code = 200 if report.is_healthy else 503
    return jsonify(report.to_dict()), status_code

@app.route("/ready")
def ready():
    return ("OK", 200) if monitor.is_ready() else ("Not Ready", 503)
```

---

## 🛣️ Roadmap

- [x] Event system and observability
- [x] Cache layer with LRU and TTL
- [x] Health checks and diagnostics
- [x] Enhanced exceptions with error codes
- [x] Protocols (abstract interfaces)
- [ ] Full Databricks UC integration (volumes)
- [ ] Incremental metrics (streaming)
- [ ] Web UI for visualization
- [ ] MLflow integration
- [ ] Distributed model support

---

## 📄 License

MIT - See LICENSE in project root.
