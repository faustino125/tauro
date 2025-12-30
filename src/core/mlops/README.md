# MLOps Module - Tauro Framework

**Production-ready MLOps system** for managing ML models and experiments with automatic versioning, comprehensive tracking, and enterprise-grade reliability.

## 📋 Quick Facts

- ✅ **Model Registry** with automatic versioning and lifecycle management
- ✅ **Experiment Tracking** with metrics, parameters, artifacts, and run comparison
- ✅ **Dual Backend Support**: Local (Parquet) or Databricks (Unity Catalog)
- ✅ **Security-First** (v2.1+): Path validation, credential masking, disk checks
- ✅ **Production-Ready**: Health checks, resilience patterns, comprehensive observability
- ✅ **Zero Config**: Sensible defaults for ETL; auto-activates for ML workloads

---

## 🚀 Quick Start

### Installation

```bash
# Basic (local backend only)
pip install tauro

# With Databricks support
pip install tauro[databricks]

# Full (all backends + MLflow integration)
pip install tauro[mlops]
```

### 5-Minute Example

```python
from core.mlops import init_mlops

# Initialize MLOps
ctx = init_mlops(backend_type="local", storage_path="./mlops_data")

# Register a model
model_v1 = ctx.model_registry.register_model(
    name="credit_risk",
    artifact_path="./models/model.pkl",
    framework="sklearn",
    metrics={"accuracy": 0.92, "auc": 0.95}
)

# Track an experiment
exp = ctx.experiment_tracker.create_experiment("tuning_v1")
with ctx.experiment_tracker.run_context(exp.id, name="trial_1") as run:
    for epoch in range(10):
        ctx.experiment_tracker.log_metric(run.run_id, "loss", 0.5 - epoch * 0.05)
    ctx.experiment_tracker.log_artifact(run.run_id, "./models/model.pkl")

print(f"✅ Model v{model_v1.version} registered")
```

---

## 📚 Feature Overview

### Model Registry

Manage and version your ML models with full lifecycle support:

```python
registry = ctx.model_registry

# Register
model = registry.register_model(
    name="fraud_detector",
    artifact_path="./model.pkl",
    framework="sklearn",
    hyperparameters={"n_estimators": 100},
    metrics={"accuracy": 0.95, "precision": 0.92},
    tags={"team": "ml", "project": "fraud"}
)

# Promote through stages
registry.promote_model(name="fraud_detector", version=1, stage="Production")

# Search
models = registry.search_models(name="fraud*", stage="Production")
```

**Supported Frameworks**: sklearn, XGBoost, LightGBM, CatBoost, PyTorch, TensorFlow, ONNX, and custom.

**Stages**: Staging → Production → Archived

### Experiment Tracking

Track experiments, runs, and metrics with automatic deduplication and comparison:

```python
tracker = ctx.experiment_tracker

# Create experiment
exp = tracker.create_experiment(
    name="hyperparameter_tuning",
    tags={"team": "ml"}
)

# Track a run
with tracker.run_context(exp.id, name="run_lr_0.01") as run:
    tracker.log_param(run.run_id, "learning_rate", 0.01)
    
    for epoch in range(100):
        loss = train_epoch()
        tracker.log_metric(run.run_id, "train_loss", loss, step=epoch)
    
    tracker.log_artifact(run.run_id, "./outputs/model.pkl")

# Compare runs
comparison = tracker.compare_runs(run_ids=[run1, run2, run3])
# Returns DataFrame with metrics aligned
```

**Metrics Handling**:
- Automatic buffering (100 metrics default, configurable)
- Rolling window (10K metrics/key default, prevents OOM)
- Thread-safe with async flushing
- Timestamp and step tracking

### Storage Backends

#### Local Backend (Default)

**No external dependencies. Ideal for development and testing.**

```python
ctx = init_mlops(
    backend_type="local",
    storage_path="./mlops_data"
)
```

Files stored as Parquet (DataFrames) and JSON (metadata) in local filesystem.

**Directory structure**:
```
mlops_data/
├── model_registry/
│   ├── models/          # Model metadata
│   ├── versions/        # Version metadata
│   └── artifacts/       # Model binaries
└── experiment_tracking/
    ├── experiments/     # Experiment metadata
    ├── runs/            # Run data and metrics
    └── artifacts/       # Run artifacts
```

#### Databricks Backend

**For enterprise deployments with Unity Catalog.**

```python
import os

# Set credentials (use secrets manager!)
os.environ["DATABRICKS_HOST"] = "https://workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "dapi..."

ctx = init_mlops(
    backend_type="databricks",
    catalog="ml_catalog",
    schema="experiments"
)
```

**Requirements**:
- Databricks workspace
- Unity Catalog enabled
- Pre-created catalog + schema
- `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars

**Benefits**:
- Shared enterprise storage
- Automatic ACID guarantees (Delta Lake)
- Full audit trail
- Scalable to 1000s of concurrent runs

---

## 🔒 Security Features (v2.1+)

### 1. Path Traversal Prevention

✅ **Input validation prevents directory escape attacks**

```python
# This fails (safe!)
tracker.log_artifact(run_id, "../../etc/passwd")

# Validated paths:
tracker.log_artifact(run_id, "./artifacts/model.pkl")  ✓
```

### 2. Credential Masking

✅ **Credentials from environment variables, never in code**

```python
# ❌ WRONG - DON'T DO THIS
ctx = init_mlops(
    backend_type="databricks",
    token="dapi1234567890"  # Exposed in logs!
)

# ✅ CORRECT
export DATABRICKS_TOKEN="dapi1234567890"  # Set in environment
ctx = init_mlops(backend_type="databricks")  # Token auto-loaded
```

### 3. Disk Space Validation

✅ **Pre-flight checks prevent partial writes**

Automatically validates available disk space before:
- Writing model artifacts
- Flushing metric buffers
- Storing run data

### 4. Bounded Memory Usage

✅ **Automatic memory limits prevent OOM**

| Component | Limit | Auto-Management |
|-----------|-------|-----------------|
| Event history | 10K events | Auto-rotating |
| Metric buffer | 100 metrics | Auto-flush |
| Metrics/key | 10K metrics | Rolling window |
| Cache (L1) | 1000 items | LRU eviction |

---

## 📊 Performance & Optimization

### Metric Indexing

**O(1) metric lookups** instead of O(n) scans:

```python
# Behind the scenes: metrics indexed by key and step
# This is instant:
metrics = tracker.get_metrics_for_run(run_id, key="accuracy")
```

### Two-Level Cache

**Memory (L1) + Disk (L2) caching** for repeated reads:

```python
# First read: from backend (slow, ~100ms)
model_v1 = registry.get_model("my_model", 1)

# Cached in L1 memory
model_v1_again = registry.get_model("my_model", 1)  # <1ms

# After 5 minutes: moved to L2 disk cache
# On next read: restored to L1 memory

# Cache stats
stats = cache.get_stats()
print(f"Hit rate: {stats.hit_rate:.1%}")
```

### Batch Processing

**Automatic batching** reduces I/O operations:

```python
# Individual flushes are batched internally
for i in range(1000):
    tracker.log_metric(run_id, "loss", 0.5 - i * 0.001)

# Instead of 1000 writes, does ~10 batch writes
# Configurable: metric_buffer_size (default 100)
```

---

## 🏥 Health & Monitoring

### Health Checks

**Kubernetes-style liveness and readiness probes:**

```python
from core.mlops import get_health_monitor

monitor = get_health_monitor()
monitor.register(StorageHealthCheck(ctx.storage))
monitor.register(MemoryHealthCheck())
monitor.register(DiskHealthCheck())

# Check health
report = monitor.run_checks()
if report.is_healthy:
    print("✅ System healthy")
else:
    print("⚠️ System degraded or unhealthy")
```

### Event System

**Pub/sub for operational events:**

```python
from core.mlops import get_event_emitter, EventType

emitter = get_event_emitter()

# Subscribe to events
@emitter.on(EventType.MODEL_REGISTERED)
def notify_team(event):
    slack.send(f"Model registered: {event.data['name']}")

# Events emitted automatically on operations
# ~60 event types available
```

### Metrics Collection

**Track operational metrics:**

```python
from core.mlops import get_metrics_collector

metrics = get_metrics_collector()

metrics.increment("models_registered")
metrics.gauge("active_runs", 5)
metrics.timer("inference_latency_ms", 150)

# Get summary
summary = metrics.get_summary()
```

---

## 🔧 Configuration

### Environment Variables

```bash
# Local backend
TAURO_MLOPS_BACKEND=local
TAURO_MLOPS_PATH=./mlops_data

# Databricks backend
TAURO_MLOPS_BACKEND=databricks
TAURO_MLOPS_CATALOG=ml_catalog
TAURO_MLOPS_SCHEMA=experiments
DATABRICKS_HOST=https://workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

### Code Configuration

```python
from core.mlops import MLOpsConfig, init_mlops

config = MLOpsConfig(
    backend_type="databricks",
    catalog="ml_catalog",
    schema="experiments",
    
    # Registry settings
    model_retention_days=90,
    max_versions_per_model=100,
    
    # Tracking settings
    metric_buffer_size=100,
    auto_flush_metrics=True,
    max_active_runs=100,
    
    # Resilience
    enable_retry=True,
    max_retries=3,
    enable_circuit_breaker=True,
)

ctx = init_mlops(config)
```

---

## 🆘 Troubleshooting

### Issue: OOM with Metric Logging

**Symptom**: `MemoryError` after logging many metrics

**Solution**:
```python
# 1. Check configuration
print(ctx.experiment_tracker.max_metrics_per_key)  # Default: 10000

# 2. Reduce if needed
config = MLOpsConfig(max_metrics_per_key=1000)

# 3. Monitor memory
metrics_summary = ctx.experiment_tracker.get_stats()
print(f"Total metrics in memory: {metrics_summary['total_count']}")
```

### Issue: Disk Space Error

**Symptom**: `StorageBackendError: Insufficient disk space`

**Solution**:
```python
import shutil

# Check before critical operations
stats = shutil.disk_usage("./mlops_data")
available_gb = stats.free / (1024 ** 3)

if available_gb < 1.0:
    logger.warning(f"Low disk: {available_gb:.1f}GB remaining")
    # Clean old runs or artifacts
```

### Issue: Lock Timeout

**Symptom**: `TimeoutError: Lock timeout on registry.lock`

**Causes**: 
- Stale lock from crashed process
- 10+ concurrent processes

**Solution**:
```python
from core.mlops.concurrency import LockManager

# Clean stale locks (auto-runs, but can force)
manager = LockManager()
cleaned = manager.cleanup_stale_locks(threshold=300)
print(f"Cleaned {cleaned} stale locks")

# Or increase timeout for high contention
from core.mlops.concurrency import file_lock
with file_lock("registry.lock", timeout=60):
    registry.register_model(...)
```

### Issue: Databricks Connection Failed

**Symptom**: `ConnectionError: Failed to connect to Databricks`

**Solution**:
```bash
# Verify credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."

# Test connection
python -c "from core.mlops import init_mlops; init_mlops(backend_type='databricks')"

# Check firewall/network
curl https://your-workspace.cloud.databricks.com

# Use verbose logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📖 Best Practices

### 1. Always Use Context Managers

```python
# ✅ CORRECT - Guaranteed cleanup
with tracker.run_context(exp_id, name="trial_1") as run:
    tracker.log_metric(run.run_id, "loss", 0.5)
# Run finalized automatically

# ❌ WRONG - May not finalize if error occurs
run = tracker.start_run(exp_id, "trial_1")
tracker.log_metric(run.run_id, "loss", 0.5)
tracker.end_run(run.run_id)
```

### 2. Validate Paths

```python
from core.mlops.validators import PathValidator
from pathlib import Path

user_path = request.args.get("artifact_path")

try:
    safe_path = PathValidator.validate_path(
        user_path,
        base_path=Path("./artifacts")
    )
except ValidationError:
    return {"error": "Invalid path"}, 400
```

### 3. Manage Metrics Carefully

```python
# ❌ WRONG - Unbounded metrics
for i in range(1_000_000):
    tracker.log_metric(run_id, "metric", i)

# ✅ CORRECT - Monitor and manage
for i in range(1_000_000):
    tracker.log_metric(run_id, "metric", i)
    
    # Log periodically
    if i % 10_000 == 0:
        logger.info(f"Progress: {i}")
```

### 4. Use Batch Operations

```python
# Automatic: metrics are batched and flushed
# Control buffer size in config
config = MLOpsConfig(metric_buffer_size=1000)
```

### 5. Monitor in Production

```python
# Set up liveness probe
from core.mlops import is_healthy

@app.before_request
def check_health():
    if not is_healthy():
        return {"error": "System unhealthy"}, 503

# Or Kubernetes integration
@app.route("/health")
def liveness():
    return ("OK", 200) if is_healthy() else ("", 503)
```

---

## 🏗️ Architecture

### Components

| Component | Purpose |
|-----------|---------|
| **StorageBackend** | Abstraction for local/Databricks storage |
| **ModelRegistry** | Model versioning and lifecycle |
| **ExperimentTracker** | Experiment/run/metric tracking |
| **EventEmitter** | Pub/sub event system |
| **HealthMonitor** | System health checks |
| **LRUCache** | In-memory caching with TTL |
| **CircuitBreaker** | Resilience pattern for failures |

### Protocols (Interfaces)

All components implement protocols for extensibility:

```python
from core.mlops import (
    StorageBackendProtocol,
    ExperimentTrackerProtocol,
    ModelRegistryProtocol,
)

# Implement your own storage backend
class CustomStorage:
    def write_dataframe(self, df, path, mode="overwrite"): ...
    def read_dataframe(self, path): ...
    # ... other methods
```

---

## 🧪 Testing

```bash
# Run all MLOps tests
pytest src/core/mlops/test/ -v

# Specific test modules
pytest src/core/mlops/test/test_model_registry.py -v
pytest src/core/mlops/test/test_experiment_tracking.py -v
pytest src/core/mlops/test/test_health.py -v

# With coverage
pytest src/core/mlops/test/ --cov=src.core.mlops
```

---

## 📚 Examples

### Example 1: Simple Model Registry

```python
from core.mlops import init_mlops

ctx = init_mlops()
registry = ctx.model_registry

# Register sklearn model
model = registry.register_model(
    name="iris_classifier",
    artifact_path="./models/iris.pkl",
    framework="sklearn",
    metrics={"accuracy": 0.98, "f1": 0.97},
    tags={"version": "v1"}
)

print(f"Registered: {model.name} v{model.version}")
```

### Example 2: Full Training Pipeline

```python
from core.mlops import init_mlops

ctx = init_mlops(backend_type="local")
tracker = ctx.experiment_tracker
registry = ctx.model_registry

# Create experiment
exp = tracker.create_experiment("model_training")

# Train multiple trials
best_accuracy = 0
best_run_id = None

for lr in [0.001, 0.01, 0.1]:
    with tracker.run_context(exp.id, name=f"lr_{lr}") as run:
        model = train_model(learning_rate=lr)
        accuracy = evaluate_model(model)
        
        # Log metrics
        tracker.log_param(run.run_id, "learning_rate", lr)
        tracker.log_metric(run.run_id, "accuracy", accuracy)
        tracker.log_metric(run.run_id, "f1_score", f1_score(model))
        
        # Log artifact
        tracker.log_artifact(run.run_id, "model.pkl")
        
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_run_id = run.run_id

# Register best model
registry.register_model(
    name="iris_classifier",
    artifact_path="model.pkl",
    framework="sklearn",
    metrics={"accuracy": best_accuracy},
    experiment_run_id=best_run_id
)

print(f"✅ Best model registered with accuracy {best_accuracy:.2%}")
```

### Example 3: Databricks Integration

```python
import os
from core.mlops import init_mlops

# Credentials from environment
os.environ["DATABRICKS_HOST"] = "https://workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "dapi..."

# Initialize with Databricks
ctx = init_mlops(
    backend_type="databricks",
    catalog="ml_models",
    schema="experiments"
)

# Use exactly like local backend
tracker = ctx.experiment_tracker
registry = ctx.model_registry

# All data stored in Databricks Unity Catalog
```

---

## 🆕 What's New in v2.1

**Security & Performance Release**

### Security Improvements
- ✅ Path traversal prevention (validates resolution within base path)
- ✅ Credential masking (tokens from env vars, never in code)
- ✅ Disk space validation (pre-flight checks prevent partial writes)
- ✅ Metric buffer persistence (immediate storage, no loss on crash)

### Performance Improvements
- ✅ Metric indexing (O(1) lookups instead of O(n))
- ✅ Event history bounded (10K event limit, prevents OOM)
- ✅ Metric rolling window (auto-evicts old metrics)
- ✅ Circuit breaker (fail fast on repeated errors)

### Other
- ✅ 100% backward compatible
- ✅ Zero breaking changes
- ✅ Automatic safety defaults

---

## 📖 API Reference

See [MLOPS_ANALYSIS.md](../../MLOPS_ANALYSIS.md) for comprehensive API documentation.

Key exports:
```python
from core.mlops import (
    # Initialization
    init_mlops, get_mlops_context, MLOpsConfig,
    
    # Components
    ModelRegistry, ExperimentTracker,
    
    # Events & Monitoring
    get_event_emitter, get_metrics_collector, get_health_monitor,
    
    # Cache
    LRUCache, TwoLevelCache, BatchProcessor,
    
    # Exceptions
    ModelNotFoundError, ExperimentNotFoundError, RunNotActiveError,
    
    # Enums
    ModelStage, RunStatus, EventType, ErrorCode,
)
```

## 📄 License

MIT License. See [LICENSE](../../../LICENSE) in project root.

---



**Made with ❤️ for data teams worldwide**
