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

## 🆕 Recent Security & Performance Improvements (v2.1+)

This release includes **7 critical security and performance fixes**:

### Security & Memory Safety
1. **Event History Memory Limit** - Prevents OOM with auto-rotating history (max 10K events)
2. **Disk Space Validation** - Validates available space before write operations
3. **Metric Buffer Persistence** - Immediate storage of metric buffers to prevent loss
4. **Metrics Per-Run Limit** - Hard limit of 100K metrics/run with rolling window
5. **Path Traversal Prevention** - Improved validation using `Path.resolve()` and `relative_to()`
6. **Distributed Lock Reliability** - Enhanced file locking with stale cleanup

### Configuration Security
7. **Credential Masking** - Databricks tokens no longer appear in parameters (use ENV vars instead)

### Performance Improvements
- Event history with bounded deque for O(1) append/pop
- Pre-flight disk space checks avoid partial writes
- Metric rolling window prevents unbounded memory growth
- Index-based searches for O(1) lookups (future)

### Deployment Impact
- ✅ 100% backward compatible
- ✅ No breaking changes
- ✅ Automatic safety defaults
- ⚠️ Systems with 100K+ metrics need rolling window configured

---

## � End User Responsibilities for Databricks

**IMPORTANT**: Tauro is a **pipeline execution framework**, NOT a Databricks provisioning tool. The end user is **fully responsible** for:

### ✅ What You Must Provide

1. **Databricks Workspace and Credentials**
   - Databricks workspace URL
   - Access token or service principal credentials
   - Set via environment variables: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`

2. **Unity Catalog Infrastructure**
   - Pre-created catalogs (or permissions to create them)
   - Pre-created schemas (or permissions to create them)
   - Pre-created volumes for artifact storage (if needed)
   - Appropriate access permissions (READ/WRITE/CREATE)

3. **Spark Configuration**
   - Databricks cluster with appropriate runtime
   - Spark session properly configured
   - Unity Catalog enabled: `spark.databricks.unityCatalog.enabled=true`

4. **Network and Security**
   - Network connectivity to Databricks workspace
   - Firewall rules and security groups
   - IAM roles and policies for cloud storage access

### ⚙️ What Tauro Does

- **Executes** pipelines using your provided credentials
- **Reads/Writes** data to your pre-configured Unity Catalog tables
- **Uses** your existing Spark session
- **Optionally creates** schemas/tables if you grant permissions (via `ensure_schema_exists`)

### ❌ What Tauro Does NOT Do

- Does NOT provision Databricks workspaces
- Does NOT create Unity Catalog infrastructure automatically without permissions
- Does NOT manage authentication or credentials (except reading from env vars)
- Does NOT configure Spark clusters or runtimes
- Does NOT set up network or security policies

---

## 🛡️ Security Best Practices

### Credentials Management

**❌ DON'T: Pass credentials as parameters**
```python
# INSECURE - Credentials in logs/stack traces
ctx = init_mlops(
    backend_type="databricks",
    catalog="main",
    token="dapi1234567890"  # NEVER THIS!
)
```

**✅ DO: Use environment variables**
```python
# SECURE - Credentials from environment
# User must set these BEFORE running Tauro:
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890..."

ctx = init_mlops(
    backend_type="databricks",
    catalog="main",
    # Token loaded from DATABRICKS_TOKEN env var
)

# Or set explicitly from secure source (e.g., Azure Key Vault, AWS Secrets Manager)
os.environ["DATABRICKS_TOKEN"] = get_from_vault("databricks/token")
ctx = init_mlops(backend_type="databricks", catalog="main")
```

### Path Security

**❌ DON'T: Trust user paths directly**
```python
# VULNERABLE - User could escape sandbox
user_path = request.args.get("path")
tracker.log_artifact(run_id, user_path)
```

**✅ DO: Validate with PathValidator**
```python
from core.mlops.validators import PathValidator

user_path = request.args.get("path")
try:
    safe_path = PathValidator.validate_path(
        user_path,
        base_path=Path("./artifacts")  # Restricts to subdirectory
    )
except ValidationError as e:
    return {"error": str(e)}, 400
```

### Storage Security

**✅ DO: Check disk space before operations**
```python
import shutil

stats = shutil.disk_usage("./mlops_data")
required = 10 * 1024 * 1024  # 10MB

if stats.free < required:
    raise ResourceLimitError("storage", stats.used, stats.total)
```

---

## 💾 Memory Safety and Configuration

### Metric Buffering

**Default Behavior**:
```python
tracker = ExperimentTracker(
    storage=storage_backend,
    metric_buffer_size=100,           # Buffer 100 metrics before flush
    auto_flush_metrics=True,           # Auto-flush every 100
    max_metrics_per_key=10_000,        # Rolling window per metric key
)
```

**Configuration**:
```yaml
# config/ml_info.yaml
mlops:
  tracking:
    metric_buffer_size: 100      # Flush threshold
    auto_flush_metrics: true     # Enable auto-flush
    max_metrics_per_key: 10000   # Rolling window size
    
    # For high-volume logging (1K+ metrics/sec)
    # Increase buffer_size to reduce flush frequency:
    metric_buffer_size: 1000
    
    # For memory-constrained environments:
    # Reduce rolling window to save memory:
    max_metrics_per_key: 1000
```

**Memory Impact Example**:
```python
# Low volume: 10 metrics/sec (typical)
# Default: 100 buffer × 8 bytes = ~1KB in memory
# ✅ Safe

# High volume: 10K metrics/sec (model training)
# With buffer_size=1000: 1000 × 8 bytes = ~8KB in memory
# ✅ Safe

# Unlimited: No rolling window
# With 10K metrics: 10K × 8 bytes = ~80KB per key
# ⚠️ Monitor carefully, may grow unbounded
```

### Event History

**Default Behavior** - Auto-rotating history (latest 10K events):
```python
emitter = EventEmitter(max_history_per_type=10_000)
# Oldest events automatically dropped when limit reached
```

**Example**:
```python
# System runs for 1 hour, emits 50 events/second
# Total: 50 * 3600 = 180K events
# But history keeps only latest 10K per type
# Memory usage: constant ~10K events × 200 bytes = ~2MB

# Verify no OOM:
stats = emitter.get_stats()
print(f"History size: {sum(len(h) for h in emitter._history.values())} events")
# Output: History size: 10000 events (bounded)
```

### Cache Configuration

**LRU Cache with TTL**:
```python
from core.mlops.cache import LRUCache

# Default: 1000 items, 5 minute TTL
cache = LRUCache(max_size=1000, default_ttl=300)

# Large deployment: 10K items, 1 hour TTL
cache = LRUCache(max_size=10_000, default_ttl=3600)

# Memory-constrained: 100 items, 1 minute TTL
cache = LRUCache(max_size=100, default_ttl=60)

# Monitor stats
stats = cache.get_stats()
print(f"Hit rate: {stats.hit_rate:.2%}")
print(f"Size: {stats.size}/{stats.max_size}")
```

---

## 🚀 Production Deployment Guide

### Pre-Deployment Checklist

- [ ] Read [Security Best Practices](#-security-best-practices) section
- [ ] Set `DATABRICKS_TOKEN` via secrets manager, not config
- [ ] Configure disk space monitoring for storage backend
- [ ] Set up health checks with `HealthMonitor`
- [ ] Test metric buffer flushing under load
- [ ] Validate event history doesn't grow unbounded
- [ ] Configure log rotation for MLOps logs

### Minimal Production Config

```yaml
# config/ml_info.yaml
mlops:
  enabled: true
  backend: "databricks"
  experiment:
    name: "production-models"
  cache:
    enabled: true
    max_size: 1000
  health:
    enabled: true
    memory_threshold: 0.85
    disk_threshold: 0.90
```

### Environment Setup

```bash
#!/bin/bash
# Set credentials from secrets manager (AWS Secrets Manager example)

export DATABRICKS_TOKEN=$(aws secretsmanager get-secret-value \
    --secret-id mlops/databricks-token \
    --query SecretString --output text)

export DATABRICKS_HOST=$(aws secretsmanager get-secret-value \
    --secret-id mlops/databricks-host \
    --query SecretString --output text)

# Run application
python main.py
```

### Health Monitoring Setup

```python
from core.mlops import (
    get_health_monitor,
    StorageHealthCheck,
    MemoryHealthCheck,
    DiskHealthCheck,
)
from flask import Flask, jsonify

app = Flask(__name__)
monitor = get_health_monitor()

# Register checks
monitor.register(StorageHealthCheck("storage", ctx.storage))
monitor.register(MemoryHealthCheck("memory", warning_threshold=0.85))
monitor.register(DiskHealthCheck("disk", path="./mlops_data", warning_threshold=0.90))

@app.route("/health")
def health():
    """Kubernetes liveness probe"""
    try:
        report = monitor.run_checks()
        status = 200 if report.is_healthy else 503
        return jsonify(report.to_dict()), status
    except Exception as e:
        return {"status": "error", "message": str(e)}, 503

@app.route("/ready")
def ready():
    """Kubernetes readiness probe"""
    return ("OK", 200) if monitor.is_ready() else ("Not Ready", 503)

if __name__ == "__main__":
    app.run(port=5000)
```

### Monitoring Metrics

```python
from core.mlops import get_metrics_collector

metrics = get_metrics_collector()

# Track important operational metrics
metrics.increment("experiments_created")
metrics.increment("models_registered")
metrics.increment("artifacts_stored")
metrics.gauge("active_runs", 5)
metrics.gauge("cached_items", cache.size)

# Periodic reporting
def report_metrics():
    summary = metrics.get_summary()
    logger.info(f"Operational metrics: {summary}")
    
# Call periodically (e.g., every 5 minutes)
```

---

## ⚠️ Common Issues & Troubleshooting

### Issue: OOM with Metric Logging

**Symptom**: `MemoryError` after logging many metrics

**Root Cause**: 
- Metric buffer not flushing
- Rolling window too large
- Unflushed runs accumulating

**Solution**:
```python
# 1. Check buffer configuration
tracker = ctx.experiment_tracker
print(f"Buffer size: {tracker.metric_buffer_size}")
print(f"Max per key: {tracker.max_metrics_per_key}")

# 2. Force flush if needed
with tracker.run_context(exp_id) as run:
    for i in range(100_000):
        tracker.log_metric(run.run_id, "metric", i)
        
        # Flush every 10K metrics in high-volume scenarios
        if i % 10_000 == 0:
            tracker._flush_metric_buffer()

# 3. Reduce max_metrics_per_key in config for large runs
# config/ml_info.yaml:
mlops:
  tracking:
    max_metrics_per_key: 5000  # Reduce from default 10K
```

### Issue: Disk Full Error on Write

**Symptom**: `StorageBackendError: Insufficient disk space`

**Root Cause**: 
- Storage backend ran out of space
- No pre-flight validation

**Solution**:
```python
import shutil

# Check before critical operations
stats = shutil.disk_usage("./mlops_data")
if stats.free < 100 * 1024 * 1024:  # Less than 100MB
    logger.error(f"Low disk space: {stats.free / 1024**3:.2f}GB remaining")
    raise ResourceLimitError("disk", stats.used, stats.total)

# Or let the storage backend handle it
try:
    tracker.log_artifact(run_id, "large_file.tar.gz")
except StorageBackendError as e:
    if "Insufficient disk space" in str(e):
        # Clean old runs or artifacts
        cleanup_old_runs(days=30)
        retry_operation()
```

### Issue: Event History Growing Unbounded

**Symptom**: Memory usage keeps increasing over hours/days

**Root Cause**: Event history max_history not configured

**Solution**:
```python
from core.mlops import get_event_emitter

emitter = get_event_emitter()

# Check current history size
for event_type, events in emitter._history.items():
    print(f"{event_type}: {len(events)} events")

# Configure max history in config:
# config/ml_info.yaml:
mlops:
  events:
    max_history_per_type: 5000  # Reduce from default 10K if needed
```

### Issue: Event Callbacks Blocking Operations

**Symptom**: Event emissions take too long

**Root Cause**: Slow callback functions running synchronously

**Solution**:
```python
# ❌ DON'T: Long-running callbacks
emitter.subscribe(EventType.RUN_ENDED, lambda event: send_email(...))
# This blocks the emit() call

# ✅ DO: Use async callbacks or defer work
def async_callback(event):
    # Run in background
    executor.submit(send_email, event)

emitter.subscribe(EventType.RUN_ENDED, async_callback)
```

---

## 📊 Performance Tuning

### For High-Volume Metric Logging (1K+/sec)

```python
# Increase buffer to reduce flushes
config = MLOpsConfig(
    tracking_path="experiment_tracking",
    metric_buffer_size=1000,  # Up from 100
    auto_flush_metrics=True,
    max_metrics_per_key=50_000,  # Up from 10K
)
tracker = ExperimentTracker(storage_backend, **asdict(config))
```

### For Memory-Constrained Environments

```python
# Reduce cache and history sizes
cache = LRUCache(max_size=100, default_ttl=60)  # Smaller cache

config = MLOpsConfig(
    metric_buffer_size=50,  # Flush more frequently
    max_metrics_per_key=1000,  # Smaller rolling window
)

emitter = EventEmitter(max_history_per_type=1000)  # Smaller history
```

### For Large-Scale Deployments (1M+ models/runs)

```python
# Enable indexing and caching
from core.mlops.cache import CachedStorage

cached_storage = CachedStorage(
    storage=storage_backend,
    cache=LRUCache(max_size=10_000, default_ttl=3600)
)

# Search performance becomes O(1) with index
registry_with_index = ModelRegistry(
    storage=cached_storage,
    enable_indexing=True  # Future feature
)
```

---

## 🚀 Quick Start

### Installation

**Core Dependencies** (required):
```bash
pip install pandas loguru pyarrow
```

**Optional: Databricks Backend**
```bash
pip install databricks-sql-connector>=0.3.0
```

**Optional: MLflow Integration**
```bash
pip install mlflow>=2.0.0
```

**All-in-one Installation**:
```bash
pip install tauro[mlops]
# Installs: pandas, loguru, pyarrow, databricks-sql-connector, mlflow
```

**Version Requirements**:
- Python 3.7+
- pandas 1.0.0+
- pyarrow 3.0.0+
- loguru 0.5.3+
- databricks-sql-connector 0.3.0+ (Databricks backend only)
- mlflow 2.0.0+ (MLflow integration only)

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

### Running Tests

```bash
# Run all mlops module tests
pytest src/core/mlops/test/ -v

# Specific tests
pytest src/core/mlops/test/test_protocols.py -v
pytest src/core/mlops/test/test_events.py -v
pytest src/core/mlops/test/test_cache.py -v
pytest src/core/mlops/test/test_health.py -v

# With coverage
pytest src/core/mlops/test/ --cov=src.core.mlops --cov-report=html
```

### Testing Critical Fixes

#### Test Memory Limits (Event History)
```python
def test_event_history_memory_bounded():
    """Verify event history doesn't grow unbounded"""
    emitter = EventEmitter(max_history_per_type=1000)
    
    # Emit 5K events
    for i in range(5000):
        emitter.emit(EventType.RUN_STARTED, {"run_id": f"run_{i}"})
    
    # History should be bounded to 1000
    run_events = len(emitter._history[EventType.RUN_STARTED])
    assert run_events <= 1000, f"History size {run_events} exceeds limit"
```

#### Test Metric Buffer Limits
```python
def test_metrics_rolling_window():
    """Verify metrics use rolling window"""
    tracker = ExperimentTracker(
        storage=storage_backend,
        max_metrics_per_key=1000
    )
    
    with tracker.run_context(exp_id) as run:
        # Log 5K metrics
        for i in range(5000):
            tracker.log_metric(run.run_id, "loss", 0.5 - i * 0.0001)
        
        # Metrics should be bounded
        metric_count = len(run.metrics.get("loss", []))
        assert metric_count <= 1000, f"Metrics {metric_count} exceed limit"
```

#### Test Disk Space Validation
```python
def test_disk_space_check():
    """Verify disk space is validated before write"""
    storage = LocalStorageBackend("./mlops_data")
    
    # This should fail if less than required space
    large_df = pd.DataFrame({"x": range(1_000_000)})
    
    try:
        storage.write_dataframe(large_df, "test_large.parquet")
    except StorageBackendError as e:
        assert "Insufficient disk space" in str(e)
```

#### Test Path Traversal Prevention
```python
def test_path_traversal_prevention():
    """Verify paths can't escape sandbox"""
    from core.mlops.validators import PathValidator, ValidationError
    
    base = Path("./artifacts")
    
    # These should all fail
    invalid_paths = [
        "../../../etc/passwd",
        "subdir/../../etc/passwd",
        "/etc/passwd",
        "~/secret"
    ]
    
    for path in invalid_paths:
        with pytest.raises(ValidationError):
            PathValidator.validate_path(path, base_path=base)
```

#### Test Credential Masking
```python
def test_credentials_not_logged():
    """Verify credentials aren't exposed in logs"""
    # Capture logs
    caplog.set_level(logging.DEBUG)
    
    # Initialize with token in env (not params)
    os.environ["DATABRICKS_TOKEN"] = "test_token_12345"
    ctx = init_mlops(backend_type="databricks", catalog="main")
    
    # Verify token not in logs
    assert "test_token_12345" not in caplog.text
```

### Load Testing

```bash
# High-volume metric logging test
python -m pytest src/core/mlops/test/test_performance.py::test_high_volume_metrics -v

# Stress test with 100K+ events
python -m pytest src/core/mlops/test/test_stress.py -v
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

### Current (v2.1 - Security & Performance)
- [x] **Event history memory limits** - Prevents OOM with bounded deque
- [x] **Disk space validation** - Pre-flight checks before writes
- [x] **Metric buffer persistence** - Immediate storage of buffered metrics
- [x] **Metrics per-run limits** - Hard limits with rolling windows
- [x] **Path traversal prevention** - Improved validation
- [x] **Credential masking** - Tokens from environment, not parameters
- [x] **Distributed lock reliability** - Enhanced file locking
- [x] **Production deployment guide** - Security and health checks
- [x] **Troubleshooting guide** - Common issues and solutions
- [x] **Performance tuning guide** - Optimization for various scales

### Next (v2.2 - Performance)
- [ ] **Indexing for searches** - O(1) model/run lookups
- [ ] **Incremental metrics** - Streaming metric updates
- [ ] **Metric aggregation** - Server-side min/max/avg/percentiles
- [ ] **Cache layer optimization** - Two-level cache for distributed setups

### Future (v3.0 - Enterprise)
- [ ] **Full Databricks UC integration** - Volumes support
- [ ] **Web UI for visualization** - Dashboard for models/runs
- [ ] **MLflow compatibility** - Full MLflow client support
- [ ] **Distributed model support** - Multi-node registries
- [ ] **Data lineage tracking** - End-to-end provenance
- [ ] **Model serving integration** - Direct export to serving platforms

---

## 🎓 Best Practices

### 1. Always Use Context Managers for Runs

```python
# ❌ WRONG: Run may not be finalized if error occurs
run = tracker.start_run(exp_id, "trial_1")
tracker.log_metric(run.run_id, "loss", 0.5)
tracker.end_run(run.run_id)

# ✅ CORRECT: Guaranteed cleanup
with tracker.run_context(exp_id, name="trial_1") as run:
    tracker.log_metric(run.run_id, "loss", 0.5)
# run.end() called automatically
```

### 2. Handle Metrics Carefully in Long Runs

```python
# ❌ WRONG: Unbounded metrics
for epoch in range(1_000_000):
    tracker.log_metric(run.run_id, "loss", loss)
    # Memory keeps growing!

# ✅ CORRECT: Use rolling window
for epoch in range(1_000_000):
    tracker.log_metric(run.run_id, "loss", loss)
    # Automatically limited to max_metrics_per_key (10K default)
    
# ✅ BETTER: Monitor and flush periodically
for epoch in range(1_000_000):
    tracker.log_metric(run.run_id, "loss", loss)
    if epoch % 10_000 == 0:
        logger.info(f"Epoch {epoch}, metrics flushed")
        tracker._flush_metric_buffer()
```

### 3. Validate Paths Before Storage Operations

```python
from core.mlops.validators import PathValidator
from pathlib import Path

artifact_path = Path(request.args.get("path"))

try:
    safe_path = PathValidator.validate_path(
        str(artifact_path),
        base_path=Path("./artifacts")
    )
    tracker.log_artifact(run.run_id, str(safe_path))
except ValidationError as e:
    logger.error(f"Invalid path: {e}")
    raise HTTPException(400, detail=str(e))
```

### 4. Manage Disk Space Proactively

```python
import shutil
from core.mlops.exceptions import ResourceLimitError

def ensure_disk_space(required_mb: int):
    """Check disk space before operation"""
    stats = shutil.disk_usage("./mlops_data")
    available_mb = stats.free / (1024 ** 2)
    
    if available_mb < required_mb:
        logger.critical(
            f"Insufficient disk: need {required_mb}MB, "
            f"have {available_mb:.1f}MB"
        )
        # Clean old artifacts
        cleanup_old_artifacts(days=30)
        
        # Re-check
        stats = shutil.disk_usage("./mlops_data")
        available_mb = stats.free / (1024 ** 2)
        
        if available_mb < required_mb:
            raise ResourceLimitError(
                "disk_space",
                int(stats.used),
                int(stats.total)
            )

# Before large operations
ensure_disk_space(required_mb=1000)
tracker.log_artifact(run.run_id, "large_model.tar.gz")
```

### 5. Monitor Event History Size

```python
from core.mlops import get_event_emitter

def check_event_history():
    """Monitor event history doesn't grow unbounded"""
    emitter = get_event_emitter()
    
    total_events = sum(
        len(events) for events in emitter._history.values()
    )
    
    if total_events > 50_000:
        logger.warning(
            f"Event history large: {total_events} events. "
            f"Consider reducing max_history."
        )
    
    # Report by type
    for event_type, events in emitter._history.items():
        logger.debug(f"{event_type}: {len(events)} events")

# Call periodically
```

### 6. Use Health Checks in Production

```python
from core.mlops import get_health_monitor, is_healthy

# On application startup
if not is_healthy():
    logger.error("System health check failed, refusing to start")
    exit(1)

# In request handlers
def protected_endpoint(request):
    if not is_healthy():
        return {"error": "System unhealthy"}, 503
    
    # Process request
    ...
```

### 7. Batch Operations for Performance

```python
from core.mlops.cache import BatchProcessor

def flush_metrics(operations):
    """Batch process metric storage"""
    for op in operations:
        storage.write_json(op.data, op.path)
    logger.info(f"Flushed {len(operations)} operations")

processor = BatchProcessor(
    process_func=flush_metrics,
    batch_size=100,
    flush_interval=5.0
)

# Add metrics to batch
for i in range(1000):
    processor.add({
        "data": {"metric": "value"},
        "path": f"metrics/{i}.json"
    })
```

### 8. Secure Credential Management

```python
import os
from pathlib import Path

def init_mlops_secure():
    """Initialize MLOps with secure credential handling"""
    
    # Load credentials from environment, not code
    token = os.getenv("DATABRICKS_TOKEN")
    if not token:
        raise ValueError("DATABRICKS_TOKEN not set in environment")
    
    # Or load from credential file
    cred_file = Path.home() / ".databricks" / "token"
    if cred_file.exists():
        token = cred_file.read_text().strip()
    
    # Or load from secrets manager
    # token = get_from_aws_secrets("databricks/token")
    
    # Manually set environment variable
    os.environ["DATABRICKS_TOKEN"] = token
    
    # Initialize without passing token directly
    ctx = init_mlops(
        backend_type="databricks",
        catalog="main",
        # Token is read from DATABRICKS_TOKEN env var
    )
    
    return ctx

# Alternative: Use config file with permissions
# config/mlops.yaml (with 600 permissions, not in git)
# mlops:
#   backend: databricks
#   catalog: main
#   credentials:
#     token_env_var: DATABRICKS_TOKEN
```

### 9. Implement Retry Logic for Resilience

```python
from core.mlops.resilience import with_retry, RetryConfig

# Custom retry config
retry_config = RetryConfig(
    max_attempts=5,
    initial_delay=0.5,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True,
    retryable_exceptions=(IOError, TimeoutError),
)

@with_retry(config=retry_config)
def register_model_with_retry(name, artifact_path, **metadata):
    """Register model with automatic retries"""
    return registry.register_model(name, artifact_path, **metadata)

# Usage
try:
    version = register_model_with_retry(
        name="my_model",
        artifact_path="/path/to/model.pkl",
        artifact_type="sklearn"
    )
except Exception as e:
    logger.error(f"Failed to register after retries: {e}")
```

### 10. Set Up Proper Logging

```python
from loguru import logger
import sys

# Configure structured logging for MLOps
logger.remove()  # Remove default handler

logger.add(
    sys.stderr,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)

logger.add(
    "logs/mlops.log",
    rotation="500 MB",
    retention="10 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
)

# Track important MLOps operations
logger.info("MLOps context initialized", backend="databricks")
logger.debug(f"Registered model: {model_name} v{version}")
logger.warning(f"Disk space low: {available_mb}MB remaining")
logger.error(f"Failed to log artifact: {error}")
```

---


MIT - See LICENSE in project root.
