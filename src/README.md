# Tauro - Data Pipeline Framework

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI version](https://img.shields.io/badge/pypi-v0.1.4-orange.svg)](https://pypi.org/project/tauro/)

**Tauro** is a framework designed to simplify the development and orchestration of batch, streaming, and hybrid data workflows. Built for data engineers and ML practitioners, Tauro provides enterprise-grade reliability with developer-friendly simplicity.

**Use Tauro as a CLI tool or integrate it programmatically into your Python projects.**

---

## ⚡ Quick Start

### CLI Mode
```bash
# Install
pip install tauro

# Create a project
tauro template --template medallion_basic --project-name my_project
cd my_project

# Run a pipeline
tauro run --env dev --pipeline sales_etl
```

### Library Mode
```python
from tauro import PipelineExecutor, ContextLoader

# Load context
context = ContextLoader().load_from_env("dev")

# Execute pipeline
executor = PipelineExecutor(context)
result = executor.execute("sales_etl")

print(f"✅ Success: {result.nodes_executed} nodes in {result.execution_time_seconds}s")
```

---

## 🎯 Key Features

### 🔧 Dual Mode: CLI + Library
- **CLI**: Fast execution with simple commands
- **Library**: Programmatic integration into your Python projects
- **Both**: Use whichever fits your use case

### 📊 Multi-Pipeline Support
- **Batch Processing** — ETL with date ranges and incremental loads
- **Real-time Streaming** — Kafka, Kinesis, and file-based streaming
- **Hybrid Workflows** — Combine batch and streaming in unified pipelines
- **ML/MLOps** — Built-in experiment tracking and model registry

### 🏗️ Enterprise-Ready
- **Security** — Path validation, input sanitization, secure module loading
- **Resilience** — Automatic retries, circuit breakers, graceful degradation
- **Observability** — Structured logging, metrics, and health checks
- **Multi-Environment** — Configuration per environment (dev, staging, prod)

---

## 📦 Installation

### Basic Installation
```bash
pip install tauro
```

### Installation with Extras
```bash
# With Spark support
pip install tauro[spark]

# With API and monitoring
pip install tauro[api,monitoring]

# Complete installation
pip install tauro[all]
```


## 🚀 CLI Usage

### Basic Commands

```bash
# List available pipelines
tauro config list-pipelines --env dev

# Execute pipeline
tauro run --env dev --pipeline sales_etl

# With date range
tauro run --env dev --pipeline sales_etl \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# Validate configuration
tauro run --env prod --pipeline sales_etl --validate-only

# Execute specific node
tauro run --env dev --pipeline sales_etl --node load_data
```

### Streaming Pipelines

```bash
# Start streaming pipeline
tauro stream run --config ./config/streaming/settings.py --pipeline kafka_events

# Check status
tauro stream status --config ./config/streaming/settings.py --execution-id abc123

# Stop pipeline
tauro stream stop --config ./config/streaming/settings.py --execution-id abc123
```

### Template Generation

```bash
# List available templates
tauro template --list-templates

# Generate project from template
tauro template --template medallion_basic --project-name my_project

# With specific format (yaml, json, dsl)
tauro template --template medallion_basic --project-name my_project --format json
```

---

## 📚 Library Usage

### Example 1: Execute Pipeline

```python
from tauro import PipelineExecutor, ContextLoader

# Load context from configuration
context = ContextLoader().load_from_env("production")

# Create executor
executor = PipelineExecutor(context)

# Execute pipeline
result = executor.execute(
    pipeline_name="daily_sales",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Check result
if result.success:
    print(f"✅ Pipeline successful")
    print(f"   Nodes executed: {result.nodes_executed}")
    print(f"   Time: {result.execution_time_seconds}s")
    print(f"   Records: {result.metrics.get('records_processed')}")
else:
    print(f"❌ Error: {result.error_message}")
```

### Example 2: Programmatic Input/Output

```python
from tauro import InputLoader, DataOutputManager, ContextLoader

context = ContextLoader().load_from_env("dev")

# Load data
loader = InputLoader(context)
sales_data = loader.load("raw_sales")

# Process data
filtered = sales_data.filter(sales_data.amount > 1000)

# Save results
output = DataOutputManager(context)
output.write(
    dataframe=filtered,
    output_key="high_value_sales",
    write_mode="overwrite"
)
```

### Example 3: Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tauro import PipelineExecutor, ContextLoader

def run_tauro_pipeline(**kwargs):
    """Execute Tauro pipeline from Airflow."""
    context = ContextLoader().load_from_env("production")
    executor = PipelineExecutor(context)
    
    result = executor.execute(
        pipeline_name="daily_etl",
        start_date=kwargs['ds']
    )
    
    if not result.success:
        raise Exception(f"Pipeline failed: {result.error_message}")
    
    return result.metrics

with DAG('tauro_daily_etl', start_date=datetime(2024, 1, 1)) as dag:
    run_task = PythonOperator(
        task_id='run_tauro',
        python_callable=run_tauro_pipeline
    )
```

### Example 4: FastAPI REST API

```python
from fastapi import FastAPI, HTTPException
from tauro import PipelineExecutor, ContextLoader

app = FastAPI()

@app.post("/pipelines/{pipeline_name}/execute")
async def execute_pipeline(pipeline_name: str, env: str = "production"):
    """Execute pipeline via REST API."""
    try:
        context = ContextLoader().load_from_env(env)
        executor = PipelineExecutor(context)
        result = executor.execute(pipeline_name)
        
        return {
            "success": result.success,
            "nodes_executed": result.nodes_executed,
            "execution_time": result.execution_time_seconds,
            "metrics": result.metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

### Example 5: Streaming Pipeline

```python
from tauro import StreamingPipelineManager, StreamingContext
import time

# Create streaming context
ctx = StreamingContext.from_config("./config/streaming.yaml")
manager = StreamingPipelineManager(ctx)

# Start streaming pipeline
execution_id = manager.run_streaming_pipeline(
    pipeline_name="kafka_events",
    checkpoint_location="/tmp/checkpoints"
)

# Monitor status
for _ in range(10):
    status = manager.get_pipeline_status(execution_id)
    print(f"State: {status.state}, Records: {status.records_processed}")
    time.sleep(5)

# Stop pipeline
manager.stop_streaming_pipeline(execution_id)
```

### Example 6: MLOps Workflow

```python
from tauro import MLContext, ExperimentTracker, ModelRegistry

# Initialize MLOps context
ml_ctx = MLContext.from_config("./config/ml.yaml")
tracker = ExperimentTracker(ml_ctx)

# Track experiment
with tracker.start_run("customer_churn") as run:
    # Train model
    model = train_model(data)
    
    # Log parameters and metrics
    run.log_params({"learning_rate": 0.01, "max_depth": 5})
    run.log_metrics({"accuracy": 0.92, "f1_score": 0.89})
    
    # Register model
    registry = ModelRegistry(ml_ctx)
    registry.register_model(model, "churn_predictor", "v1.0")
```

---


## 🔧 API Reference

### Core Exports

```python
from tauro import (
    # Execution
    PipelineExecutor,        # Main pipeline executor
    BatchExecutor,           # Batch processing
    StreamingExecutor,       # Streaming processing
    HybridExecutor,          # Hybrid workflows
    NodeExecutor,            # Single node execution
    
    # Configuration
    ContextLoader,           # Load execution context
    ConfigManager,           # Manage configuration
    ConfigDiscovery,         # Auto-discover configs
    
    # Input/Output
    InputLoader,             # Load input data
    DataOutputManager,       # Write output data
    ReaderFactory,           # Create readers
    WriterFactory,           # Create writers
    
    # Streaming
    StreamingPipelineManager,  # Manage streaming pipelines
    StreamingQueryManager,     # Query management
    
    # MLOps
    MLOpsContext,            # MLOps context
    ExperimentTracker,       # Track ML experiments
    ModelRegistry,           # Register ML models
    
    # CLI
    UnifiedCLI,              # CLI interface
    main,                    # CLI entry point
)
```

### Main Classes

#### PipelineExecutor
```python
executor = PipelineExecutor(context)

# Execute complete pipeline
result = executor.execute(
    pipeline_name="etl_pipeline",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Execute specific node
node_result = executor.execute_node("etl_pipeline", "transform_data")

# Validate pipeline
is_valid = executor.validate_pipeline("etl_pipeline")

# List available pipelines
pipelines = executor.list_pipelines()
```

#### ContextLoader
```python
loader = ContextLoader()

# Load by environment
context = loader.load_from_env("production")

# Load from config file
context = loader.load_from_config("./config/settings.yaml")

# Load from dictionary
context = loader.load_from_dict(config_dict)
```

#### InputLoader & DataOutputManager
```python
# Load data
loader = InputLoader(context)
data = loader.load("sales_data")

# Write data
output = DataOutputManager(context)
output.write(
    dataframe=processed_data,
    output_key="clean_sales",
    write_mode="overwrite"
)
```

---

## 🛠️ CLI Commands Reference

| Command | Description |
|---------|-------------|
| `tauro config list-pipelines --env <env>` | List all available pipelines |
| `tauro config pipeline-info --pipeline <name> --env <env>` | Show pipeline details |
| `tauro run --env <env> --pipeline <name>` | Execute pipeline |
| `tauro run --env <env> --pipeline <name> --validate-only` | Validate without executing |
| `tauro template --template <type> --project-name <name>` | Generate new project |
| `tauro stream run --config <path> --pipeline <name>` | Start streaming pipeline |
| `tauro stream status --config <path> --execution-id <id>` | Check streaming status |
| `tauro stream stop --config <path> --execution-id <id>` | Stop streaming pipeline |

---

## 🎨 Use Cases

### 1. Batch ETL with Spark
```python
from tauro import PipelineExecutor, ContextLoader

context = ContextLoader().load_from_env("production")
executor = PipelineExecutor(context)

result = executor.execute(
    "customer_360",
    start_date="2024-01-01",
    end_date="2024-12-31"
)
```

### 2. Real-Time Streaming
```python
from tauro import StreamingPipelineManager, StreamingContext

ctx = StreamingContext.from_config("./config/streaming.yaml")
manager = StreamingPipelineManager(ctx)

exec_id = manager.run_streaming_pipeline(
    "kafka_events",
    checkpoint_location="/tmp/checkpoints"
)
```

### 3. Hybrid Pipeline (Batch + Streaming)
```python
from tauro import HybridExecutor, HybridContext

ctx = HybridContext.from_config("./config/hybrid.yaml")
executor = HybridExecutor(ctx)

result = executor.execute(
    "real_time_analytics",
    mode="hybrid"
)
```

### 4. Automated Testing
```python
import pytest
from tauro import PipelineExecutor, ContextLoader

@pytest.fixture
def test_executor():
    context = ContextLoader().load_from_env("test")
    return PipelineExecutor(context)

def test_pipeline(test_executor):
    result = test_executor.execute("test_pipeline")
    assert result.success
    assert result.nodes_executed == 3
```

---

## 🔧 Configuration

Tauro uses **environment-based configuration**:

```
project/
├── config/
│   ├── base/               # Base configuration
│   │   ├── global_settings.yaml
│   │   ├── pipelines.yaml
│   │   ├── nodes.yaml
│   │   ├── input.yaml
│   │   └── output.yaml
│   ├── dev/                # Dev overrides
│   ├── staging/            # Staging overrides
│   └── prod/               # Production overrides
└── settings.json           # Environment mapping
```

**Environment fallback chain:**
- `prod` → `base`
- `staging` → `prod` → `base`
- `dev` → `base`

---

## ✅ Best Practices

### Security
✅ Use path validation for all file operations  
✅ Sanitize user inputs  
✅ Use YAML safe_load() for configs  
✅ Validate pipeline names and execution IDs

### Performance
✅ Enable configuration caching (TTL: 5 minutes)  
✅ Use Delta format for large datasets  
✅ Configure appropriate Spark resources  
✅ Set checkpoints for streaming pipelines

### Development
✅ Test in `dev` environment first  
✅ Use `--validate-only` before production runs  
✅ Enable verbose logging for debugging  
✅ Write unit tests for custom pipeline logic

### Production
✅ Use separate environments (dev, staging, prod)  
✅ Enable monitoring and alerting  
✅ Set up retry policies  
✅ Configure resource limits

---

## 🐛 Troubleshooting

### Common Issues

**Module not found**
```bash
# Solution: Install tauro
pip install tauro

# Or from source
pip install -e .
```

**Configuration not found**
```python
# Solution: Use explicit config path
from tauro import ContextLoader

context = ContextLoader().load_from_config("./config/settings.yaml")
```

**Import errors in pipeline**
```python
# Solution: Check Python path and module installation
from tauro import PipelineExecutor

executor = PipelineExecutor(context, debug_mode=True)
result = executor.execute("pipeline")  # Will show detailed import diagnostics
```

**Verbose logging**
```bash
# Enable detailed logs
tauro run --env dev --pipeline my_pipeline --verbose
```

**Dry-run mode**
```bash
# See what will execute without running
tauro run --env dev --pipeline my_pipeline --dry-run
```

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.



## 🌟 Star Us

If you find Tauro useful, please ⭐ star the repository!

---

**Built with ❤️ by [Faustino Lopez Ramos](https://github.com/faustino125)**  
**Version**: 0.1.4 | **License**: MIT
