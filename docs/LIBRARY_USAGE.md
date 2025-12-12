# Tauro - Guía de Uso como Librería

Esta guía muestra cómo usar Tauro como librería Python reutilizable en tus propios proyectos.

---

## 📦 Instalación

### Instalación Básica
```bash
pip install tauro
```

### Instalación con Extras

```bash
# Con soporte Spark
pip install tauro[spark]

# Con API y monitoreo
pip install tauro[api,monitoring]

# Instalación completa
pip install tauro[all]
```

### Instalación desde Código Fuente
```bash
git clone https://github.com/faustino125/tauro.git
cd tauro
pip install -e .
```

---

## 🚀 Inicio Rápido

### Ejemplo 1: Ejecución Programática de Pipeline

```python
from tauro import PipelineExecutor, ContextLoader

# Cargar contexto desde configuración
context_loader = ContextLoader()
context = context_loader.load_from_env(env="dev")

# Crear executor
executor = PipelineExecutor(context)

# Ejecutar pipeline
result = executor.execute(
    pipeline_name="data_ingestion",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

if result.success:
    print(f"Pipeline completado: {result.nodes_executed} nodos")
else:
    print(f"Error: {result.error_message}")
```

### Ejemplo 2: Gestión de Configuración

```python
from tauro import ConfigManager, ConfigDiscovery

# Descubrimiento automático de configuración
discovery = ConfigDiscovery()
config_path = discovery.discover(
    root_dir="./config",
    env="production"
)

# Cargar configuración
manager = ConfigManager(config_path)
config = manager.load_config()

# Acceder a settings
print(config['global_settings']['project_name'])
print(config['pipelines']['data_ingestion'])
```

### Ejemplo 3: Input/Output Programático

```python
from tauro import InputLoader, DataOutputManager

# Cargar datos de entrada
loader = InputLoader(context)
data = loader.load("raw_sales_data")  # DataFrame

# Procesar datos
processed = data.filter(data.amount > 100)

# Guardar resultados
output_manager = DataOutputManager(context)
output_manager.write(
    dataframe=processed,
    output_key="filtered_sales",
    write_mode="overwrite"
)
```

---

## 📚 Casos de Uso Comunes

### 1. Integración con Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tauro import PipelineExecutor, ContextLoader

def run_tauro_pipeline(**kwargs):
    """Ejecuta pipeline Tauro desde Airflow."""
    context = ContextLoader().load_from_env("production")
    executor = PipelineExecutor(context)
    
    result = executor.execute(
        pipeline_name="daily_etl",
        start_date=kwargs['ds'],
        end_date=kwargs['ds']
    )
    
    if not result.success:
        raise Exception(f"Pipeline failed: {result.error_message}")
    
    return result.metrics

with DAG(
    'tauro_daily_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
) as dag:
    
    run_pipeline = PythonOperator(
        task_id='run_tauro_pipeline',
        python_callable=run_tauro_pipeline,
    )
```

### 2. Jupyter Notebook Interactivo

```python
# Notebook cell 1: Setup
from tauro import ContextLoader, InputLoader, PipelineExecutor

context = ContextLoader().load_from_env("dev")
loader = InputLoader(context)

# Notebook cell 2: Exploración de datos
raw_data = loader.load("customer_data")
raw_data.show(10)
raw_data.printSchema()

# Notebook cell 3: Ejecutar transformación específica
executor = PipelineExecutor(context)
result = executor.execute_node(
    pipeline_name="customer_analytics",
    node_name="enrich_customer_data"
)

# Notebook cell 4: Ver resultados
enriched_data = result.output_data
enriched_data.describe()
```

### 3. Testing de Pipelines

```python
import pytest
from tauro import PipelineExecutor, ContextLoader
from datetime import date

@pytest.fixture
def test_context():
    """Contexto de test con configuración aislada."""
    return ContextLoader().load_from_env("test")

def test_customer_pipeline(test_context):
    """Test de pipeline de clientes."""
    executor = PipelineExecutor(test_context)
    
    result = executor.execute(
        pipeline_name="customer_ingestion",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1)
    )
    
    assert result.success
    assert result.nodes_executed == 3
    assert "load_customers" in result.completed_nodes
    
def test_pipeline_validation(test_context):
    """Test de validación de pipeline."""
    executor = PipelineExecutor(test_context)
    
    is_valid = executor.validate_pipeline("customer_ingestion")
    assert is_valid
```

### 4. API REST con FastAPI

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tauro import PipelineExecutor, ContextLoader
from typing import Optional

app = FastAPI()

class PipelineRequest(BaseModel):
    pipeline_name: str
    env: str = "production"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

@app.post("/pipelines/execute")
async def execute_pipeline(request: PipelineRequest):
    """Endpoint para ejecutar pipeline."""
    try:
        context = ContextLoader().load_from_env(request.env)
        executor = PipelineExecutor(context)
        
        result = executor.execute(
            pipeline_name=request.pipeline_name,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        return {
            "success": result.success,
            "nodes_executed": result.nodes_executed,
            "execution_time": result.execution_time_seconds,
            "metrics": result.metrics
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipelines/list")
async def list_pipelines(env: str = "production"):
    """Lista pipelines disponibles."""
    context = ContextLoader().load_from_env(env)
    executor = PipelineExecutor(context)
    
    pipelines = executor.list_pipelines()
    return {"pipelines": pipelines}
```

### 5. Streaming en Tiempo Real

```python
from tauro import StreamingPipelineManager, StreamingContext

# Crear contexto de streaming
streaming_ctx = StreamingContext.from_config("./config/streaming.yaml")

# Iniciar pipeline de streaming
manager = StreamingPipelineManager(streaming_ctx)

execution_id = manager.run_streaming_pipeline(
    pipeline_name="realtime_alerts",
    checkpoint_location="/tmp/checkpoints/alerts"
)

print(f"Streaming pipeline iniciado: {execution_id}")

# Monitorear status
import time
while True:
    status = manager.get_pipeline_status(execution_id)
    print(f"Estado: {status.state}, Registros: {status.records_processed}")
    
    if status.state in ["FAILED", "STOPPED"]:
        break
    
    time.sleep(10)
```

### 6. MLOps Integration

```python
from tauro import MLContext, ExperimentTracker, ModelRegistry

# Crear contexto ML
ml_ctx = MLContext.from_config("./config/ml.yaml")

# Tracking de experimentos
tracker = ExperimentTracker(ml_ctx)

with tracker.start_run(experiment_name="customer_churn") as run:
    # Entrenar modelo
    model = train_model(data)
    
    # Log parámetros
    run.log_params({
        "learning_rate": 0.01,
        "max_depth": 5,
        "n_estimators": 100
    })
    
    # Log métricas
    run.log_metrics({
        "accuracy": 0.92,
        "precision": 0.89,
        "recall": 0.94
    })
    
    # Registrar modelo
    registry = ModelRegistry(ml_ctx)
    registry.register_model(
        model=model,
        model_name="churn_predictor",
        version="v1.0",
        run_id=run.run_id
    )
```

---

## 🔧 API Pública Principal

### Core - Configuración

```python
from tauro import (
    # Context Management
    Context,
    MLContext,
    StreamingContext,
    HybridContext,
    ContextFactory,
    ContextLoader,
    
    # Config Loading
    ConfigManager,
    ConfigDiscovery,
    YamlConfigLoader,
    JsonConfigLoader,
    DSLConfigLoader,
    
    # Validation
    ConfigValidator,
)
```

### Core - Ejecución

```python
from tauro import (
    # Executors
    PipelineExecutor,
    BatchExecutor,
    StreamingExecutor,
    HybridExecutor,
    NodeExecutor,
    
    # Dependency Management
    DependencyResolver,
    
    # Validation
    PipelineValidator,
)
```

### Core - Input/Output

```python
from tauro import (
    # Data Loading
    InputLoader,
    ReaderFactory,
    
    # Data Writing
    DataOutputManager,
    WriterFactory,
    
    # Unity Catalog
    UnityCatalogManager,
    UnityCatalogConfig,
    
    # Constants
    SupportedFormats,
    WriteMode,
)
```

### Core - Streaming

```python
from tauro import (
    StreamingPipelineManager,
    StreamingQueryManager,
)
```

### Core - MLOps

```python
from tauro import (
    MLOpsContext,
    MLOpsConfig,
    init_mlops,
    get_mlops_context,
    ModelRegistry,
    ExperimentTracker,
)
```

### Core - CLI (Programático)

```python
from tauro import (
    UnifiedCLI,
    TauroCLI,  # Alias legacy
    main,
    CLIPipelineExecutor,
)
```

---

## 🎨 Patrones de Diseño

### Patrón 1: Factory Pattern para Contextos

```python
from tauro import ContextFactory

# Crear contexto según tipo de pipeline
context = ContextFactory.create(
    pipeline_type="batch",
    env="production",
    config_path="./config"
)

# O usar tipos específicos
ml_context = ContextFactory.create_ml_context(
    env="production",
    mlflow_tracking_uri="http://mlflow:5000"
)
```

### Patrón 2: Builder Pattern para Configuración

```python
from tauro import ConfigManager

config = (
    ConfigManager()
    .with_env("production")
    .with_date_range("2024-01-01", "2024-12-31")
    .with_spark_config({
        "spark.sql.shuffle.partitions": "200"
    })
    .build()
)
```

### Patrón 3: Strategy Pattern para Readers/Writers

```python
from tauro import ReaderFactory, WriterFactory, SupportedFormats

# Estrategia de lectura según formato
reader = ReaderFactory.create_reader(
    format=SupportedFormats.DELTA,
    context=context
)

data = reader.read("s3://bucket/data/")

# Estrategia de escritura
writer = WriterFactory.create_writer(
    format=SupportedFormats.PARQUET,
    context=context
)

writer.write(data, "s3://bucket/output/")
```

### Patrón 4: Dependency Injection

```python
from tauro import PipelineExecutor, NodeExecutor, DependencyResolver

# Inyectar dependencias personalizadas
custom_resolver = CustomDependencyResolver()
custom_executor = CustomNodeExecutor()

executor = PipelineExecutor(
    context=context,
    dependency_resolver=custom_resolver,
    node_executor=custom_executor
)
```

---

## 🔌 Extensibilidad

### Crear Reader/Writer Personalizado

```python
from tauro.io.base import BaseReader, BaseWriter
from typing import Any

class CustomDatabaseReader(BaseReader):
    """Reader personalizado para base de datos específica."""
    
    def read(self, source: str, **options) -> Any:
        """Lee desde base de datos custom."""
        connection = self._create_connection(source)
        query = options.get('query', 'SELECT * FROM table')
        
        # Lógica de lectura personalizada
        data = connection.execute(query)
        return self._to_dataframe(data)

# Registrar en factory
from tauro import ReaderFactory
ReaderFactory.register("custom_db", CustomDatabaseReader)

# Usar
reader = ReaderFactory.create_reader("custom_db", context)
data = reader.read("custom://db/table", query="SELECT * FROM users")
```

### Crear Executor Personalizado

```python
from tauro.exec.base import BaseExecutor
from typing import Dict, Any

class CustomMLExecutor(BaseExecutor):
    """Executor personalizado para pipelines ML."""
    
    def execute(self, pipeline_name: str, **kwargs) -> Dict[str, Any]:
        """Ejecuta pipeline ML con lógica custom."""
        # Pre-processing
        self._setup_ml_environment()
        
        # Ejecución
        result = super().execute(pipeline_name, **kwargs)
        
        # Post-processing
        self._register_model_artifacts(result)
        
        return result
```

### Crear Validador Personalizado

```python
from tauro.config.validators import ConfigValidator

class CustomBusinessValidator(ConfigValidator):
    """Validador de reglas de negocio personalizadas."""
    
    def validate(self, config: Dict[str, Any]) -> bool:
        """Valida reglas de negocio específicas."""
        # Validación personalizada
        if config.get('max_records', 0) > 1_000_000:
            raise ValueError("max_records excede límite permitido")
        
        return True
```

---

## 📊 Métricas y Monitoreo

### Capturar Métricas de Ejecución

```python
from tauro import PipelineExecutor

executor = PipelineExecutor(context)
result = executor.execute("data_pipeline")

# Acceder a métricas
print(f"Tiempo ejecución: {result.execution_time_seconds}s")
print(f"Nodos ejecutados: {result.nodes_executed}")
print(f"Registros procesados: {result.metrics.get('records_processed')}")
print(f"Memoria usada: {result.metrics.get('memory_usage_mb')}MB")

# Métricas por nodo
for node_name, node_metrics in result.node_metrics.items():
    print(f"{node_name}: {node_metrics['execution_time_seconds']}s")
```

### Integración con Prometheus

```python
from tauro import PipelineExecutor
from prometheus_client import Counter, Histogram, start_http_server

# Métricas Prometheus
pipeline_executions = Counter(
    'tauro_pipeline_executions_total',
    'Total pipeline executions',
    ['pipeline', 'status']
)

execution_duration = Histogram(
    'tauro_execution_duration_seconds',
    'Pipeline execution duration',
    ['pipeline']
)

# Ejecutar con métricas
def execute_with_metrics(pipeline_name: str):
    with execution_duration.labels(pipeline=pipeline_name).time():
        result = executor.execute(pipeline_name)
        
        status = 'success' if result.success else 'failed'
        pipeline_executions.labels(
            pipeline=pipeline_name,
            status=status
        ).inc()
        
        return result

# Exponer métricas
start_http_server(8000)
```

---

## 🔒 Mejores Prácticas

### 1. Gestión de Configuración

```python
# ✅ BIEN: Usar discovery automático
from tauro import ConfigDiscovery

discovery = ConfigDiscovery()
config_path = discovery.discover(env="production")

# ❌ MAL: Hardcodear paths
config_path = "/absolute/path/to/config.yaml"
```

### 2. Manejo de Errores

```python
# ✅ BIEN: Capturar excepciones específicas
from tauro import PipelineExecutor
from tauro.core.cli.core import ExecutionError, ValidationError

try:
    result = executor.execute("pipeline")
except ValidationError as e:
    logger.error(f"Validación falló: {e}")
    # Acciones de recuperación
except ExecutionError as e:
    logger.error(f"Ejecución falló: {e}")
    # Notificar, rollback, etc.

# ❌ MAL: Captura genérica
try:
    result = executor.execute("pipeline")
except Exception:
    pass
```

### 3. Gestión de Recursos

```python
# ✅ BIEN: Usar context managers
from tauro import ContextLoader

with ContextLoader().load_from_env("production") as context:
    executor = PipelineExecutor(context)
    result = executor.execute("pipeline")
    # Recursos se limpian automáticamente

# ❌ MAL: No limpiar recursos
context = ContextLoader().load_from_env("production")
executor = PipelineExecutor(context)
result = executor.execute("pipeline")
# Spark session, conexiones, etc. permanecen abiertas
```

### 4. Testing

```python
# ✅ BIEN: Usar entorno de test aislado
import pytest
from tauro import ContextLoader

@pytest.fixture
def test_context():
    return ContextLoader().load_from_env("test")

def test_pipeline(test_context, tmp_path):
    # Usar paths temporales
    test_context.config['output_path'] = str(tmp_path)
    executor = PipelineExecutor(test_context)
    result = executor.execute("test_pipeline")
    assert result.success

# ❌ MAL: Usar entorno de producción en tests
def test_pipeline():
    context = ContextLoader().load_from_env("production")
    # Peligroso: puede modificar datos reales
```

### 5. Logging

```python
# ✅ BIEN: Usar logger configurado
from loguru import logger

logger.info(f"Ejecutando pipeline {pipeline_name}")
result = executor.execute(pipeline_name)
logger.success(f"Pipeline completado: {result.nodes_executed} nodos")

# ❌ MAL: Print statements
print(f"Ejecutando {pipeline_name}")
result = executor.execute(pipeline_name)
print("Terminado")
```

---

## 🐛 Debugging y Troubleshooting

### Activar Modo Debug

```python
from tauro import PipelineExecutor, ContextLoader
import logging

# Activar logging detallado
logging.basicConfig(level=logging.DEBUG)

context = ContextLoader().load_from_env("dev")
executor = PipelineExecutor(
    context,
    debug_mode=True,
    validate_before_execute=True
)

result = executor.execute("problematic_pipeline")
```

### Dry-Run Mode

```python
# Validar sin ejecutar
executor = PipelineExecutor(context, dry_run=True)
result = executor.execute("pipeline")

# Result contendrá plan de ejecución sin ejecutar nodos
print(f"Nodos a ejecutar: {result.planned_nodes}")
print(f"Dependencias: {result.dependency_graph}")
```

### Inspeccionar Contexto

```python
from tauro import ContextLoader

context = ContextLoader().load_from_env("production")

# Ver configuración cargada
print(context.config)
print(context.global_settings)
print(context.pipelines)

# Ver rutas de archivos
print(context.config_paths)
```

---

## 📖 Recursos Adicionales

### Documentación por Módulo

- [CLI Module](src/core/cli/README.md) - Interfaz de línea de comandos
- [Config Module](src/core/config/README.md) - Gestión de configuración
- [Exec Module](src/core/exec/README.md) - Ejecución de pipelines
- [IO Module](src/core/io/README.md) - Input/Output
- [Streaming Module](src/core/streaming/README.md) - Pipelines streaming
- [MLOps Module](src/core/mlops/README.md) - Integración MLOps

### Ejemplos Completos

Ver carpeta `examples/` para proyectos completos:
- `examples/batch_etl/` - Pipeline ETL batch
- `examples/streaming_kafka/` - Pipeline Kafka streaming
- `examples/ml_training/` - Training pipeline MLOps
- `examples/hybrid_pipeline/` - Pipeline híbrido

### API Reference

Documentación completa de API: [API_REFERENCE.md](API_REFERENCE.md)

---

## 🤝 Contribuir

Para contribuir con nuevos readers, executors, o funcionalidad:

1. Fork el repositorio
2. Crea feature branch
3. Implementa cambios con tests
4. Envía Pull Request

Ver [CONTRIBUTING.md](CONTRIBUTING.md) para detalles.

---

## 📝 Changelog

Ver [CHANGELOG.md](CHANGELOG.md) para historial de versiones.

---

## 📄 Licencia

MIT License - Ver [LICENSE](LICENSE) para detalles.
