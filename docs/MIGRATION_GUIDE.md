# Guía de Migración: CLI → Librería Programática

Esta guía te ayudará a migrar de usar Tauro como herramienta CLI a usarlo como librería Python en tus propios proyectos.

---

## 🎯 Casos de Uso

Migra a uso como librería cuando necesites:

- ✅ Integración con Airflow, Prefect u otros orquestadores
- ✅ API REST personalizada
- ✅ Notebooks interactivos (Jupyter)
- ✅ Testing automatizado de pipelines
- ✅ Control programático completo
- ✅ Integración en aplicaciones existentes

---

## 📋 Tabla de Equivalencias

### Comandos CLI → API Programática

| Comando CLI | Equivalente Programático |
|-------------|-------------------------|
| `tauro --env dev --pipeline sales` | `executor.execute("sales")` |
| `tauro --list-pipelines` | `executor.list_pipelines()` |
| `tauro --validate-config` | `executor.validate_pipeline(name)` |
| `tauro --pipeline sales --node load` | `executor.execute_node("sales", "load")` |
| `tauro --env prod --pipeline etl --start-date 2024-01-01` | `executor.execute("etl", start_date="2024-01-01")` |

---

## 🔄 Ejemplos de Migración

### Ejemplo 1: Ejecución Básica

**Antes (CLI):**
```bash
tauro --env production --pipeline daily_sales
```

**Después (Programático):**
```python
from tauro import PipelineExecutor, ContextLoader

context = ContextLoader().load_from_env("production")
executor = PipelineExecutor(context)
result = executor.execute("daily_sales")

if result.success:
    print(f"✅ Pipeline completado: {result.nodes_executed} nodos")
else:
    print(f"❌ Error: {result.error_message}")
```

---

### Ejemplo 2: Pipeline con Fechas

**Antes (CLI):**
```bash
tauro --env dev --pipeline sales_report \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

**Después (Programático):**
```python
from tauro import PipelineExecutor, ContextLoader

context = ContextLoader().load_from_env("dev")
executor = PipelineExecutor(context)

result = executor.execute(
    pipeline_name="sales_report",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

print(f"Registros procesados: {result.metrics.get('records_processed')}")
print(f"Tiempo ejecución: {result.execution_time_seconds}s")
```

---

### Ejemplo 3: Validación

**Antes (CLI):**
```bash
tauro --env production --pipeline etl --validate-only
```

**Después (Programático):**
```python
from tauro import PipelineExecutor, ContextLoader

context = ContextLoader().load_from_env("production")
executor = PipelineExecutor(context)

is_valid = executor.validate_pipeline("etl")

if is_valid:
    print("✅ Pipeline válido")
else:
    print("❌ Pipeline tiene errores")
```

---

### Ejemplo 4: Listar Pipelines

**Antes (CLI):**
```bash
tauro --env dev --list-pipelines
```

**Después (Programático):**
```python
from tauro import PipelineExecutor, ContextLoader

context = ContextLoader().load_from_env("dev")
executor = PipelineExecutor(context)

pipelines = executor.list_pipelines()

print("Pipelines disponibles:")
for name in pipelines:
    print(f"  - {name}")
```

---

### Ejemplo 5: Pipeline Streaming

**Antes (CLI):**
```bash
tauro stream run --config ./config --pipeline kafka_events
tauro stream status --execution-id abc123
tauro stream stop --execution-id abc123
```

**Después (Programático):**
```python
from tauro import StreamingPipelineManager, StreamingContext
import time

ctx = StreamingContext.from_config("./config/streaming.yaml")
manager = StreamingPipelineManager(ctx)

# Iniciar
exec_id = manager.run_streaming_pipeline(
    "kafka_events",
    checkpoint_location="/tmp/checkpoints"
)

print(f"Pipeline iniciado: {exec_id}")

# Monitorear
for _ in range(10):
    status = manager.get_pipeline_status(exec_id)
    print(f"Estado: {status.state}, Registros: {status.records_processed}")
    time.sleep(5)

# Detener
manager.stop_streaming_pipeline(exec_id)
```

---

## 🔧 Migración de Scripts

### Script CLI Típico

**Antes:**
```bash
#!/bin/bash
# run_daily_etl.sh

export ENV=production
export START_DATE=$(date -d "yesterday" +%Y-%m-%d)
export END_DATE=$(date +%Y-%m-%d)

tauro --env $ENV \
  --pipeline daily_etl \
  --start-date $START_DATE \
  --end-date $END_DATE

if [ $? -eq 0 ]; then
    echo "Pipeline succeeded"
else
    echo "Pipeline failed"
    exit 1
fi
```

**Después (Python Script):**
```python
#!/usr/bin/env python3
# run_daily_etl.py

from tauro import PipelineExecutor, ContextLoader
from datetime import date, timedelta
import sys

def main():
    # Configuración
    env = "production"
    end_date = date.today()
    start_date = end_date - timedelta(days=1)
    
    # Cargar contexto
    context = ContextLoader().load_from_env(env)
    executor = PipelineExecutor(context)
    
    # Ejecutar pipeline
    result = executor.execute(
        pipeline_name="daily_etl",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat()
    )
    
    # Resultado
    if result.success:
        print(f"✅ Pipeline succeeded")
        print(f"   Nodes: {result.nodes_executed}")
        print(f"   Time: {result.execution_time_seconds}s")
        return 0
    else:
        print(f"❌ Pipeline failed: {result.error_message}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

---

## 🔄 Integración con Orquestadores

### Airflow

**Antes (BashOperator):**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

run_tauro = BashOperator(
    task_id='run_tauro',
    bash_command='tauro --env production --pipeline daily_etl --start-date {{ ds }}'
)
```

**Después (PythonOperator):**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from tauro import PipelineExecutor, ContextLoader

def run_tauro_pipeline(**kwargs):
    context = ContextLoader().load_from_env("production")
    executor = PipelineExecutor(context)
    
    result = executor.execute(
        "daily_etl",
        start_date=kwargs['ds']
    )
    
    if not result.success:
        raise Exception(f"Pipeline failed: {result.error_message}")
    
    return result.metrics

run_tauro = PythonOperator(
    task_id='run_tauro',
    python_callable=run_tauro_pipeline
)
```

**Ventajas:**
- ✅ Mejor manejo de errores
- ✅ Acceso directo a métricas
- ✅ Logging integrado
- ✅ Menos overhead

---

### Prefect

**Migración similar:**
```python
from prefect import flow, task
from tauro import PipelineExecutor, ContextLoader

@task
def run_pipeline(name: str, env: str = "production"):
    context = ContextLoader().load_from_env(env)
    executor = PipelineExecutor(context)
    result = executor.execute(name)
    
    if not result.success:
        raise Exception(f"Failed: {result.error_message}")
    
    return result

@flow(name="Daily ETL")
def daily_etl_flow():
    result = run_pipeline("daily_etl")
    return result

# Ejecutar
if __name__ == "__main__":
    daily_etl_flow()
```

---

## 🧪 Testing

### Antes: Testing Manual con CLI

```bash
# test_pipeline.sh
tauro --env test --pipeline my_pipeline --validate-only
tauro --env test --pipeline my_pipeline
```

### Después: Testing Automatizado

```python
# test_pipeline.py
import pytest
from tauro import PipelineExecutor, ContextLoader

@pytest.fixture
def test_executor():
    context = ContextLoader().load_from_env("test")
    return PipelineExecutor(context)

def test_pipeline_validation(test_executor):
    """Test que la configuración es válida."""
    is_valid = test_executor.validate_pipeline("my_pipeline")
    assert is_valid

def test_pipeline_execution(test_executor):
    """Test que el pipeline se ejecuta correctamente."""
    result = test_executor.execute("my_pipeline")
    
    assert result.success
    assert result.nodes_executed > 0
    assert "load_data" in result.completed_nodes

def test_pipeline_with_dates(test_executor):
    """Test con rango de fechas."""
    result = test_executor.execute(
        "my_pipeline",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    assert result.success
    assert result.metrics.get('records_processed', 0) > 0

def test_pipeline_error_handling(test_executor):
    """Test de manejo de errores."""
    # Pipeline que debería fallar
    result = test_executor.execute("invalid_pipeline")
    
    assert not result.success
    assert result.error_message is not None
```

**Ejecutar tests:**
```bash
pytest test_pipeline.py -v
```

---

## 📊 Monitoreo y Métricas

### Antes: Logs en Archivos

```bash
tauro --env production --pipeline daily_etl >> /var/log/tauro.log 2>&1
```

### Después: Métricas Programáticas

```python
from tauro import PipelineExecutor, ContextLoader
from prometheus_client import Counter, Histogram, start_http_server
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Métricas Prometheus
pipeline_counter = Counter(
    'tauro_pipeline_executions_total',
    'Total executions',
    ['pipeline', 'status']
)

execution_time = Histogram(
    'tauro_execution_duration_seconds',
    'Execution duration',
    ['pipeline']
)

def run_with_metrics(pipeline_name: str):
    """Ejecuta pipeline con métricas."""
    context = ContextLoader().load_from_env("production")
    executor = PipelineExecutor(context)
    
    # Medir tiempo
    with execution_time.labels(pipeline=pipeline_name).time():
        result = executor.execute(pipeline_name)
    
    # Incrementar contador
    status = 'success' if result.success else 'failed'
    pipeline_counter.labels(
        pipeline=pipeline_name,
        status=status
    ).inc()
    
    # Log estructurado
    logging.info(
        "Pipeline execution",
        extra={
            'pipeline': pipeline_name,
            'success': result.success,
            'nodes': result.nodes_executed,
            'duration': result.execution_time_seconds,
            'metrics': result.metrics
        }
    )
    
    return result

# Exponer métricas
start_http_server(8000)

# Ejecutar
result = run_with_metrics("daily_etl")
```

---

## 🌐 API REST

### Crear API desde CLI

**Antes: Wrapper Bash + API simple:**
```bash
# api.sh - Script que expone endpoints
curl -X POST /run -d '{"pipeline":"etl"}' | \
  xargs -I {} tauro --env production --pipeline {}
```

**Después: FastAPI Completo:**
```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from tauro import PipelineExecutor, ContextLoader

app = FastAPI(title="Tauro Pipeline API")

class PipelineRequest(BaseModel):
    pipeline_name: str
    env: str = "production"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class PipelineResponse(BaseModel):
    success: bool
    execution_id: str
    nodes_executed: int
    execution_time: float
    metrics: dict

@app.post("/pipelines/execute", response_model=PipelineResponse)
async def execute_pipeline(request: PipelineRequest):
    try:
        context = ContextLoader().load_from_env(request.env)
        executor = PipelineExecutor(context)
        
        result = executor.execute(
            request.pipeline_name,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        return PipelineResponse(
            success=result.success,
            execution_id=result.execution_id,
            nodes_executed=result.nodes_executed,
            execution_time=result.execution_time_seconds,
            metrics=result.metrics
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipelines/list")
async def list_pipelines(env: str = "production"):
    context = ContextLoader().load_from_env(env)
    executor = PipelineExecutor(context)
    pipelines = executor.list_pipelines()
    
    return {"pipelines": pipelines}

# Ejecutar: uvicorn api:app --reload
```

---

## 🎓 Mejores Prácticas de Migración

### 1. Migración Gradual

**Fase 1: Wrapper Python**
```python
# wrapper.py - Mantén CLI pero usa Python
import subprocess
import sys

def run_cli_pipeline(pipeline: str, env: str):
    """Wrapper temporal que llama CLI."""
    result = subprocess.run(
        ["tauro", "--env", env, "--pipeline", pipeline],
        capture_output=True,
        text=True
    )
    return result.returncode == 0

# Usar
success = run_cli_pipeline("daily_etl", "production")
```

**Fase 2: API Híbrida**
```python
# hybrid.py - Combina CLI y API
from tauro import PipelineExecutor, ContextLoader
import subprocess

def run_pipeline(pipeline: str, env: str, use_api: bool = True):
    """Permite elegir entre CLI y API."""
    if use_api:
        # Usar API
        context = ContextLoader().load_from_env(env)
        executor = PipelineExecutor(context)
        result = executor.execute(pipeline)
        return result.success
    else:
        # Usar CLI (legacy)
        result = subprocess.run(
            ["tauro", "--env", env, "--pipeline", pipeline],
            capture_output=True
        )
        return result.returncode == 0
```

**Fase 3: API Completa**
```python
# final.py - Solo API
from tauro import PipelineExecutor, ContextLoader

def run_pipeline(pipeline: str, env: str):
    """Implementación final usando solo API."""
    context = ContextLoader().load_from_env(env)
    executor = PipelineExecutor(context)
    result = executor.execute(pipeline)
    return result
```

### 2. Manejo de Configuración

**Opción 1: Mismo archivo de config**
```python
# Usa los mismos archivos YAML/JSON que CLI
from tauro import ContextLoader

context = ContextLoader().load_from_env("production")
# Lee ./config/production/settings.yaml automáticamente
```

**Opción 2: Config programática**
```python
# Override configuración en código
from tauro import ContextLoader

context = ContextLoader().load_from_env("production")

# Modificar settings en runtime
context.config['spark_config']['spark.executor.memory'] = '8g'
context.config['max_retries'] = 5
```

### 3. Error Handling Robusto

```python
from tauro import PipelineExecutor, ContextLoader
from tauro.core.cli.core import (
    ExecutionError,
    ValidationError,
    ConfigurationError
)
import logging

logger = logging.getLogger(__name__)

def run_pipeline_safe(pipeline: str, env: str):
    """Ejecución con manejo de errores completo."""
    try:
        context = ContextLoader().load_from_env(env)
        executor = PipelineExecutor(context)
        result = executor.execute(pipeline)
        
        if result.success:
            logger.info(
                f"Pipeline {pipeline} completed successfully",
                extra={'metrics': result.metrics}
            )
            return result
        else:
            logger.error(
                f"Pipeline {pipeline} failed at node {result.failed_node}",
                extra={'error': result.error_message}
            )
            return result
            
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        raise
    except ExecutionError as e:
        logger.error(f"Execution error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise
```

---

## ✅ Checklist de Migración

### Antes de Migrar
- [ ] Documenta todos los comandos CLI actuales
- [ ] Identifica dependencias y configuraciones
- [ ] Revisa logs y métricas actuales
- [ ] Planifica estrategia de testing

### Durante la Migración
- [ ] Crea entorno de test aislado
- [ ] Implementa versión programática de cada comando
- [ ] Agrega tests automatizados
- [ ] Valida métricas y logs
- [ ] Documenta cambios

### Después de Migrar
- [ ] Ejecuta ambas versiones en paralelo (A/B testing)
- [ ] Compara resultados y métricas
- [ ] Actualiza documentación
- [ ] Entrena al equipo
- [ ] Depreca versión CLI gradualmente

---

## 🆘 Troubleshooting

### Problema: "Module not found"

```python
# Solución: Asegúrate de instalar tauro
pip install tauro

# O desde código fuente
pip install -e /path/to/tauro
```

### Problema: "Config file not found"

```python
from tauro import ConfigDiscovery
from pathlib import Path

# Debug: Ver qué archivos encuentra
discovery = ConfigDiscovery()
configs = discovery.discover(Path("./config"), env="dev")
print(f"Configs encontrados: {configs}")
```

### Problema: "Context initialization failed"

```python
# Solución: Inicializa con config explícito
from tauro import ContextLoader

try:
    context = ContextLoader().load_from_config("./config/settings.yaml")
except Exception as e:
    print(f"Error loading config: {e}")
    # Fallback a config por defecto
    context = ContextLoader().load_from_dict({
        'project_name': 'my_project',
        'pipelines': {...}
    })
```

---

