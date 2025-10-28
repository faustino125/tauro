# Tauro Orchest

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Un sistema de orquestación de pipelines robusto y resiliente para Tauro, diseñado para ejecutar flujos de trabajo complejos con alta disponibilidad y tolerancia a fallos.

## 🚀 Características Principales

- **Ejecución Resiliente**: Circuit Breaker, Bulkhead y políticas de reintentos
- **Programación Avanzada**: Soporte para intervalos y expresiones cron
- **Persistencia Robusta**: PostgreSQL con connection pooling y migraciones automáticas
- **Monitoreo Completo**: Métricas detalladas y health checks
- **CLI Completa**: Interfaz de línea de comandos para todas las operaciones
- **Ejecución Concurrente**: Pools de workers dinámicos con control de concurrencia
- **Manejo de Dependencias**: Resolución automática de dependencias en DAGs

## 📋 Tabla de Contenidos

- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso Básico](#uso-básico)
- [Ejemplos Avanzados](#ejemplos-avanzados)
- [API Reference](#api-reference)
- [CLI Commands](#cli-commands)
- [Monitoreo y Métricas](#monitoreo-y-métricas)
- [Patrones de Resiliencia](#patrones-de-resiliencia)
- [Contribución](#contribución)
- [Licencia](#licencia)

## 📦 Instalación

```bash
# Desde el directorio raíz del proyecto
pip install -e .
```

### Dependencias

- `loguru` - Logging estructurado
- `tenacity` - Reintentos robustos
- `croniter` - Expresiones cron (opcional)

## ⚙️ Configuración

### Variables de Entorno

```bash
# Configuración del entorno
export TAURO_ENV=dev
```

### Configuración de Contexto

```python
from tauro.config.contexts import Context
from tauro.orchest import OrchestratorRunner, OrchestratorStore

# Inicializar contexto
context = Context()

# Crear componentes
store = OrchestratorStore()
runner = OrchestratorRunner(context, store)
```

## 🎯 Uso Básico

### Ejecutar un Pipeline

```python
from tauro.orchest import OrchestratorRunner

# Crear runner
runner = OrchestratorRunner(context)

# Ejecutar pipeline de forma síncrona
run_id = runner.run_pipeline("mi_pipeline", params={"start_date": "2024-01-01"})

# Verificar estado
run = runner.get_run(run_id)
print(f"Estado: {run.state}")
```

### Crear y Ejecutar un Run

```python
# Crear run
pipeline_run = runner.create_run("mi_pipeline", params={"env": "prod"})

# Ejecutar con opciones avanzadas
state = runner.start_run(
    pipeline_run.id,
    retries=3,
    retry_delay_sec=10,
    concurrency=4,
    timeout_seconds=3600
)

print(f"Resultado: {state}")
```

### Programar Ejecuciones

```python
from tauro.orchest import SchedulerService, ScheduleKind

# Crear servicio de scheduler
scheduler = SchedulerService(context)

# Crear schedule cada 30 minutos
schedule = store.create_schedule(
    pipeline_id="mi_pipeline",
    kind=ScheduleKind.INTERVAL,
    expression="1800",  # 30 minutos en segundos
    max_concurrency=2,
    retry_policy={"retries": 2, "delay": 60}
)

# Iniciar scheduler
scheduler.start()
```

## 🔧 Ejemplos Avanzados

### Pipeline con Circuit Breaker

```python
from tauro.orchest.resilience import CircuitBreakerConfig

# Configurar runner con circuit breaker
runner = OrchestratorRunner(
    context,
    enable_circuit_breaker=True,
    circuit_breaker_config=CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=2,
        timeout_seconds=300.0
    )
)

# El runner automáticamente protegerá contra fallos en cascada
run_id = runner.run_pipeline("pipeline_critico")
```

### Monitoreo de Salud

```python
# Verificar estado del sistema
health = runner.get_health_status()
print(f"Runner saludable: {health['healthy']}")

if not health['healthy']:
    for issue in health['issues']:
        print(f"Problema: {issue}")

# Ver métricas detalladas
metrics = runner.get_metrics()
print(f"Ejecuciones exitosas: {metrics['successful_runs']}")
print(f"Tasa de éxito reciente: {metrics['recent_success_rate']:.1%}")
```

### Limpieza de Datos Antiguos

```python
from tauro.orchest import OrchestratorStore

store = OrchestratorStore()

# Limpiar datos mayores a 30 días
result = store.cleanup_old_data(max_days=30)
print(f"Registros eliminados: {result}")

# Optimizar base de datos
store.vacuum()
```

### Uso con Context Managers

```python
from tauro.orchest import OrchestratorStore

# El store maneja conexiones automáticamente
with OrchestratorStore() as store:
    runs = store.list_pipeline_runs(limit=10)
    for run in runs:
        print(f"{run.id}: {run.state}")
```

## 📚 API Reference

### OrchestratorRunner

```python
class OrchestratorRunner:
    def __init__(
        self,
        context: Context,
        store: Optional[OrchestratorStore] = None,
        max_workers: Optional[int] = None,
        enable_circuit_breaker: bool = True,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    )

    def create_run(self, pipeline_id: str, params: Optional[Dict] = None) -> PipelineRun
    def start_run(self, run_id: str, **kwargs) -> RunState
    def run_pipeline(self, pipeline_id: str, params: Optional[Dict] = None, **kwargs) -> str
    def cancel_run(self, run_id: str) -> bool
    def get_run(self, run_id: str) -> Optional[PipelineRun]
    def list_runs(self, **filters) -> List[PipelineRun]
    def list_task_runs(self, run_id: str, **filters) -> List[TaskRun]
    def get_metrics(self) -> Dict[str, Any]
    def get_health_status(self) -> Dict[str, Any]
    def shutdown(self, wait: bool = True) -> None
```

### OrchestratorStore

```python
class OrchestratorStore:
    def __init__(self, context: Optional[Any] = None, **kwargs)

    # Pipeline Runs
    def create_pipeline_run(self, pipeline_id: str, params: Optional[Dict] = None) -> PipelineRun
    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]
    def update_pipeline_run_state(self, run_id: str, new_state: RunState, **kwargs) -> None
    def list_pipeline_runs(self, **filters) -> List[PipelineRun]

    # Task Runs
    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun
    def update_task_run_state(self, task_run_id: str, new_state: RunState, **kwargs) -> None
    def list_task_runs(self, pipeline_run_id: str, **filters) -> List[TaskRun]

    # Schedules
    def create_schedule(self, pipeline_id: str, kind: ScheduleKind, expression: str, **kwargs) -> Schedule
    def list_schedules(self, **filters) -> List[Schedule]
    def update_schedule(self, schedule_id: str, **fields) -> None

    # Utilidades
    def get_database_stats(self) -> Dict[str, Any]
    def cleanup_old_data(self, max_days: int = 30) -> Dict[str, int]
    def vacuum(self) -> None
```

### SchedulerService

```python
class SchedulerService:
    def __init__(self, context: Context, store: Optional[OrchestratorStore] = None, **kwargs)

    def start(self, poll_interval: float = 1.0) -> None
    def stop(self, timeout: Optional[float] = 30.0) -> None
    def backfill(self, pipeline_id: str, count: int) -> None
    def get_metrics(self) -> Dict[str, Any]
    def get_health_status(self) -> Dict[str, Any]
```

## 🖥️ CLI Commands

### Gestión de Runs

```bash
# Crear un run
tauro-orchestrator --env dev --command run-create --pipeline mi_pipeline

# Ejecutar un run
tauro-orchestrator --env dev --command run-start --run-id <run_id> --retries 3

# Ver estado de un run
tauro-orchestrator --env dev --command run-status --run-id <run_id>

# Listar runs
tauro-orchestrator --env dev --command run-list --pipeline mi_pipeline

# Ver tareas de un run
tauro-orchestrator --env dev --command run-tasks --run-id <run_id>

# Cancelar un run
tauro-orchestrator --env dev --command run-cancel --run-id <run_id>
```

### Gestión de Schedules

```bash
# Crear schedule
tauro-orchestrator --env dev --command schedule-add \
    --pipeline mi_pipeline \
    --schedule-kind INTERVAL \
    --expression 3600 \
    --max-concurrency 2

# Listar schedules
tauro-orchestrator --env dev --command schedule-list

# Iniciar scheduler
tauro-orchestrator --env dev --command schedule-start

# Backfill (ejecutar múltiples runs históricos)
tauro-orchestrator --env dev --command backfill --pipeline mi_pipeline --count 10
```

### Gestión de Base de Datos

```bash
# Ver estadísticas
tauro-orchestrator --env dev --command db-stats

# Limpiar datos antiguos
tauro-orchestrator --env dev --command db-cleanup --days 30

# Optimizar base de datos
tauro-orchestrator --env dev --command db-vacuum
```

## 📊 Monitoreo y Métricas

### Métricas del Runner

```python
metrics = runner.get_metrics()

# Métricas generales
print(f"Total de runs: {metrics['total_runs']}")
print(f"Éxito: {metrics['successful_runs']}")
print(f"Fallidos: {metrics['failed_runs']}")
print(f"Cancelados: {metrics['cancelled_runs']}")

# Rendimiento
print(f"Tiempo promedio: {metrics['avg_execution_time']:.2f}s")
print(f"Tiempo máximo: {metrics['max_execution_time']:.2f}s")

# Estado actual
print(f"Runs activos: {metrics['active_runs']}")
print(f"Runs en cola: {metrics['queued_runs']}")

# Circuit Breaker
if 'circuit_breaker' in metrics:
    cb = metrics['circuit_breaker']
    print(f"Estado CB: {cb['state']}")
    print(f"Fallas: {cb['failure_count']}")

# Bulkhead
if 'bulkhead' in metrics:
    bh = metrics['bulkhead']
    print(f"Concurrentes: {bh['concurrent_calls']}/{bh['max_concurrent_calls']}")
```

### Health Checks

```python
health = runner.get_health_status()

print(f"Saludable: {health['healthy']}")
print(f"Timestamp: {health['timestamp']}")

if health['issues']:
    print("Problemas:")
    for issue in health['issues']:
        print(f"  - {issue}")

if health['warnings']:
    print("Advertencias:")
    for warning in health['warnings']:
        print(f"  - {warning}")
```

## 🛡️ Patrones de Resiliencia

### Circuit Breaker

```python
from tauro.orchest.resilience import CircuitBreaker, CircuitBreakerConfig

# Configurar circuit breaker
config = CircuitBreakerConfig(
    failure_threshold=5,      # Abrir después de 5 fallos
    success_threshold=2,      # Cerrar después de 2 éxitos
    timeout_seconds=300.0     # Esperar 5 minutos antes de intentar
)

breaker = CircuitBreaker("mi_servicio", config)

# Usar en función
@breaker.call
def operacion_critica():
    # Tu lógica aquí
    pass
```

### Bulkhead

```python
from tauro.orchest.resilience import Bulkhead

# Crear bulkhead para limitar concurrencia
bulkhead = Bulkhead("operaciones_db", max_concurrent_calls=10)

# Ejecutar con aislamiento
result = bulkhead.execute(mi_funcion_db, arg1, arg2)
```

### Retry Policy

```python
from tauro.orchest.resilience import RetryPolicy

# Política de reintentos
policy = RetryPolicy(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=True
)

# Ejecutar con reintentos
result = policy.execute(funcion_fallible)
```


### Guías de Desarrollo

- Usa type hints en todas las funciones públicas
- Agrega docstrings comprehensivos
- Incluye tests para nueva funcionalidad
- Sigue el estilo de código existente
- Actualiza documentación según corresponda

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia Apache 2.0 - ver el archivo [LICENSE](LICENSE) para más detalles.

## 🆘 Soporte

Para soporte técnico o preguntas:
- Crea un issue en el repositorio
- Revisa la documentación completa en [docs/](../../docs/)
- Contacta al equipo de desarrollo

---

**Última actualización:** Octubre 2025</content>
