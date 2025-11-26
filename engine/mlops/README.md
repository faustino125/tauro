# MLOps Layer

Capa MLOps integrada en Tauro para **Model Registry** y **Experiment Tracking**, con soporte dual para almacenamiento local (Parquet) y Databricks Unity Catalog.

## 🎯 Filosofía: Invisible hasta que se necesite

MLOps en Tauro está diseñado para ser:
- **Zero-config para ETL**: No interfiere con pipelines de solo datos
- **Auto-activado para ML**: Detecta automáticamente nodos ML
- **Progresivamente complejo**: Configuración simple por defecto, control fino cuando se necesita

---

## Características

### Model Registry
- ✅ Versionado automático de modelos
- ✅ Metadatos estructurados (framework, hiperparámetros, métricas)
- ✅ Almacenamiento de artefactos (sklearn, XGBoost, PyTorch, etc.)
- ✅ Gestión del ciclo de vida (Staging → Production → Archived)
- ✅ Búsqueda por nombre, versión y etapa
- ✅ Tags y anotaciones

### Experiment Tracking
- ✅ Creación de experimentos y runs
- ✅ Logging de métricas (con timestamps y steps)
- ✅ Logging de hiperparámetros
- ✅ Almacenamiento de artefactos por run
- ✅ Comparación de runs (DataFrame)
- ✅ Búsqueda de runs por métricas
- ✅ Soporte para runs anidados (parent-child)

### Backends
- ✅ **Local**: Almacenamiento en Parquet (sin dependencias externas)
- ✅ **Databricks**: Unity Catalog (con databricks-sql-connector)

### Integración con Exec
- ✅ **Auto-detection**: Detecta nodos ML automáticamente
- ✅ **Lazy initialization**: Solo se carga si hay nodos ML
- ✅ **Factory pattern**: Auto-configura backend (local/Databricks)
- ✅ **ml_info.yaml**: Configuración ML centralizada (opcional)

---

## Quick Start

### Instalación

```bash
pip install pandas loguru pyarrow

# Para Databricks (opcional)
pip install databricks-sql-connector
```

### Uso Básico

#### 1. Inicializar MLOps Context

**Opción A: Desde Tauro Context (RECOMENDADO - Auto mode detection)**

```python
from tauro.core.config import Context
from tauro.core.mlops.config import MLOpsContext

# Crear context de Tauro
context = Context(
    global_settings="config/global_settings.yaml",
    pipelines_config="config/pipelines.yaml",
    nodes_config="config/nodes.yaml",
    input_config="config/input.yaml",
    output_config="config/output.yaml",
)

# MLOps auto-detecta modo (local/databricks) desde context
mlops = MLOpsContext.from_context(context)
# ✅ Auto-configura backend basado en execution_mode
# ✅ Usa configuración de global_settings
# ✅ Soporta P1 features (buffering, locking, etc.)
```

**Opción B: Manual (Para uso standalone)**

```python
from tauro.core.mlops.config import MLOpsContext

# Local backend explícito
ctx = MLOpsContext(
    backend_type="local",
    storage_path="./mlops_data"
)

# Databricks backend explícito
ctx = MLOpsContext(
    backend_type="databricks",
    databricks_catalog="main",
    databricks_schema="ml_tracking",
)
```

**Opción C: Desde variables de entorno (DEPRECATED)**

```python
# ⚠️ DEPRECATED: Use from_context() for auto mode detection
ctx = MLOpsContext.from_env()
```

#### 2. Model Registry

```python
registry = ctx.model_registry

# Registrar modelo
model_v1 = registry.register_model(
    name="credit_risk_model",
    artifact_path="/path/to/model.pkl",
    artifact_type="sklearn",
    framework="scikit-learn",
    hyperparameters={"n_estimators": 100, "max_depth": 10},
    metrics={"accuracy": 0.92, "auc": 0.95},
    tags={"team": "ds", "project": "credit"}
)

# Listar modelos
models = registry.list_models()

# Obtener versión específica
model = registry.get_model_version("credit_risk_model", version=1)

# Promover a producción
registry.promote_model("credit_risk_model", 1, ModelStage.PRODUCTION)

# Descargar artefacto
registry.download_artifact("credit_risk_model", None, "/local/path")
```

#### 3. Experiment Tracking

```python
tracker = ctx.experiment_tracker

# Crear experimento
exp = tracker.create_experiment(
    name="model_tuning_v1",
    description="Hyperparameter tuning",
    tags={"team": "ds"}
)

# Iniciar run
run = tracker.start_run(
    exp.experiment_id,
    name="trial_1",
    parameters={"lr": 0.01, "batch_size": 32}
)

# Loguear métricas
for epoch in range(10):
    tracker.log_metric(run.run_id, "loss", 0.5 - epoch * 0.05, step=epoch)
    tracker.log_metric(run.run_id, "accuracy", 0.7 + epoch * 0.03, step=epoch)

# Loguear artefactos
tracker.log_artifact(run.run_id, "/path/to/model.pkl")

# Terminar run
tracker.end_run(run.run_id, RunStatus.COMPLETED)

# Buscar runs
matching_runs = tracker.search_runs(
    exp.experiment_id,
    metric_filter={"accuracy": (">", 0.85)}
)

# Comparar runs
comparison_df = tracker.compare_runs([run1.run_id, run2.run_id])
```

---

## 📦 Configuración con ml_info.yaml

Para proyectos ML complejos, puedes centralizar la configuración en `ml_info.yaml`:

```yaml
# config/ml_info.yaml
mlops:
  enabled: true
  backend: "databricks"
  experiment:
    name: "customer-churn-prediction"
    description: "Modelo de abandono de clientes"
  model_registry:
    catalog: "main"
    schema: "ml_models"
  tracking:
    catalog: "main"
    schema: "ml_experiments"
  auto_log: true
```

### Precedencia de Configuración

La configuración MLOps sigue esta jerarquía (de mayor a menor prioridad):

1. **Node config** (`nodes.yaml` - específico del nodo)
2. **Pipeline config** (`pipelines.yaml` - nivel pipeline)
3. **ml_info.yaml** (configuración ML centralizada)
4. **Global settings** (`global_settings.yaml`)
5. **Auto-defaults** (valores por defecto inteligentes)

**Ejemplo de uso combinado:**

```yaml
# config/nodes.yaml
nodes:
  train_model:
    type: "ml_training"
    config:
      mlops:
        experiment_name: "xgboost-tuning"  # ← Override solo esto
        # Resto hereda de ml_info.yaml o global_settings
```

**Ventajas:**
- ✅ Un solo lugar para configuración ML común
- ✅ Override selectivo a nivel pipeline/nodo
- ✅ Separación clara entre config ML y config datos
- ✅ Reusabilidad entre pipelines ML

**Ver más:**
- [SIMPLIFICATION_PROPOSAL.md](../../../SIMPLIFICATION_PROPOSAL.md) - Diseño completo
- [MLOPS_SIMPLE_GUIDE.md](../../../MLOPS_SIMPLE_GUIDE.md) - Guía rápida con ejemplos

---

## Arquitectura

```
tauro/core/mlops/
├── __init__.py              # Public API
├── storage.py               # Storage backends (Local, Databricks)
├── model_registry.py        # Model Registry implementation
├── experiment_tracking.py   # Experiment Tracking implementation
├── config.py                # MLOpsContext y configuración
├── example.py               # Ejemplos de uso
└── README.md                # Esta documentación

tauro/core/exec/
├── mlops_auto_config.py     # Auto-detection y config merge
└── executor.py              # Lazy initialization en BaseExecutor
```

### Componentes Clave

1. **StorageBackend** (`storage.py`):
   - Abstracción para local (Parquet) y Databricks (Unity Catalog)
   - API unificada: write_dataframe, read_dataframe, write_json, etc.

2. **ModelRegistry** (`model_registry.py`):
   - Versionado de modelos
   - Lifecycle management (Staging/Production/Archived)
   - Metadatos y artefactos

3. **ExperimentTracker** (`experiment_tracking.py`):
   - Experiments y runs
   - Métricas, hiperparámetros, artefactos
   - Comparación de runs

4. **MLOpsContext** (`config.py`):
   - Factory para backend selection
   - Configuración centralizada
   - from_context() para auto mode detection

5. **MLOpsAutoConfigurator** (`tauro/core/exec/mlops_auto_config.py`):
   - Detecta automáticamente nodos ML (patterns)
   - Genera configuración por defecto inteligente
   - Merge jerárquico: node → pipeline → ml_info → global → auto

6. **BaseExecutor Integration** (`tauro/core/exec/executor.py`):
   - Lazy initialization: solo carga si hay nodos ML
   - Property `mlops_context`: acceso on-demand
   - Auto-skip para pipelines ETL puros

### Storage Backend Abstraction

Todos los componentes usan una abstracción `StorageBackend`:

```python
class StorageBackend(ABC):
    def write_dataframe(df, path) -> StorageMetadata
    def read_dataframe(path) -> pd.DataFrame
    def write_json(data, path) -> StorageMetadata
    def read_json(path) -> Dict
    def write_artifact(src, dest) -> StorageMetadata
    def read_artifact(src, dest_local) -> None
    def exists(path) -> bool
    def list_paths(prefix) -> List[str]
    def delete(path) -> None
```

**LocalStorageBackend**: Usa Parquet para DataFrames, JSON para metadatos, archivos nativos para artefactos.

**DatabricksStorageBackend**: Integración con Unity Catalog (requiere API adicional para escritura).

---

## Estructura de Datos

### Model Registry

```
model_registry/
├── models/
│   ├── index.parquet                    # Índice de modelos
│   └── .registry_marker.json
├── metadata/
│   └── {model_id}/
│       ├── v1.json                      # Metadata v1
│       ├── v2.json                      # Metadata v2
│       └── ...
└── artifacts/
    └── {model_id}/
        ├── v1/                          # Artefactos v1
        ├── v2/                          # Artefactos v2
        └── ...
```

### Experiment Tracking

```
experiment_tracking/
├── experiments/
│   ├── index.parquet                    # Índice de experimentos
│   ├── {exp_id}.json                    # Metadata experimento
│   └── ...
├── runs/
│   └── {exp_id}/
│       ├── index.parquet                # Índice de runs
│       ├── {run_id}.json                # Metadata run
│       └── ...
└── artifacts/
    └── {run_id}/                        # Artefactos del run
        ├── model.pkl
        ├── predictions.parquet
        └── ...
```

---

## Configuración

### Variables de Entorno

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

### Inicialización Programática

```python
# Local
ctx = MLOpsContext(
    backend_type="local",
    storage_path="./mlops_data"
)

# Databricks
ctx = MLOpsContext(
    backend_type="databricks",
    databricks_catalog="my_catalog",
    databricks_schema="mlops",
    databricks_workspace_url="https://...",
    databricks_token="dapi..."
)
```

---

## API Reference

### ModelRegistry

#### `register_model()`
```python
def register_model(
    name: str,
    artifact_path: str,
    artifact_type: str,
    framework: str,
    description: str = "",
    hyperparameters: Dict = None,
    metrics: Dict = None,
    tags: Dict = None,
    input_schema: Dict = None,
    output_schema: Dict = None,
    dependencies: List = None,
    experiment_run_id: str = None,
) -> ModelVersion
```

Registra un nuevo modelo o versión. Incrementa automáticamente el número de versión si el modelo ya existe.

#### `get_model_version()`
```python
def get_model_version(
    name: str,
    version: int = None,
) -> ModelVersion
```

Obtiene una versión específica (o la última si `version=None`).

#### `list_models()` → `List[Dict]`

Lista todos los modelos con su versión más reciente.

#### `list_model_versions()` → `List[Dict]`

Lista todas las versiones de un modelo.

#### `promote_model()`
```python
def promote_model(
    name: str,
    version: int,
    stage: ModelStage
) -> ModelVersion
```

Promueve modelo a Staging, Production o Archived.

#### `download_artifact()`
```python
def download_artifact(
    name: str,
    version: int,
    local_destination: str
) -> None
```

Descarga artefacto del modelo a ruta local.

---

### ExperimentTracker

#### `create_experiment()`
```python
def create_experiment(
    name: str,
    description: str = "",
    tags: Dict = None,
) -> Experiment
```

Crea nuevo experimento.

#### `start_run()`
```python
def start_run(
    experiment_id: str,
    name: str = "",
    parameters: Dict = None,
    tags: Dict = None,
    parent_run_id: str = None,
) -> Run
```

Inicia nuevo run (se mantiene en memoria hasta `end_run()`).

#### `log_metric()`
```python
def log_metric(
    run_id: str,
    key: str,
    value: float,
    step: int = 0,
    metadata: Dict = None,
) -> None
```

Loguea métrica para run (ej: loss, accuracy).

#### `log_parameter()`
```python
def log_parameter(
    run_id: str,
    key: str,
    value: Any,
) -> None
```

Loguea hiperparámetro.

#### `log_artifact()`
```python
def log_artifact(
    run_id: str,
    artifact_path: str,
    destination: str = "",
) -> str
```

Loguea artefacto (archivo o directorio). Retorna URI en storage.

#### `end_run()`
```python
def end_run(
    run_id: str,
    status: RunStatus = RunStatus.COMPLETED,
) -> Run
```

Termina run y persiste a storage.

#### `get_run()` → `Run`

Obtiene run por ID (activo o persistido).

#### `list_runs()` → `List[Dict]`
```python
def list_runs(
    experiment_id: str,
    status_filter: RunStatus = None,
    tag_filter: Dict = None,
) -> List[Dict]
```

Lista runs en experimento con filtros opcionales.

#### `compare_runs()` → `pd.DataFrame`
```python
def compare_runs(
    run_ids: List[str]
) -> pd.DataFrame
```

Compara múltiples runs como DataFrame (columnas = métricas/parámetros).

#### `search_runs()` → `List[str]`
```python
def search_runs(
    experiment_id: str,
    metric_filter: Dict = None,  # {"metric": (">", threshold)}
) -> List[str]
```

Busca runs que cumplen condiciones de métricas.

---

## Ejemplos Completos

### Entrenamiento de Modelo

```python
from tauro.core.mlops.config import MLOpsContext
from tauro.core.mlops.experiment_tracking import RunStatus
import pickle

ctx = MLOpsContext(backend_type="local", storage_path="./mlops")
tracker = ctx.experiment_tracker
registry = ctx.model_registry

# Crear experimento
exp = tracker.create_experiment("xgboost_tuning")

# Trial 1
run1 = tracker.start_run(
    exp.experiment_id,
    name="trial_1",
    parameters={"depth": 5, "lr": 0.1, "n_estimators": 100}
)

# Entrenar y loguear
model1 = train_model(depth=5, lr=0.1, n_estimators=100)
for epoch, metrics in training_loop(model1, train_data):
    tracker.log_metric(run1.run_id, "train_loss", metrics["loss"], step=epoch)
    tracker.log_metric(run1.run_id, "train_auc", metrics["auc"], step=epoch)

# Guardar y loguear artefacto
with open("model_trial1.pkl", "wb") as f:
    pickle.dump(model1, f)
tracker.log_artifact(run1.run_id, "model_trial1.pkl")

# Evaluar
eval_metrics = evaluate(model1, test_data)
tracker.log_metric(run1.run_id, "test_auc", eval_metrics["auc"], step=0)
tracker.log_metric(run1.run_id, "test_accuracy", eval_metrics["accuracy"], step=0)

tracker.end_run(run1.run_id, RunStatus.COMPLETED)

# Trial 2 (mejor config)
run2 = tracker.start_run(
    exp.experiment_id,
    name="trial_2",
    parameters={"depth": 8, "lr": 0.05, "n_estimators": 200}
)
# ... similar logging ...

# Comparar y elegir mejor
comparison = tracker.compare_runs([run1.run_id, run2.run_id])
print(comparison)

best_run_id = run2.run_id
best_run = tracker.get_run(best_run_id)

# Registrar en Model Registry
registry.register_model(
    name="xgboost_classifier",
    artifact_path="model_trial2.pkl",
    artifact_type="xgboost",
    framework="xgboost",
    hyperparameters=best_run.parameters,
    metrics={"test_auc": 0.97, "test_accuracy": 0.91},
    experiment_run_id=best_run_id,
)

# Promover a producción
registry.promote_model("xgboost_classifier", 1, ModelStage.PRODUCTION)
```

---

## 🎓 Resumen: Tres Formas de Usar MLOps

### 1. ETL Pipeline (Sin MLOps)
```yaml
# config/nodes.yaml
nodes:
  load_data:
    function: "etl.load_csv"
  transform:
    function: "etl.clean_data"
# ✅ MLOps auto-deshabilitado → Sin overhead
```

### 2. ML Pipeline Simple (Auto todo)
```yaml
# config/nodes.yaml
nodes:
  train_model:  # ← AUTO-DETECTADO
    function: "ml.train_xgboost"
# ✅ MLOps auto-habilitado
# ✅ Backend desde global_settings
# ✅ Experiment tracking automático
```

### 3. ML Production (ml_info.yaml)
```yaml
# config/ml_info.yaml
mlops:
  enabled: true
  backend: "databricks"
  experiment:
    name: "production-model"
  model_registry:
    catalog: "main"
    schema: "ml_models"

# config/nodes.yaml
nodes:
  train_model:
    function: "ml.train_xgboost"
    mlops:
      experiment_name: "xgboost-v2"  # Override selectivo
# ✅ Configuración centralizada
# ✅ Override granular
# ✅ Reusabilidad entre pipelines
```

## Integración con Spark/Databricks

Para escribir en Unity Catalog desde Spark:

```python
# En Databricks notebook
spark.createDataFrame(
    comparison_df
).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("catalog.schema.run_comparison")
```

---

## Limitaciones Actuales

1. **DatabricksStorageBackend**: Actualmente es una integración parcial. Para operaciones de lectura/escritura en UC se recomienda usar Spark API directamente.
2. **Métricas**: Se almacenan en memoria durante run y se persisten al terminar.
3. **Runs anidados**: Soportados pero sin validación de ciclos.
4. **Concurrencia**: No hay mecanismo de locking para escribura concurrente.

---

## Roadmap

- [ ] Integración completa con Databricks UC (volumes)
- [ ] Métricas incrementales (sin cargar todo en memoria)
- [ ] Validación de esquemas (input/output)
- [ ] Modelo Registry API HTTP
- [ ] UI Web para visualización
- [ ] Integración con MLflow

---

## Desarrollo

Ejecutar ejemplos:

```bash
cd tauro/core/mlops
python example.py
```

---

## License

MIT - Ver LICENSE en raíz del proyecto.
