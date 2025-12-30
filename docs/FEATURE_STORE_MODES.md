# Feature Store Integration - Modo de Operación Configurable

## Guía de Uso: Feature Store ↔ Virtualization

Esta guía muestra cómo usar el Feature Store de Tauro con los diferentes modos de operación.

---

## 🎯 Modos de Operación

Tauro Feature Store ahora soporta **3 modos configurables**:

### 1. **Materialized** (Por defecto)
- Features pre-calculados y almacenados
- Mejor para: alta velocidad de lectura, features estables
- Trade-off: consume storage, puede estar desactualizado

### 2. **Virtualized**
- Query-on-demand desde capas fuente (Silver/Gold)
- Mejor para: datos frescos, bajo storage
- Trade-off: mayor latencia, carga en fuente

### 3. **Hybrid** (Recomendado)
- Cambio inteligente entre materialized y virtualized
- Mejor para: balance óptimo de rendimiento/frescura
- Decide automáticamente basándose en:
  - Tamaño del dataset
  - Patrones de acceso
  - Complejidad de queries
  - Freshness requirements

---

## 📦 Configuración

### Opción 1: Configuración Global (YAML)

```yaml
# config/base/global_settings.yaml

# Feature Store Configuration
feature_store:
  mode: "hybrid"  # "materialized", "virtualized", or "hybrid"
  
  # Materialized settings
  storage_path: "data/feature_store"
  storage_format: "parquet"  # "parquet", "delta", "orc"
  
  # Virtualized settings
  enable_virtualization: true
  query_executor_type: "spark"  # "spark" or "duckdb"
  register_virtual_tables: true
  virtual_table_prefix: "features_"
  
  # Hybrid mode settings
  hybrid_threshold_rows: 10000  # Materialize if > threshold
  hybrid_cache_ttl: 3600  # Cache decisions for 1 hour
  auto_materialize_on_read: true  # Auto-materialize hot features

# Integration with VirtualDataLayer
virtualization:
  enabled: true
  default_executor: "spark"
  cache_strategy: "smart"
```

### Opción 2: Configuración en Pipeline (YAML)

```yaml
# config/base/pipelines.yaml

gold_feature_store:
  description: "Feature engineering in Gold layer"
  type: "batch"
  nodes:
    - create_customer_features
    - write_features_hybrid
  
  # Feature store específico para este pipeline
  feature_store_config:
    mode: "hybrid"
    hybrid_threshold_rows: 5000
    auto_materialize: true
```

### Opción 3: Configuración por Nodo

```yaml
# config/base/nodes.yaml

write_customer_features:
  type: "feature_store"
  operation: "write"
  params:
    feature_group: "customer_360"
    output_key: "customer_features_df"
    store_mode: "hybrid"  # Override global mode
    enable_virtual_layer: true
    register_virtual_table: true
    schema:
      name: "customer_360"
      description: "Customer 360 features"
      features:
        - name: "total_purchases"
          data_type: "DOUBLE"
          feature_type: "NUMERICAL"
        - name: "avg_ticket"
          data_type: "DOUBLE"
          feature_type: "NUMERICAL"

read_features_for_model:
  type: "feature_store"
  operation: "read"
  params:
    input_key: "model_features"
    feature_names:
      - "customer_360.total_purchases"
      - "customer_360.avg_ticket"
    store_mode: "virtualized"  # Force virtualized for freshness
    point_in_time: "${run_date}"
```

---

## 🚀 Uso Programático

### Ejemplo 1: Modo Materialized

```python
from tauro import Context, FeatureStoreNodeHandler
from core.feature_store import FeatureGroupSchema, FeatureSchema, DataType

# Setup context
context = Context.load("config.yaml")
context.feature_store_mode = "materialized"

# Create handler
handler = FeatureStoreNodeHandler(context, store_mode="materialized")

# Define schema
schema = FeatureGroupSchema(
    name="customer_features",
    description="Customer behavioral features",
    features=[
        FeatureSchema(
            name="total_purchases",
            data_type=DataType.DOUBLE,
            description="Total purchase amount"
        ),
        FeatureSchema(
            name="purchase_frequency",
            data_type=DataType.INT,
            description="Number of purchases"
        ),
    ]
)

# Write features (materialized)
store = handler.materialized_store
store.register_features(schema)
store.write_features(
    feature_group="customer_features",
    data={
        "customer_id": [1, 2, 3],
        "total_purchases": [1000.0, 2500.0, 500.0],
        "purchase_frequency": [10, 25, 5]
    }
)

# Read features (fast, from storage)
features = store.get_features(
    feature_names=["customer_features.total_purchases"],
    entity_ids={"customer_id": [1, 2]}
)
print(features)
```

### Ejemplo 2: Modo Virtualized con VirtualDataLayer

```python
from core.feature_store import VirtualizedFeatureStore, FeatureGroupSchema
from core.virtualization import VirtualDataLayer

# Setup
context = Context.load("config.yaml")
virtual_layer = VirtualDataLayer()

# Create virtualized store
virt_store = VirtualizedFeatureStore(context)
virt_store.set_virtual_layer(virtual_layer)

# Register features with query template
schema = FeatureGroupSchema(
    name="customer_360",
    description="Real-time customer metrics",
    features=[...],
    metadata={
        "query_template": """
            SELECT 
                customer_id,
                SUM(amount) as total_purchases,
                COUNT(*) as purchase_frequency
            FROM gold.transactions
            WHERE created_date >= '{start_date}'
            GROUP BY customer_id
        """
    }
)

virt_store.register_features(schema)

# Register as virtual table
virtual_table = virt_store.register_as_virtual_table(
    feature_group="customer_360",
    table_prefix="features_"
)

# Read features (query-on-demand, always fresh)
features = virt_store.get_features(
    feature_names=["customer_360.total_purchases"],
    entity_ids={"customer_id": [1, 2]},
)

# Query via virtual layer with optimization
optimized_features = virt_store.query_via_virtual_layer(
    feature_group="customer_360",
    features=["total_purchases", "purchase_frequency"],
    predicates=[("customer_id", "IN", [1, 2, 3])]
)
```

### Ejemplo 3: Modo Hybrid (Recomendado)

```python
from core.feature_store import HybridFeatureStore, FeatureStoreConfig, FeatureStoreMode

# Configure hybrid mode
config = FeatureStoreConfig(
    mode=FeatureStoreMode.HYBRID,
    storage_path="data/feature_store",
    storage_format="delta",
    enable_virtualization=True,
    hybrid_threshold_rows=10000,
    auto_materialize_on_read=True,
)

# Create hybrid store
hybrid_store = HybridFeatureStore(context, config)

# Optional: integrate with virtual layer
hybrid_store.set_virtual_layer(virtual_layer)

# Register features
hybrid_store.register_features(schema)

# Write features (goes to materialized)
hybrid_store.write_features(
    feature_group="customer_360",
    data=customer_data
)

# Read features (automatic strategy selection)
features = hybrid_store.get_features(
    feature_names=[
        "customer_360.total_purchases",
        "customer_360.purchase_frequency"
    ],
    entity_ids={"customer_id": [1, 2, 3]}
)
# Strategy is chosen automatically based on:
# - Data size, access patterns, cache availability

# Monitor access patterns
metrics = hybrid_store.get_access_metrics("customer_360")
print(f"Access count: {metrics['access_count']}")
print(f"Cache hit rate: {metrics['cache_hit_rate']:.2%}")
print(f"Avg query time: {metrics['avg_query_time_ms']:.2f}ms")

# Force specific strategy if needed
hybrid_store.force_strategy("customer_360", use_materialized=False)

# Get optimization recommendations
recommendations = hybrid_store.optimize_strategies()
print(recommendations)
# Output: {"customer_360": "MATERIALIZED", "product_features": "VIRTUALIZED"}
```

---

## 🏗️ Arquitectura Medallion con Feature Store

### Pipeline Completo: Bronze → Silver → Gold (Feature Store)

```yaml
# config/base/pipelines.yaml

# 1. Bronze: Raw ingestion (Ingeniería de Datos)
bronze_ingestion:
  description: "Ingest raw data"
  nodes: [load_transactions, load_customers]
  layer: "bronze"

# 2. Silver: Cleaning and enrichment (Ingeniería de Datos)
silver_transformation:
  description: "Clean and enrich data"
  nodes: [clean_transactions, enrich_customers]
  dependencies: [bronze_ingestion]
  layer: "silver"

# 3. Gold: Feature Store (Científicos de Datos)
gold_feature_store:
  description: "Feature engineering for ML"
  nodes: [create_features, write_to_feature_store]
  dependencies: [silver_transformation]
  layer: "gold"
  feature_store_config:
    mode: "hybrid"
    enable_virtualization: true

# 4. ML: Model training (Acceso virtualizado)
ml_training:
  description: "Train ML model with virtualized features"
  nodes: [read_features, train_model, evaluate]
  dependencies: [gold_feature_store]
  feature_store_config:
    mode: "virtualized"  # Always fresh for training
```

```yaml
# config/base/nodes.yaml

# GOLD LAYER: Feature Engineering
create_features:
  module: "pipelines.features"
  function: "create_customer_features"
  input: ["silver.customers", "silver.transactions"]
  output: ["customer_features_df"]

write_to_feature_store:
  type: "feature_store"
  operation: "write"
  params:
    feature_group: "customer_360"
    output_key: "customer_features_df"
    store_mode: "hybrid"
    enable_virtual_layer: true
    schema:
      name: "customer_360"
      features: [...]

# ML PIPELINE: Feature Retrieval
read_features:
  type: "feature_store"
  operation: "read"
  params:
    input_key: "training_features"
    feature_names:
      - "customer_360.total_purchases"
      - "customer_360.avg_ticket"
      - "customer_360.purchase_frequency"
    store_mode: "virtualized"  # Fresh data for training
    point_in_time: "${training_date}"
```

---

## 🔄 Flujo de Trabajo Completo

### Escenario Real: Equipo de Data Science + Data Engineering

```python
# === DATA ENGINEERING: Bronze → Silver → Gold ===

from tauro import PipelineExecutor, ContextLoader

# Load context
context = ContextLoader().load_from_env("production")
executor = PipelineExecutor(context)

# Run data engineering pipelines
print("Running Bronze ingestion...")
executor.execute("bronze_ingestion")

print("Running Silver transformation...")
executor.execute("silver_transformation")

print("Running Gold feature store...")
executor.execute("gold_feature_store")
# Features written to Gold layer in HYBRID mode


# === DATA SCIENCE: Feature Engineering ===

from core.feature_store import FeatureGroupSchema, FeatureSchema, DataType, FeatureType

# Define new feature group
new_features = FeatureGroupSchema(
    name="product_affinity",
    description="Product purchase affinity scores",
    features=[
        FeatureSchema(
            name="affinity_score",
            data_type=DataType.DOUBLE,
            feature_type=FeatureType.NUMERICAL,
            description="Product affinity score [0-1]"
        ),
        FeatureSchema(
            name="last_purchase_days",
            data_type=DataType.INT,
            feature_type=FeatureType.NUMERICAL,
            description="Days since last purchase"
        ),
    ],
    entity_keys=["customer_id", "product_id"],
    timestamp_key="computed_at",
    tags=["ml", "recommendation", "gold"]
)

# Register with hybrid store
from core.feature_store import HybridFeatureStore, FeatureStoreConfig

config = FeatureStoreConfig(
    mode="hybrid",
    enable_virtualization=True,
    register_virtual_tables=True,
)

feature_store = HybridFeatureStore(context, config)
feature_store.register_features(new_features)

# Compute and write features
affinity_data = compute_product_affinity()  # Your custom logic
feature_store.write_features("product_affinity", affinity_data)


# === ML MODEL: Consume via Virtualization ===

# Read features for model training (virtualized = fresh)
training_features = feature_store.get_features(
    feature_names=[
        "customer_360.total_purchases",
        "customer_360.purchase_frequency",
        "product_affinity.affinity_score",
        "product_affinity.last_purchase_days",
    ],
    entity_ids={
        "customer_id": training_customer_ids,
        "product_id": training_product_ids
    },
    point_in_time=training_cutoff_date,
)

# Train model
model = train_ml_model(training_features)

# === PRODUCTION: Real-time scoring ===

# Read features for inference (hybrid decides best strategy)
inference_features = feature_store.get_features(
    feature_names=[
        "customer_360.total_purchases",
        "product_affinity.affinity_score",
    ],
    entity_ids={"customer_id": [12345]}
)

prediction = model.predict(inference_features)
```

---

## 📊 Monitoreo y Optimización

### Ver métricas de acceso

```python
# Get metrics for specific feature group
metrics = hybrid_store.get_access_metrics("customer_360")

print(f"""
Feature Group: {metrics['feature_group']}
Access Count: {metrics['access_count']}
Cache Hit Rate: {metrics['cache_hit_rate']:.2%}
Avg Query Time: {metrics['avg_query_time_ms']:.2f}ms
Estimated Rows: {metrics['estimated_row_count']:,}
""")

# Get all metrics
all_metrics = hybrid_store.get_access_metrics()

# Get optimization recommendations
recommendations = hybrid_store.optimize_strategies()
for group, strategy in recommendations.items():
    print(f"{group}: {strategy}")
```

### Forzar estrategia específica

```python
# Force materialized for high-traffic feature
hybrid_store.force_strategy("customer_360", use_materialized=True)

# Force virtualized for real-time freshness
hybrid_store.force_strategy("real_time_metrics", use_materialized=False)
```

---

## 🎓 Mejores Prácticas

### 1. **Cuándo usar cada modo**

| Escenario | Modo Recomendado | Razón |
|-----------|------------------|-------|
| Features estáticos/históricos | Materialized | Alta velocidad, no cambian |
| Features en tiempo real | Virtualized | Siempre frescos |
| Features mix (estáticos + dinámicos) | Hybrid | Balance automático |
| Training ML (histórico) | Materialized | Velocidad |
| Inference ML (producción) | Hybrid | Balance velocidad/frescura |
| Exploración (Data Science) | Virtualized | Flexibilidad, freshness |

### 2. **Configuración de Hybrid Mode**

```yaml
# Para datasets pequeños (<10K rows)
hybrid_threshold_rows: 5000
auto_materialize_on_read: false

# Para datasets medianos (10K-1M rows)
hybrid_threshold_rows: 50000
auto_materialize_on_read: true

# Para datasets grandes (>1M rows)
hybrid_threshold_rows: 100000
auto_materialize_on_read: true
```

### 3. **Integración con VirtualDataLayer**

```python
# Siempre configurar VirtualDataLayer para mejor optimización
from core.virtualization import VirtualDataLayer

virtual_layer = VirtualDataLayer.from_config({
    "cache_strategy": "smart",
    "default_executor": "spark",
})

# Integrar con feature stores
handler = FeatureStoreNodeHandler(context)
handler.set_virtual_layer(virtual_layer)

# Ahora queries virtualizados usan optimizaciones de predicate pushdown
```

---

## 🔧 Troubleshooting

### Problema: "Virtualized features muy lentos"
**Solución**: Cambiar a hybrid mode o forzar materialized

```python
hybrid_store.force_strategy("slow_feature_group", use_materialized=True)
```

### Problema: "Features desactualizados en materialized"
**Solución**: Refresh manual o usar virtualized/hybrid

```python
# Refresh manual
materialized_store.refresh_features("customer_360")

# O cambiar a virtualized
config.store_mode = "virtualized"
```

### Problema: "Hybrid mode siempre elige materialized"
**Solución**: Ajustar threshold y habilitar auto-materialize

```yaml
hybrid_threshold_rows: 100000  # Aumentar threshold
auto_materialize_on_read: true
```

---

## 📚 Referencias

- [Feature Store README](../src/core/feature_store/README.md)
- [Virtualization README](../src/core/virtualization/README.md)
- [Hybrid Store Implementation](../src/core/feature_store/hybrid/store.py)
- [Integration Module](../src/core/exec/feature_store_integration.py)
