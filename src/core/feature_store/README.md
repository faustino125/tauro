# Feature Store Module

## Overview

The **Feature Store** module is a core component of the Tauro data pipeline framework that provides a unified interface for managing, storing, and retrieving features in the Gold layer of the Medallion Architecture. It supports both **materialized** (pre-computed and stored) and **virtualized** (query-on-demand) feature management strategies.

### Key Capabilities

- **Dual Storage Paradigms**: Choose between materialized features for performance or virtualized features for freshness
- **Schema Management**: Define and validate feature schemas with rich metadata
- **Integration with Tauro IO**: Seamless integration with input/output managers for pipeline workflows
- **Metadata Tracking**: Registry system for feature discovery, lineage tracking, and versioning
- **Multi-Engine Support**: Execute virtualized queries using DuckDB or Spark SQL executors
- **Point-in-Time Queries**: Support for temporal feature retrieval with timestamp-based filtering

## Directory Structure

```
feature_store/
├── base.py                  # Abstract base classes and interfaces
├── exceptions.py            # Custom exception hierarchy
├── schema/
│   ├── __init__.py
│   └── feature_schema.py   # Feature and feature group schema definitions
├── materialized/
│   ├── __init__.py
│   └── store.py            # Materialized feature store implementation
├── virtualized/
│   ├── __init__.py
│   └── store.py            # Virtualized feature store implementation
└── __init__.py             # Public API and exports
```

## Core Components

### 1. **BaseFeatureStore** (`base.py`)

Abstract base class that defines the interface for all feature store implementations.

**Key Methods:**
- `register_features(schema)` - Register a feature group schema
- `get_features(feature_names, entity_ids, point_in_time)` - Retrieve features for given entities
- `write_features(feature_group, data, mode)` - Write features to the store
- `health_check()` - Verify feature store health

**Example:**
```python
from core.feature_store import BaseFeatureStore

class CustomFeatureStore(BaseFeatureStore):
    def register_features(self, schema: FeatureGroupSchema) -> None:
        # Implementation
        pass
    
    def get_features(self, feature_names, entity_ids=None, point_in_time=None):
        # Implementation
        pass
```

### 2. **FeatureStoreMetadata** (`base.py`)

Manages the feature registry, metadata, and lineage information.

**Key Methods:**
- `register_feature_group(schema)` - Register a new feature group
- `get_feature_group(name)` - Retrieve feature group by name
- `get_feature(group_name, feature_name)` - Get specific feature
- `list_feature_groups()` - List all registered groups
- `list_features(group_name)` - List features in a group
- `set_lineage(feature_name, dependencies)` - Track feature lineage
- `get_last_updated(group_name)` - Get last update timestamp

**Example:**
```python
from core.feature_store.base import FeatureStoreMetadata

metadata = FeatureStoreMetadata()
metadata.register_feature_group(user_features_schema)
features = metadata.list_features("user_features")
```

### 3. **Schema System** (`schema/`)

Define and validate feature schemas with strong typing and metadata support.

#### **DataType** Enum
Supported data types for features:
- Numeric: `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- Text: `STRING`
- Temporal: `TIMESTAMP`, `DATE`
- Complex: `ARRAY`, `MAP`, `STRUCT`, `BOOLEAN`

#### **FeatureType** Enum
Semantic classification of features:
- `NUMERICAL` - Numeric values
- `CATEGORICAL` - Discrete categories
- `TEXT` - Text content
- `VECTOR` - Embedding vectors
- `TIMESERIES` - Time series data
- `COMPLEX` - Composite structures

#### **FeatureSchema**
Defines individual feature properties:

```python
from core.feature_store.schema import FeatureSchema, DataType, FeatureType

user_age = FeatureSchema(
    name="age",
    data_type=DataType.INT,
    feature_type=FeatureType.NUMERICAL,
    description="User age in years",
    nullable=False,
    metadata={
        "min": 0,
        "max": 150,
        "unit": "years"
    }
)
```

#### **FeatureGroupSchema**
Groups related features together:

```python
from core.feature_store.schema import FeatureGroupSchema

user_features = FeatureGroupSchema(
    name="user_features",
    version=1,
    entity_keys=["user_id"],
    timestamp_key="event_timestamp",
    description="Core user demographic and behavioral features",
    features=[user_age, user_region, user_spending],
    tags={
        "team": "data-science",
        "environment": "production"
    }
)
```

### 4. **MaterializedFeatureStore** (`materialized/store.py`)

Physical feature replication strategy - stores computed features in the Gold layer for high-performance access.

**Characteristics:**
- Pre-computed and stored features
- Low-latency retrieval
- Higher storage costs
- Updated on a scheduled basis or on-demand

**Key Methods:**
- `write_features(feature_group, data, mode, backfill)` - Write features with Tauro IO infrastructure
- `get_features()` - Retrieve cached features
- `backfill()` - Populate historical data
- Integrated caching mechanism

**Example:**
```python
from core.feature_store import MaterializedFeatureStore

store = MaterializedFeatureStore(
    context=app_context,
    storage_path="/data/gold/features",
    storage_format="parquet"
)

# Register feature group
store.register_features(user_features_schema)

# Write materialized features
store.write_features(
    feature_group="user_features",
    data=user_features_df,
    mode="overwrite"
)

# Retrieve features
features = store.get_features(
    feature_names=["age", "region"],
    entity_ids={"user_id": [123, 456, 789]}
)
```

### 5. **VirtualizedFeatureStore** (`virtualized/store.py`)

On-demand query strategy - computes features dynamically from Silver layer data without materialization.

**Characteristics:**
- Computed on-demand from source data
- Always fresh (minimal latency from source)
- Lower storage footprint
- Query latency depends on computation complexity

#### **QueryExecutor** Interface
Abstract interface for pluggable query engines:

**Implementations:**

- **DuckDBQueryExecutor** - Lightweight, in-process query execution
  ```python
  from core.feature_store import DuckDBQueryExecutor
  
  executor = DuckDBQueryExecutor(context)
  results = executor.execute("SELECT * FROM silver.users")
  ```

- **SparkQueryExecutor** - Distributed query execution via Spark SQL
  ```python
  from core.feature_store import SparkQueryExecutor
  
  executor = SparkQueryExecutor(context)
  results = executor.execute("SELECT * FROM silver.users DISTRIBUTE BY user_id")
  ```

**Example:**
```python
from core.feature_store import VirtualizedFeatureStore, SparkQueryExecutor

executor = SparkQueryExecutor(context)
store = VirtualizedFeatureStore(
    context=app_context,
    query_executor=executor
)

# Register feature group
store.register_features(user_features_schema)

# Define virtualized features
store.define_virtual_features(
    feature_group="user_features",
    queries={
        "age": "SELECT user_id, (YEAR(CURRENT_DATE) - YEAR(birth_date)) AS age FROM silver.users",
        "purchase_count": "SELECT user_id, COUNT(*) AS purchase_count FROM silver.purchases GROUP BY user_id"
    }
)

# Query features (computed on-demand)
features = store.get_features(
    feature_names=["age", "purchase_count"],
    entity_ids={"user_id": [123, 456]}
)
```

## Exception Hierarchy

All feature store operations use a consistent exception hierarchy:

```
FeatureStoreException (base)
├── FeatureNotFoundError
├── FeatureGroupNotFoundError
├── SchemaValidationError
├── FeatureMaterializationError
├── VirtualizationQueryError
├── MetadataError
└── FeatureRegistryError
```

**Usage:**
```python
from core.feature_store.exceptions import (
    FeatureNotFoundError,
    FeatureMaterializationError
)

try:
    feature = metadata.get_feature("user_features", "age")
except FeatureNotFoundError as e:
    logger.error(f"Feature not found: {e}")
```

## Pipeline Integration

The Feature Store integrates seamlessly with Tauro pipeline execution through the **FeatureStoreExecutorAdapter** in `core.exec.feature_store_executor`:

```python
from core.exec.feature_store_executor import write_features_node, read_features_node

# In a Tauro pipeline definition
pipeline = {
    "nodes": [
        write_features_node(
            name="materialize_user_features",
            feature_store="materialized",
            feature_group="user_features",
            input_source="process_user_data"
        ),
        read_features_node(
            name="fetch_training_features",
            feature_store="virtualized",
            feature_names=["age", "region", "purchase_count"],
            entity_ids_source="load_entity_ids"
        )
    ]
}
```

## Usage Patterns

### Pattern 1: Materialized Feature Pipeline

Suitable for high-traffic, low-latency requirements:

```python
from core.feature_store import MaterializedFeatureStore, FeatureGroupSchema, FeatureSchema
from core.feature_store.schema import DataType, FeatureType

# Define schema
schema = FeatureGroupSchema(
    name="transaction_features",
    entity_keys=["transaction_id"],
    timestamp_key="transaction_date",
    features=[
        FeatureSchema("amount", DataType.DOUBLE, FeatureType.NUMERICAL),
        FeatureSchema("merchant_id", DataType.STRING, FeatureType.CATEGORICAL),
    ]
)

# Initialize store
store = MaterializedFeatureStore(context)
store.register_features(schema)

# Write features from pipeline
store.write_features("transaction_features", computed_features_df)

# Retrieve for serving
features = store.get_features(
    feature_names=["amount", "merchant_id"],
    entity_ids={"transaction_id": transaction_ids}
)
```

### Pattern 2: Virtualized Feature Pipeline

Suitable for fresh data requirements with variable computation:

```python
from core.feature_store import VirtualizedFeatureStore, SparkQueryExecutor

# Initialize store with Spark executor
executor = SparkQueryExecutor(context)
store = VirtualizedFeatureStore(context, query_executor=executor)

# Register schema
store.register_features(schema)

# Define virtual features
store.define_virtual_features(
    "transaction_features",
    queries={
        "daily_spending": """
            SELECT 
                customer_id,
                SUM(amount) as daily_spending
            FROM silver.transactions
            WHERE DATE(transaction_date) = CURRENT_DATE()
            GROUP BY customer_id
        """
    }
)

# Query features (computed on-demand)
features = store.get_features(
    feature_names=["daily_spending"],
    entity_ids={"customer_id": customer_ids}
)
```

### Pattern 3: Point-in-Time Queries

Retrieve historical feature values for model training:

```python
from datetime import datetime, timedelta

# Get features as of 30 days ago (useful for time-series analysis)
features = store.get_features(
    feature_names=["age", "purchase_count"],
    entity_ids={"user_id": training_user_ids},
    point_in_time=datetime.now() - timedelta(days=30)
)
```

## Configuration

Feature stores inherit configuration from the Tauro context. Common configuration options:

```python
# In your Tauro configuration
feature_store_config = {
    "materialized": {
        "storage_path": "/data/gold/features",
        "storage_format": "parquet",
        "cache_enabled": True,
        "cache_size_mb": 512
    },
    "virtualized": {
        "executor_type": "spark",  # or "duckdb"
        "query_timeout_seconds": 300,
        "enable_caching": True
    }
}
```

## Logging

The module uses `loguru` for structured logging:

```python
from loguru import logger

# Enable debug logging for feature store operations
logger.enable("core.feature_store")
```

Logged events include:
- Feature group registration
- Feature retrieval operations
- Materialization status
- Query execution results
- Metadata operations
- Health check results

## Best Practices

1. **Schema First**: Define complete schemas before registering feature groups
   ```python
   schema = FeatureGroupSchema(...)
   store.register_features(schema)
   ```

2. **Entity Keys**: Always specify entity keys for point-in-time queries
   ```python
   entity_keys=["user_id", "date_key"]
   ```

3. **Metadata Enrichment**: Include rich metadata for feature discovery
   ```python
   metadata={
       "unit": "days",
       "range": [0, 365],
       "distribution": "normal"
   }
   ```

4. **Version Management**: Update schema version when making breaking changes
   ```python
   FeatureGroupSchema(name="features", version=2)
   ```

5. **Error Handling**: Catch specific feature store exceptions
   ```python
   try:
       features = store.get_features(...)
   except FeatureNotFoundError:
       # Handle missing feature
   except FeatureMaterializationError:
       # Handle materialization failure
   ```

6. **Choose Strategy Wisely**:
   - **Materialized**: High-frequency access, serving systems, consistency requirements
   - **Virtualized**: Fresh data needs, exploratory analysis, memory constraints

7. **Monitor Health**: Use health checks in production
   ```python
   if store.health_check():
       # Proceed with operations
   ```

## Testing

Test feature stores with mock context and data:

```python
from unittest.mock import Mock
from core.feature_store import MaterializedFeatureStore

# Create mock context
mock_context = Mock()
mock_context.feature_store_path = "/tmp/test_features"

# Initialize store
store = MaterializedFeatureStore(mock_context)

# Test feature registration
store.register_features(test_schema)

# Verify metadata
assert "test_group" in store.metadata.list_feature_groups()
```

## Performance Considerations

- **Materialized Features**: Storage I/O for retrieval; consider indexing by entity keys
- **Virtualized Features**: Query execution time; optimize SQL and executor parallelism
- **Caching**: Enable caching for frequently accessed features
- **Batch Operations**: Use batch get/write operations for better throughput
- **Point-in-Time**: Adds temporal filtering overhead; consider time-bucketing for optimization

## License

Copyright © 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.
