"""
Feature Store module - Phase 1 Foundation.

Provides both materialized (replicated) and virtualized (query-on-demand) 
feature stores for the Gold layer of Medallion architecture.

Supports three modes of operation:
- Materialized: Pre-computed features stored in physical storage
- Virtualized: Query-on-demand from source layers with data virtualization
- Hybrid: Intelligent switching between materialized and virtualized

Integrates natively with Tauro pipelines using the IO module infrastructure.
Executes in core.exec module context with input/output managers.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from core.feature_store.base import (
    BaseFeatureStore,
    FeatureStoreMetadata,
    FeatureStoreMode,
    FeatureStoreConfig,
)
from core.feature_store.materialized import MaterializedFeatureStore
from core.feature_store.virtualized import (
    VirtualizedFeatureStore,
    QueryExecutor,
    DuckDBQueryExecutor,
    SparkQueryExecutor,
)
from core.feature_store.hybrid import HybridFeatureStore
from core.feature_store.schema import (
    DataType,
    FeatureType,
    FeatureSchema,
    FeatureGroupSchema,
)
from core.feature_store.exceptions import (
    FeatureStoreException,
    FeatureNotFoundError,
    FeatureGroupNotFoundError,
    SchemaValidationError,
    FeatureMaterializationError,
    VirtualizationQueryError,
    MetadataError,
    FeatureRegistryError,
)

# Pipeline integration is now in core.exec module for better context handling
# Import here for backward compatibility
try:
    from core.exec.feature_store_executor import (
        FeatureStoreExecutorAdapter,
        write_features_node,
        read_features_node,
        create_feature_store_for_pipeline,
    )

    _EXEC_INTEGRATION_AVAILABLE = True
except ImportError:
    _EXEC_INTEGRATION_AVAILABLE = False
    FeatureStoreExecutorAdapter = None
    write_features_node = None
    read_features_node = None
    create_feature_store_for_pipeline = None

__all__ = [
    # Base classes
    "BaseFeatureStore",
    "FeatureStoreMetadata",
    "FeatureStoreMode",
    "FeatureStoreConfig",
    # Store implementations
    "MaterializedFeatureStore",
    "VirtualizedFeatureStore",
    "HybridFeatureStore",
    # Query executors
    "QueryExecutor",
    "DuckDBQueryExecutor",
    "SparkQueryExecutor",
    # Schema types
    "DataType",
    "FeatureType",
    "FeatureSchema",
    "FeatureGroupSchema",
    # Exceptions
    "FeatureStoreException",
    "FeatureNotFoundError",
    "FeatureGroupNotFoundError",
    "SchemaValidationError",
    "FeatureMaterializationError",
    "VirtualizationQueryError",
    "MetadataError",
    "FeatureRegistryError",
    # Pipeline integration (from core.exec)
    "FeatureStoreExecutorAdapter",
    "write_features_node",
    "read_features_node",
    "create_feature_store_for_pipeline",
]
