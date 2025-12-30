"""
Virtualized Feature Store package.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from core.feature_store.virtualized.store import (
    VirtualizedFeatureStore,
    QueryExecutor,
    DuckDBQueryExecutor,
    SparkQueryExecutor,
)

__all__ = [
    "VirtualizedFeatureStore",
    "QueryExecutor",
    "DuckDBQueryExecutor",
    "SparkQueryExecutor",
]
