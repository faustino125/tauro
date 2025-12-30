"""
Base Feature Store interface.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass

from loguru import logger  # type: ignore

from core.feature_store.schema import FeatureGroupSchema, FeatureSchema
from core.feature_store.exceptions import (
    FeatureNotFoundError,
    FeatureGroupNotFoundError,
    MetadataError,
)


class FeatureStoreMode(Enum):
    """Modes of operation for Feature Store."""

    MATERIALIZED = "materialized"  # Pre-computed, stored features
    VIRTUALIZED = "virtualized"  # Query-on-demand from source
    HYBRID = "hybrid"  # Both strategies, auto-select best


@dataclass
class FeatureStoreConfig:
    """Configuration for Feature Store mode selection."""

    mode: FeatureStoreMode = FeatureStoreMode.MATERIALIZED

    # Materialized settings
    storage_path: Optional[str] = None
    storage_format: str = "parquet"

    # Virtualized settings
    enable_virtualization: bool = False
    query_executor_type: str = "spark"  # "spark" or "duckdb"

    # Hybrid settings
    hybrid_threshold_rows: int = 10000  # Use materialized if > threshold
    hybrid_cache_ttl: int = 3600  # Cache TTL for hybrid mode
    auto_materialize_on_read: bool = False  # Auto-materialize virtual features

    # Integration with VirtualDataLayer
    register_virtual_tables: bool = True
    virtual_table_prefix: str = "features_"

    def __post_init__(self):
        """Validate configuration."""
        if self.mode == FeatureStoreMode.MATERIALIZED and not self.storage_path:
            logger.warning("Materialized mode without storage_path, using default")

        if self.mode == FeatureStoreMode.VIRTUALIZED and not self.enable_virtualization:
            self.enable_virtualization = True

        if self.mode == FeatureStoreMode.HYBRID:
            self.enable_virtualization = True


class FeatureStoreMetadata:
    """Manages feature store metadata and registry."""

    def __init__(self):
        """Initialize metadata store."""
        self._feature_groups: Dict[str, FeatureGroupSchema] = {}
        self._feature_registry: Dict[str, Dict[str, FeatureSchema]] = {}
        self._lineage: Dict[str, List[str]] = {}
        self._last_updated: Dict[str, datetime] = {}

    def register_feature_group(self, schema: FeatureGroupSchema) -> None:
        """Register a feature group schema."""
        schema.validate()
        self._feature_groups[schema.name] = schema
        self._feature_registry[schema.name] = {f.name: f for f in schema.features}
        self._last_updated[schema.name] = datetime.now(timezone.utc)
        logger.info(f"Registered feature group: {schema.name} with {len(schema.features)} features")

    def get_feature_group(self, name: str) -> FeatureGroupSchema:
        """Retrieve a feature group schema."""
        if name not in self._feature_groups:
            raise FeatureGroupNotFoundError(f"Feature group '{name}' not found in registry")
        return self._feature_groups[name]

    def get_feature(self, group_name: str, feature_name: str) -> FeatureSchema:
        """Retrieve a specific feature."""
        if group_name not in self._feature_registry:
            raise FeatureGroupNotFoundError(f"Feature group '{group_name}' not found")
        if feature_name not in self._feature_registry[group_name]:
            raise FeatureNotFoundError(
                f"Feature '{feature_name}' not found in group '{group_name}'"
            )
        return self._feature_registry[group_name][feature_name]

    def list_feature_groups(self) -> List[str]:
        """List all registered feature groups."""
        return list(self._feature_groups.keys())

    def list_features(self, group_name: str) -> List[str]:
        """List features in a feature group."""
        if group_name not in self._feature_registry:
            raise FeatureGroupNotFoundError(f"Feature group '{group_name}' not found")
        return list(self._feature_registry[group_name].keys())

    def set_lineage(self, feature_name: str, dependencies: List[str]) -> None:
        """Set data lineage for a feature."""
        self._lineage[feature_name] = dependencies
        logger.debug(f"Set lineage for {feature_name}: {dependencies}")

    def get_lineage(self, feature_name: str) -> List[str]:
        """Get data lineage for a feature."""
        return self._lineage.get(feature_name, [])

    def get_last_updated(self, group_name: str) -> Optional[datetime]:
        """Get last update time for a feature group."""
        return self._last_updated.get(group_name)


class BaseFeatureStore(ABC):
    """Abstract base class for Feature Store implementations."""

    def __init__(self, context: Any):
        """Initialize Feature Store.

        Args:
            context: Application context with configuration
        """
        self.context = context
        self.metadata = FeatureStoreMetadata()
        logger.info("BaseFeatureStore initialized")

    @abstractmethod
    def register_features(self, schema: FeatureGroupSchema) -> None:
        """Register a feature group."""
        pass

    @abstractmethod
    def get_features(
        self,
        feature_names: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Retrieve features for given entities."""
        pass

    @abstractmethod
    def write_features(
        self,
        feature_group: str,
        data: Dict[str, List[Any]],
        mode: str = "append",
    ) -> None:
        """Write features to the store."""
        pass

    def health_check(self) -> bool:
        """Check Feature Store health."""
        try:
            return len(self.metadata.list_feature_groups()) > 0
        except Exception as e:
            logger.error(f"Feature Store health check failed: {e}")
            return False
