"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass

from loguru import logger  # type: ignore

from core.config.contexts import Context
from core.feature_store import (
    MaterializedFeatureStore,
    VirtualizedFeatureStore,
    FeatureGroupSchema,
)
from core.feature_store.base import FeatureStoreConfig, FeatureStoreMode
from core.feature_store.hybrid import HybridFeatureStore
from core.feature_store.exceptions import FeatureStoreException
from core.io.input import InputLoader
from core.io.output import DataOutputManager


@dataclass
class FeatureStoreNodeConfig:
    """Configuration for feature store nodes"""

    operation: str  # "write", "read", "transform"
    feature_group: Optional[str] = None
    input_key: Optional[str] = None  # For read operations
    output_key: Optional[str] = None  # For write operations
    schema: Optional[FeatureGroupSchema] = None  # For write/transform
    feature_names: Optional[List[str]] = None  # For read operations
    storage_path: Optional[str] = None
    storage_format: str = "parquet"
    mode: str = "append"  # "append" or "overwrite"
    backfill: bool = False
    entity_ids: Optional[Dict[str, List[Any]]] = None
    point_in_time: Optional[datetime] = None
    as_dataframe: bool = True

    # Feature Store mode selection
    store_mode: str = "materialized"  # "materialized", "virtualized", or "hybrid"

    # Hybrid mode settings
    hybrid_threshold_rows: int = 10000
    auto_materialize: bool = False

    # Virtualization settings
    enable_virtual_layer: bool = False
    register_virtual_table: bool = True


class FeatureStoreNodeHandler:
    """
    Handler for Feature Store nodes - integrates Feature Store as a native module type.
    """

    def __init__(self, context: Context, store_mode: Optional[str] = None):
        """Initialize Feature Store node handler with pipeline context."""
        self.context = context
        self.input_loader = InputLoader(context)
        self.output_manager = DataOutputManager(context)

        # Determine store mode
        if store_mode is None:
            store_mode = getattr(context, "feature_store_mode", "materialized")

        self.store_mode = FeatureStoreMode(
            store_mode
            if store_mode in ["materialized", "virtualized", "hybrid"]
            else "materialized"
        )

        # Lazy initialize stores
        self._materialized_store: Optional[MaterializedFeatureStore] = None
        self._virtualized_store: Optional[VirtualizedFeatureStore] = None
        self._hybrid_store: Optional[HybridFeatureStore] = None
        self._virtual_layer = None

        logger.info(f"FeatureStoreNodeHandler initialized with mode: {self.store_mode.value}")

    @property
    def materialized_store(self) -> MaterializedFeatureStore:
        """Lazy-load materialized store"""
        if self._materialized_store is None:
            storage_path = getattr(self.context, "feature_store_path", None)
            storage_format = getattr(self.context, "feature_store_format", "parquet")

            self._materialized_store = MaterializedFeatureStore(
                self.context, storage_path=storage_path, storage_format=storage_format
            )
        return self._materialized_store

    @property
    def virtualized_store(self) -> VirtualizedFeatureStore:
        """Lazy-load virtualized store"""
        if self._virtualized_store is None:
            self._virtualized_store = VirtualizedFeatureStore(self.context)
            if self._virtual_layer:
                self._virtualized_store.set_virtual_layer(self._virtual_layer)
        return self._virtualized_store

    @property
    def hybrid_store(self) -> HybridFeatureStore:
        """Lazy-load hybrid store"""
        if self._hybrid_store is None:
            config = FeatureStoreConfig(
                mode=FeatureStoreMode.HYBRID,
                storage_path=getattr(self.context, "feature_store_path", None),
                storage_format=getattr(self.context, "feature_store_format", "parquet"),
                enable_virtualization=True,
                hybrid_threshold_rows=getattr(self.context, "hybrid_threshold_rows", 10000),
                auto_materialize_on_read=getattr(self.context, "auto_materialize", False),
            )
            self._hybrid_store = HybridFeatureStore(self.context, config)
            if self._virtual_layer:
                self._hybrid_store.set_virtual_layer(self._virtual_layer)
        return self._hybrid_store

    def set_virtual_layer(self, virtual_layer: Any) -> None:
        """Configure VirtualDataLayer for feature store integration."""
        self._virtual_layer = virtual_layer

        # Update existing stores if already initialized
        if self._virtualized_store:
            self._virtualized_store.set_virtual_layer(virtual_layer)
        if self._hybrid_store:
            self._hybrid_store.set_virtual_layer(virtual_layer)

        logger.info("VirtualDataLayer configured for feature stores")

    def get_store(self, config: FeatureStoreNodeConfig):
        """Get appropriate store based on configuration."""
        # Config overrides instance mode
        mode_str = config.store_mode or self.store_mode.value

        if mode_str == "hybrid":
            return self.hybrid_store
        elif mode_str == "virtualized":
            store = self.virtualized_store
            # Auto-register as virtual table if enabled
            if (
                config.enable_virtual_layer
                and config.register_virtual_table
                and config.feature_group
            ):
                store.register_as_virtual_table(config.feature_group)
            return store
        else:  # materialized
            return self.materialized_store

    def handle_write_node(self, config: FeatureStoreNodeConfig) -> Dict[str, Any]:
        """
        Handle feature_write node type.
        """
        try:
            if not config.feature_group:
                raise ValueError("feature_group is required for write operations")

            if not config.schema:
                raise ValueError("schema is required for write operations")

            if not config.output_key:
                raise ValueError("output_key is required for write operations")

            # Get data from execution context
            if not hasattr(self.context, "execution_outputs"):
                self.context.execution_outputs = {}

            if config.output_key not in self.context.execution_outputs:
                raise ValueError(f"Output key '{config.output_key}' not found in execution context")

            data = self.context.execution_outputs[config.output_key]

            # Get store instance based on configuration
            store = self.get_store(config)

            # Register schema
            store.register_features(config.schema)

            # Write features
            store.write_features(
                feature_group=config.feature_group,
                data=data,
                mode=config.mode,
                backfill=config.backfill,
                **{
                    k: v
                    for k, v in vars(config).items()
                    if k
                    not in [
                        "operation",
                        "feature_group",
                        "output_key",
                        "input_key",
                        "feature_names",
                        "schema",
                        "mode",
                        "backfill",
                        "point_in_time",
                        "entity_ids",
                        "as_dataframe",
                        "virtualized",
                        "kwargs",
                    ]
                },
            )

            return {
                "status": "success",
                "feature_group": config.feature_group,
                "rows_written": len(data) if isinstance(data, dict) else 0,
                "mode": config.mode,
                "backfill": config.backfill,
            }

        except Exception as e:
            logger.error(f"Feature write node failed: {e}")
            raise

    def handle_read_node(self, config: FeatureStoreNodeConfig) -> Dict[str, Any]:
        """
        Handle feature_read node type.
        """
        try:
            if not config.input_key:
                raise ValueError("input_key is required for read operations")

            if not config.feature_names:
                raise ValueError("feature_names is required for read operations")

            # Get appropriate store based on configuration
            store = self.get_store(config)

            # Read features
            features = store.get_features(
                feature_names=config.feature_names,
                entity_ids=config.entity_ids,
                point_in_time=config.point_in_time,
                as_dataframe=config.as_dataframe,
            )

            # Register in context for pipeline use
            if not hasattr(self.context, "feature_store_inputs"):
                self.context.feature_store_inputs = {}

            self.context.feature_store_inputs[config.input_key] = features

            # Also set as execution output for next nodes
            self.context.execution_outputs[config.input_key] = features

            logger.info(
                f"Feature read node: retrieved {len(config.feature_names)} features "
                f"into input key '{config.input_key}'"
            )

            return {
                "status": "success",
                "input_key": config.input_key,
                "features_count": len(config.feature_names),
                "point_in_time": str(config.point_in_time) if config.point_in_time else "current",
                "data": features,
            }

        except Exception as e:
            logger.error(f"Feature read node failed: {e}")
            raise

    def handle_transform_node(
        self, config: FeatureStoreNodeConfig, transform_func=None
    ) -> Dict[str, Any]:
        """
        Handle feature_transform node type.
        """
        try:
            if not config.feature_group:
                raise ValueError("feature_group is required for transform operations")

            if not config.schema:
                raise ValueError("schema is required for transform operations")

            if not config.input_key:
                raise ValueError("input_key is required for transform operations")

            # Get input data
            if config.input_key not in self.context.execution_outputs:
                raise ValueError(f"Input key '{config.input_key}' not found in execution context")

            data = self.context.execution_outputs[config.input_key]

            # Apply transformation if provided
            if transform_func:
                try:
                    data = transform_func(data)
                    logger.debug("Applied custom transformation function")
                except Exception as e:
                    logger.error(f"Transformation failed: {e}")
                    raise

            # Register and write using configured store
            store = self.get_store(config)
            store.register_features(config.schema)

            store.write_features(
                feature_group=config.feature_group,
                data=data,
                mode=config.mode,
                backfill=config.backfill,
            )

            return {
                "status": "success",
                "feature_group": config.feature_group,
                "rows_transformed": len(data) if isinstance(data, dict) else 0,
                "transformation_applied": transform_func is not None,
            }

        except Exception as e:
            logger.error(f"Feature transform node failed: {e}")
            raise

    def handle_feature_store_node(self, node_config: Dict[str, Any]) -> Any:
        """
        Main handler for feature_store node type.
        """
        # Parse configuration
        config = FeatureStoreNodeConfig(**node_config)

        logger.info(f"Executing feature_store node: {config.operation}")

        try:
            if config.operation == "write":
                return self.handle_write_node(config)

            elif config.operation == "read":
                return self.handle_read_node(config)

            elif config.operation == "transform":
                return self.handle_transform_node(config)

            else:
                raise ValueError(
                    f"Unknown feature_store operation: {config.operation}. "
                    f"Supported: write, read, transform"
                )

        except FeatureStoreException as e:
            logger.error(f"Feature Store operation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Feature Store node error: {e}")
            raise


def create_feature_store_handler(context: Context) -> FeatureStoreNodeHandler:
    """
    Factory function to create Feature Store node handler.
    """
    return FeatureStoreNodeHandler(context)
