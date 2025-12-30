"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from loguru import logger  # type: ignore

from core.config.contexts import Context
from core.feature_store import MaterializedFeatureStore, VirtualizedFeatureStore
from core.feature_store.schema import FeatureGroupSchema
from core.io.input import InputLoader
from core.io.output import DataOutputManager


class FeatureStoreExecutorAdapter:
    """Adapter for executing feature store operations within pipeline context."""

    def __init__(self, context: Context):
        """Initialize Feature Store Executor Adapter."""
        self.context = context
        self.input_loader = InputLoader(context)
        self.output_manager = DataOutputManager(context)
        self.materialized_store = None
        self.virtualized_store = None

        logger.info("FeatureStoreExecutorAdapter initialized with pipeline context")

    def create_materialized_store(
        self, storage_path: Optional[str] = None, storage_format: str = "parquet"
    ) -> MaterializedFeatureStore:
        """Create materialized feature store with executor context."""
        self.materialized_store = MaterializedFeatureStore(
            self.context, storage_path=storage_path, storage_format=storage_format
        )
        return self.materialized_store

    def create_virtualized_store(self) -> "VirtualizedFeatureStore":
        """Create virtualized feature store with executor context."""
        self.virtualized_store = VirtualizedFeatureStore(self.context)
        return self.virtualized_store

    def write_features_from_output(
        self,
        feature_group: str,
        output_key: str,
        schema: FeatureGroupSchema,
        mode: str = "append",
        backfill: bool = False,
        **write_options,
    ) -> None:
        """Write features from pipeline output to materialized store."""
        if not self.materialized_store:
            self.create_materialized_store()

        try:
            # Register feature group schema
            self.materialized_store.register_features(schema)

            # Get output data from context
            if (
                hasattr(self.context, "execution_outputs")
                and output_key in self.context.execution_outputs
            ):
                data = self.context.execution_outputs[output_key]

                # Write to materialized store
                self.materialized_store.write_features(
                    feature_group=feature_group,
                    data=data,
                    mode=mode,
                    backfill=backfill,
                    **write_options,
                )

                logger.info(
                    f"Successfully wrote features to '{feature_group}' "
                    f"from output '{output_key}'"
                )
            else:
                raise ValueError(f"Output key '{output_key}' not found in execution context")

        except Exception as e:
            logger.error(f"Failed to write features from output: {e}")
            raise

    def read_features_as_input(
        self,
        input_key: str,
        feature_names: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
        as_dataframe: bool = True,
    ) -> Any:
        """Read features from materialized store and register as pipeline input."""
        if not self.materialized_store:
            self.create_materialized_store()

        try:
            # Retrieve features from materialized store
            features_data = self.materialized_store.get_features(
                feature_names=feature_names,
                entity_ids=entity_ids,
                point_in_time=point_in_time,
                as_dataframe=as_dataframe,
            )

            # Register in context for pipeline use
            if not hasattr(self.context, "feature_store_inputs"):
                self.context.feature_store_inputs = {}

            self.context.feature_store_inputs[input_key] = features_data

            logger.info(f"Retrieved {len(feature_names)} features as input '{input_key}'")

            return features_data

        except Exception as e:
            logger.error(f"Failed to read features as input: {e}")
            raise


def write_features_node(
    context: Context,
    feature_group: str,
    output_key: str,
    schema: FeatureGroupSchema,
    storage_path: Optional[str] = None,
    mode: str = "append",
    backfill: bool = False,
    **write_options,
) -> None:
    """Pipeline node function for writing features to materialized store."""
    adapter = FeatureStoreExecutorAdapter(context)

    if storage_path:
        adapter.create_materialized_store(storage_path=storage_path)
    else:
        adapter.create_materialized_store()

    adapter.write_features_from_output(
        feature_group=feature_group,
        output_key=output_key,
        schema=schema,
        mode=mode,
        backfill=backfill,
        **write_options,
    )


def read_features_node(
    context: Context,
    input_key: str,
    feature_names: List[str],
    storage_path: Optional[str] = None,
    entity_ids: Optional[Dict[str, List[Any]]] = None,
    point_in_time: Optional[datetime] = None,
    as_dataframe: bool = True,
) -> Any:
    """Pipeline node function for reading features from materialized store."""
    adapter = FeatureStoreExecutorAdapter(context)

    if storage_path:
        adapter.create_materialized_store(storage_path=storage_path)
    else:
        adapter.create_materialized_store()

    return adapter.read_features_as_input(
        input_key=input_key,
        feature_names=feature_names,
        entity_ids=entity_ids,
        point_in_time=point_in_time,
        as_dataframe=as_dataframe,
    )


def create_feature_store_for_pipeline(context: Context) -> FeatureStoreExecutorAdapter:
    """Create a feature store adapter for use in pipeline execution."""
    return FeatureStoreExecutorAdapter(context)
