"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import json
from pathlib import Path

from loguru import logger  # type: ignore

from core.feature_store.base import BaseFeatureStore
from core.feature_store.schema import FeatureGroupSchema
from core.feature_store.exceptions import (
    FeatureMaterializationError,
    FeatureNotFoundError,
)
from core.io.input import InputLoader
from core.io.output import DataOutputManager


class MaterializedFeatureStore(BaseFeatureStore):
    """Feature Store with physical data materialization."""

    def __init__(
        self, context: Any, storage_path: Optional[str] = None, storage_format: str = "parquet"
    ):
        """Initialize Materialized Feature Store."""
        super().__init__(context)
        self.storage_format = storage_format
        self.storage_path = storage_path or self._get_default_storage_path()

        # Initialize Tauro IO managers from context
        self.input_loader = InputLoader(context)
        self.output_manager = DataOutputManager(context)

        # Ensure storage directory exists
        self._ensure_storage_exists()

        self._feature_cache: Dict[str, Dict[str, Any]] = {}

        logger.info(
            f"MaterializedFeatureStore initialized with Tauro IO "
            f"(format={storage_format}, path={self.storage_path})"
        )

    def _get_default_storage_path(self) -> str:
        """Get default storage path from context or use standard location."""
        if hasattr(self.context, "feature_store_path"):
            return self.context.feature_store_path
        return "/data/gold/features"

    def _ensure_storage_exists(self) -> None:
        """Ensure storage directory exists."""
        try:
            p = Path(self.storage_path)
            p.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Feature store storage path ensured: {self.storage_path}")
        except Exception as e:
            logger.warning(f"Could not create local storage path: {e}")

    def register_features(self, schema: FeatureGroupSchema) -> None:
        """Register and prepare materialized feature group."""
        try:
            self.metadata.register_feature_group(schema)
            logger.info(f"Materialized feature group registered: {schema.name}")
        except Exception as e:
            raise FeatureMaterializationError(
                f"Failed to register feature group '{schema.name}': {e}"
            ) from e

    def write_features(
        self,
        feature_group: str,
        data: Union[Dict[str, List[Any]], Any],
        mode: str = "append",
        backfill: bool = False,
        **write_options,
    ) -> None:
        """Write features to materialized store using Tauro IO infrastructure."""
        try:
            schema = self.metadata.get_feature_group(feature_group)

            # Convert data to DataFrame if needed
            df = self._prepare_dataframe(data, schema)

            # Configure output with feature group metadata
            output_config = {
                "format": write_options.get("format", self.storage_format),
                "mode": mode,
                "schema": "gold",
                "table_name": feature_group,
            }

            # Add backfill metadata
            if backfill:
                output_config["backfill"] = True

            # Add any additional partition or write options
            output_config.update({k: v for k, v in write_options.items() if k not in ["format"]})

            # Use output manager to write features
            self.output_manager.save_output(
                env=getattr(self.context, "env", "dev"),
                node={"output": [feature_group], "name": f"write_features_{feature_group}"},
                df=df,
                **output_config,
            )

            logger.info(
                f"Successfully wrote features to '{feature_group}' "
                f"(mode={mode}, format={self.storage_format}, backfill={backfill})"
            )
        except Exception as e:
            raise FeatureMaterializationError(
                f"Failed to write features to '{feature_group}': {e}"
            ) from e

    def get_features(
        self,
        feature_names: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
        as_dataframe: bool = False,
    ) -> Union[Dict[str, Any], Any]:
        """Retrieve features from materialized store."""
        groups_features = self._group_feature_refs(feature_names)
        dataframes = self._load_group_dataframes(groups_features, entity_ids, point_in_time)

        if not dataframes:
            raise FeatureNotFoundError("No features found matching the criteria")

        combined_df = (
            dataframes[0][1] if len(dataframes) == 1 else self._join_dataframes(dataframes)
        )

        return combined_df if as_dataframe else self._dataframe_to_dict(combined_df, feature_names)

    def _group_feature_refs(self, feature_names: List[str]) -> Dict[str, List[str]]:
        """Parse and group feature references 'group.feature' into a dict."""
        groups_features: Dict[str, List[str]] = {}
        for feature_ref in feature_names:
            try:
                group_name, feature_name = feature_ref.split(".")
            except ValueError:
                raise FeatureNotFoundError(
                    f"Invalid feature reference format: {feature_ref}. Expected 'group.feature'"
                )
            groups_features.setdefault(group_name, []).append(feature_name)
        return groups_features

    def _load_group_dataframes(
        self,
        groups_features: Dict[str, List[str]],
        entity_ids: Optional[Dict[str, List[Any]]],
        point_in_time: Optional[datetime],
    ) -> List[tuple]:
        """Load dataframes for each feature group using InputLoader and return list of (group_name, df)."""
        dataframes: List[tuple] = []
        for group_name, features in groups_features.items():
            try:
                input_config = {
                    "format": self.storage_format,
                    "filepath": str(Path(self.storage_path) / group_name),
                }

                if entity_ids or point_in_time:
                    input_config["partition_filter"] = self._build_filter_expression(
                        entity_ids, point_in_time, group_name
                    )

                # Ensure context has input_config mapping
                self.context.input_config = getattr(self.context, "input_config", {}) or {}
                self.context.input_config[group_name] = input_config

                dfs = self.input_loader.load_inputs([group_name], fail_fast=True)
                if dfs and dfs[0] is not None:
                    df = dfs[0]
                    dataframes.append((group_name, df))
                    logger.debug(f"Retrieved {len(features)} features from '{group_name}'")
            except Exception as e:
                logger.error(f"Failed to retrieve features from '{group_name}': {e}")
                raise
        return dataframes

    def _build_filter_expression(
        self,
        entity_ids: Optional[Dict[str, List[Any]]],
        point_in_time: Optional[datetime],
        group_name: str,
    ) -> Optional[str]:
        """Build filter expression for feature retrieval."""
        filters = []

        # Entity filters
        if entity_ids:
            for key, values in entity_ids.items():
                values_str = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
                filters.append(f"{key} IN ({values_str})")

        # Point-in-time filter
        if point_in_time:
            try:
                schema = self.metadata.get_feature_group(group_name)
                if hasattr(schema, "timestamp_key") and schema.timestamp_key:
                    timestamp_str = point_in_time.isoformat()
                    filters.append(f"{schema.timestamp_key} <= '{timestamp_str}'")
            except Exception as e:
                logger.warning(f"Could not apply point-in-time filter: {e}")

        return " AND ".join(filters) if filters else None

    def _join_dataframes(self, dataframes: List[tuple]) -> Any:
        """Join multiple dataframes on entity keys."""
        if not dataframes:
            return None

        # Start with first dataframe
        result = dataframes[0][1]

        # Join with remaining dataframes
        for group_name, df in dataframes[1:]:
            try:
                schema = self.metadata.get_feature_group(group_name)
                join_keys = getattr(schema, "entity_keys", [])

                if not join_keys:
                    logger.warning(f"No entity keys defined for {group_name}, skipping join")
                    continue

                if hasattr(result, "join"):
                    # Spark DataFrame
                    result = result.join(df, on=join_keys, how="inner")
                elif hasattr(result, "merge"):
                    # Pandas DataFrame
                    result = result.merge(df, on=join_keys, how="inner")
                else:
                    logger.warning(f"Cannot join dataframes of type {type(result)}")
                    return result
            except Exception as e:
                logger.warning(f"Failed to join dataframe for '{group_name}': {e}")

        return result

    def _dataframe_to_dict(self, df: Any, feature_names: List[str]) -> Dict[str, Any]:
        """Convert DataFrame to dictionary format."""
        result = {}

        try:
            if hasattr(df, "toPandas"):
                # Spark DataFrame
                pdf = df.toPandas()
                for col in pdf.columns:
                    # Match requested feature names
                    matching_refs = [fn for fn in feature_names if fn.endswith(f".{col}")]
                    if matching_refs:
                        result[matching_refs[0]] = pdf[col].tolist()
            elif hasattr(df, "to_dict"):
                # Pandas DataFrame
                for col in df.columns:
                    matching_refs = [fn for fn in feature_names if fn.endswith(f".{col}")]
                    if matching_refs:
                        result[matching_refs[0]] = df[col].tolist()
            else:
                logger.warning(f"Unknown DataFrame type: {type(df)}")
        except Exception as e:
            logger.error(f"Failed to convert DataFrame to dict: {e}")

        return result

    def _prepare_dataframe(
        self, data: Union[Dict[str, List[Any]], Any], schema: FeatureGroupSchema
    ) -> Any:
        """Convert data to DataFrame suitable for Tauro IO."""
        # If already a DataFrame, return as-is
        if hasattr(data, "select") or hasattr(data, "columns"):
            return data

        # Convert dict to DataFrame
        if isinstance(data, dict):
            try:
                # Try Spark first
                spark = getattr(self.context, "spark", None)
                if spark:
                    # Validate data
                    self._validate_features(data, schema)

                    # Convert to Spark DataFrame
                    records = self._lists_to_records(data)
                    return spark.createDataFrame(records)
                else:
                    # Fallback to Pandas
                    try:
                        import pandas as pd

                        self._validate_features(data, schema)
                        return pd.DataFrame(data)
                    except ImportError:
                        raise FeatureMaterializationError(
                            "Neither Spark nor Pandas available to create DataFrame"
                        )
            except Exception as e:
                raise FeatureMaterializationError(
                    f"Failed to convert data to DataFrame: {e}"
                ) from e

        raise FeatureMaterializationError(
            f"Unsupported data type: {type(data)}. Expected dict or DataFrame"
        )

    def _validate_features(self, data: Dict[str, List[Any]], schema: FeatureGroupSchema) -> None:
        """Validate feature data against schema."""
        feature_names = self.metadata.list_features(schema.name)
        provided_names = set(data.keys())

        if not provided_names.issubset(set(feature_names)):
            unknown = provided_names - set(feature_names)
            raise ValueError(f"Unknown features: {unknown}")

    def _lists_to_records(self, data: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        """Convert lists format to records format."""
        if not data:
            return []

        # Get length from first feature list
        length = len(next(iter(data.values())))
        records = [{} for _ in range(length)]

        for feature_name, values in data.items():
            if len(values) != length:
                raise ValueError(f"Feature '{feature_name}' has mismatched length")
            for i, value in enumerate(values):
                records[i][feature_name] = value

        return records

    def refresh_features(self, feature_group: str) -> None:
        """Refresh materialized features from source."""
        try:
            # validate that the feature group exists
            self.metadata.get_feature_group(feature_group)
            logger.info(f"Refreshing materialized feature group: {feature_group}")
            # In production, would re-execute transformations
        except Exception as e:
            logger.error(f"Failed to refresh features: {e}")
            raise

    def get_storage_info(self, feature_group: Optional[str] = None) -> Dict[str, Any]:
        """Get storage information for feature groups."""
        info = {}

        groups = [feature_group] if feature_group else self.metadata.list_feature_groups()

        for group in groups:
            group_path = Path(self.storage_path) / group
            try:
                if group_path.exists():
                    info[group] = {
                        "materialized": True,
                        "path": str(group_path),
                        "format": self.storage_format,
                    }
            except Exception as e:
                logger.error(f"Failed to get storage info for {group}: {e}")

        return info

    def list_feature_groups(self) -> List[str]:
        """List all materialized feature groups."""
        return self.metadata.list_feature_groups()

    def delete_feature_group(self, feature_group: str) -> None:
        """Delete a materialized feature group."""
        try:
            group_path = Path(self.storage_path) / feature_group
            if group_path.exists():
                import shutil

                shutil.rmtree(group_path)
                logger.info(f"Deleted feature group: {feature_group}")
        except Exception as e:
            logger.error(f"Failed to delete feature group '{feature_group}': {e}")
            raise
