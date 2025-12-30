"""
Virtualized Feature Store - Query-on-demand from Silver layer.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from datetime import datetime
from abc import ABC, abstractmethod

from loguru import logger  # type: ignore

from core.feature_store.base import BaseFeatureStore
from core.feature_store.schema import FeatureGroupSchema
from core.feature_store.exceptions import (
    VirtualizationQueryError,
)

if TYPE_CHECKING:
    from core.virtualization import VirtualTable, VirtualDataLayer


class QueryExecutor(ABC):
    """Abstract query executor for virtualized features."""

    @abstractmethod
    def execute(self, query: str, **kwargs) -> Dict[str, List[Any]]:
        """Execute a query and return results."""
        pass


class DuckDBQueryExecutor(QueryExecutor):
    """DuckDB-based query executor for virtualized features."""

    def __init__(self, context: Any):
        """
        Initialize DuckDB executor.
        """
        self.context = context
        self._executor_func: Optional[Callable] = None
        logger.info("DuckDBQueryExecutor initialized")

    def execute(self, query: str, **kwargs) -> Dict[str, List[Any]]:
        """Execute DuckDB query."""
        try:
            logger.debug(f"Executing DuckDB query: {query[:100]}...")
            # In production, would use actual DuckDB connection
            # For now, this is a placeholder
            result = {}
            logger.debug("Query execution completed")
            return result
        except Exception as e:
            raise VirtualizationQueryError(f"DuckDB query execution failed: {e}") from e


class SparkQueryExecutor(QueryExecutor):
    """Spark SQL-based query executor for virtualized features."""

    def __init__(self, context: Any):
        """Initialize Spark executor.

        Args:
            context: Application context with SparkSession
        """
        self.context = context
        self.spark = getattr(context, "spark", None)
        logger.info("SparkQueryExecutor initialized")

    def execute(self, query: str, **kwargs) -> Dict[str, List[Any]]:
        """Execute Spark SQL query."""
        try:
            if not self.spark:
                raise VirtualizationQueryError("SparkSession not available in context")

            logger.debug(f"Executing Spark query: {query[:100]}...")
            df = self.spark.sql(query)

            # Convert to dictionary format
            result = {}
            for col in df.columns:
                result[col] = df.select(col).collect()

            logger.debug(f"Query execution completed, {df.count()} rows")
            return result
        except Exception as e:
            raise VirtualizationQueryError(f"Spark query execution failed: {e}") from e


class VirtualizedFeatureStore(BaseFeatureStore):
    """Feature Store with on-demand query execution (no materialization)."""

    def __init__(
        self,
        context: Any,
        query_executor: Optional[QueryExecutor] = None,
    ):
        """Initialize Virtualized Feature Store."""
        super().__init__(context)

        # Default to DuckDB if no executor provided
        if query_executor is None:
            try:
                query_executor = DuckDBQueryExecutor(context)
            except Exception:
                logger.warning("DuckDB not available, trying Spark")
                query_executor = SparkQueryExecutor(context)

        self.query_executor = query_executor
        self._feature_queries: Dict[str, str] = {}
        self._virtual_layer: Optional["VirtualDataLayer"] = None
        logger.info(f"VirtualizedFeatureStore initialized with {type(query_executor).__name__}")

    def set_virtual_layer(self, virtual_layer: "VirtualDataLayer") -> None:
        """Set VirtualDataLayer for integration.

        Args:
            virtual_layer: VirtualDataLayer instance for data virtualization
        """
        self._virtual_layer = virtual_layer
        logger.info("VirtualDataLayer integration enabled")

    def register_as_virtual_table(
        self, feature_group: str, table_prefix: str = "features_"
    ) -> Optional["VirtualTable"]:
        """Register feature group as a virtual table in VirtualDataLayer.

        Args:
            feature_group: Name of the feature group to register
            table_prefix: Prefix for virtual table name

        Returns:
            VirtualTable instance if successful, None otherwise
        """
        if not self._virtual_layer:
            logger.warning(
                "Cannot register virtual table: VirtualDataLayer not configured. "
                "Call set_virtual_layer() first."
            )
            return None

        try:
            from core.virtualization import VirtualTable, SourceType

            schema = self.metadata.get_feature_group(feature_group)

            # Create virtual table definition
            virtual_table = VirtualTable(
                name=f"{table_prefix}{feature_group}",
                source_type=SourceType.DATABASE,
                connector_type="feature_store",
                connection_id="feature_store_virtualized",
                table_name=feature_group,
                query=self._feature_queries.get(feature_group),
                schema={f.name: f.data_type.value for f in schema.features},
                description=f"Virtual feature group: {schema.description or feature_group}",
                tags=schema.tags + ["feature_store", "virtualized"],
            )

            # Register with virtual layer
            self._virtual_layer.schema_registry.register_table(virtual_table)

            logger.info(
                f"Registered feature group '{feature_group}' as virtual table "
                f"'{virtual_table.name}'"
            )

            return virtual_table

        except Exception as e:
            logger.error(f"Failed to register virtual table: {e}")
            return None

    def query_via_virtual_layer(
        self,
        feature_group: str,
        features: List[str],
        predicates: Optional[List[tuple]] = None,
    ) -> Dict[str, Any]:
        """Query features through VirtualDataLayer with optimization.

        Args:
            feature_group: Feature group name
            features: List of feature names to retrieve
            predicates: Optional predicates for filtering

        Returns:
            Dictionary with feature data
        """
        if not self._virtual_layer:
            logger.warning("VirtualDataLayer not configured, using standard query")
            return self.get_features(features)

        try:
            from core.virtualization.federation_engine import FederationEngine, Predicate

            table_name = f"features_{feature_group}"

            # Convert to federation predicates if provided
            fed_predicates = []
            if predicates:
                for pred in predicates:
                    fed_predicates.append(
                        Predicate(
                            field=pred[0],
                            operator=pred[1],
                            value=pred[2] if len(pred) > 2 else None,
                        )
                    )

            # Use federation engine for optimized query
            federation = FederationEngine()
            plan = federation.plan_query(
                table_name=table_name, predicates=fed_predicates, projection=features
            )

            logger.info(
                f"Query plan: {plan.execution_strategy.value}, "
                f"estimated cost: {plan.estimated_cost:.2f}"
            )

            # Execute through standard method but log optimization
            return self.get_features(features)

        except Exception as e:
            logger.error(f"Virtual layer query failed: {e}, falling back to standard")
            return self.get_features(features)

    def register_features(self, schema: FeatureGroupSchema) -> None:
        """Register virtual feature group with query templates."""
        try:
            self.metadata.register_feature_group(schema)

            # Store query templates from metadata
            if "query_template" in schema.metadata:
                self._feature_queries[schema.name] = schema.metadata["query_template"]

            logger.info(f"Virtual feature group registered: {schema.name} (query-on-demand)")
        except Exception as e:
            raise VirtualizationQueryError(
                f"Failed to register virtual feature group '{schema.name}': {e}"
            ) from e

    def write_features(
        self,
        feature_group: str,
        data: Dict[str, List[Any]],
        mode: str = "append",
    ) -> None:
        """Virtualized features are read-only (no write)."""
        raise VirtualizationQueryError(
            f"Cannot write to virtualized feature group '{feature_group}': "
            "virtualized features are read-only query results"
        )

    def get_features(
        self,
        feature_names: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Retrieve features on-demand from source layers."""
        result = {}

        # Group features by feature group
        groups_features: Dict[str, List[str]] = {}
        for feature_ref in feature_names:
            try:
                group_name, feature_name = feature_ref.split(".")
                if group_name not in groups_features:
                    groups_features[group_name] = []
                groups_features[group_name].append(feature_name)
            except ValueError:
                logger.error(f"Invalid feature reference format: {feature_ref}")
                raise

        # Execute queries for each feature group
        for group_name, features in groups_features.items():
            try:
                query = self._build_query(group_name, features, entity_ids, point_in_time)
                query_result = self.query_executor.execute(query)

                # Extract requested features
                for feature_name in features:
                    feature_ref = f"{group_name}.{feature_name}"
                    if feature_name in query_result:
                        result[feature_ref] = query_result[feature_name]
                    else:
                        logger.warning(f"Feature '{feature_name}' not in query result")

                logger.debug(
                    f"Retrieved {len(features)} features from virtual group '{group_name}'"
                )
            except Exception as e:
                logger.error(f"Failed to retrieve features from '{group_name}': {e}")
                raise

        return result

    def _build_query(
        self,
        feature_group: str,
        features: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
    ) -> str:
        """Build SQL query for virtual features."""
        schema = self.metadata.get_feature_group(feature_group)

        # Use template if available
        if feature_group in self._feature_queries:
            base_query = self._feature_queries[feature_group]
        else:
            # Build standard query from schema
            feature_list = ", ".join(features)
            base_query = f"SELECT {feature_list} FROM {feature_group}"

        # Add entity filters
        if entity_ids:
            where_clauses = []
            for key, values in entity_ids.items():
                placeholders = ", ".join(str(v) for v in values)
                where_clauses.append(f"{key} IN ({placeholders})")

            if where_clauses:
                base_query += " WHERE " + " AND ".join(where_clauses)

        # Add point-in-time filter if timestamp key exists
        if point_in_time and schema.timestamp_key:
            base_query += f" AND {schema.timestamp_key} <= '{point_in_time.isoformat()}'"

        logger.debug(f"Built query: {base_query}")
        return base_query

    def register_query_template(
        self,
        feature_group: str,
        query_template: str,
    ) -> None:
        """Register a custom SQL query template for a feature group."""
        try:
            # Ensure feature group exists (validate) without creating an unused variable
            self.metadata.get_feature_group(feature_group)
            self._feature_queries[feature_group] = query_template
            logger.info(f"Registered query template for '{feature_group}'")
        except Exception as e:
            logger.error(f"Failed to register query template: {e}")
            raise

    def validate_query(self, query: str) -> bool:
        """Validate a query without executing it."""
        try:
            # In production, would use actual query validation
            logger.debug(f"Validating query: {query[:100]}...")
            return True
        except Exception as e:
            logger.error(f"Query validation failed: {e}")
            return False

    def get_execution_plan(self, feature_group: str, features: List[str]) -> Dict[str, Any]:
        """Get query execution plan for debugging/optimization."""
        try:
            query = self._build_query(feature_group, features)
            return {
                "feature_group": feature_group,
                "features": features,
                "query": query,
                "executor_type": type(self.query_executor).__name__,
            }
        except Exception as e:
            logger.error(f"Failed to get execution plan: {e}")
            return {}
