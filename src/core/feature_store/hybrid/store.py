"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import time

from loguru import logger  # type: ignore

from core.feature_store.base import BaseFeatureStore, FeatureStoreConfig, FeatureStoreMode
from core.feature_store.schema import FeatureGroupSchema
from core.feature_store.materialized import MaterializedFeatureStore
from core.feature_store.virtualized import VirtualizedFeatureStore
from core.feature_store.exceptions import FeatureStoreException


@dataclass
class AccessMetrics:
    """Track access patterns for hybrid mode decisions."""

    feature_group: str
    access_count: int = 0
    cache_hits: int = 0
    avg_query_time_ms: float = 0.0
    last_accessed: Optional[datetime] = None
    estimated_row_count: int = 0

    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate."""
        if self.access_count == 0:
            return 0.0
        return self.cache_hits / self.access_count

    def should_materialize(self, threshold: int) -> bool:
        """Decide if feature group should be materialized."""
        # Materialize if frequently accessed or large dataset
        if self.access_count > 10 and self.cache_hit_rate < 0.3:
            return True
        if self.estimated_row_count > threshold:
            return True
        return False


class HybridFeatureStore(BaseFeatureStore):
    """
    Hybrid Feature Store that intelligently switches between strategies.
    """

    def __init__(
        self,
        context: Any,
        config: Optional[FeatureStoreConfig] = None,
    ):
        """Initialize Hybrid Feature Store."""
        super().__init__(context)

        if config is None:
            config = FeatureStoreConfig(mode=FeatureStoreMode.HYBRID)

        self.config = config

        # Initialize both stores
        self.materialized_store = MaterializedFeatureStore(
            context=context,
            storage_path=config.storage_path,
            storage_format=config.storage_format,
        )

        self.virtualized_store = VirtualizedFeatureStore(context=context)

        # Track access patterns
        self._access_metrics: Dict[str, AccessMetrics] = {}

        # Cache for materialization decisions
        self._materialization_cache: Dict[str, bool] = {}

        # Share metadata registry
        self.metadata = self.materialized_store.metadata

        logger.info(
            f"HybridFeatureStore initialized with threshold={config.hybrid_threshold_rows} rows"
        )

    def set_virtual_layer(self, virtual_layer: Any) -> None:
        """Set VirtualDataLayer for both stores.

        Args:
            virtual_layer: VirtualDataLayer instance
        """
        self.virtualized_store.set_virtual_layer(virtual_layer)
        logger.info("VirtualDataLayer configured for hybrid store")

    def register_features(self, schema: FeatureGroupSchema) -> None:
        """Register feature group in both stores."""
        # Register in materialized store (handles metadata)
        self.materialized_store.register_features(schema)

        # If virtualization enabled, also register there
        if self.config.enable_virtualization:
            self.virtualized_store.register_features(schema)

        # Initialize access metrics
        self._access_metrics[schema.name] = AccessMetrics(feature_group=schema.name)

        logger.info(f"Registered feature group '{schema.name}' in hybrid mode")

    def write_features(
        self,
        feature_group: str,
        data: Union[Dict[str, List[Any]], Any],
        mode: str = "append",
    ) -> None:
        """Write features to materialized store."""
        # Always write to materialized store
        self.materialized_store.write_features(
            feature_group=feature_group,
            data=data,
            mode=mode,
        )

        # Update metrics
        if feature_group in self._access_metrics:
            if isinstance(data, dict) and data:
                first_key = next(iter(data))
                self._access_metrics[feature_group].estimated_row_count = len(data[first_key])

        logger.debug(f"Features written to materialized store: {feature_group}")

    def get_features(
        self,
        feature_names: List[str],
        entity_ids: Optional[Dict[str, List[Any]]] = None,
        point_in_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Retrieve features using optimal strategy."""
        start_time = time.time()

        # Determine which feature groups are needed
        feature_groups = set()
        for feature_ref in feature_names:
            if "." in feature_ref:
                group_name = feature_ref.split(".")[0]
                feature_groups.add(group_name)

        # Decide strategy for each feature group
        use_materialized = self._should_use_materialized(
            list(feature_groups), entity_ids, point_in_time
        )

        result: Dict[str, Any] = {}

        # Prepare group -> features mapping and ensure metrics
        group_to_features = {
            group: [f for f in feature_names if f.startswith(f"{group}.")]
            for group in feature_groups
        }

        for group in feature_groups:
            if group not in self._access_metrics:
                self._access_metrics[group] = AccessMetrics(feature_group=group)
            metrics = self._access_metrics[group]
            metrics.access_count += 1
            metrics.last_accessed = datetime.now()

        # Fetch each group's features using a helper to reduce complexity
        for group, group_features in group_to_features.items():
            self._fetch_group_features(
                group=group,
                group_features=group_features,
                use_materialized_flag=use_materialized.get(group, True),
                entity_ids=entity_ids,
                point_in_time=point_in_time,
                result=result,
            )

        # Update query time metrics
        query_time = (time.time() - start_time) * 1000  # ms
        for group in feature_groups:
            metrics = self._access_metrics[group]
            # Running average
            metrics.avg_query_time_ms = (
                metrics.avg_query_time_ms * (metrics.access_count - 1) + query_time
            ) / metrics.access_count

        # Auto-materialize if needed
        if self.config.auto_materialize_on_read:
            self._check_auto_materialize(list(feature_groups))

        logger.info(
            f"Retrieved {len(feature_names)} features in {query_time:.2f}ms "
            f"using hybrid strategy"
        )

        return result

    def _fetch_group_features(
        self,
        group: str,
        group_features: List[str],
        use_materialized_flag: bool,
        entity_ids: Optional[Dict[str, List[Any]]],
        point_in_time: Optional[datetime],
        result: Dict[str, Any],
    ) -> None:
        """Fetch features for a single group, handling fallback and metrics updates."""
        metrics = self._access_metrics[group]
        try:
            if use_materialized_flag:
                logger.debug(f"Using MATERIALIZED strategy for '{group}'")
                group_result = self.materialized_store.get_features(
                    feature_names=group_features,
                    entity_ids=entity_ids,
                    point_in_time=point_in_time,
                )
                metrics.cache_hits += 1
            else:
                logger.debug(f"Using VIRTUALIZED strategy for '{group}'")
                group_result = self.virtualized_store.get_features(
                    feature_names=group_features,
                    entity_ids=entity_ids,
                    point_in_time=point_in_time,
                )
            result.update(group_result)
        except Exception as e:
            logger.warning(f"Primary strategy failed for '{group}': {e}, trying fallback")
            if use_materialized_flag:
                # Fallback to virtualized
                group_result = self.virtualized_store.get_features(
                    feature_names=group_features,
                    entity_ids=entity_ids,
                    point_in_time=point_in_time,
                )
            else:
                # Fallback to materialized
                group_result = self.materialized_store.get_features(
                    feature_names=group_features,
                    entity_ids=entity_ids,
                    point_in_time=point_in_time,
                )
                metrics.cache_hits += 1
            result.update(group_result)

    def _should_use_materialized(
        self,
        feature_groups: List[str],
        entity_ids: Optional[Dict[str, List[Any]]],
        point_in_time: Optional[datetime],
    ) -> Dict[str, bool]:
        """Decide which strategy to use for each feature group."""
        decisions = {}

        for group in feature_groups:
            # Check cache first
            if group in self._materialization_cache:
                cache_time = self._access_metrics[group].last_accessed
                if (
                    cache_time
                    and (datetime.now() - cache_time).seconds < self.config.hybrid_cache_ttl
                ):
                    decisions[group] = self._materialization_cache[group]
                    continue

            # Get metrics
            metrics = self._access_metrics.get(group)

            if not metrics:
                # Default to materialized for unknown groups
                decisions[group] = True
                continue

            # Delegate complex rule evaluation to a helper to reduce cognitive complexity
            use_materialized = self._decide_materialized_for_metrics(
                metrics, entity_ids, point_in_time
            )

            decisions[group] = use_materialized
            self._materialization_cache[group] = use_materialized

            logger.debug(
                f"Strategy decision for '{group}': "
                f"{'MATERIALIZED' if use_materialized else 'VIRTUALIZED'} "
                f"(rows={metrics.estimated_row_count}, "
                f"access_count={metrics.access_count}, "
                f"cache_rate={metrics.cache_hit_rate:.2f})"
            )

        return decisions

    def _decide_materialized_for_metrics(
        self,
        metrics: AccessMetrics,
        entity_ids: Optional[Dict[str, List[Any]]],
        point_in_time: Optional[datetime],
    ) -> bool:
        """Helper to evaluate materialization rules with clear precedence."""
        # Evaluate simple boolean flags
        recent_pit = bool(point_in_time and (datetime.now() - point_in_time) < timedelta(hours=1))
        complex_filters = bool(entity_ids and len(entity_ids) > 3)
        large_dataset = metrics.estimated_row_count > self.config.hybrid_threshold_rows
        high_cache = metrics.cache_hit_rate > 0.7
        frequent = metrics.access_count > 50

        # Apply precedence matching original logic:
        # 1) recent point-in-time -> virtualized
        if recent_pit:
            return False
        # 2) large dataset -> materialized
        if large_dataset:
            return True
        # 3) high cache hit rate or frequently accessed -> materialized
        if high_cache or frequent:
            return True
        # 4) complex filters -> virtualized
        if complex_filters:
            return False
        # Default
        return True

    def _check_auto_materialize(self, feature_groups: List[str]) -> None:
        """Check if features should be auto-materialized."""
        for group in feature_groups:
            metrics = self._access_metrics.get(group)

            if not metrics:
                continue

            if metrics.should_materialize(self.config.hybrid_threshold_rows):
                logger.info(
                    f"Auto-materializing feature group '{group}' "
                    f"(access_count={metrics.access_count}, "
                    f"cache_rate={metrics.cache_hit_rate:.2f})"
                )

                # Trigger materialization
                try:
                    # This would trigger a background job in production
                    self.materialized_store.refresh_features(group)
                except Exception as e:
                    logger.warning(f"Auto-materialization failed for '{group}': {e}")

    def get_access_metrics(self, feature_group: Optional[str] = None) -> Dict[str, Any]:
        """Get access metrics for monitoring and optimization."""
        if feature_group:
            metrics = self._access_metrics.get(feature_group)
            if not metrics:
                return {}
            return {
                "feature_group": metrics.feature_group,
                "access_count": metrics.access_count,
                "cache_hit_rate": metrics.cache_hit_rate,
                "avg_query_time_ms": metrics.avg_query_time_ms,
                "last_accessed": metrics.last_accessed.isoformat()
                if metrics.last_accessed
                else None,
                "estimated_row_count": metrics.estimated_row_count,
            }
        else:
            return {
                group: {
                    "access_count": m.access_count,
                    "cache_hit_rate": m.cache_hit_rate,
                    "avg_query_time_ms": m.avg_query_time_ms,
                    "estimated_row_count": m.estimated_row_count,
                }
                for group, m in self._access_metrics.items()
            }

    def force_strategy(self, feature_group: str, use_materialized: bool) -> None:
        """Force a specific strategy for a feature group."""
        self._materialization_cache[feature_group] = use_materialized
        logger.info(
            f"Forced strategy for '{feature_group}': "
            f"{'MATERIALIZED' if use_materialized else 'VIRTUALIZED'}"
        )

    def optimize_strategies(self) -> Dict[str, str]:
        """Analyze access patterns and recommend optimal strategies."""
        recommendations = {}

        for group, metrics in self._access_metrics.items():
            if metrics.should_materialize(self.config.hybrid_threshold_rows):
                recommendations[group] = "MATERIALIZED"
            else:
                recommendations[group] = "VIRTUALIZED"

        logger.info(f"Strategy recommendations generated for {len(recommendations)} groups")
        return recommendations
