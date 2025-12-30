"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root directory.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Iterator, Tuple
from abc import ABC, abstractmethod
import time
import re

from loguru import logger  # type: ignore


def sanitize_sql_value(value: Any) -> str:
    """Sanitize value for SQL to prevent injection."""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    else:
        # Escape single quotes by doubling them (standard SQL)
        str_value = str(value).replace("'", "''")
        # Remove any dangerous SQL keywords/patterns
        dangerous_patterns = [";--", "/*", "*/", "xp_", "sp_", "EXEC", "EXECUTE"]
        for pattern in dangerous_patterns:
            str_value = str_value.replace(pattern, "")
        return f"'{str_value}'"


def sanitize_sql_identifier(identifier: str) -> str:
    """Sanitize SQL identifier (table/column names)."""
    # Only allow alphanumeric, underscore, and dot
    if not re.match(r"^[a-zA-Z0-9_.]+$", identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


class ExecutionStrategy(Enum):
    """Query execution strategies."""

    DIRECT = "direct"  # Execute directly against source
    CACHE = "cache"  # Use cached result
    MATERIALIZED = "materialized"  # Use materialized view
    STREAM = "stream"  # Stream result
    DISTRIBUTED = "distributed"  # Federated across sources


class PredicateOperator(Enum):
    """Supported predicate operators."""

    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass
class Predicate:
    """Represents a filter predicate."""

    field: str
    operator: PredicateOperator
    value: Optional[Any] = None

    def to_sql(self) -> str:
        """Convert predicate to SQL WHERE clause with sanitization."""
        safe_field = sanitize_sql_identifier(self.field)

        if self.operator == PredicateOperator.IS_NULL:
            return f"{safe_field} IS NULL"
        elif self.operator == PredicateOperator.IS_NOT_NULL:
            return f"{safe_field} IS NOT NULL"
        elif self.operator == PredicateOperator.IN:
            if isinstance(self.value, list):
                safe_values = ", ".join(sanitize_sql_value(v) for v in self.value)
            else:
                safe_values = sanitize_sql_value(self.value)
            return f"{safe_field} IN ({safe_values})"
        elif self.operator == PredicateOperator.LIKE:
            safe_value = sanitize_sql_value(self.value)
            return f"{safe_field} LIKE {safe_value}"
        else:
            safe_value = sanitize_sql_value(self.value)
            return f"{safe_field} {self.operator.value} {safe_value}"

    def is_pushdown_compatible(self, supported_operators: List[PredicateOperator]) -> bool:
        """Check if this predicate can be pushed down to source."""
        return self.operator in supported_operators


@dataclass
class QueryPlan:
    """
    Optimized execution plan for a query.
    """

    table_name: str
    predicates: List[Predicate] = field(default_factory=list)
    pushdown_predicates: List[Predicate] = field(default_factory=list)
    projection_fields: List[str] = field(default_factory=list)  # Column list
    limit: Optional[int] = None
    offset: Optional[int] = None

    execution_strategy: ExecutionStrategy = ExecutionStrategy.DIRECT
    estimated_cost: float = 0.0
    estimated_rows: int = 0
    estimated_bytes: int = 0

    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    planner_version: str = "1.0"

    def get_filter_selectivity(self) -> float:
        """
        Estimate filter selectivity (0.0 = all rows, 1.0 = no rows).
        """
        # Simplified heuristic: each predicate reduces by ~50%
        return max(0.0, 1.0 - (len(self.predicates) * 0.3))


@dataclass
class JoinPlan:
    """Plan for executing joins between virtual tables."""

    left_table: str
    right_table: str
    join_type: str = "INNER"  # INNER, LEFT, RIGHT, FULL
    join_condition: Optional[str] = None
    join_order: List[str] = field(default_factory=list)
    estimated_cost: float = 0.0

    def estimate_join_cost(self, left_rows: int, right_rows: int) -> float:
        """Estimate cost of join operation."""
        # Simplified cost model: O(n log n) for sorted merge join
        import math

        return (left_rows + right_rows) * math.log(max(left_rows, right_rows) + 1)


@dataclass
class QueryStatistics:
    """Statistics about a query execution."""

    query_id: str
    table_name: str
    execution_time_ms: float
    rows_returned: int
    bytes_returned: int
    execution_strategy: ExecutionStrategy
    cache_hit: bool = False
    executed_at: datetime = field(default_factory=datetime.utcnow)


class StatCollectingIterator:
    """Iterator wrapper that collects statistics as data is consumed."""

    def __init__(self, data: Iterator[Dict[str, Any]], stats: QueryStatistics, start_time: float):
        """Initialize stat collector.

        Args:
            data: Source data iterator
            stats: QueryStatistics object to update
            start_time: Query start time for elapsed calculation
        """
        self._data = data
        self._stats = stats
        self._start_time = start_time
        self._consumed = False

    def __iter__(self):
        """Return iterator."""
        return self

    def __next__(self) -> Dict[str, Any]:
        """Get next row and update statistics."""
        try:
            row = next(self._data)
            self._stats.rows_returned += 1
            self._stats.bytes_returned += len(str(row).encode("utf-8"))
            return row
        except StopIteration:
            # Update execution time when iterator is exhausted
            if not self._consumed:
                self._stats.execution_time_ms = (time.time() - self._start_time) * 1000
                self._consumed = True
            raise


class QueryOptimizer:
    """
    Optimizes queries for efficient execution.
    """

    def __init__(self):
        """Initialize query optimizer."""
        self._statistics: Dict[str, QueryStatistics] = {}
        logger.debug("QueryOptimizer initialized")

    def optimize_query(
        self,
        table_name: str,
        predicates: List[Predicate],
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        supports_pushdown: bool = True,
    ) -> QueryPlan:
        """
        Create optimized query plan.
        """
        plan = QueryPlan(
            table_name=table_name,
            predicates=predicates,
            projection_fields=projection or [],
            limit=limit,
        )

        # Apply predicate pushdown if supported
        if supports_pushdown:
            plan.pushdown_predicates = predicates.copy()
            logger.debug(f"Pushing down {len(predicates)} predicates")

        # Estimate selectivity
        selectivity = plan.get_filter_selectivity()
        plan.estimated_rows = max(1, int(1000000 * selectivity))  # Assume 1M base rows
        plan.estimated_bytes = plan.estimated_rows * 100  # Rough estimate: 100 bytes/row

        # Estimate cost (lower is better)
        plan.estimated_cost = self._estimate_cost(plan)

        # Choose execution strategy
        plan.execution_strategy = self._choose_strategy(plan)

        return plan

    def optimize_join(self, tables: List[str], join_predicates: Dict[str, str]) -> JoinPlan:
        """
        Optimize join order between virtual tables.
        """
        if len(tables) < 2:
            raise ValueError("Need at least 2 tables to join")

        # Simplified: assume tables ordered by size (smallest first)
        join_plan = JoinPlan(
            left_table=tables[0],
            right_table=tables[1],
            join_order=tables,
            join_condition=list(join_predicates.values())[0] if join_predicates else None,
        )

        # Estimate join cost
        # In production: use actual table statistics
        left_rows = 100000
        right_rows = 50000
        join_plan.estimated_cost = join_plan.estimate_join_cost(left_rows, right_rows)

        logger.info(f"Optimized join: {' -> '.join(tables)}")
        return join_plan

    def _estimate_cost(self, plan: QueryPlan) -> float:
        """
        Estimate execution cost of query plan.
        """

        base_cost = plan.estimated_rows / 10000  # Normalize to 10K rows = cost 1

        # Add predicate cost
        predicate_cost = len(plan.pushdown_predicates) * 0.1

        # Add projection overhead if not all fields
        projection_cost = 0.05 if plan.projection_fields else 0

        total_cost = base_cost + predicate_cost + projection_cost

        return max(0.1, total_cost)

    def _choose_strategy(self, plan: QueryPlan) -> ExecutionStrategy:
        """
        Choose optimal execution strategy for plan.
        """
        if plan.estimated_cost < 0.5:
            return ExecutionStrategy.DIRECT
        elif plan.estimated_cost < 10:
            return ExecutionStrategy.CACHE
        else:
            return ExecutionStrategy.MATERIALIZED

    def record_statistics(self, stats: QueryStatistics) -> None:
        """Record statistics for a query execution."""
        self._statistics[stats.query_id] = stats

    def get_statistics(self, query_id: str) -> Optional[QueryStatistics]:
        """Get statistics for a query."""
        return self._statistics.get(query_id)

    def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get metrics about query optimization effectiveness."""
        if not self._statistics:
            return {}

        cache_hits = sum(1 for s in self._statistics.values() if s.cache_hit)
        avg_time = sum(s.execution_time_ms for s in self._statistics.values()) / len(
            self._statistics
        )

        return {
            "total_queries": len(self._statistics),
            "cache_hits": cache_hits,
            "cache_hit_rate": cache_hits / len(self._statistics),
            "avg_execution_time_ms": avg_time,
            "min_execution_time_ms": min(s.execution_time_ms for s in self._statistics.values()),
            "max_execution_time_ms": max(s.execution_time_ms for s in self._statistics.values()),
        }


class FederationEngine:
    """
    Main federation engine for executing queries across virtual tables.
    """

    def __init__(self, query_optimizer: Optional[QueryOptimizer] = None):
        """
        Initialize federation engine."""
        self.query_optimizer = query_optimizer or QueryOptimizer()
        self._execution_contexts: Dict[str, Dict[str, Any]] = {}
        logger.debug("FederationEngine initialized")

    def plan_query(
        self,
        table_name: str,
        predicates: List[Predicate],
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        supports_pushdown: bool = True,
    ) -> QueryPlan:
        """
        Create optimized plan for query.
        """
        plan = self.query_optimizer.optimize_query(
            table_name=table_name,
            predicates=predicates,
            projection=projection,
            limit=limit,
            supports_pushdown=supports_pushdown,
        )

        logger.info(
            f"Query plan created: {table_name} "
            f"(cost={plan.estimated_cost:.2f}, "
            f"strategy={plan.execution_strategy.value})"
        )

        return plan

    def execute_query(
        self,
        plan: QueryPlan,
        executor_func,  # Callable to fetch data from source
        query_id: Optional[str] = None,
    ) -> Tuple[Iterator[Dict[str, Any]], QueryStatistics]:
        """
        Execute query plan and collect statistics.
        """
        if not query_id:
            query_id = f"q_{int(time.time() * 1000)}"

        start_time = time.time()

        try:
            # Execute against source
            data = executor_func(plan)

            # Create statistics object (will be updated as iterator is consumed)
            stats = QueryStatistics(
                query_id=query_id,
                table_name=plan.table_name,
                execution_time_ms=0.0,  # Updated when iterator exhausted
                rows_returned=0,  # Updated per row
                bytes_returned=0,  # Updated per row
                execution_strategy=plan.execution_strategy,
                cache_hit=plan.execution_strategy == ExecutionStrategy.CACHE,
            )

            # Wrap iterator to collect statistics
            tracked_iterator = StatCollectingIterator(data, stats, start_time)

            self.query_optimizer.record_statistics(stats)

            logger.info(f"Query started: {query_id} (strategy={plan.execution_strategy.value})")

            return tracked_iterator, stats

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def plan_federated_join(self, tables: List[str], join_predicates: Dict[str, str]) -> JoinPlan:
        """
        Plan a federated join across multiple virtual tables.
        """
        return self.query_optimizer.optimize_join(tables, join_predicates)
