"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Core virtualization module for Tauro data pipeline framework.
Enables unified access to heterogeneous data sources without physical data movement.
"""

from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Iterator, Protocol
from abc import ABC, abstractmethod
from pathlib import Path

from loguru import logger  # type: ignore


class SourceType(Enum):
    """Supported virtual data source types."""

    DATABASE = "database"
    FILESYSTEM = "filesystem"
    API = "api"
    STREAM = "stream"
    DATA_WAREHOUSE = "data_warehouse"


class CacheStrategy(Enum):
    """Caching strategies for virtual data."""

    NEVER = "never"
    ALWAYS = "always"
    SMART = "smart"
    PERIODIC = "periodic"


@dataclass
class EncryptionConfig:
    """Field-level encryption configuration."""

    enabled: bool = False
    fields: List[str] = field(default_factory=list)
    algorithm: str = "AES-256-GCM"
    key_vault_backend: str = "aws-secrets"
    key_vault_path: str = ""


@dataclass
class VirtualTable:
    """Represents a virtual table abstraction over physical data source."""

    name: str
    source_type: SourceType
    connector_type: str  # "postgresql", "snowflake", "s3", "kafka", etc.
    connection_id: str

    # Source location/query
    query: Optional[str] = None  # SQL query for databases
    table_name: Optional[str] = None  # Table name if not query-based
    path: Optional[str] = None  # File path for filesystem sources

    # Schema and metadata
    schema: Dict[str, str] = field(default_factory=dict)  # {column: type}
    partitions: List[str] = field(default_factory=list)

    # Data lineage
    source_tables: List[str] = field(default_factory=list)
    transformation: Optional[str] = None  # Description of any transforms

    # Caching
    cache_strategy: CacheStrategy = CacheStrategy.SMART
    cache_ttl_seconds: int = 3600

    # Security
    encryption_config: Optional[EncryptionConfig] = None
    access_control: Dict[str, List[str]] = field(default_factory=dict)  # {role: [fields]}

    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    description: str = ""
    tags: List[str] = field(default_factory=list)

    def validate(self) -> bool:
        """Validate virtual table configuration."""
        if not self.name or not self.connector_type:
            raise ValueError("name and connector_type are required")

        # Source must be specified
        if self.source_type == SourceType.DATABASE and not (self.query or self.table_name):
            raise ValueError("Database source requires query or table_name")

        if self.source_type == SourceType.FILESYSTEM and not self.path:
            raise ValueError("Filesystem source requires path")

        if self.source_type == SourceType.STREAM and not self.table_name:
            raise ValueError("Stream source requires table_name (topic/stream name)")

        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "source_type": self.source_type.value,
            "connector_type": self.connector_type,
            "connection_id": self.connection_id,
            "query": self.query,
            "table_name": self.table_name,
            "path": self.path,
            "schema": self.schema,
            "partitions": self.partitions,
            "source_tables": self.source_tables,
            "transformation": self.transformation,
            "cache_strategy": self.cache_strategy.value,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "encryption_config": self.encryption_config.__dict__
            if self.encryption_config
            else None,
            "access_control": self.access_control,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "description": self.description,
            "tags": self.tags,
        }


@dataclass
class TableStatistics:
    """Statistics about a virtual table."""

    table_name: str
    row_count: int
    size_bytes: int
    partition_count: int = 0
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    access_count: int = 0
    cache_hit_rate: float = 0.0


class SchemaRegistry:
    """
    Central catalog for virtual table definitions.

    Manages:
    - Virtual table registration and lookup
    - Schema versioning and evolution
    - Data lineage tracking
    - Table statistics

    No data is stored, only metadata.
    """

    def __init__(self):
        """Initialize empty schema registry."""
        self._tables: Dict[str, VirtualTable] = {}
        self._statistics: Dict[str, TableStatistics] = {}
        self._lineage_graph: Dict[str, List[str]] = {}
        logger.debug("SchemaRegistry initialized")

    def register_table(self, table: VirtualTable) -> None:
        """
        Register a new virtual table.

        Args:
            table: VirtualTable instance to register

        Raises:
            ValueError: If table validation fails or name conflicts
        """
        if not table.validate():
            raise ValueError(f"Invalid table configuration: {table.name}")

        if table.name in self._tables:
            logger.warning(f"Virtual table '{table.name}' already exists, overwriting")

        self._tables[table.name] = table
        self._update_lineage(table)
        logger.info(f"Virtual table registered: {table.name}")

    def get_table(self, name: str) -> Optional[VirtualTable]:
        """
        Retrieve virtual table definition.

        Args:
            name: Virtual table name

        Returns:
            VirtualTable if found, None otherwise
        """
        return self._tables.get(name)

    def list_tables(
        self,
        source_type: Optional[SourceType] = None,
        connector_type: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> List[VirtualTable]:
        """
        List registered virtual tables with optional filtering.

        Args:
            source_type: Filter by source type
            connector_type: Filter by connector (e.g., "postgresql")
            tag: Filter by tag

        Returns:
            List of matching VirtualTable objects
        """
        results = list(self._tables.values())

        if source_type:
            results = [t for t in results if t.source_type == source_type]

        if connector_type:
            results = [t for t in results if t.connector_type == connector_type]

        if tag:
            results = [t for t in results if tag in t.tags]

        return results

    def update_table(self, name: str, **kwargs) -> None:
        """
        Update virtual table metadata.

        Args:
            name: Virtual table name
            **kwargs: Fields to update
        """
        if name not in self._tables:
            raise ValueError(f"Virtual table not found: {name}")

        table = self._tables[name]
        for key, value in kwargs.items():
            if hasattr(table, key):
                setattr(table, key, value)

        table.updated_at = datetime.now(timezone.utc)
        self._update_lineage(table)
        logger.info(f"Virtual table updated: {name}")

    def delete_table(self, name: str) -> None:
        """
        Delete virtual table definition.

        Args:
            name: Virtual table name
        """
        if name not in self._tables:
            raise ValueError(f"Virtual table not found: {name}")

        del self._tables[name]
        if name in self._statistics:
            del self._statistics[name]
        if name in self._lineage_graph:
            del self._lineage_graph[name]

        logger.info(f"Virtual table deleted: {name}")

    def get_table_statistics(self, name: str) -> Optional[TableStatistics]:
        """Get statistics for a virtual table."""
        return self._statistics.get(name)

    def update_table_statistics(self, stats: TableStatistics) -> None:
        """Update statistics for a virtual table."""
        self._statistics[stats.table_name] = stats

    def get_lineage(self, table_name: str) -> Dict[str, List[str]]:
        """
        Get data lineage for a table.

        Returns:
            Dict with 'upstream' and 'downstream' tables
        """
        upstream = self._lineage_graph.get(table_name, [])
        downstream = [t for t, deps in self._lineage_graph.items() if table_name in deps]

        return {
            "upstream": upstream,
            "downstream": downstream,
            "table": table_name,
        }

    def _update_lineage(self, table: VirtualTable) -> None:
        """Update lineage graph for a table."""
        self._lineage_graph[table.name] = table.source_tables

    def validate_schema_compatibility(self, table1_name: str, table2_name: str) -> bool:
        """
        Check if two tables have compatible schemas for joining.

        Args:
            table1_name: First table name
            table2_name: Second table name

        Returns:
            True if schemas are compatible
        """
        t1 = self.get_table(table1_name)
        t2 = self.get_table(table2_name)

        if not t1 or not t2:
            return False

        # Check for overlapping columns with same types
        common_cols = set(t1.schema.keys()) & set(t2.schema.keys())
        for col in common_cols:
            if t1.schema[col] != t2.schema[col]:
                logger.warning(f"Schema mismatch for column {col}")
                return False

        return True

    def export_catalog(self, format: str = "json") -> Dict[str, Any]:
        """
        Export schema catalog for documentation/sharing.

        Args:
            format: Export format (currently only "json")

        Returns:
            Dictionary representation of catalog
        """
        return {
            "tables": {name: table.to_dict() for name, table in self._tables.items()},
            "statistics": {
                name: {
                    "row_count": stats.row_count,
                    "size_bytes": stats.size_bytes,
                    "last_updated": stats.last_updated.isoformat(),
                    "cache_hit_rate": stats.cache_hit_rate,
                }
                for name, stats in self._statistics.items()
            },
            "lineage": self._lineage_graph,
            "exported_at": datetime.now(timezone.utc).isoformat(),
        }


class DataConnector(Protocol):
    """Protocol for data connectors in virtualization layer."""

    def connect(self) -> None:
        """Establish connection to data source."""
        ...

    def disconnect(self) -> None:
        """Close connection to data source."""
        ...

    def fetch_schema(self, source: str) -> Dict[str, str]:
        """Get schema for data source."""
        ...

    def fetch_data(
        self, source: str, filters: Optional[List[tuple]] = None, limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch data from source with optional filters.

        Args:
            source: Source identifier (query, table name, path, etc.)
            filters: List of (column, operator, value) tuples
            limit: Maximum rows to return

        Yields:
            Row dictionaries
        """
        ...

    def supports_pushdown(self) -> bool:
        """Check if connector supports predicate pushdown."""
        ...


class VirtualDataLayer:
    """
    Main virtualization layer interface.

    Coordinates:
    - Schema registry
    - Query federation
    - Access control
    - Caching
    - Connectors
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize virtual data layer.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.schema_registry = SchemaRegistry()
        self._connectors: Dict[str, Any] = {}
        logger.info("VirtualDataLayer initialized")

    def register_connector(self, connector_id: str, connector: DataConnector) -> None:
        """Register a data connector."""
        self._connectors[connector_id] = connector
        logger.debug(f"Connector registered: {connector_id}")

    def get_table(self, name: str) -> Optional[VirtualTable]:
        """Get virtual table by name."""
        return self.schema_registry.get_table(name)

    def list_tables(self, **filters) -> List[VirtualTable]:
        """List virtual tables with optional filters."""
        return self.schema_registry.list_tables(**filters)

    @classmethod
    def from_config(cls, config_dict: Dict[str, Any]) -> "VirtualDataLayer":
        """
        Create VirtualDataLayer from configuration.

        Args:
            config_dict: Configuration dictionary

        Returns:
            Initialized VirtualDataLayer instance
        """
        layer = cls(config_dict)

        # Register virtual tables from config
        if "virtual_tables" in config_dict:
            for table_name, table_config in config_dict["virtual_tables"].items():
                table = cls._config_to_virtual_table(table_name, table_config)
                layer.schema_registry.register_table(table)

        logger.info(
            f"VirtualDataLayer loaded from config with {len(layer.schema_registry._tables)} tables"
        )
        return layer

    @staticmethod
    def _config_to_virtual_table(name: str, config: Dict[str, Any]) -> VirtualTable:
        """Convert configuration dictionary to VirtualTable."""
        encryption = None
        if "encryption" in config:
            enc_cfg = config["encryption"]
            encryption = EncryptionConfig(
                enabled=enc_cfg.get("enabled", False),
                fields=enc_cfg.get("fields", []),
                key_vault_backend=enc_cfg.get("key_vault_backend", "aws-secrets"),
                key_vault_path=enc_cfg.get("key_vault_path", ""),
            )

        return VirtualTable(
            name=name,
            source_type=SourceType(config.get("source_type", "filesystem")),
            connector_type=config.get("connector_type", ""),
            connection_id=config.get("connection", "default"),
            query=config.get("query"),
            table_name=config.get("table"),
            path=config.get("path"),
            schema=config.get("schema", {}),
            partitions=config.get("partitions", []),
            cache_strategy=CacheStrategy(config.get("cache_strategy", "smart")),
            cache_ttl_seconds=config.get("cache_ttl_seconds", 3600),
            encryption_config=encryption,
            access_control=config.get("access_control", {}),
            description=config.get("description", ""),
            tags=config.get("tags", []),
        )


# Import submodule classes for top-level access (after main classes defined)
from .security import (
    SecurityEnforcer,
    TableSecurityPolicy,
    FieldSecurityPolicy,
    AccessLevel,
    Operation,
    AuditLog,
)
from .federation_engine import (
    FederationEngine,
    QueryOptimizer,
    QueryPlan,
    Predicate,
    PredicateOperator,
    ExecutionStrategy,
    QueryStatistics,
)
from .readers import (
    VirtualReaderFactory,
    VirtualDataSourceReader,
    FilesystemVirtualReader,
    DatabaseVirtualReader,
    DataWarehouseVirtualReader,
)


# Public API exports
__all__ = [
    # Core types and enums
    "SourceType",
    "CacheStrategy",
    "EncryptionConfig",
    "VirtualTable",
    "SchemaRegistry",
    "TableStatistics",
    "VirtualDataLayer",
    # Security
    "SecurityEnforcer",
    "TableSecurityPolicy",
    "FieldSecurityPolicy",
    "AccessLevel",
    "Operation",
    "AuditLog",
    # Federation
    "FederationEngine",
    "QueryOptimizer",
    "QueryPlan",
    "Predicate",
    "PredicateOperator",
    "ExecutionStrategy",
    "QueryStatistics",
    # Readers
    "VirtualReaderFactory",
    "VirtualDataSourceReader",
    "FilesystemVirtualReader",
    "DatabaseVirtualReader",
    "DataWarehouseVirtualReader",
]
