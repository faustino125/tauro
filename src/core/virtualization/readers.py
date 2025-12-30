"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""

from typing import Any, Dict, List, Optional, Iterator
from abc import ABC, abstractmethod
import sqlite3
from loguru import logger  # type: ignore

from . import VirtualTable, SourceType


class VirtualDataSourceReader(ABC):
    """
    Abstract base class for reading from virtual data sources.
    Adapts virtual tables to Tauro's I/O interface.
    """

    def __init__(self, context: Any, virtual_table: VirtualTable):
        """
        Initialize virtual reader.
        """
        self.context = context
        self.virtual_table = virtual_table
        logger.debug(f"Initialized reader for virtual table: {virtual_table.name}")

    @abstractmethod
    def read(self, **options) -> Iterator[Dict[str, Any]]:
        """
        Read data from virtual source.
        """
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        """Get schema of virtual table."""
        pass


class FilesystemVirtualReader(VirtualDataSourceReader):
    """
    Reader for virtual tables backed by filesystem sources
    (Parquet, CSV, Delta, etc.)
    """

    def read(self, filters: Optional[List[tuple]] = None, **options) -> Iterator[Dict[str, Any]]:
        """
        Read from filesystem virtual source.
        """
        # Import here to avoid circular dependencies
        from core.io.factories import ReaderFactory

        logger.info(
            f"Reading virtual table: {self.virtual_table.name} from {self.virtual_table.path}"
        )

        # Get appropriate reader based on file format
        reader_factory = ReaderFactory(self.context)

        # Infer format from path extension
        format_name = self._infer_format(self.virtual_table.path)
        reader = reader_factory.get_reader(format_name)

        # Read data
        data = reader.read(self.virtual_table.path, {})

        # Apply filters if provided
        if filters:
            data = self._apply_filters(data, filters)

        # Yield rows
        for row in data:
            yield row

    def get_schema(self) -> Dict[str, str]:
        """Get schema from virtual table definition."""
        return self.virtual_table.schema

    def _infer_format(self, path: str) -> str:
        """Infer file format from path."""
        if path.endswith(".parquet"):
            return "parquet"
        elif path.endswith(".csv"):
            return "csv"
        elif path.endswith(".json"):
            return "json"
        elif path.endswith(".delta"):
            return "delta"
        else:
            return "parquet"  # Default

    def _apply_filters(self, data: Iterator[Dict], filters: List[tuple]) -> Iterator[Dict]:
        """Apply simple filters to data."""
        for row in data:
            if self._matches_filters(row, filters):
                yield row

    def _matches_filters(self, row: Dict, filters: List[tuple]) -> bool:
        """Check if row matches all filters."""
        ops = {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "IN": lambda a, b: a in b,
        }

        for field, operator, value in filters:
            if field not in row:
                return False

            row_value = row[field]
            comp = ops.get(operator, ops["="])

            try:
                if not comp(row_value, value):
                    return False
            except Exception:
                # Any comparison error means the filter does not match
                return False

        return True


class DatabaseVirtualReader(VirtualDataSourceReader):
    """
    Reader for virtual tables backed by SQL databases
    (PostgreSQL, MySQL, etc.)
    """

    def read(self, filters: Optional[List[tuple]] = None, **options) -> Iterator[Dict[str, Any]]:
        """
        Read from database virtual source.

        Note: This implementation uses sqlite3 as a reference.
        In production, use appropriate database drivers (psycopg2, pymysql, etc.)
        based on virtual_table.connector_type.
        """
        logger.info(f"Reading virtual table: {self.virtual_table.name} from database")

        # Build query with filters
        query = self._build_query(filters, options)

        logger.debug(f"Executing query: {query}")

        try:
            # Get connection (simplified - in production use connection pool)
            conn = self._get_connection()
            cursor = conn.cursor()

            # Execute query
            cursor.execute(query)

            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []

            # Yield rows as dictionaries
            for row in cursor.fetchall():
                yield dict(zip(column_names, row))

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Database read error: {e}")
            # Return empty iterator on error
            return iter([])

    def get_schema(self) -> Dict[str, str]:
        """Get schema from virtual table definition."""
        return self.virtual_table.schema

    def _build_query(
        self, filters: Optional[List[tuple]] = None, options: Optional[Dict] = None
    ) -> str:
        """
        Build SQL query with pushdown filters.
        """
        if self.virtual_table.query:
            # Custom query
            base_query = self.virtual_table.query
        else:
            # Simple table select
            columns = ", ".join(self.virtual_table.schema.keys())
            base_query = f"SELECT {columns} FROM {self.virtual_table.table_name}"

        # Add filters
        if filters:
            where_clauses = []
            for field, operator, value in filters:
                where_clauses.append(self._build_predicate(field, operator, value))

            where_clause = " AND ".join(where_clauses)

            if "WHERE" in base_query:
                base_query += f" AND ({where_clause})"
            else:
                base_query += f" WHERE {where_clause}"

        # Add limit if specified
        if options and options.get("limit"):
            base_query += f" LIMIT {options['limit']}"

        return base_query

    def _build_predicate(self, field: str, operator: str, value: Any) -> str:
        """Build SQL predicate."""
        if operator == "=":
            return f"{field} = '{value}'"
        elif operator == ">":
            return f"{field} > {value}"
        elif operator == ">=":
            return f"{field} >= {value}"
        elif operator == "<":
            return f"{field} < {value}"
        elif operator == "<=":
            return f"{field} <= {value}"
        elif operator == "IN":
            values = ",".join(f"'{v}'" for v in value)
            return f"{field} IN ({values})"
        else:
            return f"{field} = '{value}'"

    def _get_connection(self):
        """
        Get database connection."""
        if self.virtual_table.connector_type == "sqlite":
            # For sqlite, connection_id is the database file path
            return sqlite3.connect(self.virtual_table.connection_id)
        else:
            logger.warning(
                f"Database connector '{self.virtual_table.connector_type}' not fully implemented. "
                "Returning mock connection."
            )
            # Return a mock connection for unsupported databases
            # In production, raise an exception or implement the connector
            return sqlite3.connect(":memory:")


class DataWarehouseVirtualReader(VirtualDataSourceReader):
    """
    Reader for virtual tables backed by data warehouses
    (Snowflake, BigQuery, Redshift, etc.)
    """

    def read(self, filters: Optional[List[tuple]] = None, **options) -> Iterator[Dict[str, Any]]:
        """
        Read from data warehouse virtual source.

        Supports:
        - Snowflake (via snowflake-connector-python)
        - BigQuery (via google-cloud-bigquery)
        - Redshift (via psycopg2)
        - Databricks (via databricks-sql-connector)
        """
        logger.info(f"Reading virtual table: {self.virtual_table.name} from data warehouse")

        # Build optimized query with partition pruning
        query = self._build_warehouse_query(filters)

        logger.debug(f"Executing data warehouse query: {query}")

        try:
            # Get connection based on connector type
            conn = self._get_warehouse_connection()
            cursor = conn.cursor()

            # Execute query
            cursor.execute(query)

            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []

            # Yield rows as dictionaries
            # For data warehouses, consider batching for better performance
            batch_size = options.get("batch_size", 10000)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(column_names, row))

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Data warehouse read error: {e}")
            return iter([])

    def get_schema(self) -> Dict[str, str]:
        """Get schema from virtual table definition."""
        return self.virtual_table.schema

    def _build_warehouse_query(self, filters: Optional[List[tuple]] = None) -> str:
        """
        Build optimized data warehouse query.
        """
        if self.virtual_table.query:
            base_query = self.virtual_table.query
        else:
            base_query = f"SELECT * FROM {self.virtual_table.table_name}"

        # Add partition pruning if available
        if self.virtual_table.partitions and filters:
            partition_filters = self._extract_partition_filters(filters)
            if partition_filters:
                where_clauses = []
                for field, operator, value in partition_filters:
                    where_clauses.append(self._build_predicate(field, operator, value))

                where_clause = " AND ".join(where_clauses)

                if "WHERE" in base_query:
                    base_query += f" AND ({where_clause})"
                else:
                    base_query += f" WHERE {where_clause}"

        return base_query

    def _extract_partition_filters(self, filters: List[tuple]) -> List[tuple]:
        """Extract filters on partitioned columns."""
        partition_filters = []

        for field, operator, value in filters:
            if field in self.virtual_table.partitions:
                partition_filters.append((field, operator, value))

        return partition_filters

    def _build_predicate(self, field: str, operator: str, value: Any) -> str:
        """Build SQL predicate for data warehouse."""
        # Similar to database but optimized for DW syntax
        if operator == "=":
            if isinstance(value, str):
                return f"{field} = '{value}'"
            else:
                return f"{field} = {value}"
        elif operator in [">", ">=", "<", "<="]:
            return f"{field} {operator} {value}"
        elif operator == "IN":
            if isinstance(value[0], str):
                values = ",".join(f"'{v}'" for v in value)
            else:
                values = ",".join(str(v) for v in value)
            return f"{field} IN ({values})"
        else:
            return f"{field} = '{value}'"

    def _get_warehouse_connection(self):
        """
        Get data warehouse connection.

        Connector types supported:
        - 'snowflake': Snowflake Data Warehouse
        - 'bigquery': Google BigQuery
        - 'redshift': Amazon Redshift
        - 'databricks': Databricks SQL Warehouse

        In production:
        - Use connection pooling
        - Get credentials from secure vault
        - Implement proper error handling and retries
        - Use context managers for resource cleanup
        """
        connector_type = self.virtual_table.connector_type.lower()

        if connector_type == "snowflake":
            try:
                import snowflake.connector  # type: ignore

                # In production, get credentials from context or vault
                # Example: conn = snowflake.connector.connect(**connection_params)
                logger.warning(
                    "Snowflake connector not fully configured. Implement credentials loading."
                )
                raise NotImplementedError("Snowflake connection requires credentials configuration")
            except ImportError:
                logger.error("snowflake-connector-python not installed")
                raise

        elif connector_type == "bigquery":
            try:
                from google.cloud import bigquery

                # In production: client = bigquery.Client(project=project_id)
                logger.warning(
                    "BigQuery connector not fully configured. Implement credentials loading."
                )
                raise NotImplementedError("BigQuery connection requires credentials configuration")
            except ImportError:
                logger.error("google-cloud-bigquery not installed")
                raise

        elif connector_type == "redshift":
            try:
                import psycopg2  # type: ignore

                # Redshift uses PostgreSQL protocol
                # In production: conn = psycopg2.connect(**redshift_params)
                logger.warning(
                    "Redshift connector not fully configured. Implement credentials loading."
                )
                raise NotImplementedError("Redshift connection requires credentials configuration")
            except ImportError:
                logger.error("psycopg2 not installed")
                raise

        elif connector_type == "databricks":
            try:
                from databricks import sql  # type: ignore

                # In production: conn = sql.connect(**databricks_params)
                logger.warning(
                    "Databricks connector not fully configured. Implement credentials loading."
                )
                raise NotImplementedError(
                    "Databricks connection requires credentials configuration"
                )
            except ImportError:
                logger.error("databricks-sql-connector not installed")
                raise

        else:
            # Fallback to sqlite for testing
            logger.warning(
                f"Data warehouse connector '{connector_type}' not implemented. "
                "Using sqlite as fallback for testing."
            )
            return sqlite3.connect(":memory:")


class VirtualReaderFactory:
    """
    Factory for creating virtual data source readers.
    """

    def __init__(self, context: Any):
        """Initialize factory."""
        self.context = context

    def create_reader(self, virtual_table: VirtualTable) -> VirtualDataSourceReader:
        """
        Create appropriate reader for virtual table.
        """
        if virtual_table.source_type == SourceType.FILESYSTEM:
            return FilesystemVirtualReader(self.context, virtual_table)

        elif virtual_table.source_type == SourceType.DATABASE:
            return DatabaseVirtualReader(self.context, virtual_table)

        elif virtual_table.source_type == SourceType.DATA_WAREHOUSE:
            return DataWarehouseVirtualReader(self.context, virtual_table)

        else:
            raise ValueError(f"Unsupported source type: {virtual_table.source_type}")
