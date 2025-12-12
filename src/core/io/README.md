# Tauro IO Module

## Overview

The `tauro.io` package is a robust, production-ready framework for unified data input/output operations across diverse environments—local filesystems, distributed cloud storage (AWS S3, Azure Data Lake, GCP, Databricks), and Spark/Delta Lake ecosystems. It is designed for reliability, extensibility, and advanced validation, supporting both batch and incremental processing, custom formats, and registry management for machine learning artifacts.

The module provides enterprise-grade data I/O capabilities with comprehensive error handling, validation, and observability through structured logging.

---

## Features

- **Unified API:** Consistent interface for reading/writing data from local, distributed, and cloud sources.
- **Advanced Validation:** Configuration and data validation using specialized validators; secure SQL query sanitization.
- **Spark/Delta Lake Integration:** Native support for Spark DataFrames, Delta Lake tables, partitioning, and schema evolution.
- **Cloud Compatibility:** Seamless handling of S3, ABFSS, GS, DBFS, and local filesystem paths.
- **Unity Catalog Support:** Automated management of Databricks Unity Catalog schemas and tables, with post-write operations (comments, optimize, vacuum).
- **Error Handling:** Centralized exception management for configuration, read/write, and format-related errors.
- **Custom Formats:** Easily extensible framework supporting Avro, ORC, XML, Pickle, and custom SQL query sources.
- **Artifact Management:** Registry for ML model artifacts with robust metadata and versioning support.
- **Observability:** Comprehensive structured logging via loguru for production debugging and monitoring.

---

## 🆕 Recent Security & Performance Improvements

This release includes critical security and performance enhancements:

### Security Enhancements
- **AST-Based SQL Validation:** SQL queries are now validated using AST parsing (sqlglot) when available, providing robust protection against injection attacks. Falls back to regex-based validation if sqlglot is not installed.
- **PickleReader Memory Safety:** Distributed pickle reading now enforces safe memory limits (default: 10,000 records) to prevent driver Out-Of-Memory errors. Configurable via `max_records` parameter.

### Performance Improvements
- **Optimized Partition Push-Down:** Partition filters are now applied using Spark's `filter()` method with intelligent fallback to `where()`, enabling better query optimization and reduced data transfer.
- **Better Logging:** Enhanced debug logging for filter application, time-travel operations, and error handling.

### Installation
```bash
# For enhanced SQL security (recommended)
pip install sqlglot>=23.0.0

# Without sqlglot, regex-based validation is used automatically
```

---

## Architecture Overview

The `tauro.io` module is organized into specialized components:

- **Context Management:** Provides unified access to runtime configurations, Spark sessions, and execution modes.
- **Readers:** Format-specific data ingestion with factory pattern for extensibility.
- **Writers:** Format-specific data output with support for partitioning and transaction control.
- **Validators:** Configuration and data validation with detailed error reporting.
- **Factories:** Factory pattern implementations for reader and writer instantiation.
- **SQL Utilities:** SQL query sanitization and safe execution for data loading.
- **Exception Handling:** Comprehensive exception hierarchy for production error handling.

---

## Key Components & Technical Usage

### 1. Context Management

All operations are driven by a configurable context object (dict or custom class). The `ContextManager` enables seamless access to Spark sessions, execution modes, I/O configs, and global settings.

```python
from tauro.io.context_manager import ContextManager

context = {
    "spark": spark_session,
    "execution_mode": "distributed",  # or "local"
    "input_config": {...},
    "output_path": "/data/outputs",
    "global_settings": {"fill_none_on_error": True}
}

cm = ContextManager(context)
spark = cm.get_spark()
mode = cm.get_execution_mode()
output_path = cm.get_output_path()
```

**Context Components:**

- `spark`: Active PySpark session for distributed operations
- `execution_mode`: "local" for single-node or "distributed" for cluster execution
- `input_config`: Dictionary mapping input names to their configurations
- `output_path`: Base path for all output operations
- `global_settings`: Global configuration including error handling policies and artifact paths

---

### 2. Input Loading

Flexible data ingestion supporting batch and incremental loading from local files, cloud URIs, and SQL queries.

```python
from tauro.io.input import InputLoader

input_loader = InputLoader(context)
datasets = input_loader.load_inputs(["dataset_1", "dataset_2"])
```

**Supported Formats:** Parquet, Delta, CSV, JSON, Avro, ORC, XML, Pickle, Query (SQL)

**Key Capabilities:**

- **Local & Cloud Paths:** Automatic detection and handling of local filesystem or cloud storage URIs (S3, ABFSS, GS, DBFS).
- **Glob Pattern Support:** Load multiple files matching wildcard patterns in local mode for batch operations.
- **Dependency Verification:** Automatic checks for required packages (Delta, XML support, etc.).
- **Flexible Error Handling:** Choose between fail-fast or graceful degradation with configurable `fill_none_on_error`.
- **Partition Push-Down:** Efficient filtering at read time for improved performance.

**Example: Mixed Format Loading**

```python
context = {
    "input_config": {
        "users_csv": {"format": "csv", "filepath": "s3://bucket/users.csv"},
        "events_parquet": {"format": "parquet", "filepath": "s3://bucket/events/"},
        "delta_data": {"format": "delta", "filepath": "s3://bucket/delta/events"},
        "xml_data": {"format": "xml", "filepath": "data/input.xml", "rowTag": "record"}
    },
    "execution_mode": "distributed"
}

input_loader = InputLoader(context)
users, events, delta_df, xml_df = input_loader.load_inputs(
    ["users_csv", "events_parquet", "delta_data", "xml_data"]
)
```

---

### 3. Data Reading (Readers)

Readers are instantiated via the factory pattern for extensibility and consistency.

```python
from tauro.io.factories import ReaderFactory

reader_factory = ReaderFactory(context)
parquet_reader = reader_factory.get_reader("parquet")
data = parquet_reader.read("/data/myfile.parquet", config)
```

**Reader Types:**

- **ParquetReader:** Efficient columnar format with schema inference.
- **DeltaReader:** Delta Lake with time-travel (versionAsOf, timestampAsOf).
- **CSVReader:** Configurable delimiter, header, and type handling.
- **JSONReader:** Line-delimited and compact JSON support.
- **QueryReader:** Secure SQL SELECT execution with injection prevention.
- **PickleReader:** Python object serialization with distributed read support and OOM safeguards.
  - **Memory Safety:** Default limit of 10,000 records for distributed reads prevents driver Out-Of-Memory errors.
  - **Configurable Limits:** Use `max_records` to customize: `-1` (default), `0` (unlimited, use with caution), or positive integer (custom limit).
  - **Absolute Maximum:** Enforces 1,000,000 record ceiling to prevent extreme memory issues.
  - **Security Requirement:** Requires `allow_untrusted_pickle=True` due to arbitrary code execution risks in pickle deserialization.
- **AvroReader, ORCReader, XMLReader:** Additional format support.

**Features:**

- **Partition Filtering:** Push-down predicates for efficient data loading using optimized Spark filters.
- **Safe SQL Execution:** Query validation and sanitization for security with AST parsing when available.
- **Distributed Pickle Reading:** Memory-efficient distributed deserialization with configurable limits.

---

### 4. Data Writing (Writers)

Writers support transactional and batch writes with partitioning, overwrite strategies, and schema evolution.

```python
from tauro.io.factories import WriterFactory

writer_factory = WriterFactory(context)
delta_writer = writer_factory.get_writer("delta")
delta_writer.write(df, "/output/mytable", config)
```

**Writer Capabilities:**

- **Advanced Delta Writes:** Supports `replaceWhere` for selective partition overwrites.
- **Partitioning:** Automatic validation and optimization for partitioned writes.
- **Format Support:** Native Spark support for CSV, JSON, Parquet, ORC, and Delta formats.
- **Schema Evolution:** Automatic handling of schema changes.
- **Write Modes:** Support for overwrite, append, ignore, and error modes.

---

### 5. Output Management & Unity Catalog

Automated output saving with Databricks Unity Catalog integration:

```python
from tauro.io.output import DataOutputManager

output_manager = DataOutputManager(context)
output_manager.save_output(
    env="prod",
    node={"output": ["sales_table"], "name": "etl_job"},
    df=result_dataframe,
    start_date="2025-09-01",
    end_date="2025-09-30"
)
```

**Unity Catalog Features:**

- Automated catalog and schema creation.
- Table registration and linking in the catalog.
- Metadata management with comments and tags.
- Automatic table optimization and cleanup (vacuum).

**Artifact Registry:**

- Save ML model artifacts with comprehensive metadata.
- Version tracking for model reproducibility.
- Integration with model registry paths.

---

### 6. Validation & Error Handling

**ConfigValidator:**

- Validates required fields in configuration objects.
- Ensures proper output key format (schema.sub_folder.table_name).
- Date format validation (YYYY-MM-DD).

**DataValidator:**

- Verifies DataFrame integrity and non-emptiness.
- Column existence validation.
- Supports Spark, Pandas, and Polars DataFrames.

**SQLSanitizer:**

- Prevents dangerous SQL operations (only SELECT and CTE allowed).
- **AST-Based Validation:** When sqlglot is available, uses Abstract Syntax Tree parsing for robust injection attack prevention. Provides protection against encoding bypasses and obfuscated queries.
- **Regex Fallback:** Automatically falls back to regex-based validation if sqlglot is not installed, ensuring compatibility while recommending the AST approach for maximum security.
- Comment safety validation to prevent hidden malicious code.

Example with enhanced validation:

```python
from tauro.io.sql import SQLSanitizer

# ✅ Safe queries (will pass)
SQLSanitizer.sanitize_query("SELECT * FROM users WHERE id = 1")
SQLSanitizer.sanitize_query("WITH temp AS (SELECT * FROM users) SELECT * FROM temp")

# ❌ Dangerous queries (will be rejected)
try:
    SQLSanitizer.sanitize_query("SELECT * FROM users; DROP TABLE users")
except ConfigurationError as e:
    logger.error(f"Query validation failed: {e}")

# ❌ Encoded attacks (detected with sqlglot)
try:
    SQLSanitizer.sanitize_query("SELECT * FROM users WHERE name = 0x44524f50")  # 0x44524f50 = "DROP"
except ConfigurationError as e:
    logger.error(f"Injection attempt detected: {e}")
```

**Note:** Install sqlglot for best security: `pip install sqlglot>=23.0.0`

---

## Example Scenarios

### Scenario A: Initial Batch Write (Full Load)

Write the entire table from scratch, partitioning by one or more key columns:

```python
config = {
    "format": "delta",
    "schema": "sales",
    "sub_folder": "full_load",
    "table_name": "transactions",
    "partition": ["date", "country"],
    "write_mode": "overwrite",
}

output_manager.save_output(
    env="prod",
    node={"output": ["sales_full"], "name": "batch_full"},
    df=df_complete
)
```

**Behavior:** Replaces the entire dataset, ensuring partitions are defined for optimal query performance and storage management.

---

### Scenario B: Incremental Update

Write only new or modified partitions using Delta Lake's efficient `replaceWhere`:

```python
config = {
    "format": "delta",
    "schema": "sales",
    "sub_folder": "incremental",
    "table_name": "transactions",
    "partition": ["date"],
    "write_mode": "overwrite",
    "overwrite_strategy": "replaceWhere",
    "partition_col": "date",
    "start_date": "2025-09-01",
    "end_date": "2025-09-24",
}

output_manager.save_output(
    env="prod",
    node={"output": ["sales_incremental"], "name": "incremental"},
    df=df_incremental,
    start_date="2025-09-01",
    end_date="2025-09-24"
)
```

**Benefit:** Only affected partitions are updated, minimizing write operations and preserving unmodified data.

---

### Scenario C: Selective Reprocessing

Rewrite specific date ranges or subsets without affecting other partitions:

```python
config = {
    "format": "delta",
    "schema": "sales",
    "table_name": "transactions",
    "partition": ["date"],
    "write_mode": "overwrite",
    "overwrite_strategy": "replaceWhere",
    "partition_col": "date",
    "start_date": "2025-09-10",
    "end_date": "2025-09-12",
}

output_manager.save_output(
    env="prod",
    node={"output": ["sales_reprocess"], "name": "selective_reprocess"},
    df=df_subset,
    start_date="2025-09-10",
    end_date="2025-09-12"
)
```

**Use Case:** Correcting data quality issues or updating specific time windows without full reprocessing.

---

### Scenario D: Dynamic Partitioning

Determine partition columns dynamically based on configuration or data characteristics:

```python
# Example: Load partition column from config or discover from data
partition_cols = config.get("partition_columns", ["date"])

output_config = {
    "format": "delta",
    "schema": "sales",
    "table_name": "transactions",
    "partition": partition_cols,
    "write_mode": "overwrite",
}

output_manager.save_output(
    env="prod",
    node={"output": ["sales_dynamic"], "name": "dynamic_partition"},
    df=df
)
```

**Advantage:** Enables adaptive pipelines that adjust partitioning based on operational requirements.

---

### Scenario E: Efficient Partition Push-Down Reading

Read only specific partitions for reduced data transfer and computation using optimized filtering:

```python
input_config = {
    "format": "delta",
    "filepath": "s3://bucket/delta/sales",
    "partition_filter": "date >= '2025-09-10' AND date <= '2025-09-12'",
}

context["input_config"] = {"sales_data": input_config}
input_loader = InputLoader(context)
df = input_loader.load_inputs(["sales_data"])[0]
```

**Performance Optimization:** Partition filters are applied using Spark's `filter()` method, enabling Catalyst optimizer to push predicates down to the storage layer. This reduces data transfer and memory consumption significantly for large partitioned tables.

**Example Performance Impact:**
- Without partition filter: Reads 100GB dataset, filters in memory → slow, high memory usage
- With partition filter: Spark pushes predicate → reads only 5GB relevant partitions → 20x faster

**Fallback Behavior:** If `filter()` fails to optimize, the framework automatically retries with `where()` to ensure compatibility while prioritizing performance.

---

### Scenario F: Secure Query Execution

Execute validated SQL queries safely against Spark with comprehensive injection prevention:

```python
context = {
    "input_config": {
        "query_data": {
            "format": "query",
            "query": "SELECT * FROM analytics.events WHERE date = '2025-09-24' LIMIT 1000"
        }
    },
    "execution_mode": "distributed"
}

input_loader = InputLoader(context)
query_df = input_loader.load_inputs(["query_data"])[0]
```

**Security Features:**
- **AST Validation:** When sqlglot is available, queries are parsed into Abstract Syntax Trees for bulletproof validation.
- **Injection Prevention:** Only SELECT and CTE statements are allowed; all other operations (INSERT, DELETE, DROP, etc.) are blocked.
- **Comment Analysis:** Comments are scanned for embedded dangerous keywords.
- **Encoding Detection:** Hex-encoded bypass attempts (e.g., `0x DROP`) are detected and rejected.
- **Multiple Statement Detection:** Semicolon-separated multiple statements are prevented.

**Validation Examples:**
```python
# ✅ PASS: Safe SELECT
SQLSanitizer.sanitize_query("SELECT * FROM users LIMIT 10")

# ❌ FAIL: Dangerous operation (with AST)
SQLSanitizer.sanitize_query("DROP TABLE users")  # Detected by AST parser

# ❌ FAIL: Encoding bypass (with AST) 
SQLSanitizer.sanitize_query("SELECT * WHERE name = 0x44524f50")  # Detected as "DROP"

# ❌ FAIL: Multiple statements
SQLSanitizer.sanitize_query("SELECT * FROM users; DELETE FROM users")
```

**Fallback:** If sqlglot is not installed, regex-based validation provides protection while the AST approach is recommended for production environments.

---

### Scenario G: Model Artifact Registry

Save trained models with comprehensive metadata for reproducibility:

```python
context = {
    "global_settings": {"model_registry_path": "/mnt/models"},
    "output_path": "/mnt/output",
}

node = {
    "model_artifacts": [
        {
            "name": "classifier",
            "type": "sklearn",
            "metrics": {"accuracy": 0.99, "precision": 0.98}
        }
    ],
    "name": "train_model"
}

output_manager = DataOutputManager(context)
output_manager.save_output("prod", node, df, model_version="v1.0.0")
```

**Organization:** Maintains organized model files and metadata for audit trails and reproducibility.

---

### Scenario H: Error-Tolerant Loading

Gracefully handle missing files or format errors without failing the entire pipeline:

```python
context = {
    "input_config": {
        "main_data": {"format": "csv", "filepath": "data/main.csv"},
        "optional_data": {"format": "parquet", "filepath": "data/missing.parquet"},
    },
    "global_settings": {"fill_none_on_error": True},
}

input_loader = InputLoader(context)
datasets = input_loader.load_inputs(["main_data", "optional_data"], fail_fast=False)
# Result: [main_df, None] if optional_data fails to load
```

**Flexibility:** Enables robust ETL pipelines that handle missing or corrupted data gracefully.

---

## Error Handling & Logging

The module provides comprehensive error handling and structured logging:

**Exception Hierarchy:**

- `IOManagerError`: Base exception for all module operations.
- `ConfigurationError`: Invalid or missing configuration.
- `ReadOperationError`: Data loading failures.
- `WriteOperationError`: Data saving failures.
- `FormatNotSupportedError`: Unsupported data formats.
- `DataValidationError`: Data integrity issues.

**Logging:**

All operations are logged via [loguru](https://github.com/Delgan/loguru) for production observability:

```python
from loguru import logger

logger.info(f"Loading {len(input_keys)} datasets")
logger.debug(f"Successfully loaded dataset: {key}")
logger.warning(f"Completed loading with {len(errors)} errors")
logger.error(f"Critical error during write operation")
```

---

## Extensibility

To add support for a new data format (e.g., Parquet V2):

1. **Implement a Reader Class:**
   ```python
   class ParquetV2Reader(SparkReaderBase):
       def read(self, source: str, config: Dict[str, Any]) -> Any:
           # Implementation
   ```

2. **Register in ReaderFactory:**
   ```python
   @staticmethod
   def get_reader(format_name: str) -> BaseReader:
       if format_name == "parquetv2":
           return ParquetV2Reader(context)
   ```

3. **Update SupportedFormats Constant:**
   ```python
   class SupportedFormats:
       PARQUETV2 = "parquetv2"
   ```

The factory pattern allows seamless integration without modifying core logic.

---

## Installation & Requirements

Tauro IO is part of the Tauro platform ecosystem. Install required dependencies:

```bash
pip install pyspark delta-spark pandas polars loguru
```

**Recommended for Enhanced Security (SQL Injection Prevention):**

```bash
# For AST-based SQL validation with sqlglot
pip install sqlglot>=23.0.0
```
This enables robust SQL query validation using Abstract Syntax Tree parsing, preventing encoding-based and obfuscated injection attacks. If not installed, the system automatically falls back to regex-based validation.

**Optional dependencies for additional formats:**

```bash
# For XML support
pip install spark-xml

# For Avro support (high-performance)
pip install fastavro>=1.4.0

# For advanced Delta operations
pip install delta-spark>=2.0.0
```

**Environment Requirements:**
- Python 3.7 or higher
- Spark 2.4.0 or higher (3.x recommended)
- Databricks Runtime 10.4+ (if using Databricks)
- 2GB minimum memory (8GB+ recommended for distributed operations)

---

---

## Memory Management & Pickle Safety

The PickleReader includes built-in memory safeguards for distributed environments:

### Default Behavior

```python
# Default: Limits to 10,000 records
reader = PickleReader(context)
df = reader.read("data.pkl", {
    "allow_untrusted_pickle": True,
    # max_records defaults to -1 → uses DEFAULT_MAX_RECORDS (10000)
})
```

**When to adjust max_records:**

| Scenario | Configuration | Example |
|----------|---|---|
| **Small files** | Use default (10k) | Default behavior for safety |
| **Known size < 50k** | Set custom limit | `"max_records": 50000` |
| **Large files** | Increase carefully | `"max_records": 500000` |
| **Read all** | Disable limit ⚠️ | `"max_records": 0` → logs CRITICAL |
| **Exceeded limit** | Auto-capped | `"max_records": 2000000` → capped at 1M |

### Examples

```python
# ✅ SAFE: Use default limit
config_default = {"allow_untrusted_pickle": True}
df = reader.read("small_file.pkl", config_default)
# Reads up to 10,000 records (default)

# ✅ SAFE: Custom limit for known size
config_custom = {
    "allow_untrusted_pickle": True,
    "max_records": 100000  # For larger files
}
df = reader.read("medium_file.pkl", config_custom)

# ⚠️ CAUTION: No limit (logs CRITICAL warning)
config_unlimited = {
    "allow_untrusted_pickle": True,
    "max_records": 0  # Reads ALL records
}
# Logger: "Reading ALL pickle records without limit. May cause OOM."

# ❌ INVALID: Without security flag
reader.read("data.pkl", {})
# Raises: ReadOperationError: "requires allow_untrusted_pickle=True"
```

### Memory Safety Features

1. **Default Limits:** 10,000 records default prevents accidental OOM
2. **Absolute Maximum:** 1,000,000 record ceiling enforced
3. **Distributed Deserialization:** Uses executors instead of driver for large files
4. **Warning Logs:** Clear CRITICAL/ERROR/WARNING messages guide users
5. **Graceful Degradation:** Falls back to local pickle reading if distributed fails

---

## Performance Considerations

1. **Partition Push-Down:** Always use `partition_filter` to reduce data transfer for large tables. The framework now uses optimized Spark `filter()` operations for better query optimization.
   - Example: Reading 100GB partitioned table with date filter reduces to 5GB actual read (20x improvement)
   
2. **Batch Operations:** Use glob patterns for batch loading multiple files efficiently in local mode.

3. **Write Strategies:** Prefer `replaceWhere` for incremental updates over full table rewrites.

4. **Pickle Limits:** Distributed pickle reading applies configurable memory limits; adjust via `max_records` based on available driver memory.
   - Default: 10,000 records (safe for most environments)
   - Custom: Set based on driver memory availability
   - Unlimited: Use `max_records=0` only with adequate memory

5. **SQL Query Validation:** AST-based validation (with sqlglot) adds minimal overhead (~2-5ms) for robust security. Regex fallback is faster (~1-2ms) but less secure.

6. **Schema Caching:** Reuse reader instances for repeated operations on the same format to avoid repeated initialization overhead.

---

## Troubleshooting

**Issue: "Spark session is not available"**

- Ensure Spark is properly initialized in the context.
- Verify execution_mode setting aligns with your environment.

**Issue: "Format not supported"**

- Check that required dependencies are installed.
- Verify format name matches supported formats exactly.

**Issue: "Out of memory errors with pickle"**

- **Default:** PickleReader limits to 10,000 records by default. If exceeded:
  ```python
  config = {
      "allow_untrusted_pickle": True,
      "max_records": 5000  # Reduce limit further
  }
  reader.read("data.pkl", config)
  ```
- Check available driver memory: `spark.driver.memory`
- Increase driver memory if needed: `--driver-memory 4g`
- Consider reading in batches instead of all at once

**Issue: "SQL query execution errors"**

- Verify query contains only SELECT or CTE statements (not INSERT, DELETE, DROP, etc.)
- Check table/column names are available in Spark context
- Ensure no dangerous keywords like `DROP TABLE` are in query:
  ```python
  # ❌ INVALID
  SQLSanitizer.sanitize_query("DROP TABLE users")
  
  # ✅ VALID
  SQLSanitizer.sanitize_query("SELECT * FROM users")
  ```

**Issue: "SQL injection attack detected" (query rejected)**

- This is expected behavior - malicious patterns are blocked
- If a legitimate query is rejected, check for:
  - Hex-encoded values (use string literals instead)
  - Multiple statements separated by semicolons (split into separate queries)
  - Suspicious patterns like `0x` encodings or `CHAR()` functions
- Example fixes:
  ```python
  # ❌ Rejected
  "WHERE name = 0x414243"  # Hex encoding
  
  # ✅ Accepted
  "WHERE name = 'ABC'"  # String literal
  ```

**Issue: "Partition filter not optimizing as expected"**

- Ensure partition column exists in the data
- Use proper syntax: `"partition_filter": "date >= '2025-09-01' AND date <= '2025-09-30'"`
- Enable debug logging to see filter application:
  ```python
  from loguru import logger
  logger.enable("core.io")  # View detailed operations
  ```

---

## Best Practices

1. **Always Validate Input:** Use ConfigValidator for configuration objects.
   ```python
   validator = ConfigValidator()
   validator.validate(config, ["format", "filepath"], "input_config")
   ```

2. **Enable Error Logging:** Set appropriate log levels for debugging and monitoring.
   ```python
   from loguru import logger
   logger.enable("core.io")  # Enable detailed I/O logging
   ```

3. **Use Context Manager:** Leverage ContextManager for configuration consistency.
   ```python
   cm = ContextManager(context)
   spark = cm.get_spark()
   mode = cm.get_execution_mode()
   ```

4. **Handle Errors Gracefully:** Implement proper exception handling in production workflows.
   ```python
   try:
       datasets = input_loader.load_inputs(keys, fail_fast=False)
   except ReadOperationError as e:
       logger.error(f"Read failed: {e}")
       # Handle gracefully
   ```

5. **Monitor Performance:** Use logging to track read/write operation times.
   - Enable debug logging: `logger.enable("core.io")`
   - Monitor filter application success/failure
   - Track partition push-down effectiveness

6. **Test Format Support:** Verify required packages are installed before production use.
   ```bash
   pip install sqlglot spark-xml delta-spark  # Recommended
   ```

7. **Security First:**
   - Always use `allow_untrusted_pickle=True` intentionally (not by accident)
   - Validate user-provided SQL queries before passing to framework
   - Keep sqlglot updated for latest security patterns
   - Monitor logs for injection attempt detections

8. **Memory Management:**
   - Set appropriate `max_records` for pickle reading based on driver memory
   - Use partition filters to minimize data transfer
   - Monitor driver memory usage in logs
   - Test with realistic data volumes before production

9. **SQL Query Best Practices:**
   - Use parameterized queries when possible (framework sanitizes but parameterization is safer)
   - Keep queries simple and readable
   - Test queries independently before adding to pipeline
   - Monitor query execution time in logs

---

## API Reference

### Key Classes

- **InputLoader:** Main entry point for data loading operations.
- **DataOutputManager:** Main entry point for data output operations.
- **ReaderFactory:** Factory for instantiating format-specific readers.
- **WriterFactory:** Factory for instantiating format-specific writers.
- **ContextManager:** Centralized context configuration management.
- **ConfigValidator:** Configuration validation and parsing.
- **DataValidator:** DataFrame validation and column checking.

### Common Methods

```python
# Loading data
input_loader.load_inputs(input_keys: List[str]) -> List[Any]

# Saving data
output_manager.save_output(
    env: str,
    node: Dict[str, Any],
    df: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> None

# Reading specific format
reader = reader_factory.get_reader(format_name: str)
data = reader.read(source: str, config: Dict[str, Any]) -> Any

# Writing specific format
writer = writer_factory.get_writer(format_name: str)
writer.write(df: Any, path: str, config: Dict[str, Any]) -> None
```

---

## Contributing

To contribute improvements or bug fixes:

1. Write comprehensive tests for new features.
2. Ensure all messages and docstrings are in English.
3. Follow the established naming conventions and code style.
4. Submit pull requests with detailed descriptions.

---

## License

```
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root.
```

---

## Contact & Support

For support, suggestions, or contributions, please contact:

- **Author:** Faustino Lopez Ramos
- **GitHub:** [faustino125](https://github.com/faustino125)
- **Project:** [Tauro](https://github.com/faustino125/tauro)

For issues, feature requests, or discussions, please open an issue on the GitHub repository.

---

## Changelog

### Version 1.1.0 (Latest - Security & Performance)

**Security Enhancements:**
- AST-based SQL query validation using sqlglot with automatic regex fallback
- Enhanced injection attack detection with encoding/obfuscation prevention
- Improved comment safety analysis for hidden malicious code
- Support for CTE (Common Table Expressions) in addition to SELECT

**Performance Improvements:**
- Optimized partition push-down using Spark's `filter()` with intelligent fallback
- Better query optimization through predicate pushdown to storage layer
- Improved logging for filter application success/failure tracking
- Reduced data transfer for large partitioned tables (up to 20x improvement reported)

**PickleReader Enhancements:**
- Default memory safety limits (10,000 records) to prevent driver OOM
- Configurable limits with absolute maximum (1,000,000 records)
- Improved distributed deserialization using executors instead of driver
- Enhanced logging for memory-related decisions
- More descriptive error messages and security warnings

**Documentation:**
- Comprehensive README updates with security/performance examples
- Detailed memory management guidelines
- Enhanced troubleshooting section with real-world scenarios
- Best practices for production environments

**Dependencies:**
- sqlglot (optional, recommended): `pip install sqlglot>=23.0.0`
  - Provides AST-based SQL validation
  - Automatic fallback to regex if not installed

### Version 1.0.0 (Initial Release)

- Initial production release
- Full support for major cloud providers (AWS, Azure, GCP)
- Delta Lake and Unity Catalog integration
- Comprehensive validation and error handling
- Distributed pickle reading with OOM safeguards
- XML, Avro, and ORC format support
