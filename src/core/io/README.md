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
- **AvroReader, ORCReader, XMLReader:** Additional format support.

**Features:**

- **Partition Filtering:** Push-down predicates for efficient data loading.
- **Safe SQL Execution:** Query validation and sanitization for security.
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
- Injection attack prevention.

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

Read only specific partitions for reduced data transfer and computation:

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

**Performance:** Partition filters are pushed down to Spark, loading only relevant data.

---

### Scenario F: Secure Query Execution

Execute validated SQL queries safely against Spark with injection prevention:

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

**Security:** Framework validates queries (only SELECT/CTE), preventing dangerous operations.

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

**Optional dependencies for additional formats:**

```bash
# For XML support
pip install spark-xml

# For Avro support
pip install pyspark[avro]

# For advanced Delta operations
pip install delta-spark
```

---

## Performance Considerations

1. **Partition Push-Down:** Always use `partition_filter` to reduce data transfer for large tables.
2. **Batch Operations:** Use glob patterns for batch loading multiple files efficiently.
3. **Write Strategies:** Prefer `replaceWhere` for incremental updates over full table rewrites.
4. **Pickle Limits:** Distributed pickle reading applies default memory limits; adjust via `max_records`.
5. **Schema Caching:** Reuse reader instances for repeated operations on the same format.

---

## Troubleshooting

**Issue: "Spark session is not available"**

- Ensure Spark is properly initialized in the context.
- Verify execution_mode setting aligns with your environment.

**Issue: "Format not supported"**

- Check that required dependencies are installed.
- Verify format name matches supported formats exactly.

**Issue: "Out of memory errors with pickle"**

- Set `max_records` limit: `config["max_records"] = 5000`
- Or disable limits: `config["max_records"] = 0`

**Issue: "SQL query execution errors"**

- Verify query contains only SELECT or CTE statements.
- Check table/column names are available in Spark context.

---

## Best Practices

1. **Always Validate Input:** Use ConfigValidator for configuration objects.
2. **Enable Error Logging:** Set appropriate log levels for debugging.
3. **Use Context Manager:** Leverage ContextManager for configuration consistency.
4. **Handle Errors Gracefully:** Implement proper exception handling in production workflows.
5. **Monitor Performance:** Use logging to track read/write operation times.
6. **Test Format Support:** Verify required packages are installed before production use.

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

### Version 1.0.0 (Current)

- Initial production release
- Full support for major cloud providers (AWS, Azure, GCP)
- Delta Lake and Unity Catalog integration
- Comprehensive validation and error handling
- Distributed pickle reading with OOM safeguards
- XML, Avro, and ORC format support
