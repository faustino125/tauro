# Tauro IO Module

## Overview

The `tauro.io` package is a robust, production-ready framework for unified data input/output operations across diverse environments—local filesystems, distributed cloud storage (AWS S3, Azure Data Lake, GCP, Databricks), and Spark/Delta Lake ecosystems. It is designed for reliability, extensibility, and advanced validation, supporting both batch and incremental processing, custom formats, and registry management for machine learning artifacts.

---

## Features

- **Unified API:** Consistent interface for reading/writing data from local, distributed, and cloud sources.
- **Advanced Validation:** Configuration and data validation using specialized validators; secure SQL query sanitization.
- **Spark/Delta Lake Integration:** Native support for Spark DataFrames, Delta Lake tables, partitioning, and schema evolution.
- **Cloud Compatibility:** Handles S3, ABFSS, GS, DBFS, and local paths seamlessly.
- **Unity Catalog Support:** Automated management of Databricks Unity Catalog schemas/tables, with post-write operations (comments, optimize, vacuum).
- **Error Handling:** Centralized exception management for configuration, read/write, and format errors.
- **Custom Formats:** Easily extensible for Avro, ORC, XML, Pickle, and custom query sources.
- **Artifact Management:** Registry for ML model artifacts with robust metadata and versioning.

---

## Key Components & Technical Usage

### 1. Context Management

All operations are driven by a configurable context object (dict or custom class). The `ContextManager` enables seamless access to Spark sessions, execution modes, I/O configs, and global settings.

```python
from tauro.io.context_manager import ContextManager

context = {...}  # Your config dict or object
cm = ContextManager(context)

spark = cm.get_spark()
mode = cm.get_execution_mode()  # "local" or "distributed"
output_path = cm.get_output_path()
```

---

### 2. Input Loading

Supports batch and incremental loading from local files, cloud URIs, and SQL queries (with glob patterns, error handling, and format validation).

```python
from tauro.io.input import InputLoader

input_loader = InputLoader(context)
datasets = input_loader.load_inputs(node_config)
```

**Supported formats:** Parquet, Delta, CSV, JSON, Avro, ORC, XML, Pickle, Query

- **Local/Cloud Path Handling:** Automatically detects and processes local or cloud URIs.
- **Glob Patterns:** Handles glob file patterns in local mode for batch loading.
- **Custom Format Registration:** Automatically checks dependencies for formats like Delta and XML.

---

### 3. Data Reading (Readers)

Readers are instantiated via the factory pattern for extensibility.

```python
from tauro.io.factories import ReaderFactory

reader_factory = ReaderFactory(context)
parquet_reader = reader_factory.get_reader("parquet")
data = parquet_reader.read("/data/myfile.parquet", config)
```

- **QueryReader:** Executes sanitized SQL SELECT queries via Spark.
- **PickleReader:** Supports secure, distributed reading (with OOM safeguards).

---

### 4. Data Writing (Writers)

Writers support transactional and batch writes with partitioning, overwrite strategies, and schema evolution.

```python
from tauro.io.factories import WriterFactory

writer_factory = WriterFactory(context)
delta_writer = writer_factory.get_writer("delta")
delta_writer.write(df, "/output/mytable", config)
```

- **Advanced Delta Writes:** Supports `replaceWhere` for selective overwrites.
- **Partitioning:** Automatic validation and application for partitioned writes.
- **CSV/JSON/Parquet/ORC:** Native Spark support with option injection.

---

### 5. Output Management & Unity Catalog

Automated output saving with Unity Catalog integration for Databricks:

```python
from tauro.io.output import DataOutputManager

output_manager = DataOutputManager(context)
output_manager.save_output(env, node, df, start_date, end_date, model_version)
```

- **Unity Catalog:** Handles catalog/schema creation, table linking, comments, optimize, and vacuum.
- **Artifact Registry:** Saves ML artifacts with metadata in model registry paths.

---

### 6. Validation & Error Handling

- **ConfigValidator:** Validates required fields, output key formats, date formats.
- **DataValidator:** Checks DataFrame integrity, emptiness, column existence.
- **SQLSanitizer:** Prevents dangerous SQL operations (only SELECT/CTE allowed).

---

## Example Scenarios

### a) Initial Batch Write (Batch Full)

Write the entire table from scratch, partitioning by one or more key columns (e.g., `date`, `country`):

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
    df=df,  # DataFrame with all data
)
```
- Ensures full replacement of the dataset.
- Partitions are defined in config for optimal query performance and storage management.

---

### b) Incremental Load

Write only new or modified partitions. With Delta Lake, use `replaceWhere` to overwrite just the affected partitions:

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
    df=df_increment,  # Only incremental data
    start_date="2025-09-01",
    end_date="2025-09-24",
)
```
- Efficiently updates only the necessary partitions.
- Minimizes write operations and preserves other data.

---

### c) Selective Reprocessing

Rewrite only specific partitions (e.g., a date range or a subset):

```python
config = {
    "format": "delta",
    "schema": "sales",
    "sub_folder": "selective_reprocess",
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
    df=df_subset,  # DataFrame with only the dates to reprocess
    start_date="2025-09-10",
    end_date="2025-09-12",
)
```
- Uses `replaceWhere` to target specific partitions.
- Useful for correcting issues or updating data for particular time windows.

---

### d) Dynamic Partitioning

Determine partition columns dynamically from configuration or even from the data itself:

```python
dynamic_partition_col = get_partition_col_from_config_or_data(df)
config = {
    "format": "delta",
    "schema": "sales",
    "sub_folder": "dynamic_partitioning",
    "table_name": "transactions",
    "partition": [dynamic_partition_col],  # Could be "region", "date", etc.
    "write_mode": "overwrite",
}
output_manager.save_output(
    env="prod",
    node={"output": ["sales_dynamic"], "name": "dynamic_partition"},
    df=df,
)
```
- The partitioning logic can be abstracted, making pipelines adaptive to changing data schemas.

---

### e) Efficient Reading (Partition Push-Down)

Read only specific partitions for efficient data access and reduced compute:

```python
input_config = {
    "format": "delta",
    "filepath": "s3://bucket/delta/sales",
    "partition_filter": "date BETWEEN '2025-09-10' AND '2025-09-12'",
}
context["input_config"] = {"sales_data": input_config}
input_loader = InputLoader(context)
df = input_loader.load_inputs({"input": ["sales_data"]})[0]
```
- The `partition_filter` is pushed down to Spark, so only relevant data is loaded.

---

## Other Useful Scenarios

### f) Multi-format Input Handling

Load multiple input datasets of different formats in a single pipeline step:

```python
context = {
    "input_config": {
        "users_csv": {"format": "csv", "filepath": "data/users.csv"},
        "events_parquet": {"format": "parquet", "filepath": "data/events.parquet"},
        "delta_data": {"format": "delta", "filepath": "s3://bucket/delta/events"},
        "xml_data": {"format": "xml", "filepath": "data/input.xml"},
    }
}
input_loader = InputLoader(context)
all_data = input_loader.load_inputs({"input": ["users_csv", "events_parquet", "delta_data", "xml_data"]})
```
- Enables heterogeneous data ingestion, normalization, and transformation.

---

### g) Reading with Glob Patterns

Automatically match and read multiple files using wildcard (glob) patterns in local mode:

```python
context = {
    "input_config": {
        "monthly_data": {"format": "csv", "filepath": "data/2025-09-*.csv"},
    },
    "execution_mode": "local",
}
input_loader = InputLoader(context)
df = input_loader.load_inputs({"input": ["monthly_data"]})[0]
```
- Useful for batch loading of time-series or partitioned files.

---

### h) Secure Query Execution

Run only safe, validated SQL queries against Spark, preventing injection or dangerous commands:

```python
context = {
    "input_config": {
        "query_data": {"format": "query", "query": "SELECT * FROM analytics.events WHERE date = '2025-09-24'"},
    },
    "execution_mode": "distributed",
}
input_loader = InputLoader(context)
query_df = input_loader.load_inputs({"input": ["query_data"]})[0]
```
- The framework will reject queries not starting with SELECT or containing unsafe patterns.

---

### i) Model Artifact Registry

Save model artifacts (e.g., after ML training) with metadata and versioning:

```python
context = {
    "global_settings": {"model_registry_path": "/mnt/models"},
    "output_path": "/mnt/output",
}
node = {
    "model_artifacts": [{"name": "classifier", "type": "sklearn", "metrics": {"accuracy": 0.99}}],
    "name": "train_model"
}
output_manager = DataOutputManager(context)
output_manager.save_output("dev", node, df, model_version="v1.0.0")
```
- Organizes model files and metadata for reproducibility and audit.

---

### j) Unity Catalog Table Management

Automate creation and management of tables in Databricks Unity Catalog:

```python
config = {
    "format": "unity_catalog",
    "catalog_name": "main",
    "schema": "events",
    "table_name": "daily_summary",
    "output_path": "s3://bucket/delta/output",
}
output_manager.save_output(
    env="prod",
    node={"output": ["uc_table"], "name": "unity_catalog_node"},
    df=some_df,
    start_date="2025-09-01",
    end_date="2025-09-24"
)
```
- Automatically ensures schema, links tables, adds comments, and optimizes storage.

---

### k) Error Tolerant Loading

Gracefully handle missing files or format errors, filling with `None` or skipping faulty datasets:

```python
context = {
    "input_config": {
        "main_data": {"format": "csv", "filepath": "data/main.csv"},
        "optional_data": {"format": "parquet", "filepath": "data/missing.parquet"},
    },
    "global_settings": {"fill_none_on_error": True},
}
input_loader = InputLoader(context)
datasets = input_loader.load_inputs({"input": ["main_data", "optional_data"]})
```
- Prevents pipeline failure, enables flexible ETL and reporting.

---

## Error Handling & Logging

All major operations are wrapped with detailed error handling and logging via [loguru](https://github.com/Delgan/loguru):

- **ConfigurationError:** Invalid or missing configuration.
- **ReadOperationError:** Input/data loading failures.
- **WriteOperationError:** Output/data saving failures.
- **FormatNotSupportedError:** Unsupported data formats.
- **DataValidationError:** Data integrity issues.

---

## Extensibility

To add a new format (e.g., ParquetV2):

1. Implement a reader/writer in `readers.py`/`writers.py`.
2. Register it in `ReaderFactory`/`WriterFactory`.
3. Update `SupportedFormats` in `constants.py`.

---

## Installation

Tauro IO is intended for use as part of the Tauro platform. To install the required dependencies:

```bash
pip install pyspark delta-spark pandas polars loguru
```

Additional packages are required for XML/Avro/ORC support.

---

## License

```
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root.
```

---

## Contact

For support, suggestions, or contributions, please contact [Faustino Lopez Ramos](https://github.com/faustino125).
