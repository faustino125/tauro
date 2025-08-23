# tauro.io

A lightweight, modular IO layer for reading and writing data in Tauro pipelines. It supports both local and distributed (Spark) environments, multiple file formats, Delta Lake, and Databricks Unity Catalog, with clear configuration and sane defaults.

This document explains how to set up the context, read and write data, and use advanced features like schema overwrite, replaceWhere, and model artifact handling.

---

## Key Features

- Unified context model: works with both a plain Python dict or Tauro Config Context objects.
- Readers for Parquet, CSV, JSON, Delta, Avro, ORC, XML, Pickle, and SQL queries.
- Writers for Delta, Parquet, CSV, JSON, ORC; plus a Unity Catalog manager.
- Parallel input loading using Spark when available.
- Safe, consistent helpers for Spark and environment access.
- Robust validation and error handling.
- Pluggable factory patterns for readers and writers.

---

## Installation and Requirements

- Python 3.8+
- Spark (for distributed mode and Spark-based formats)
- Optional: Delta Lake and XML plugins
  - Delta: pip install delta-spark
  - XML: com.databricks:spark-xml on the Spark classpath (or cluster-installed)

If you use Unity Catalog:
- Run on Databricks or a Spark environment with UC enabled.
- Ensure spark.databricks.unityCatalog.enabled=true in Spark conf.

---

## Unified Context

All io classes accept either:
- A Context object (from tauro.config.contexts), or
- A dict-based context (lighter setup)

Internally, tauro.io uses helpers to read context consistently (no matter if it's a dict or object):
- _ctx_get(key, default=None)
- _ctx_spark()
- _spark_available()
- _is_local()

This means your code stays the same whether you pass a dict or a Context object.

### Example: using a Context object

```python
from tauro.config import ContextFactory
from tauro.io import InputLoader, OutputManager

context = ContextFactory.from_files(
    global_settings="config/global.yml",
    pipelines_config="config/pipelines.yml",
    nodes_config="config/nodes.yml",
    input_config="config/input.yml",
    output_config="config/output.yml",
)

loader = InputLoader(context)
outputs = OutputManager(context)
```

### Example: using a dict

```python
context = {
    "input_config": {
        "dataset_parquet": {"format": "parquet", "filepath": "/data/input/data.parquet"},
        "dataset_csv": {"format": "csv", "filepath": "/data/input/data.csv", "options": {"header": "true"}},
        "dataset_query": {"format": "query", "query": "SELECT 1 AS id"},
    },
    "output_config": {
        "out_delta:schema/sub/table": {
            "format": "delta",
            "table_name": "table",
            "schema": "schema",
        },
        "uc_out:analytics/sales/fact_sales": {
            "format": "unity_catalog",
            "catalog_name": "main",
            "schema": "analytics",
            "table_name": "fact_sales",
            "partition_col": "event_date",
            "overwrite_strategy": "replaceWhere",
        },
    },
    "output_path": "/data/output",
    "execution_mode": "local",  # or "distributed" / "databricks"
    "spark": spark_session,     # optional unless you use Spark-based operations
    "global_settings": {"fail_on_error": True, "model_registry_path": "/data/models"},
}
```

---

## Reading Data

Use InputLoader to fetch inputs declared in your node. It supports sequential and (optionally) parallel loading when Spark is available.

```python
from tauro.io import InputLoader

node = {
    "name": "example_node",
    "input": ["dataset_parquet", "dataset_csv", "dataset_query"],
    "parallel": True,       # parallelizes with Spark if available
    "fail_fast": True,      # stop on first error (default True)
}

loader = InputLoader(context)
dfs = loader.load_inputs(node)  # returns a list of DataFrames/objects
```

Supported input formats (via readers):
- parquet, csv, json, delta, avro, orc, xml, pickle, query
- Note: Some formats need Spark and/or plugins. See “Installation and Requirements”.

CSV defaults can be overridden via options. For Delta, you may specify versionAsOf or timestampAsOf.

---

## Writing Data

Use OutputManager to write the output(s) declared on your node. It routes to:
- DataWriter (filesystem/object storage), or
- UnityCatalogManager (Databricks Unity Catalog), based on format.

```python
from tauro.io import OutputManager

node = {"name": "writer_node", "output": ["out_delta:schema/sub/table"]}

outputs = OutputManager(context)
outputs.save_output(node, df, start_date="2025-01-01", end_date="2025-01-31")
```

### File/Object storage (DataWriter)

- Supported formats: delta, parquet, csv, json, orc (see SupportedFormats).
- Common writer config:
  - format: required, e.g., "delta"
  - write_mode: "overwrite" (default) or "append" (see WriteMode)
  - overwrite_schema: True (format dependent; default True for Delta)
  - partition: "col" or ["col1", "col2"]
  - options: dict of format-specific options

Example output_config:

```yaml
out_parquet:logs/app/events:
  format: parquet
  table_name: events
  schema: logs
  sub_folder: app
  write_mode: append
  partition: event_date
```

Path resolution:
- Base path from context.output_path
- Final path: <output_path>/<schema>/<sub_folder>/<table_name>
- Cloud URIs supported (s3://, abfss://, gs://, dbfs:/). Local mode auto-creates folders.

### Unity Catalog (UnityCatalogManager)

- Requires Spark and UC enabled.
- The manager writes Delta files to storage then ensures a UC table exists and points to that location.
- Config keys:
  - catalog_name: UC catalog
  - schema: UC schema
  - table_name
  - partition_col: optional
  - write_mode: optional, defaults to overwrite
  - overwrite_schema: optional True for Delta
  - overwrite_strategy: optional "replaceWhere"
    - Requires partition_col, start_date, end_date
  - optimize: optional True (default). Requires partition_col and date range.
  - vacuum / vacuum_retention_hours: optional.

Example:

```yaml
uc_out:analytics/sales/fact_sales:
  format: unity_catalog
  catalog_name: main
  schema: analytics
  table_name: fact_sales
  partition_col: event_date
  overwrite_strategy: replaceWhere
  optimize: true
  vacuum: true
  vacuum_retention_hours: 168
  description: "Daily sales fact table"
```

Writing:

```python
outputs.save_output(node={"output": ["uc_out:analytics/sales/fact_sales"]},
                    df=df,
                    start_date="2025-01-01",
                    end_date="2025-01-31")
```

---

## DataFrame Conversion

If you pass pandas or polars DataFrames to OutputManager, they will be converted to Spark DataFrames (if Spark is available):
- pandas.DataFrame -> Spark via createDataFrame
- polars.DataFrame -> Spark via to_pandas -> createDataFrame

If Spark is not available and conversion is required, an error is raised.

---

## Model Artifacts

ModelArtifactManager can persist artifacts (e.g., metrics, models) under a model registry path.

- context.global_settings.model_registry_path must be set (string path).
- Node config should include a list under model_artifacts (with at least a name).

Example:

```python
node = {
    "name": "train_model",
    "model_artifacts": [
        {"name": "my_model"},       # will be saved under <model_registry_path>/my_model/<version>
        {"name": "feature_importance"}
    ]
}

outputs = OutputManager(context)
outputs.save_output(node, df, model_version="v1")
```

In local mode, directories are created if missing.

---

## Error Handling

OutputManager uses ErrorHandler:
- global_settings.fail_on_error (default True) controls whether to raise on error.
- Errors in individual outputs are isolated; logs include context.

Best practice: keep fail_on_error=True in production to avoid silent failures.

---

## Configuration Recap

- input_config: keyed by dataset name. Each entry describes a source with a format.
- output_config: keyed by output key (e.g., out_parquet:schema/sub/table). Each entry describes format and write options.
- context-level:
  - output_path: base directory/URI for outputs.
  - execution_mode: "local", "distributed", or "databricks" (normalized internally).
  - spark: SparkSession (optional, required for Spark operations).
  - global_settings: general flags (fail_on_error, model_registry_path, etc).

---

## Tips and Best Practices

- Use partition columns whenever appropriate to improve write/read performance.
- For Delta in UC, prefer overwrite_strategy=replaceWhere for incremental updates.
- Always validate date formats in replaceWhere ranges.
- Ensure the delta-spark package is installed if you use Delta outside of Databricks.
- For XML, ensure the spark-xml library is available in the cluster/session.
- Keep output keys descriptive and aligned with schema/sub_folder/table conventions.

---

## Minimal End-to-End Example

```python
from tauro.io import InputLoader, OutputManager

context = {
    "input_config": {
        "in": {"format": "parquet", "filepath": "/data/in.parquet"},
    },
    "output_config": {
        "out_delta:demo/raw/events": {"format": "delta", "table_name": "events", "schema": "demo", "sub_folder": "raw"},
    },
    "output_path": "/data/output",
    "execution_mode": "local",
    "spark": spark,  # your SparkSession
    "global_settings": {"fail_on_error": True},
}

inp = InputLoader(context).load_inputs({"input": ["in"]})[0]
OutputManager(context).save_output({"output": ["out_delta:demo/raw/events"]}, inp)
```

---

## API at a Glance

- InputLoader
  - load_inputs(node)
- OutputManager
  - save_output(node, df, start_date=None, end_date=None, model_version=None)
- PathResolver
  - resolve_output_path(dataset_config, out_key)
- DataWriter
  - write_data(df, path, config)
- UnityCatalogManager
  - write_to_unity_catalog(df, config, start_date, end_date, out_key)
- ModelArtifactManager
  - save_model_artifacts(node, model_version)

Implementation uses BaseIO helpers to access context consistently.

---

## Troubleshooting

- Spark not available:
  - Ensure context["spark"] or context.spark is set when required.
- Delta errors:
  - Install delta-spark or ensure the cluster has Delta enabled.
- UC errors:
  - Check that UC is enabled in Spark (spark.databricks.unityCatalog.enabled=true) and you have permissions for catalog/schema/table.
- File not found (local):
  - Verify file paths and that execution_mode is set correctly.

---
