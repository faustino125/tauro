## Introduction

The `tauro.io` module provides a unified interface for reading and writing data across multiple formats and environments, with support for Apache Spark and Unity Catalog. It is designed to handle input/output (IO) operations for various data formats, including Parquet, CSV, Delta, JSON, ORC, Avro, XML, Pickle, and SQL queries. The module is highly configurable and supports both local and distributed environments, making it an excellent choice for big data applications.

## Setting Up the Context

To use the reading and writing classes in `tauro.io`, you must first provide a context containing the necessary configuration. The context is a dictionary that includes input and output configurations and, optionally, a SparkSession instance for distributed environments.

**Example Context:**

```python
context = {
    "input_config": {
        "dataset1": {"format": "parquet", "filepath": "/path/to/dataset1"},
        "dataset2": {"format": "csv", "filepath": "/path/to/dataset2", "options": {"header": "true"}},
    },
    "output_config": {
        "output1": {"format": "delta", "table_name": "my_table", "schema": "my_schema", "catalog_name": "my_catalog"},
    },
    "spark": spark_session,  # Optional, required only if using Spark
    "execution_mode": "distributed",  # Options: "distributed" or "local"
}
```

## Reading Data

The `tauro.io` module provides the `InputLoader` class for loading input data. This class supports both sequential and parallel loading strategies, depending on the configuration and Spark availability.

To load data, instantiate `InputLoader` with the context and call the `load_inputs` method, passing a node configuration that specifies the input keys.

**General Example:**

```python
from tauro.io import InputLoader

node = {
    "input": ["dataset1", "dataset2"],
    "parallel": True,  # Optional, enables parallel loading if Spark is available
}

loader = InputLoader(context)
inputs = loader.load_inputs(node)
```

### Reading Examples by Format

#### Reading Parquet

```python
# Configuration in context["input_config"]
"dataset_parquet": {"format": "parquet", "filepath": "/path/to/data.parquet"}

# Loading
inputs = loader.load_inputs({"input": ["dataset_parquet"]})
df_parquet = inputs[0]  # Returns a Spark DataFrame
```

#### Reading CSV

```python
# Configuration in context["input_config"]
"dataset_csv": {"format": "csv", "filepath": "/path/to/data.csv", "options": {"header": "true", "inferSchema": "true"}}

# Loading
inputs = loader.load_inputs({"input": ["dataset_csv"]})
df_csv = inputs[0]  # Returns a Spark DataFrame
```

#### Reading Delta

```python
# Configuration in context["input_config"]
"dataset_delta": {"format": "delta", "filepath": "/path/to/delta_table", "version": 5}  # Optional: version or timestamp

# Loading
inputs = loader.load_inputs({"input": ["dataset_delta"]})
df_delta = inputs[0]  # Returns a Spark DataFrame
```

#### Reading Pickle

```python
# Configuration in context["input_config"]
"dataset_pickle": {"format": "pickle", "filepath": "/path/to/data.pkl", "use_pandas": True}

# Loading
inputs = loader.load_inputs({"input": ["dataset_pickle"]})
data_pickle = inputs[0]  # Returns a Pandas DataFrame if use_pandas=True, otherwise a Spark DataFrame or RDD
```

#### Reading SQL Query

```python
# Configuration in context["input_config"]
"dataset_query": {"format": "query", "query": "SELECT * FROM my_table WHERE condition"}

# Loading
inputs = loader.load_inputs({"input": ["dataset_query"]})
df_query = inputs[0]  # Returns a Spark DataFrame
```

## Writing Data

The `tauro.io` module provides the `OutputManager` class for writing output data. This class supports writing to various formats and integrates with Unity Catalog for Delta tables.

To write data, instantiate `OutputManager` with the context and call the `save_output` method, passing the node configuration, the DataFrame to write, and optional parameters like start/end dates for partitioning or a model version for artifacts.

**General Example:**

```python
from tauro.io import OutputManager

node = {
    "output": ["output1"],
    "model_artifacts": [{"name": "my_model"}],
}

df = ...  # Spark DataFrame, Pandas DataFrame, etc.
model_version = "v1.0"

manager = OutputManager(context)
manager.save_output(node, df, model_version=model_version)
```

### Writing Examples by Format

#### Writing to Delta (Unity Catalog)

```python
# Configuration in context["output_config"]
"output_delta_uc": {
    "format": "unity_catalog",
    "table_name": "my_table",
    "schema": "my_schema",
    "catalog_name": "my_catalog",
    "write_mode": "overwrite",
    "partition_col": "date",
    "description": "My Delta table",
}

# Writing
manager.save_output({"output": ["output_delta_uc"]}, df, start_date="2023-01-01", end_date="2023-01-31")
```

#### Writing to Parquet

```python
# Configuration in context["output_config"]
"output_parquet": {
    "format": "parquet",
    "filepath": "/path/to/output.parquet",
    "write_mode": "append",
    "partition": ["year", "month"],
}

# Writing
manager.save_output({"output": ["output_parquet"]}, df)
```

#### Writing to CSV

```python
# Configuration in context["output_config"]
"output_csv": {
    "format": "csv",
    "filepath": "/path/to/output.csv",
    "write_mode": "overwrite",
    "options": {"header": "true", "delimiter": ","},
}

# Writing
manager.save_output({"output": ["output_csv"]}, df)
```

## Exception Handling

The `tauro.io` module may raise custom exceptions during read and write operations to indicate specific errors. Proper exception handling ensures application robustness.

**Common Exceptions:**

- `ConfigurationError`: Raised when the configuration is invalid or missing required fields.
- `DataValidationError`: Raised when data fails validation, such as an empty DataFrame.
- `FormatNotSupportedError`: Raised when an unsupported format is specified.
- `WriteOperationError` and `ReadOperationError`: Raised when write or read operations fail, respectively.

**Example of Exception Handling:**

```python
from tauro.io.exceptions import ConfigurationError, ReadOperationError, WriteOperationError
import logging

logger = logging.getLogger(__name__)

try:
    inputs = loader.load_inputs(node)
except ConfigurationError as e:
    logger.error(f"Configuration error: {e}")
except ReadOperationError as e:
    logger.error(f"Read operation failed: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")

try:
    manager.save_output(node, df)
except ConfigurationError as e:
    logger.error(f"Configuration error: {e}")
except WriteOperationError as e:
    logger.error(f"Write operation failed: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```

## Summary

The `tauro.io` module offers a comprehensive and flexible solution for managing IO operations in data applications, supporting a wide range of formats and environments. Its modular, object-oriented design simplifies data reading and writing, as well as model artifact management, with a focus on robustness and scalability.

For advanced use cases, such as parallel loading of multiple datasets or optimized writing to Unity Catalog, the module provides detailed configuration options and customization capabilities.