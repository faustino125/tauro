Configuration Guide
====================

This guide explains how to configure your Tauro pipelines. Don't worry—it's just YAML, which is much simpler than it looks!

The Big Picture
---------------

A Tauro project has five core configuration files plus a small descriptor that maps environments to them.

1. **global_settings.* (yaml/json/dsl)** - General settings (paths, Spark config, hyperparameters)
2. **pipelines.* (yaml/json/dsl)** - Which pipelines you have and how they link to nodes
3. **nodes.* (yaml/json/dsl)** - What each node does, which module to import, and its dependencies
4. **input.* (yaml/json/dsl)** - Data sources tied to logical input names
5. **output.* (yaml/json/dsl)** - Output targets and write modes

The CLI templates generate a single **settings_<format>.json** file (``settings_json.json`` by default) that tells Tauro where to find each of those five files for every environment (``base``, ``dev``, ``sandbox``, ``prod``). That descriptor drives both the CLI and the Python API, so keep it in your project root alongside the ``settings_*`` variants.

Environment Descriptor (``settings_json.json``)
----------------------------------------------

Tauro keeps the environment-to-file mapping in a JSON file with the suffix that matches your chosen format (``settings_json.json``, ``settings_yml.json``, or ``settings_dsl.json``). Each entry lists absolute or relative paths to the five configuration files used by that environment. The template generator produces something like the following:

.. code-block:: json

   {
     "base_path": ".",
     "env_config": {
       "base": {
         "global_settings_path": "config/global_settings.yaml",
         "pipelines_config_path": "config/pipelines.yaml",
         "nodes_config_path": "config/nodes.yaml",
         "input_config_path": "config/input.yaml",
         "output_config_path": "config/output.yaml"
       },
       "dev": {
         "global_settings_path": "config/dev/global_settings.yaml",
         "input_config_path": "config/dev/input.yaml",
         "output_config_path": "config/dev/output.yaml"
       }
     }
   }

Tauro expects the descriptor to expose at least the five main files, but you can add more keys if you have service-specific configs.

Each file has a simple purpose. Let's look at each one.

Getting Started: The Basics
----------------------------

Here's the simplest possible configuration:

**config/global_settings.yaml** - Paths and Settings

.. code-block:: yaml

   input_path: /data/input           # Where to read data from
   output_path: /data/output         # Where to save results
   log_level: INFO                   # How verbose (DEBUG, INFO, WARNING, ERROR)

**config/pipelines.yaml** - List Your Pipelines

.. code-block:: yaml

   pipelines:
     my_pipeline:
       nodes: [extract, transform, load]
       description: "My first pipeline"

**config/nodes.yaml** - Define Steps

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract"        # Python module path
       description: "Read data from file"
       dependencies: []                      # No dependencies

     transform:
       function: "src.nodes.transform"      # Python module path
       description: "Clean the data"
       dependencies: [extract]               # Depends on extract node

     load:
       function: "src.nodes.load"           # Python module path
       description: "Save results"
       dependencies: [transform]             # Depends on transform node

**Note:** Dependencies determine execution order. Nodes run in parallel when possible,
but respect dependency chains. See the "Node Dependencies" section below.

**config/input.yaml** - Data Sources

.. code-block:: yaml

   inputs:
     raw_data:
       path: data/input/sample.csv
       format: csv

**config/output.yaml** - Save Results

.. code-block:: yaml

   outputs:
     results:
       path: data/output/results.parquet
       format: parquet

That's a complete working pipeline! Now let's dive deeper.

Detailed Configuration
----------------------

Global Settings (global_settings.*)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This file controls overall behavior:

.. code-block:: yaml

   # Required
   input_path: /data/input
   output_path: /data/output

   # Optional but useful
   log_level: INFO
   max_workers: 4              # Parallel execution (default: 4)
   timeout_seconds: 3600       # Max time (1 hour)

   # For Spark (if using large data)
   mode: local                 # or "databricks", "distributed"
   spark:
     shuffle_partitions: 200
     memory: "4g"

Pipelines (pipelines.yaml)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define what pipelines you have. A pipeline is a sequence of steps:

.. code-block:: yaml

   pipelines:
     # Simple pipeline
     basic_etl:
       nodes: [extract, transform, load]

     # More complex pipeline
     advanced_pipeline:
       nodes: [validate, transform, aggregate, load]
       description: "Process sales data"
       schedule: "0 9 * * *"           # Run at 9 AM daily (cron format)

Nodes (nodes.yaml)
~~~~~~~~~~~~~~~~~~~

Each node is a step that does work. Point it to Python code:

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract"           # Path to Python function
       description: "Read from source"
       timeout: 300                             # Max 5 minutes
       retry: 3                                 # Retry 3 times if it fails

     transform:
       function: "src.nodes.transform"
       input: raw_data                          # Use "raw_data" from inputs.yaml
       output: processed_data                   # Save result as "processed_data"

     load:
       function: "src.nodes.load"

The ``function`` field points to your Python code. For example, ``src.nodes.extract`` means there's a file ``src/nodes/extract.py`` with a function inside.

Inputs (inputs.yaml)
~~~~~~~~~~~~~~~~~~~~~

Define where data comes from:

.. code-block:: yaml

   inputs:
     # File on local computer
     local_file:
       path: /data/input/sales.csv
       format: csv

     # Multiple files
     all_parquets:
       path: /data/input/*.parquet
       format: parquet

     # Cloud storage (AWS S3)
     s3_data:
       path: s3://my-bucket/data/
       format: parquet
       aws_profile: default              # AWS credentials

     # Databricks table
     databricks_table:
       path: catalog.schema.table_name
       format: delta

Outputs (outputs.yaml)
~~~~~~~~~~~~~~~~~~~~~~~

Define where to save results:

.. code-block:: yaml

   outputs:
     # Save to local file
     local_output:
       path: /data/output/results.parquet
       format: parquet
       mode: overwrite                   # or "append"

     # Save to cloud
     s3_output:
       path: s3://my-bucket/results/
       format: parquet

     # Save to Databricks
     databricks_output:
       path: catalog.schema.output_table
       format: delta

Real-World Example: Sales ETL
------------------------------

Let's build a realistic sales pipeline:

**pipelines.yaml**

.. code-block:: yaml

   pipelines:
     daily_sales_etl:
       nodes: [extract_sales, clean_data, load_results]
       description: "Daily sales data pipeline"

**nodes.yaml**

.. code-block:: yaml

   nodes:
     extract_sales:
       function: "src.nodes.extract.get_daily_sales"
       description: "Fetch sales from database"
       timeout: 600

     clean_data:
       function: "src.nodes.transform.clean_sales"
       description: "Remove bad records, normalize"
       timeout: 900

     load_results:
       function: "src.nodes.load.save_to_warehouse"
       description: "Save to data warehouse"
       timeout: 600

**inputs.yaml**

.. code-block:: yaml

   inputs:
     sales_database:
       path: "postgresql://localhost/sales"
       table: "transactions"
       query: "SELECT * FROM transactions WHERE date >= {{START_DATE}}"

**outputs.yaml**

.. code-block:: yaml

   outputs:
     warehouse:
       path: s3://my-company/data/sales/
       format: parquet
       partitioned_by: date

**src/nodes/extract.py**

.. code-block:: python

   import pandas as pd

   def get_daily_sales(sales_database):
       """Fetch today's sales."""
       df = pd.read_sql(
           "SELECT * FROM transactions WHERE date = TODAY()",
           sales_database
       )
       return df

**src/nodes/transform.py**

.. code-block:: python

   import pandas as pd

   def clean_sales(df):
       """Clean the sales data."""
       # Remove nulls
       df = df.dropna()
       # Remove negative amounts
       df = df[df['amount'] > 0]
       return df

**src/nodes/load.py**

.. code-block:: python

   import pandas as pd

   def save_to_warehouse(df):
       """Save results."""
       df.to_parquet('data/output/sales.parquet')
       print(f"Saved {len(df)} records")

Using Environment Variables
----------------------------

Never hardcode secrets or paths. Use environment variables:

**config/global_settings.yaml**

.. code-block:: yaml

   input_path: ${INPUT_PATH}          # Read from environment
   output_path: ${OUTPUT_PATH}

**.env file (never commit this!)**

.. code-block:: bash

  INPUT_PATH=/data/input
  OUTPUT_PATH=/data/output
  DB_PASSWORD=secret123

Tauro does not load ``.env`` by default. Source the file yourself (for example ``source .env`` on Unix or ``python -m dotenv run --``) before invoking the CLI or library calls.

**Run it:**

.. code-block:: bash

  source .env
  tauro --env dev --pipeline my_pipeline

Node Dependencies
------------------

Each node can depend on other nodes. Dependencies define execution order and form a DAG (Directed Acyclic Graph).

**Single dependency (linear chain):**

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract"
       dependencies: []                  # No dependencies, runs first
     
     transform:
       function: "src.nodes.transform"
       dependencies: [extract]           # Waits for extract to finish
     
     load:
       function: "src.nodes.load"
       dependencies: [transform]         # Waits for transform to finish

Execution order: extract → transform → load

**Multiple dependencies (merge):**

.. code-block:: yaml

   nodes:
     extract_sales:
       function: "src.nodes.extract_sales"
       dependencies: []                  # Runs first
     
     extract_customers:
       function: "src.nodes.extract_customers"
       dependencies: []                  # Runs first (parallel with extract_sales)
     
     merge:
       function: "src.nodes.merge"
       dependencies: [extract_sales, extract_customers]  # Waits for both to finish
     
     load:
       function: "src.nodes.load"
       dependencies: [merge]             # Runs last

Execution: extract_sales and extract_customers run in parallel, then merge, then load

**Important:**

- Dependencies are optional. Nodes without dependencies run immediately.
- Circular dependencies cause errors. Tauro detects and rejects them early.
- Parallel execution respects the dependency graph.

Variable Interpolation
-----------------------

Tauro supports environment variables and configuration variable references:

**Environment variables:**

.. code-block:: yaml

   database:
     password: ${DB_PASSWORD}        # Must be set, error if missing
     timeout: ${TIMEOUT|3600}        # With default value

**Configuration variables:**

.. code-block:: yaml

   base_path: /data/project
   paths:
     input: ${base_path}/raw         # Resolves to /data/project/raw
     output: ${base_path}/processed  # Resolves to /data/project/processed

**Default values:**

.. code-block:: yaml

   timeout_seconds: ${TIMEOUT|3600}      # Use TIMEOUT or default to 3600
   log_level: ${LOG_LEVEL|INFO}          # Use LOG_LEVEL or default to INFO

**Setting environment variables:**

.. code-block:: bash

   export DB_PASSWORD="my_secret_password"
   export TIMEOUT="5000"
   tauro --env prod --pipeline my_pipeline

Or use a ``.env`` file:

.. code-block:: bash

   # .env (add to .gitignore)
   DB_PASSWORD=my_secret_password
   TIMEOUT=5000

.. note::

   Tauro does **not** automatically load .env files. Use ``python-dotenv`` or load manually.

Different Configurations per Environment
-----------------------------------------

It's common to have dev, staging, and production environments with different settings.

**Directory structure:**

.. code-block:: text

     config/
     ├── global_settings.yaml     # Base (used by all)
     ├── input.yaml
     ├── output.yaml
     ├── pipelines.yaml
     ├── nodes.yaml
     ├── dev/
     │   └── global_settings.yaml # Override for dev
     ├── staging/
     │   └── global_settings.yaml # Override for staging
     └── prod/
       └── global_settings.yaml # Override for production

When you run ``tauro --env prod``, Tauro loads:
  1. Base config files (config/global_settings.yaml)
  2. Environment overrides (config/prod/global_settings.yaml)

**Example:**

Base config/global_settings.yaml:
.. code-block:: yaml

   log_level: INFO
   max_workers: 4

Override config/prod/global_settings.yaml:
.. code-block:: yaml

   log_level: WARNING        # Less logging in prod
   max_workers: 8            # More workers in prod

Performance Tuning
------------------

Running slow? Try these:

.. code-block:: yaml

   # Use more workers for parallelism
   max_workers: 8

   # Increase Spark memory for large datasets
   spark:
     driver_memory: "8g"
     executor_memory: "8g"
     shuffle_partitions: 500

   # Cache results between runs
   enable_caching: true

Common Mistakes
---------------

❌ **Don't:**

.. code-block:: yaml

   # Hardcoding passwords
   database:
     password: "my_secret"

   # Using absolute Windows paths
   path: "C:\\Users\\John\\data.csv"

   # Forgetting to define nodes
   pipelines:
     my_pipeline:
       nodes: [missing_node]   # This node not defined!

✅ **Do:**

.. code-block:: yaml

   # Use environment variables
   database:
     password: ${DB_PASSWORD}

   # Use relative or cloud paths
   path: /data/input/data.csv
   path: s3://bucket/data/

   # Define every node
   nodes:
     extract:
       function: "src.nodes.extract"

Troubleshooting
---------------

**"Config file not found"**

Make sure files are in the right place:

.. code-block:: bash

   ls -la config/
  # Should show: global_settings.yaml, pipelines.yaml, nodes.yaml, input.yaml, output.yaml, settings_json.json.

**"Invalid YAML"**

YAML is sensitive to spaces. Use 2 spaces for indentation:

.. code-block:: yaml

   # ✓ Correct (2 spaces)
   pipelines:
     my_pipeline:
       nodes: [a, b, c]

   # ✗ Wrong (1 space)
   pipelines:
    my_pipeline:
      nodes: [a, b, c]

**"Pipeline not found"**

The pipeline name in your command must exactly match pipelines.yaml:

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline

   # The pipeline in pipelines.yaml must be exactly:
   # pipelines:
   #   my_pipeline:

Next Steps
----------

- :doc:`guides/batch_etl` - Build a complete ETL example
- :doc:`cli_usage` - Learn CLI commands
- :doc:`best_practices` - Learn patterns for success
           rule: "not_null"
         - column: "amount"
           rule: "positive"
         - column: "date"
           rule: "valid_date"
     outputs:
       - "silver_sales"

   deduplicate_customers:
     type: "transform"
     module: "tauro.exec.node_executor"
     class: "SparkSQLNode"
     params:
       sql: |
         SELECT DISTINCT
           customer_id,
           first_name,
           last_name,
           email,
           MAX(updated_at) as updated_at
         FROM bronze_customers
         GROUP BY customer_id, first_name, last_name, email
     outputs:
       - "silver_customers"

   # Gold Nodes
   calculate_daily_sales:
     type: "aggregation"
     module: "tauro.exec.node_executor"
     class: "AggregationNode"
     params:
       input: "silver_sales"
       group_by:
         - "date"
         - "region"
       aggregations:
         total_sales: "SUM(amount)"
         transaction_count: "COUNT(*)"
         avg_transaction: "AVG(amount)"
     outputs:
       - "gold_daily_sales"

Input Configuration
-------------------

``config/base/input.yaml``:

.. code-block:: yaml

   # CSV Input
   raw_sales:
     format: "csv"
     path: "data/raw/sales/*.csv"
     options:
       header: true
       inferSchema: true
       dateFormat: "yyyy-MM-dd"
       encoding: "UTF-8"
     schema:
       - name: "transaction_id"
         type: "string"
         nullable: false
       - name: "customer_id"
         type: "string"
         nullable: false
       - name: "amount"
         type: "decimal(10,2)"
         nullable: false
       - name: "date"
         type: "date"
         nullable: false

   # Parquet Input
   raw_customers:
     format: "parquet"
     path: "data/raw/customers/*.parquet"
     options:
       mergeSchema: true

   # Delta Table Input
   raw_inventory:
     format: "delta"
     path: "data/raw/inventory"
     options:
       versionAsOf: "latest"

   # Database Input
   product_master:
     format: "jdbc"
     connection:
       url: "jdbc:postgresql://localhost:5432/products"
       driver: "org.postgresql.Driver"
       user: "${DB_USER}"
       password: "${DB_PASSWORD}"
     query: "SELECT * FROM products WHERE active = true"
     options:
       fetchSize: 10000
       numPartitions: 4

Output Configuration
--------------------

``config/base/output.yaml``:

.. code-block:: yaml

   # Parquet Output
   bronze_sales:
     format: "parquet"
     path: "data/bronze/sales"
     write_mode: "append"
     partition_by:
       - "year"
       - "month"
     options:
       compression: "snappy"

   # Delta Table Output
   silver_customers:
     format: "delta"
     path: "data/silver/customers"
     write_mode: "merge"
     merge_condition: "target.customer_id = source.customer_id"
     options:
       mergeSchema: true
       optimizeWrite: true

   # Database Output
   gold_metrics:
     format: "jdbc"
     connection:
       url: "jdbc:postgresql://localhost:5432/analytics"
       driver: "org.postgresql.Driver"
       user: "${DB_USER}"
       password: "${DB_PASSWORD}"
     table: "daily_metrics"
     write_mode: "append"
     options:
       batchSize: 10000

Environment-Specific Overrides
-------------------------------

Development (``config/dev/global_settings.yaml``):

.. code-block:: yaml

   spark:
     master: "local[2]"
     config:
       spark.driver.memory: "2g"
       spark.executor.memory: "2g"

   logging:
     level: "DEBUG"

   resources:
     max_workers: 2
     timeout_seconds: 600

Production (``config/prod/global_settings.yaml``):

.. code-block:: yaml

   spark:
     master: "yarn"
     config:
       spark.driver.memory: "8g"
       spark.executor.memory: "16g"
       spark.executor.instances: 10

   logging:
     level: "WARNING"
     handlers:
       - type: "syslog"
         host: "log-server.company.com"

   resources:
     max_workers: 16
     timeout_seconds: 7200

Configuration Interpolation
----------------------------

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

Use ``${VAR_NAME}`` syntax:

.. code-block:: yaml

   database:
     host: "${DB_HOST}"
     port: "${DB_PORT:5432}"  # With default value
     password: "${DB_PASSWORD}"

Date Variables
~~~~~~~~~~~~~~

.. code-block:: yaml

   paths:
     input: "data/raw/${execution_date}"
     output: "data/processed/${execution_date}"

Runtime Variables
~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipeline:
     name: "${pipeline_name}"
     start_date: "${start_date}"
     end_date: "${end_date}"

Validation
----------

Schema Validation
~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   validation:
     enabled: true
     schema_file: "schemas/sales_schema.json"
     strict_mode: true
     on_error: "fail"  # or "skip", "log"

Data Quality Rules
~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   quality_checks:
     - name: "check_nulls"
       column: "customer_id"
       rule: "not_null"
       threshold: 0.95  # 95% non-null
     - name: "check_range"
       column: "amount"
       rule: "between"
       min: 0
       max: 1000000
     - name: "check_duplicates"
       columns: ["transaction_id"]
       rule: "unique"

Best Practices
--------------

1. **Use environment-specific configs**

   Keep sensitive data in environment configs, not in base configs.

2. **Use configuration inheritance**

   .. code-block:: yaml

      # base/pipelines.yaml
      common_pipeline: &common
        retry:
          enabled: true
          max_attempts: 3

      my_pipeline:
        <<: *common
        nodes: [...]

3. **Validate configurations**

   .. code-block:: python

      from tauro import ConfigManager

      config = ConfigManager()
      is_valid = config.validate("production")

4. **Use version control**

   Track all configuration changes in Git.

5. **Document your configs**

   .. code-block:: yaml

      # Pipeline for daily sales aggregation
      # Runs at 2 AM daily
      # Dependencies: bronze_ingestion
      daily_sales:
        description: "Aggregate daily sales metrics"
        nodes: [...]

Advanced Features
-----------------

Dynamic Configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import ConfigManager

   config = ConfigManager()
   
   # Load with variables
   context = config.load_with_vars(
       env="production",
       variables={
           "start_date": "2024-01-01",
           "end_date": "2024-01-31"
       }
   )

Configuration Caching
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import ConfigCache

   # Cache is automatic with 5-minute TTL
   context1 = ContextLoader().load_from_env("prod")  # Loads from file
   context2 = ContextLoader().load_from_env("prod")  # Loads from cache

   # Invalidate cache
   ConfigCache.invalidate("prod")

Next Steps
----------

- See :doc:`cli_usage` for CLI configuration options
- Check :doc:`library_usage` for programmatic configuration
- Read :doc:`best_practices` for configuration guidelines
- Explore :doc:`advanced/security` for security best practices
