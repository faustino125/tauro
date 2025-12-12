Configuration
=============

This guide covers Tauro's configuration system in detail.

Overview
--------

Tauro uses a flexible, environment-based configuration system that supports:

- Multiple file formats (YAML, JSON, DSL)
- Environment-specific overrides
- Configuration inheritance
- Dynamic interpolation
- Validation and type checking

Configuration Structure
-----------------------

Standard Layout
~~~~~~~~~~~~~~~

.. code-block:: text

   project/
   ├── config/
   │   ├── base/                    # Base configuration
   │   │   ├── global_settings.yaml # Global settings
   │   │   ├── pipelines.yaml       # Pipeline definitions
   │   │   ├── nodes.yaml           # Node definitions
   │   │   ├── input.yaml           # Input sources
   │   │   └── output.yaml          # Output destinations
   │   ├── dev/                     # Development overrides
   │   │   ├── global_settings.yaml
   │   │   └── ...
   │   ├── staging/                 # Staging overrides
   │   └── prod/                    # Production overrides
   └── settings.json                # Environment mapping

Environment Mapping
~~~~~~~~~~~~~~~~~~~

``settings.json``:

.. code-block:: json

   {
     "environments": {
       "dev": {
         "config_dir": "config/dev",
         "fallback": "base"
       },
       "staging": {
         "config_dir": "config/staging",
         "fallback": "prod"
       },
       "prod": {
         "config_dir": "config/prod",
         "fallback": "base"
       }
     },
     "default_environment": "dev"
   }

Global Settings
---------------

``config/base/global_settings.yaml``:

.. code-block:: yaml

   # Spark Configuration
   spark:
     app_name: "Tauro Pipeline"
     master: "local[*]"
     config:
       spark.sql.shuffle.partitions: 200
       spark.sql.adaptive.enabled: true
       spark.sql.adaptive.coalescePartitions.enabled: true
       spark.driver.memory: "4g"
       spark.executor.memory: "4g"

   # Logging Configuration
   logging:
     level: "INFO"
     format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
     handlers:
       - type: "console"
       - type: "file"
         filename: "logs/tauro.log"
         max_bytes: 10485760  # 10MB
         backup_count: 5

   # Resource Management
   resources:
     max_workers: 4
     timeout_seconds: 3600
     memory_limit: "8G"
     enable_cache: true
     cache_ttl_seconds: 300

   # Security
   security:
     validate_paths: true
     allowed_paths:
       - "/data"
       - "/tmp"
     sanitize_inputs: true

Pipeline Configuration
----------------------

``config/base/pipelines.yaml``:

.. code-block:: yaml

   # Bronze Layer Pipeline
   bronze_ingestion:
     description: "Ingest raw data into Bronze layer"
     nodes:
       - load_raw_sales
       - load_raw_customers
       - load_raw_products
     dependencies: []
     schedule: "0 2 * * *"  # Daily at 2 AM
     tags:
       - bronze
       - ingestion
     retry:
       enabled: true
       max_attempts: 3
       backoff_seconds: 60

   # Silver Layer Pipeline
   silver_cleansing:
     description: "Clean and validate data in Silver layer"
     nodes:
       - validate_sales
       - deduplicate_customers
       - enrich_products
     dependencies:
       - bronze_ingestion
     schedule: "0 3 * * *"  # Daily at 3 AM
     tags:
       - silver
       - cleansing

   # Gold Layer Pipeline
   gold_aggregation:
     description: "Aggregate metrics in Gold layer"
     nodes:
       - calculate_daily_sales
       - customer_360_view
       - product_performance
     dependencies:
       - silver_cleansing
     schedule: "0 4 * * *"  # Daily at 4 AM
     tags:
       - gold
       - metrics

Node Configuration
------------------

``config/base/nodes.yaml``:

.. code-block:: yaml

   # Bronze Nodes
   load_raw_sales:
     type: "input"
     module: "core.io.readers"
     class: "CSVReader"
     params:
       input_key: "raw_sales"
       schema_validation: true
       error_handling: "skip"
     outputs:
       - "bronze_sales"

   load_raw_customers:
     type: "input"
     module: "core.io.readers"
     class: "ParquetReader"
     params:
       input_key: "raw_customers"
     outputs:
       - "bronze_customers"

   # Silver Nodes
   validate_sales:
     type: "transform"
     module: "core.exec.node_executor"
     class: "PythonNode"
     params:
       function: "validators.validate_sales_data"
       inputs:
         - "bronze_sales"
       validation_rules:
         - column: "amount"
           rule: "not_null"
         - column: "amount"
           rule: "positive"
         - column: "date"
           rule: "valid_date"
     outputs:
       - "silver_sales"

   deduplicate_customers:
     type: "transform"
     module: "core.exec.node_executor"
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
     module: "core.exec.node_executor"
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
