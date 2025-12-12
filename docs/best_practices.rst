Best Practices
==============

This guide covers best practices for using Tauro effectively.

Security
--------

Path Validation
~~~~~~~~~~~~~~~

Always validate file paths to prevent directory traversal attacks:

.. code-block:: python

   from tauro import PathValidator

   validator = PathValidator(allowed_paths=["/data", "/tmp"])
   
   # This will pass
   validator.validate("/data/sales.csv")
   
   # This will raise SecurityError
   validator.validate("/etc/passwd")

Configuration
.. code-block:: yaml

   security:
     validate_paths: true
     allowed_paths:
       - "/data"
       - "/tmp"
       - "/app/output"

Input Sanitization
~~~~~~~~~~~~~~~~~~

Sanitize all user inputs:

.. code-block:: python

   from tauro import InputSanitizer

   sanitizer = InputSanitizer()
   
   # Sanitize pipeline name
   safe_name = sanitizer.sanitize_pipeline_name(user_input)
   
   # Sanitize SQL
   safe_sql = sanitizer.sanitize_sql(user_query)

Credential Management
~~~~~~~~~~~~~~~~~~~~~

Never hardcode credentials:

.. code-block:: yaml

   # ❌ BAD
   database:
     password: "my_secret_password"

   # ✅ GOOD
   database:
     password: "${DB_PASSWORD}"

Use environment variables or secret managers:

.. code-block:: bash

   export DB_PASSWORD="secret"
   
   # Or use AWS Secrets Manager, Azure Key Vault, etc.

Performance
-----------

Enable Caching
~~~~~~~~~~~~~~

Cache frequently accessed configurations:

.. code-block:: python

   from tauro import ContextLoader

   # Configuration is cached automatically (TTL: 5 minutes)
   context = ContextLoader().load_from_env("prod")

Use Appropriate Data Formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   # For large datasets
   output:
     format: "delta"  # or "parquet"
     compression: "snappy"

   # For small lookup tables
   output:
     format: "csv"
     compression: "gzip"

Optimize Spark Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   spark:
     config:
       # Enable adaptive query execution
       spark.sql.adaptive.enabled: true
       spark.sql.adaptive.coalescePartitions.enabled: true
       
       # Optimize shuffle
       spark.sql.shuffle.partitions: 200
       spark.sql.autoBroadcastJoinThreshold: 10485760  # 10MB
       
       # Memory tuning
       spark.memory.fraction: 0.8
       spark.memory.storageFraction: 0.3

Partition Data Effectively
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   output:
     partition_by:
       - "year"
       - "month"
       - "day"
     # Creates: /data/year=2024/month=01/day=15/

   # ✅ Good: 1000-10000 partitions
   # ❌ Bad: Too many (>50000) or too few (<10)

Development
-----------

Use Version Control
~~~~~~~~~~~~~~~~~~~

Track all code and configuration:

.. code-block:: bash

   git add config/ pipelines/ notebooks/
   git commit -m "Update pipeline configuration"
   git push

Separate Environments
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

   ├── config/
   │   ├── base/     # Shared configuration
   │   ├── dev/      # Development overrides
   │   ├── staging/  # Staging overrides
   │   └── prod/     # Production overrides

Always Test in Dev First
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # 1. Test in dev
   tauro --env dev --pipeline new_pipeline --validate-only
   tauro --env dev --pipeline new_pipeline

   # 2. Test in staging
   tauro --env staging --pipeline new_pipeline

   # 3. Deploy to production
   tauro --env prod --pipeline new_pipeline

Write Unit Tests
~~~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   from tauro import PipelineExecutor, ContextLoader

   @pytest.fixture
   def test_executor():
       context = ContextLoader().load_from_env("test")
       return PipelineExecutor(context)

   def test_bronze_ingestion(test_executor):
       result = test_executor.execute("bronze_ingestion")
       assert result.success
       assert result.nodes_executed == 3
       assert result.metrics['records_loaded'] > 0

   def test_data_quality(test_executor):
       result = test_executor.execute("quality_checks")
       assert result.success
       assert result.metrics['null_count'] == 0
       assert result.metrics['duplicate_count'] == 0

Use Linting and Formatting
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Install pre-commit hooks
   pip install pre-commit
   pre-commit install

   # Format code
   black src/
   isort src/

   # Lint code
   flake8 src/
   pylint src/

Production
----------

Enable Monitoring
~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   monitoring:
     enabled: true
     metrics:
       - type: "prometheus"
         port: 9090
       - type: "cloudwatch"
         namespace: "Tauro/Pipelines"

   logging:
     level: "WARNING"
     handlers:
       - type: "syslog"
         host: "log-server.company.com"
       - type: "file"
         filename: "/var/log/tauro/pipeline.log"

Set Up Alerts
~~~~~~~~~~~~~

.. code-block:: yaml

   alerts:
     - name: "pipeline_failure"
       condition: "pipeline.status == 'FAILED'"
       channels:
         - slack: "#data-alerts"
         - email: "data-team@company.com"
     
     - name: "long_execution"
       condition: "pipeline.duration > 3600"
       channels:
         - slack: "#data-monitoring"

Configure Retry Policies
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipelines:
     critical_etl:
       retry:
         enabled: true
         max_attempts: 3
         backoff_seconds: 60
         exponential_backoff: true

Use Circuit Breakers
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import CircuitBreaker

   breaker = CircuitBreaker(
       failure_threshold=5,
       timeout_seconds=60
   )

   @breaker.protected
   def call_external_api():
       # API call that might fail
       pass

Set Resource Limits
~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   resources:
     max_workers: 16
     timeout_seconds: 7200
     memory_limit: "32G"
     cpu_limit: "16"

Logging
-------

Structured Logging
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import logging
   import json

   logger = logging.getLogger(__name__)

   # Structured log entry
   logger.info(json.dumps({
       "event": "pipeline_started",
       "pipeline": "sales_etl",
       "environment": "production",
       "start_date": "2024-01-01",
       "end_date": "2024-01-31"
   }))

Log Levels
~~~~~~~~~~

Use appropriate log levels:

.. code-block:: python

   # DEBUG: Detailed diagnostic information
   logger.debug(f"Processing record: {record_id}")

   # INFO: General informational messages
   logger.info(f"Pipeline started: {pipeline_name}")

   # WARNING: Warning messages
   logger.warning(f"Skipping invalid record: {record_id}")

   # ERROR: Error messages
   logger.error(f"Failed to load data: {error}")

   # CRITICAL: Critical errors
   logger.critical(f"Database connection lost")

Include Context
~~~~~~~~~~~~~~~

.. code-block:: python

   logger.info(
       f"Pipeline completed: {pipeline_name}",
       extra={
           "nodes_executed": result.nodes_executed,
           "execution_time": result.execution_time_seconds,
           "records_processed": result.metrics['records_processed']
       }
   )

Error Handling
--------------

Graceful Degradation
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import PipelineExecutor

   try:
       result = executor.execute("pipeline")
       
       if not result.success:
           # Try alternative approach
           logger.warning("Primary pipeline failed, trying backup")
           result = executor.execute("backup_pipeline")
   
   except Exception as e:
       logger.error(f"Both pipelines failed: {e}")
       # Notify ops team
       send_alert("Pipeline failure", str(e))

Detailed Error Messages
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   if not result.success:
       logger.error(
           f"Pipeline failed: {result.error_message}\n"
           f"Failed node: {result.failed_node}\n"
           f"Traceback: {result.traceback}"
       )

Recovery Mechanisms
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Implement checkpointing
   checkpoint_dir = "/tmp/checkpoints"
   
   result = executor.execute(
       "long_pipeline",
       checkpoint_location=checkpoint_dir
   )
   
   # Pipeline can resume from last checkpoint on failure

Data Quality
------------

Validate Schemas
~~~~~~~~~~~~~~~~

.. code-block:: yaml

   validation:
     schema:
       - name: "customer_id"
         type: "string"
         nullable: false
       - name: "amount"
         type: "decimal(10,2)"
         nullable: false
         min: 0
         max: 1000000

Check Data Quality
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import DataQualityChecker

   checker = DataQualityChecker()
   
   # Check for nulls
   null_check = checker.check_nulls(df, ["customer_id", "amount"])
   
   # Check for duplicates
   dup_check = checker.check_duplicates(df, ["transaction_id"])
   
   # Check ranges
   range_check = checker.check_range(df, "amount", min=0, max=1000000)
   
   if not all([null_check, dup_check, range_check]):
       raise ValueError("Data quality checks failed")

Implement Data Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   data_contract:
     bronze_sales:
       schema_version: "1.0"
       columns:
         customer_id:
           type: "string"
           nullable: false
         amount:
           type: "decimal"
           min: 0
       quality_checks:
         - null_threshold: 0.05
         - duplicate_threshold: 0.01

Documentation
-------------

Document Pipelines
~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipelines:
     customer_360:
       description: |
         Creates a 360-degree view of customers by combining:
         - Transaction data from sales system
         - Profile data from CRM
         - Interaction data from support tickets
       
       owner: "data-team@company.com"
       schedule: "0 2 * * *"
       sla_hours: 4
       
       inputs:
         - sales_transactions
         - crm_profiles
         - support_tickets
       
       outputs:
         - customer_360_view

Document Nodes
~~~~~~~~~~~~~~

.. code-block:: python

   def transform_sales_data(df):
       """
       Transform raw sales data for analysis.
       
       Args:
           df: Raw sales DataFrame with columns:
               - transaction_id (str)
               - customer_id (str)
               - amount (decimal)
               - date (date)
       
       Returns:
           Transformed DataFrame with additional columns:
               - year (int)
               - month (int)
               - quarter (int)
       
       Raises:
           ValueError: If required columns are missing
       """
       # Implementation
       pass

Maintain Changelog
~~~~~~~~~~~~~~~~~~

.. code-block:: text

   ## [1.2.0] - 2024-01-15
   
   ### Added
   - New customer_360 pipeline
   - Data quality checks for bronze layer
   
   ### Changed
   - Improved performance of silver transformation
   - Updated Spark configuration for production
   
   ### Fixed
   - Fixed null handling in gold aggregation

Checklist
---------

Before Production Deployment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- [ ] All tests passing
- [ ] Configuration validated
- [ ] Documentation updated
- [ ] Monitoring configured
- [ ] Alerts set up
- [ ] Resource limits defined
- [ ] Retry policies configured
- [ ] Data quality checks in place
- [ ] Security review completed
- [ ] Performance tested
- [ ] Backup strategy defined
- [ ] Rollback plan documented

Next Steps
----------

- Read :doc:`advanced/security` for security details
- Check :doc:`advanced/performance` for optimization
- See :doc:`advanced/testing` for testing strategies
- Review :doc:`advanced/troubleshooting` for common issues
