Best Practices
==============

Learn how to use Tauro effectively and build reliable data pipelines.

Project Organization
---------------------

**Keep your project clean and organized.** A good structure makes collaboration easier and reduces errors:

.. code-block:: text

   my_project/
   ├── config/              # Configuration files
   │   ├── global.yaml
   │   ├── pipelines.yaml
   │   ├── nodes.yaml
   │   ├── inputs.yaml
   │   ├── outputs.yaml
   │   ├── dev/             # Dev overrides
   │   ├── staging/         # Staging overrides
   │   └── prod/            # Production overrides
   │
   ├── src/                 # Your Python code
   │   └── nodes/
   │       ├── __init__.py
   │       ├── extract.py
   │       ├── transform.py
   │       └── load.py
   │
   ├── tests/               # Unit tests
   │   ├── test_extract.py
   │   ├── test_transform.py
   │   └── test_load.py
   │
   ├── data/                # Local test data (add to .gitignore)
   │   ├── input/
   │   └── output/
   │
   ├── logs/                # Execution logs (add to .gitignore)
   │
   ├── .env                 # Secrets (add to .gitignore!)
   ├── .gitignore
   ├── requirements.txt
   ├── README.md
   └── Makefile             # Optional: helpful shortcuts

Writing Good Node Functions
-----------------------------

**Specifying Node Function Paths**

Node functions are referenced via Python import paths. Two formats are supported:

.. code-block:: yaml

   # Option 1: Module path (recommended for single function per module)
   nodes:
     extract:
       function: "src.nodes.extract"  # Calls extract() function inside src/nodes/extract.py

   # Option 2: Full function path (explicit, recommended for clarity)
   nodes:
     extract:
       function: "src.nodes.extract.extract_sales"  # Calls extract_sales() in src/nodes/extract.py

**How Tauro resolves paths:**

1. Attempts to import the module
2. If module is callable, uses it
3. If not, searches for a function named after the last path component
4. If explicit function name given, imports module and gets that function

**Best practice:** Use full function paths for clarity, especially in shared projects:

.. code-block:: yaml

   nodes:
     extract_sales:
       function: "src.nodes.extract.get_sales"  # Crystal clear which function
     
     extract_customers:
       function: "src.nodes.extract.get_customers"  # Different from above
     
     clean_data:
       function: "src.nodes.transform.clean"

**Simple and Clear**

Each node function should do one thing well:

.. code-block:: python

   # ✓ Good: Simple, clear purpose
   def extract_sales_data(df):
       """Load sales data from database."""
       return df.dropna()

   # ✗ Bad: Does too much
   def do_everything(df):
       """Process all data."""
       df = df.dropna()
       df = df.groupby(...).sum()
       df = df[df['amount'] > 0]
       return df

**Always Include Docstrings**

Write a one-line description of what the function does:

.. code-block:: python

   def clean_customer_data(df):
       """Remove duplicate customers and invalid emails."""
       df = df.drop_duplicates(subset=['customer_id'])
       df = df[df['email'].str.contains('@')]
       return df

**Handle Errors Gracefully**

Don't let silent failures happen:

.. code-block:: python

   # ✗ Bad: Ignores errors
   def transform_data(df):
       try:
           return df['amount'].apply(float)
       except:
           pass  # Oops, lost data

   # ✓ Good: Handles errors properly
   def transform_data(df):
       """Convert amounts to numbers."""
       try:
           return df['amount'].astype(float)
       except ValueError as e:
           raise ValueError(f"Could not convert amounts: {e}")

**Validate Your Input**

Don't assume data is correct:

.. code-block:: python

   # ✓ Good: Validates data
   def process_sales(df):
       """Process sales data."""
       if df is None or df.empty:
           raise ValueError("No data provided")

       required_cols = ['id', 'amount', 'date']
       missing = [c for c in required_cols if c not in df.columns]
       if missing:
           raise ValueError(f"Missing columns: {missing}")

       return df[df['amount'] > 0]

Configuration Best Practices
------------------------------

**Use Environment Variables, Never Hardcode Secrets**

.. code-block:: yaml

   # ✗ Never do this
   database:
     password: "my_secret_password"

   # ✓ Always do this
   database:
     password: ${DB_PASSWORD}

Then set the variable:

.. code-block:: bash

   export DB_PASSWORD="my_secret_password"
   tauro --env prod --pipeline my_pipeline

Or use a ``.env`` file:

.. code-block:: bash

   # .env (add to .gitignore)
   DB_PASSWORD=my_secret_password
   API_KEY=abc123

**Use Relative Paths or Cloud Storage**

.. code-block:: yaml

   # ✗ Don't use absolute paths
   input_path: /home/john/data/input

   # ✓ Use relative paths
   input_path: data/input

   # ✓ Or use cloud storage
   input_path: s3://my-bucket/data/input

**Name Nodes Clearly**

Use short, descriptive names:

.. code-block:: yaml

   # ✗ Unclear
   nodes:
     n1:
       function: "src.nodes.a"
     n2:
       function: "src.nodes.b"

   # ✓ Clear
   nodes:
     extract_customers:
       function: "src.nodes.extract.get_customers"
     clean_emails:
       function: "src.nodes.clean.normalize_emails"

**Organize Pipelines by Layer**

Follow medallion architecture:

.. code-block:: yaml

   pipelines:
     # Bronze: Raw data ingestion
     bronze_load_raw_sales:
       nodes: [ingest_sales]

     # Silver: Clean and validate
     silver_clean_sales:
       nodes: [deduplicate, validate]

     # Gold: Business metrics
     gold_sales_metrics:
       nodes: [aggregate, calculate_kpis]

Deployment Checklist
---------------------

Before running in production, verify:

✅ **Configuration**

.. code-block:: bash

   # Validate config before running
   tauro --env prod --pipeline my_pipeline --validate

✅ **Test the Pipeline Locally First**

.. code-block:: bash

   # Run on dev with small data
   tauro --env dev --pipeline my_pipeline

✅ **Check for Secrets**

.. code-block:: bash

   # Make sure no passwords in code
   grep -r "password" config/
   grep -r "api_key" src/

   # Should return nothing!

✅ **Add Logging**

.. code-block:: bash

   # Run with debug logging
   tauro --env prod --pipeline my_pipeline --log-level DEBUG

✅ **Set Up Monitoring**

Plan for what to do if something fails:

.. code-block:: bash

   # Save logs for debugging
   tauro --env prod --pipeline my_pipeline 2>&1 | tee logs/pipeline.log

✅ **Document Your Pipeline**

Add a README explaining:
   - What the pipeline does
   - When it runs
   - What data it needs
   - What it produces

.. code-block:: markdown

   # Sales ETL Pipeline

   ## Purpose
   Daily sales data transformation

   ## Schedule
   Runs at 9 AM every weekday

   ## Inputs
   - Raw sales database
   - Customer master data

   ## Outputs
   - Processed sales data in S3

   ## Troubleshooting
   If the pipeline fails:
   1. Check database connection
   2. Verify S3 permissions
   3. See logs/ directory

Error Handling Strategy
-----------------------

**Plan for Failures**

Data pipelines fail. Plan for it:

.. code-block:: yaml

   nodes:
     critical_transform:
       function: "src.nodes.transform"
       timeout: 600
       retry: 3              # Retry 3 times

     non_critical:
       function: "src.nodes.aggregate"
       timeout: 300
       continue_on_error: true  # Continue if it fails

**Log Everything**

.. code-block:: python

   import logging

   logger = logging.getLogger(__name__)

   def extract_data():
       """Extract with logging."""
       logger.info("Starting extraction...")

       df = read_data()
       logger.info(f"Extracted {len(df)} records")

       return df

**Monitor Results**

Check that output looks right:

.. code-block:: python

   def load_data(df):
       """Load with validation."""
       if df.empty:
           raise ValueError("No data to load")

       # Log some statistics
       logger.info(f"Saving {len(df)} records")
       logger.info(f"Columns: {', '.join(df.columns)}")

       df.to_parquet('output.parquet')

Testing Strategy
-----------------

**Write Unit Tests**

Test each node function:

.. code-block:: python

   # test_extract.py
   import pandas as pd
   from src.nodes.extract import extract_data

   def test_extract_data():
       """Test extraction."""
       df = extract_data()

       assert not df.empty
       assert 'id' in df.columns
       assert len(df) > 0

   def test_handles_missing_file():
       """Test error handling."""
       with pytest.raises(FileNotFoundError):
           extract_data(path="nonexistent.csv")

**Run Tests Before Deploying**

.. code-block:: bash

   # Run all tests
   pytest tests/

   # Run with coverage
   pytest --cov=src tests/

**Test with Real Data (Locally)**

.. code-block:: bash

   # Test with actual data structure
   tauro --env dev --pipeline my_pipeline

Common Mistakes to Avoid
------------------------

❌ **Mistake: Assuming data is clean**

.. code-block:: python

   # ✗ Don't assume
   def bad_transform(df):
       return df['amount'].apply(float)  # Crashes if invalid values

   # ✓ Validate first
   def good_transform(df):
       df = df[df['amount'].notna()]
       return df['amount'].astype(float)

❌ **Mistake: Hardcoding values**

.. code-block:: python

   # ✗ Don't hardcode
   def bad_extract():
       return pd.read_csv("/home/john/data.csv")

   # ✓ Use configuration
   def good_extract(input_data):
       return pd.read_csv(input_data['path'])

❌ **Mistake: Ignoring errors**

.. code-block:: python

   # ✗ Don't ignore
   try:
       process_data()
   except:
       pass

   # ✓ Handle properly
   try:
       process_data()
   except ValueError as e:
       logger.error(f"Processing failed: {e}")
       raise

❌ **Mistake: Large data in memory**

.. code-block:: python

   # ✗ Don't load everything
   df = pd.read_csv("huge_file.csv")  # Crashes on large files

   # ✓ Process in chunks
   for chunk in pd.read_csv("huge_file.csv", chunksize=10000):
       process_chunk(chunk)

Performance Tips
-----------------

**Use Parquet for Large Files**

.. code-block:: yaml

   # ✗ CSV is slow
   format: csv

   # ✓ Parquet is fast
   format: parquet

**Partition Your Data**

.. code-block:: yaml

   outputs:
     results:
       path: data/output/results
       format: parquet
       partitioned_by: date  # Stores by date folder

**Run Nodes in Parallel**

.. code-block:: yaml

   # Tauro automatically runs independent nodes in parallel
   # Configure how many:
   max_workers: 8  # Up from default 4

**Use Date Ranges Wisely**

.. code-block:: bash

   # Process only what changed
   tauro --env prod --pipeline daily_etl \
     --start-date 2024-01-15 \
     --end-date 2024-01-15

Operationalizing Pipelines
----------------------------

**Schedule with Cron (Linux/Mac)**

.. code-block:: bash

   # Run at 9 AM every weekday
   0 9 * * 1-5 cd /home/user/project && tauro --env prod --pipeline daily_etl

**Schedule with Windows Task Scheduler**

Create a batch file:

.. code-block:: batch

   REM run_pipeline.bat
   cd C:\Users\user\project
   tauro --env prod --pipeline daily_etl

Then schedule it in Task Scheduler.

**Monitor Execution**

Save logs and monitor them:

.. code-block:: bash

   # Run with logging
   tauro --env prod --pipeline my_pipeline \
     >> logs/execution.log 2>&1

   # Check for errors
   grep ERROR logs/execution.log

**Set Alerts**

Get notified if pipeline fails:

.. code-block:: bash

   # Example with email
   tauro --env prod --pipeline my_pipeline || \
     mail -s "Pipeline failed" admin@company.com

Security Best Practices
------------------------

**Never Commit Secrets**

.. code-block:: bash

   # .gitignore
   .env
   logs/
   data/
   *.log

**Use Principle of Least Privilege**

Give users/services only the permissions they need:

.. code-block:: bash

   # Don't give admin access
   # Only give read/write to specific paths and tables

**Validate All Inputs**

.. code-block:: python

   # Validate file paths
   import os
   path = user_input
   if not os.path.exists(path):
       raise ValueError(f"File not found: {path}")

**Rotate Credentials Regularly**

Change passwords and API keys monthly.

Conclusion
----------

Remember:

✅ Keep things simple  
✅ Test everything  
✅ Handle errors gracefully  
✅ Never hardcode secrets  
✅ Document your work  
✅ Monitor in production  
✅ Learn from failures  

Next Steps
----------

- :doc:`guides/batch_etl` - Build a complete example
- :doc:`guides/troubleshooting` - Solve common problems
- :doc:`guides/deployment` - Deploy to production

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
