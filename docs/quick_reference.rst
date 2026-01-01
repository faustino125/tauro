Quick Reference
================

A quick cheat sheet of common Tauro commands and patterns.

Command-Line Basics
--------------------

**List all pipelines**

.. code-block:: bash

   tauro --list-pipelines

**Run a pipeline**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline

**Run with date range**

.. code-block:: bash

   tauro --env prod --pipeline daily_etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

**Run a specific step only**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --node extract

**Validate without running**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --validate

**Show help**

.. code-block:: bash

   tauro --help

Common Configuration Patterns
------------------------------

**Simple 3-step pipeline**

.. code-block:: yaml

   # config/pipelines.yaml
   pipelines:
     my_etl:
       nodes: [extract, transform, load]

   # config/nodes.yaml
   nodes:
     extract:
       function: "src.nodes.extract"
     transform:
       function: "src.nodes.transform"
     load:
       function: "src.nodes.load"

**Read from multiple sources**

.. code-block:: yaml

   # config/inputs.yaml
   inputs:
     customers:
       path: data/input/customers.csv
       format: csv
     products:
       path: data/input/products.parquet
       format: parquet

**Save to different formats**

.. code-block:: yaml

   # config/outputs.yaml
   outputs:
     clean_data:
       path: data/output/clean_data.parquet
       format: parquet
     summary:
       path: data/output/summary.csv
       format: csv

Common Node Patterns
---------------------

**Read data**

.. code-block:: python

   def extract():
       """Load data from file."""
       import pandas as pd
       return pd.read_csv("data/input/sales.csv")

**Filter and clean**

.. code-block:: python

   def transform(df):
       """Remove bad data."""
       df = df.dropna()  # Remove rows with missing values
       df = df[df['amount'] > 0]  # Keep only positive amounts
       return df

**Group and aggregate**

.. code-block:: python

   def aggregate(df):
       """Summarize by customer."""
       return df.groupby('customer_id').agg({
           'amount': 'sum',
           'date': 'max'
       }).reset_index()

**Save results**

.. code-block:: python

   def load(df):
       """Write output."""
       df.to_csv("data/output/results.csv", index=False)

**Multi-step processing**

.. code-block:: python

   def transform(df):
       """Multiple transformations."""
       # Step 1: Clean
       df = df.dropna(subset=['customer_id'])
       
       # Step 2: Enrich
       df['full_date'] = pd.to_datetime(df['date'])
       
       # Step 3: Validate
       if df.empty:
           raise ValueError("No valid data after cleaning")
       
       return df

Environment-Specific Setup
---------------------------

**Structure**

.. code-block:: text

   config/
   ├── global.yaml         # Shared settings
   ├── dev/
   │   └── global.yaml     # Override for dev
   ├── staging/
   │   └── global.yaml     # Override for staging
   └── prod/
       └── global.yaml     # Override for prod

**Example configurations**

.. code-block:: yaml

   # config/global.yaml (default)
   input_path: data/input
   output_path: data/output
   log_level: INFO

.. code-block:: yaml

   # config/dev/global.yaml (dev overrides)
   input_path: data/input/test
   output_path: data/output/test
   log_level: DEBUG

.. code-block:: yaml

   # config/prod/global.yaml (prod overrides)
   input_path: s3://my-bucket/input
   output_path: s3://my-bucket/output
   log_level: WARNING

**Use**

.. code-block:: bash

   # Dev with test data
   tauro --env dev --pipeline my_pipeline
   
   # Prod with real data
   tauro --env prod --pipeline my_pipeline

Common Issues & Solutions
--------------------------

**Pipeline not found**

.. code-block:: bash

   # List available pipelines
   tauro --list-pipelines
   
   # Check spelling in config/pipelines.yaml

**Module not found error**

.. code-block:: bash

   # Ensure src/nodes/ has __init__.py
   touch src/nodes/__init__.py
   
   # Check function name matches config

**Data not loading**

.. code-block:: bash

   # Validate paths
   ls data/input/
   
   # Check file format matches config (csv, parquet, etc.)

**Running out of memory**

.. code-block:: yaml

   # Split into smaller steps
   # Reduce partition size for Spark
   spark:
     shuffle_partitions: 100  # Reduce from default

Testing Locally
---------------

**Test a single step**

.. code-block:: python

   # test_nodes.py
   from src.nodes import extract, transform
   
   def test_extract():
       df = extract()
       assert len(df) > 0
       assert 'customer_id' in df.columns
   
   def test_transform():
       df = extract()
       cleaned = transform(df)
       assert cleaned.isna().sum().sum() == 0  # No nulls

**Test the full pipeline**

.. code-block:: bash

   # Use dev environment for testing
   tauro --env dev --pipeline my_pipeline --validate
   
   # Then run it
   tauro --env dev --pipeline my_pipeline

**Add test data**

.. code-block:: text

   data/input/
   ├── small_test.csv    # For testing
   └── large_data.csv    # For production

Logging & Debugging
-------------------

**Increase logging detail**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --log-level DEBUG

**Capture logs to file**

.. code-block:: bash

   tauro --env prod --pipeline my_pipeline 2>&1 | tee logs/pipeline.log

**Add debugging to your code**

.. code-block:: python

   import logging
   logger = logging.getLogger(__name__)
   
   def transform(df):
       logger.info(f"Input rows: {len(df)}")
       df = df.dropna()
       logger.info(f"Output rows: {len(df)}")
       return df

Useful Files to Remember
------------------------

.. code-block:: text

   Key files:
   
   config/global.yaml          # General settings
   config/pipelines.yaml       # Define pipelines
   config/nodes.yaml           # Define steps
   config/inputs.yaml          # Data sources
   config/outputs.yaml         # Output targets
   
   src/nodes/__init__.py       # Your pipeline code
   
   .env                        # Secrets (keep private!)
   .gitignore                  # Files not to commit
   requirements.txt            # Python dependencies

Documentation Links
-------------------

- **Getting started**: :doc:`getting_started`
- **Configuration guide**: :doc:`configuration`
- **CLI reference**: :doc:`cli_usage`
- **Library usage**: :doc:`library_usage`
- **Examples**: :doc:`tutorials/batch_etl`
- **Troubleshooting**: :doc:`advanced/troubleshooting`
