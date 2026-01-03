Batch ETL Tutorial
==================

This tutorial teaches you to build a complete batch ETL pipeline using the Medallion architecture.

What You'll Build
-----------------

A production-ready ETL pipeline that:

- Ingests raw sales data (Bronze layer)
- Cleans and validates data (Silver layer)
- Calculates business metrics (Gold layer)
- Runs daily on a schedule

Prerequisites
-------------

- Tauro installed: ``pip install tauro[spark]``
- Sample data (provided)
- 30 minutes

Step 1: Project Setup
----------------------

Create a new project:

.. code-block:: bash

  tauro template --template medallion_basic --project-name sales_etl
   cd sales_etl

This creates:

.. code-block:: text

   sales_etl/
   ├── config/
   │   ├── base/
   │   └── dev/
   ├── data/
   ├── notebooks/
   └── settings.json

Step 2: Configure Data Sources
-------------------------------

Edit ``config/base/input.yaml``:

.. code-block:: yaml

   # Raw sales data (CSV)
   raw_sales:
     format: "csv"
     path: "data/raw/sales/*.csv"
     options:
       header: true
       inferSchema: true
       dateFormat: "yyyy-MM-dd"
     schema:
       - name: "transaction_id"
         type: "string"
       - name: "date"
         type: "date"
       - name: "customer_id"
         type: "string"
       - name: "product_id"
         type: "string"
       - name: "quantity"
         type: "integer"
       - name: "amount"
         type: "decimal(10,2)"

   # Customer master data (Parquet)
   customer_master:
     format: "parquet"
     path: "data/raw/customers/*.parquet"

Step 3: Configure Output
-------------------------

Edit ``config/base/output.yaml``:

.. code-block:: yaml

   # Bronze layer - raw data as-is
   bronze_sales:
     format: "parquet"
     path: "data/bronze/sales"
     write_mode: "append"
     partition_by: ["year", "month", "day"]
     options:
       compression: "snappy"

   # Silver layer - cleaned data
   silver_sales:
     format: "delta"
     path: "data/silver/sales"
     write_mode: "merge"
     merge_condition: "target.transaction_id = source.transaction_id"
     options:
       mergeSchema: true

   # Gold layer - business metrics
   gold_daily_sales:
     format: "delta"
     path: "data/gold/daily_sales"
     write_mode: "overwrite"
     partition_by: ["year", "month"]

Step 4: Define Pipeline Nodes
------------------------------

Edit ``config/base/nodes.yaml``:

.. code-block:: yaml

   # Bronze: Load raw data
   load_raw_sales:
     type: "input"
     module: "core.io.readers"
     class: "CSVReader"
     params:
       input_key: "raw_sales"
     outputs:
       - "bronze_sales_df"

   # Silver: Clean and validate
   clean_sales:
     type: "transform"
     module: "core.exec.node_executor"
     class: "PythonNode"
     params:
       function: "transformations.clean_sales_data"
       inputs:
         - "bronze_sales_df"
     outputs:
       - "silver_sales_df"

   # Silver: Enrich with customer data
   enrich_sales:
     type: "transform"
     module: "core.exec.node_executor"
     class: "SparkSQLNode"
     params:
       sql: |
         SELECT 
           s.*,
           c.customer_name,
           c.customer_segment,
           c.region
         FROM silver_sales_df s
         LEFT JOIN customer_master c
           ON s.customer_id = c.customer_id
     outputs:
       - "enriched_sales_df"

   # Gold: Calculate daily metrics
   calculate_daily_metrics:
     type: "aggregation"
     module: "core.exec.node_executor"
     class: "AggregationNode"
     params:
       input: "enriched_sales_df"
       group_by:
         - "date"
         - "region"
         - "customer_segment"
       aggregations:
         total_sales: "SUM(amount)"
         transaction_count: "COUNT(*)"
         avg_transaction: "AVG(amount)"
         unique_customers: "COUNT(DISTINCT customer_id)"
     outputs:
       - "gold_daily_sales_df"

Step 5: Create Pipeline Definitions
------------------------------------

Edit ``config/base/pipelines.yaml``:

.. code-block:: yaml

   # Bronze layer pipeline
   bronze_ingestion:
     description: "Ingest raw sales data"
     nodes:
       - load_raw_sales
     schedule: "0 1 * * *"  # 1 AM daily

   # Silver layer pipeline
   silver_cleansing:
     description: "Clean and enrich sales data"
     nodes:
       - clean_sales
       - enrich_sales
     dependencies:
       - bronze_ingestion
     schedule: "0 2 * * *"  # 2 AM daily

   # Gold layer pipeline
   gold_aggregation:
     description: "Calculate business metrics"
     nodes:
       - calculate_daily_metrics
     dependencies:
       - silver_cleansing
     schedule: "0 3 * * *"  # 3 AM daily

Step 6: Implement Transformation Logic
---------------------------------------

Create ``transformations.py``:

.. code-block:: python

   import pyspark.sql.functions as F
   from pyspark.sql import DataFrame

   def clean_sales_data(df: DataFrame) -> DataFrame:
       """Clean and validate sales data."""
       
       # Remove nulls
       df_clean = df.filter(
           F.col("transaction_id").isNotNull() &
           F.col("amount").isNotNull() &
           F.col("quantity").isNotNull()
       )
       
       # Remove invalid amounts
       df_clean = df_clean.filter(F.col("amount") > 0)
       df_clean = df_clean.filter(F.col("quantity") > 0)
       
       # Remove duplicates
       df_clean = df_clean.dropDuplicates(["transaction_id"])
       
       # Add audit columns
       df_clean = df_clean.withColumn(
           "ingestion_timestamp",
           F.current_timestamp()
       )
       
       return df_clean

Step 7: Run the Pipeline (CLI)
-------------------------------

.. code-block:: bash

   # Validate configuration
   tauro --env dev --pipeline bronze_ingestion --validate-only

   # Run bronze layer
   tauro --env dev --pipeline bronze_ingestion

   # Run silver layer
   tauro --env dev --pipeline silver_cleansing

   # Run gold layer
   tauro --env dev --pipeline gold_aggregation

   # Run with specific date
   tauro --env dev --pipeline bronze_ingestion \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

Step 8: Run Programmatically
-----------------------------

Create ``run_pipeline.py``:

.. code-block:: python

   from tauro import PipelineExecutor, ContextLoader
   import logging

   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)

   def run_complete_etl():
       """Run complete ETL pipeline."""
       
       # Load context
       context = ContextLoader().load_from_env("dev")
       executor = PipelineExecutor(context)
       
       # Bronze layer
       logger.info("Starting Bronze layer...")
       bronze_result = executor.execute("bronze_ingestion")
       
       if not bronze_result.success:
           logger.error(f"Bronze failed: {bronze_result.error_message}")
           return
       
       logger.info(f"Bronze completed in {bronze_result.execution_time_seconds}s")
       
       # Silver layer
       logger.info("Starting Silver layer...")
       silver_result = executor.execute("silver_cleansing")
       
       if not silver_result.success:
           logger.error(f"Silver failed: {silver_result.error_message}")
           return
       
       logger.info(f"Silver completed in {silver_result.execution_time_seconds}s")
       
       # Gold layer
       logger.info("Starting Gold layer...")
       gold_result = executor.execute("gold_aggregation")
       
       if not gold_result.success:
           logger.error(f"Gold failed: {gold_result.error_message}")
           return
       
       logger.info(f"Gold completed in {gold_result.execution_time_seconds}s")
       
       # Print summary
       total_time = (
           bronze_result.execution_time_seconds +
           silver_result.execution_time_seconds +
           gold_result.execution_time_seconds
       )
       
       print(f"\n✅ ETL completed successfully!")
       print(f"   Total time: {total_time}s")
       print(f"   Nodes executed: {bronze_result.nodes_executed + silver_result.nodes_executed + gold_result.nodes_executed}")

   if __name__ == "__main__":
       run_complete_etl()

Run it:

.. code-block:: bash

   python run_pipeline.py

Step 9: Monitor Results
------------------------

Check output data:

.. code-block:: bash

   # List bronze data
   ls data/bronze/sales/

   # List silver data
   ls data/silver/sales/

   # List gold data
   ls data/gold/daily_sales/

Query results with Spark:

.. code-block:: python

   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("Check Results").getOrCreate()

   # Read gold data
   gold_df = spark.read.format("delta").load("data/gold/daily_sales")
   
   # Show sample
   gold_df.show()
   
   # Check metrics
   gold_df.groupBy("region").agg({
       "total_sales": "sum",
       "transaction_count": "sum"
   }).show()

Step 10: Schedule with Airflow
-------------------------------

Create ``airflow_dag.py``:

.. code-block:: python

   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from datetime import datetime, timedelta
   from tauro import PipelineExecutor, ContextLoader

   default_args = {
       'owner': 'data-team',
       'depends_on_past': False,
       'start_date': datetime(2024, 1, 1),
       'email_on_failure': True,
       'email_on_retry': False,
       'retries': 3,
       'retry_delay': timedelta(minutes=5),
   }

   def run_tauro_pipeline(pipeline_name, **kwargs):
       context = ContextLoader().load_from_env("production")
       executor = PipelineExecutor(context)
       result = executor.execute(pipeline_name, start_date=kwargs['ds'])
       
       if not result.success:
           raise Exception(f"Pipeline failed: {result.error_message}")

   with DAG(
       'sales_etl',
       default_args=default_args,
       schedule_interval='0 2 * * *',  # Daily at 2 AM
       catchup=False
   ) as dag:

       bronze = PythonOperator(
           task_id='bronze_ingestion',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'bronze_ingestion'}
       )

       silver = PythonOperator(
           task_id='silver_cleansing',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'silver_cleansing'}
       )

       gold = PythonOperator(
           task_id='gold_aggregation',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'gold_aggregation'}
       )

       bronze >> silver >> gold

Next Steps
----------

- Add data quality checks: :doc:`../advanced/testing`
- Optimize performance: :doc:`../advanced/performance`
- Add monitoring: :doc:`../best_practices`
- Deploy to production: :doc:`../advanced/deployment`

Complete Code
-------------

All code from this tutorial is available at:
https://github.com/faustino125/tauro/tree/main/examples/batch_etl
