Migration Guide: CLI to Library
================================

This guide helps you migrate from using Tauro's CLI to using it as a Python library for more advanced integrations.

When to Migrate
---------------

Use the library when you need:

- Integration with Airflow, Prefect, or other orchestrators
- Custom REST API endpoints
- Jupyter notebook automation
- Automated pipeline testing
- Complete programmatic control
- Embedding in existing applications

CLI vs Library
--------------

.. code-block:: bash

   # CLI - Simple command-line execution
   tauro --env prod --pipeline sales_etl

.. code-block:: python

   # Library - Programmatic control
   import tauro
   config = tauro.load_project("./my_project", environment="prod")
   result = config.run_pipeline("sales_etl")

Basic Migration
---------------

**Before (CLI)**

.. code-block:: bash

   tauro --env production --pipeline daily_sales

**After (Library)**

.. code-block:: python

   import tauro
   
   # Load project configuration
   config = tauro.load_project(".", environment="production")
   
   # Execute pipeline
   result = config.run_pipeline("daily_sales")
   
   if result.success:
       print(f"✓ Pipeline completed in {result.execution_time}s")
   else:
       print(f"✗ Failed: {result.error}")

Common Migrations
-----------------

**1. Running a specific node**

.. code-block:: bash

   # CLI
   tauro --env dev --pipeline my_pipeline --node extract

.. code-block:: python

   # Library
   config = tauro.load_project(".", environment="dev")
   result = config.run_step(pipeline="my_pipeline", step="extract")

**2. Validating configuration**

.. code-block:: bash

   # CLI
   tauro --validate --pipeline my_pipeline

.. code-block:: python

   # Library
   config = tauro.load_project(".")
   is_valid = config.validate_pipeline("my_pipeline")

**3. Running with date range**

.. code-block:: bash

   # CLI
   tauro --env prod --pipeline etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

.. code-block:: python

   # Library
   config = tauro.load_project(".", environment="prod")
   result = config.run_pipeline(
       "etl",
       start_date="2024-01-01",
       end_date="2024-01-31"
   )

Integration Examples
--------------------

**Airflow Integration**

.. code-block:: python

   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from datetime import datetime
   import tauro
   
   def run_tauro_pipeline(**kwargs):
       config = tauro.load_project(".", environment="production")
       result = config.run_pipeline(
           "daily_etl",
           start_date=kwargs['ds']
       )
       if not result.success:
           raise Exception(f"Pipeline failed: {result.error}")
       return result.metrics
   
   with DAG('tauro_pipeline', start_date=datetime(2024, 1, 1)) as dag:
       PythonOperator(
           task_id='run_tauro',
           python_callable=run_tauro_pipeline
       )

**FastAPI REST Endpoint**

.. code-block:: python

   from fastapi import FastAPI
   from pydantic import BaseModel
   import tauro
   
   app = FastAPI()
   config = tauro.load_project(".")
   
   class PipelineRequest(BaseModel):
       name: str
       start_date: str = None
   
   @app.post("/execute")
   async def execute_pipeline(request: PipelineRequest):
       result = config.run_pipeline(
           request.name,
           start_date=request.start_date
       )
       return {"success": result.success, "time": result.execution_time}

**Jupyter Notebooks**

.. code-block:: python

   import tauro
   
   # Load project
   config = tauro.load_project(".")
   
   # Explore data
   raw_data = config.load_input("sales_raw")
   print(f"Records: {len(raw_data)}")
   
   # Run transformation
   result = config.run_step("sales_pipeline", "transform")
   
   # Analyze results
   processed = result.get_data()
   processed.describe()

Error Handling
--------------

.. code-block:: python

   import tauro
   
   try:
       config = tauro.load_project(".")
       result = config.run_pipeline("my_pipeline")
       
       if not result.success:
           print(f"Pipeline failed: {result.error}")
           # Handle failure
       else:
           print(f"Success: {result.nodes_executed} steps")
           
   except Exception as e:
       print(f"Unexpected error: {e}")
       # Handle error

Next Steps
----------

- :doc:`library_usage` - Complete library reference
- :doc:`best_practices` - Recommended practices
- :doc:`tutorials/airflow_integration` - Airflow example
- :doc:`tutorials/fastapi_integration` - FastAPI example
