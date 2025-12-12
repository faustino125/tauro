Airflow Integration
===================

This tutorial demonstrates how to orchestrate your Tauro pipelines using Apache Airflow. By combining Tauro and Airflow, you can build robust, scheduled, and scalable data workflows.

**Benefits of using Tauro with Airflow:**

- **Decoupling**: Your core business logic resides in your Tauro project, completely independent of Airflow. The Airflow DAGs are only responsible for triggering the pipelines.
- **Simplicity**: The Airflow DAGs remain simple and clean. All the complexity of data sources, transformations, and destinations is managed by Tauro's configuration.
- **Testability**: You can easily test your Tauro pipelines locally without needing an Airflow environment.

A Basic ETL DAG
---------------

Here is an example of an Airflow DAG that runs a sequence of Tauro pipelines for a typical Bronze-Silver-Gold ETL process.

Each task in the DAG corresponds to a Tauro pipeline. When a task runs, it invokes the Tauro library, executes the specified pipeline, and reports success or failure back to Airflow.

.. code-block:: python

   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from datetime import datetime, timedelta
   from tauro import PipelineExecutor, ContextLoader

   # Define the path to your Tauro project directory
   # In a real Airflow deployment, this would be a fixed path on your Airflow worker nodes
   # where your Tauro project is deployed.
   PROJECT_PATH = "/path/to/your/tauro_project"

   default_args = {
       'owner': 'data-team',
       'depends_on_past': False,
       'start_date': datetime(2024, 1, 1),
       'email_on_failure': False,
       'email_on_retry': False,
       'retries': 1,
       'retry_delay': timedelta(minutes=5),
   }

   def run_tauro_pipeline(pipeline_name: str, ds: str):
       """
       A generic Python callable that executes a Tauro pipeline.
       """
       print(f"Executing Tauro pipeline: {pipeline_name} for date {ds}")
       
       # 1. Load the production context from your Tauro project
       context = ContextLoader(PROJECT_PATH).load_from_env("production")
       
       # 2. Initialize the executor
       executor = PipelineExecutor(context)
       
       # 3. Execute the pipeline for a specific date
       result = executor.execute(
           pipeline_name=pipeline_name,
           start_date=ds,
           end_date=ds
       )
       
       # 4. If the pipeline fails, raise an exception to fail the Airflow task
       if not result.success:
           raise Exception(f"Tauro pipeline '{pipeline_name}' failed: {result.error_message}")
       
       print(f"✅ Tauro pipeline '{pipeline_name}' completed successfully.")
       return {
           'nodes_executed': result.nodes_executed,
           'execution_time': result.execution_time_seconds,
           'metrics': result.metrics,
       }

   with DAG(
       dag_id='tauro_daily_etl',
       default_args=default_args,
       description='Daily ETL DAG to run Tauro pipelines for Bronze, Silver, and Gold layers.',
       schedule_interval='0 2 * * *',  # Runs daily at 2 AM
       catchup=False,
       tags=['tauro', 'etl'],
   ) as dag:

       run_load_pipeline = PythonOperator(
           task_id='run_load_pipeline',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'load', 'ds': '{{ ds }}'},
       )

       run_transform_pipeline = PythonOperator(
           task_id='run_transform_pipeline',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'transform', 'ds': '{{ ds }}'},
       )

       run_aggregate_pipeline = PythonOperator(
           task_id='run_aggregate_pipeline',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'aggregate', 'ds': '{{ ds }}'},
       )

       # Define the task dependencies
       run_load_pipeline >> run_transform_pipeline >> run_aggregate_pipeline

How it Works
------------

1.  **`PROJECT_PATH`**: You must define the absolute path to your deployed Tauro project. Your Airflow workers need access to this directory.

2.  **`run_tauro_pipeline` function**: This is the core of the integration. It's a `PythonOperator` callable that:
    - Loads the Tauro context for your `production` environment.
    - Executes a specific pipeline, passing in the execution date (`ds`) from Airflow.
    - Raises an exception if the pipeline fails, which correctly marks the Airflow task as failed.

3.  **DAG Definition**: The DAG itself is standard Airflow code. We define three `PythonOperator` tasks, one for each of our `load`, `transform`, and `aggregate` pipelines.

4.  **Task Dependencies**: We use `>>` to set the execution order, ensuring that the `load` pipeline runs before `transform`, and `transform` runs before `aggregate`.

Next Steps
----------

- **Deploy**: Place your Tauro project in a directory accessible to your Airflow workers and update `PROJECT_PATH`.
- **Customize**: Modify the `pipeline_name` in the `op_kwargs` to match your project's pipelines.
- **XComs**: The `run_tauro_pipeline` function returns a dictionary of results. You can configure Airflow to pull these into XComs to pass metadata between tasks if needed.