Library Usage
=============

This guide covers using Tauro as a Python library in your applications.

Overview
--------

Tauro can be used programmatically to:

- Execute pipelines from Python code
- Integrate with orchestration tools (Airflow, Prefect)
- Build custom data applications
- Create REST APIs for pipeline execution
- Automate testing and validation

Basic Concepts
--------------

Context
~~~~~~~

The ``Context`` object contains all configuration and runtime information:

.. code-block:: python

   from tauro import ContextLoader

   # Load from environment
   context = ContextLoader().load_from_env("production")

   # Load from file
   context = ContextLoader().load_from_config("./config/settings.yaml")

   # Load from dictionary
   config_dict = {"environment": "dev", ...}
   context = ContextLoader().load_from_dict(config_dict)

Executor
~~~~~~~~

The ``PipelineExecutor`` runs pipelines:

.. code-block:: python

   from tauro import PipelineExecutor

   executor = PipelineExecutor(context)
   result = executor.execute("pipeline_name")

Results
~~~~~~~

Execution results contain detailed information:

.. code-block:: python

   if result.success:
       print(f"Nodes executed: {result.nodes_executed}")
       print(f"Time: {result.execution_time_seconds}s")
       print(f"Metrics: {result.metrics}")
   else:
       print(f"Error: {result.error_message}")
       print(f"Failed node: {result.failed_node}")

Core Examples
-------------

Example 1: Simple Pipeline Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import PipelineExecutor, ContextLoader

   # Load configuration
   context = ContextLoader().load_from_env("production")

   # Create executor
   executor = PipelineExecutor(context)

   # Execute pipeline
   result = executor.execute(
       pipeline_name="daily_sales",
       start_date="2024-01-01",
       end_date="2024-01-31"
   )

   # Check results
   if result.success:
       print(f"✅ Pipeline successful")
       print(f"   Nodes: {result.nodes_executed}")
       print(f"   Time: {result.execution_time_seconds}s")
       print(f"   Records: {result.metrics.get('records_processed', 0)}")
   else:
       print(f"❌ Failed: {result.error_message}")

Example 2: Data Input/Output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import InputLoader, DataOutputManager, ContextLoader

   context = ContextLoader().load_from_env("dev")

   # Load input data
   loader = InputLoader(context)
   sales_data = loader.load("raw_sales")
   customer_data = loader.load("customer_master")

   # Process data
   enriched = sales_data.join(customer_data, on="customer_id")
   filtered = enriched.filter(enriched.amount > 1000)

   # Write output
   output = DataOutputManager(context)
   output.write(
       dataframe=filtered,
       output_key="high_value_sales",
       write_mode="overwrite",
       partition_cols=["date", "region"]
   )

Example 3: Pipeline Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import PipelineExecutor, ContextLoader

   context = ContextLoader().load_from_env("production")
   executor = PipelineExecutor(context)

   # Validate pipeline configuration
   is_valid = executor.validate_pipeline("critical_etl")

   if not is_valid:
       print("❌ Pipeline configuration invalid")
       exit(1)

   # List all available pipelines
   pipelines = executor.list_pipelines()
   print(f"Available: {', '.join(pipelines)}")

Integration Examples
--------------------

Airflow Integration
~~~~~~~~~~~~~~~~~~~

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

   def run_tauro_pipeline(**kwargs):
       """Execute Tauro pipeline."""
       context = ContextLoader().load_from_env("production")
       executor = PipelineExecutor(context)
       
       result = executor.execute(
           pipeline_name=kwargs['pipeline_name'],
           start_date=kwargs['ds'],
           end_date=kwargs['ds']
       )
       
       if not result.success:
           raise Exception(f"Pipeline failed: {result.error_message}")
       
       # Push metrics to XCom
       return {
           'nodes_executed': result.nodes_executed,
           'execution_time': result.execution_time_seconds,
           'records_processed': result.metrics.get('records_processed', 0)
       }

   with DAG(
       'tauro_daily_etl',
       default_args=default_args,
       schedule_interval='0 2 * * *',  # Daily at 2 AM
       catchup=False
   ) as dag:

       run_bronze = PythonOperator(
           task_id='run_bronze_layer',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'bronze_ingestion'}
       )

       run_silver = PythonOperator(
           task_id='run_silver_layer',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'silver_cleansing'}
       )

       run_gold = PythonOperator(
           task_id='run_gold_layer',
           python_callable=run_tauro_pipeline,
           op_kwargs={'pipeline_name': 'gold_aggregation'}
       )

       run_bronze >> run_silver >> run_gold

FastAPI Integration
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from fastapi import FastAPI, HTTPException, BackgroundTasks
   from pydantic import BaseModel
   from tauro import PipelineExecutor, ContextLoader
   from typing import Optional
   import uuid

   app = FastAPI(title="Tauro Pipeline API")

   # Pydantic models
   class PipelineRequest(BaseModel):
       pipeline_name: str
       environment: str = "production"
       start_date: Optional[str] = None
       end_date: Optional[str] = None

   class PipelineResponse(BaseModel):
       execution_id: str
       status: str
       message: str

   class ExecutionResult(BaseModel):
       success: bool
       nodes_executed: int
       execution_time_seconds: float
       metrics: dict

   # In-memory storage (use Redis/DB in production)
   executions = {}

   def execute_pipeline(execution_id: str, request: PipelineRequest):
       """Background task to execute pipeline."""
       try:
           context = ContextLoader().load_from_env(request.environment)
           executor = PipelineExecutor(context)
           
           result = executor.execute(
               pipeline_name=request.pipeline_name,
               start_date=request.start_date,
               end_date=request.end_date
           )
           
           executions[execution_id] = {
               'status': 'completed' if result.success else 'failed',
               'result': result
           }
       except Exception as e:
           executions[execution_id] = {
               'status': 'failed',
               'error': str(e)
           }

   @app.post("/pipelines/execute", response_model=PipelineResponse)
   async def execute(
       request: PipelineRequest,
       background_tasks: BackgroundTasks
   ):
       """Execute a pipeline asynchronously."""
       execution_id = str(uuid.uuid4())
       executions[execution_id] = {'status': 'running'}
       
       background_tasks.add_task(
           execute_pipeline,
           execution_id,
           request
       )
       
       return PipelineResponse(
           execution_id=execution_id,
           status="started",
           message=f"Pipeline {request.pipeline_name} execution started"
       )

   @app.get("/pipelines/status/{execution_id}")
   async def get_status(execution_id: str):
       """Get execution status."""
       if execution_id not in executions:
           raise HTTPException(status_code=404, detail="Execution not found")
       
       execution = executions[execution_id]
       
       if execution['status'] == 'running':
           return {"status": "running"}
       elif execution['status'] == 'completed':
           result = execution['result']
           return {
               "status": "completed",
               "success": result.success,
               "nodes_executed": result.nodes_executed,
               "execution_time": result.execution_time_seconds,
               "metrics": result.metrics
           }
       else:
           return {
               "status": "failed",
               "error": execution.get('error', 'Unknown error')
           }

   @app.get("/pipelines/list")
   async def list_pipelines(environment: str = "production"):
       """List available pipelines."""
       context = ContextLoader().load_from_env(environment)
       executor = PipelineExecutor(context)
       pipelines = executor.list_pipelines()
       return {"pipelines": pipelines}

Streaming Example
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from tauro import StreamingPipelineManager, StreamingContext
   import time
   import logging

   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)

   def run_streaming_pipeline():
       """Run streaming pipeline with monitoring."""
       
       # Create streaming context
       ctx = StreamingContext.from_config("./config/streaming.yaml")
       manager = StreamingPipelineManager(ctx)
       
       # Start streaming pipeline
       execution_id = manager.run_streaming_pipeline(
           pipeline_name="kafka_events",
           checkpoint_location="/tmp/checkpoints/kafka",
           trigger_interval="10 seconds"
       )
       
       logger.info(f"Started streaming pipeline: {execution_id}")
       
       try:
           # Monitor pipeline
           while True:
               status = manager.get_pipeline_status(execution_id)
               
               logger.info(
                   f"Pipeline {execution_id}: "
                   f"State={status.state}, "
                   f"Records={status.records_processed}, "
                   f"Rate={status.processing_rate}/s"
               )
               
               # Check for errors
               if status.state == "FAILED":
                   logger.error(f"Pipeline failed: {status.error_message}")
                   break
               
               time.sleep(30)  # Check every 30 seconds
               
       except KeyboardInterrupt:
           logger.info("Stopping pipeline...")
           manager.stop_streaming_pipeline(execution_id)
           logger.info("Pipeline stopped")

   if __name__ == "__main__":
       run_streaming_pipeline()

MLOps Example
~~~~~~~~~~~~~

.. code-block:: python

   from tauro import MLContext, ExperimentTracker, ModelRegistry
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.metrics import accuracy_score, f1_score
   import pandas as pd

   # Initialize MLOps context
   ml_ctx = MLContext.from_config("./config/ml.yaml")
   tracker = ExperimentTracker(ml_ctx)
   registry = ModelRegistry(ml_ctx)

   # Load training data
   train_data = pd.read_parquet("data/train.parquet")
   test_data = pd.read_parquet("data/test.parquet")

   X_train = train_data.drop('target', axis=1)
   y_train = train_data['target']
   X_test = test_data.drop('target', axis=1)
   y_test = test_data['target']

   # Track experiment
   with tracker.start_run("customer_churn_v1") as run:
       
       # Log parameters
       params = {
           "n_estimators": 100,
           "max_depth": 10,
           "min_samples_split": 5,
           "random_state": 42
       }
       run.log_params(params)
       
       # Train model
       model = RandomForestClassifier(**params)
       model.fit(X_train, y_train)
       
       # Evaluate
       y_pred = model.predict(X_test)
       accuracy = accuracy_score(y_test, y_pred)
       f1 = f1_score(y_test, y_pred, average='weighted')
       
       # Log metrics
       run.log_metrics({
           "accuracy": accuracy,
           "f1_score": f1,
           "test_samples": len(y_test)
       })
       
       # Log model
       run.log_model(model, "random_forest_model")
       
       # Register model if performance is good
       if accuracy > 0.85:
           registry.register_model(
               model=model,
               name="churn_predictor",
               version="v1.0",
               tags={"accuracy": accuracy, "framework": "sklearn"}
           )
           print(f"✅ Model registered with accuracy: {accuracy:.3f}")

Testing Example
~~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   from tauro import PipelineExecutor, ContextLoader

   @pytest.fixture
   def test_context():
       """Fixture for test context."""
       return ContextLoader().load_from_env("test")

   @pytest.fixture
   def test_executor(test_context):
       """Fixture for test executor."""
       return PipelineExecutor(test_context)

   class TestPipelineExecution:
       
       def test_bronze_ingestion(self, test_executor):
           """Test bronze layer ingestion."""
           result = test_executor.execute("bronze_ingestion")
           
           assert result.success
           assert result.nodes_executed > 0
           assert "load_data" in result.completed_nodes
           assert result.metrics['records_loaded'] > 0
       
       def test_silver_transformation(self, test_executor):
           """Test silver layer transformation."""
           result = test_executor.execute("silver_cleansing")
           
           assert result.success
           assert "validate_schema" in result.completed_nodes
           assert "deduplicate" in result.completed_nodes
           assert result.metrics['records_processed'] > 0
       
       def test_pipeline_validation(self, test_executor):
           """Test pipeline configuration validation."""
           is_valid = test_executor.validate_pipeline("bronze_ingestion")
           assert is_valid
       
       def test_invalid_pipeline(self, test_executor):
           """Test handling of invalid pipeline."""
           with pytest.raises(ValueError):
               test_executor.execute("nonexistent_pipeline")
       
       def test_node_execution(self, test_executor):
           """Test individual node execution."""
           result = test_executor.execute_node(
               "bronze_ingestion",
               "load_data"
           )
           
           assert result.success
           assert result.metrics is not None

   class TestDataOperations:
       
       def test_input_loader(self, test_context):
           """Test input data loading."""
           from tauro import InputLoader
           
           loader = InputLoader(test_context)
           data = loader.load("test_data")
           
           assert data is not None
           assert len(data) > 0
       
       def test_output_writer(self, test_context):
           """Test output data writing."""
           from tauro import DataOutputManager
           import pandas as pd
           
           output = DataOutputManager(test_context)
           test_df = pd.DataFrame({"col1": [1, 2, 3]})
           
           output.write(
               dataframe=test_df,
               output_key="test_output",
               write_mode="overwrite"
           )

API Reference
-------------

See the complete :doc:`api/core` for detailed API documentation.

Best Practices
--------------

1. **Use context managers**

   .. code-block:: python

      with PipelineExecutor(context) as executor:
          result = executor.execute("pipeline")

2. **Handle errors gracefully**

   .. code-block:: python

      try:
          result = executor.execute("pipeline")
          if not result.success:
              logger.error(f"Pipeline failed: {result.error_message}")
              # Handle failure
      except Exception as e:
          logger.exception("Unexpected error")
          # Handle exception

3. **Use type hints**

   .. code-block:: python

      from tauro import PipelineExecutor, ExecutionResult, Context
      
      def run_pipeline(context: Context) -> ExecutionResult:
          executor = PipelineExecutor(context)
          return executor.execute("pipeline")

4. **Log important events**

   .. code-block:: python

      import logging
      
      logger = logging.getLogger(__name__)
      
      result = executor.execute("pipeline")
      logger.info(
          f"Pipeline completed: {result.nodes_executed} nodes "
          f"in {result.execution_time_seconds}s"
      )

5. **Test your code**

   .. code-block:: python

      # Use test environment
      test_context = ContextLoader().load_from_env("test")
      
      # Write unit tests
      def test_pipeline():
          assert result.success

Next Steps
----------

- Explore :doc:`api/core` for complete API documentation
- Follow :doc:`tutorials/airflow_integration` for orchestration
- Check :doc:`tutorials/fastapi_integration` for REST APIs
- Read :doc:`advanced/testing` for testing strategies
