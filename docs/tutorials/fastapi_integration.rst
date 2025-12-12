FastAPI Integration
===================

This tutorial shows how to wrap your Tauro project in a REST API using FastAPI. This allows you to trigger pipelines, check their status, and get results via HTTP requests, which is useful for integrating with web applications or other services.

**Why use FastAPI with Tauro?**

- **Remote Execution**: Trigger pipelines from anywhere, without direct access to the machine.
- **Service Integration**: Allow other microservices to run and interact with your data pipelines.
- **UI Integration**: Provide a backend for a web-based user interface that can start and monitor pipelines.

A Basic Pipeline API
--------------------

Here is an example of a FastAPI application that exposes endpoints to:
- Asynchronously start a pipeline execution.
- Check the status and result of a pipeline run.
- List all available pipelines in the project.

**Project Setup**

1.  Make sure you have FastAPI and an ASGI server like Uvicorn installed:
    .. code-block:: bash

       pip install "fastapi[all]" uvicorn

2.  Save the following code as `main.py` in the root of your Tauro project (e.g., alongside `settings.json`).

**API Code (`main.py`)**

.. code-block:: python

   from fastapi import FastAPI, HTTPException, BackgroundTasks
   from pydantic import BaseModel
   from tauro import PipelineExecutor, ContextLoader
   from typing import Optional, Dict, Any
   import uuid
   import os

   # --- Configuration ---
   # Define the path to your Tauro project.
   PROJECT_PATH = os.getcwd()

   # --- FastAPI App Initialization ---
   app = FastAPI(
       title="Tauro Pipeline API",
       description="An API to execute and monitor Tauro data pipelines."
   )

   # --- In-memory "database" for storing execution status ---
   # In a real production environment, you should use Redis, a database, or another
   # persistent storage solution instead of a simple Python dictionary.
   executions: Dict[str, Dict[str, Any]] = {}

   # --- Pydantic Models for API requests and responses ---
   class PipelineRequest(BaseModel):
       pipeline_name: str
       environment: str = "dev"
       start_date: Optional[str] = None
       end_date: Optional[str] = None

   class PipelineStartResponse(BaseModel):
       execution_id: str
       status: str
       message: str

   # --- Background Task for Pipeline Execution ---
   def run_pipeline_task(execution_id: str, request: PipelineRequest):
       """
       This function runs in the background and executes the Tauro pipeline.
       It updates the `executions` dictionary with the status and result.
       """
       try:
           print(f"Starting pipeline '{request.pipeline_name}' (ID: {execution_id})")
           context = ContextLoader(PROJECT_PATH).load_from_env(request.environment)
           executor = PipelineExecutor(context)
           
           result = executor.execute(
               pipeline_name=request.pipeline_name,
               start_date=request.start_date,
               end_date=request.end_date
           )
           
           executions[execution_id] = {
               'status': 'completed' if result.success else 'failed',
               'result': result.dict()  # Convert result to a dictionary for storage
           }
           print(f"Finished pipeline '{request.pipeline_name}' (ID: {execution_id})")
       except Exception as e:
           print(f"Error during pipeline execution (ID: {execution_id}): {e}")
           executions[execution_id] = {
               'status': 'error',
               'error_message': str(e)
           }

   # --- API Endpoints ---
   @app.post("/pipelines/run", response_model=PipelineStartResponse)
   async def run_pipeline(
       request: PipelineRequest,
       background_tasks: BackgroundTasks
   ):
       """
       Asynchronously starts a pipeline execution.
       """
       execution_id = str(uuid.uuid4())
       executions[execution_id] = {'status': 'running'}
       
       # Add the pipeline execution to the background tasks
       background_tasks.add_task(run_pipeline_task, execution_id, request)
       
       return PipelineStartResponse(
           execution_id=execution_id,
           status="started",
           message=f"Pipeline '{request.pipeline_name}' execution has been started."
       )

   @app.get("/pipelines/status/{execution_id}")
   async def get_pipeline_status(execution_id: str):
       """
       Retrieves the status and result of a pipeline execution.
       """
       execution = executions.get(execution_id)
       if not execution:
           raise HTTPException(status_code=404, detail="Execution ID not found.")
       
       return execution

   @app.get("/pipelines/list")
   async def list_available_pipelines(environment: str = "dev"):
       """
       Lists all pipelines available in the specified environment.
       """
       try:
           context = ContextLoader(PROJECT_PATH).load_from_env(environment)
           executor = PipelineExecutor(context)
           pipelines = executor.list_pipelines()
           return {"environment": environment, "pipelines": pipelines}
       except Exception as e:
           raise HTTPException(status_code=500, detail=f"Failed to load pipelines: {e}")

How to Run the API
------------------

1.  **Start the Server**: From your terminal, in your Tauro project directory, run:
    .. code-block:: bash

       uvicorn main:app --reload

2.  **Access the Docs**: Open your browser and go to `http://127.0.0.1:8000/docs`. You will see the interactive Swagger UI for your API.

3.  **Use the API**:
    - Use the `/pipelines/list` endpoint to see your available pipelines (e.g., `load`, `transform`).
    - Use the `/pipelines/run` endpoint to start one of them. Copy the `execution_id` from the response.
    - Use the `/pipelines/status/{execution_id}` endpoint, pasting the ID, to check the result.