Library Usage
=============

While the CLI is great for interactive use, the Python library is the key to integrating Tauro into your applications, tests, and orchestration workflows.

This guide covers the fundamental components for programmatic pipeline execution.

Core Concepts
-------------

There are three main objects you'll work with:

1.  **`ContextLoader`**: This object finds, parses, and validates your project's YAML configuration for a specific environment. It's the entry point for loading your project settings into memory.

2.  **`PipelineExecutor`**: This is the workhorse. It takes a loaded `Context` and provides methods to execute or validate your pipelines and their individual nodes.

3.  **`ExecutionResult`**: Every execution returns this object. It contains detailed information about the run, including its success status, which nodes were executed, timing metrics, and any errors that occurred.

Executing a Pipeline: A Practical Example
-----------------------------------------

Let's replicate the CLI command `tauro --env dev --pipeline load` using the Python library.

Assume you are in the `my_first_pipeline` project directory created in the :doc:`getting_started` guide.

**Step 1: Create a Python Script**

Create a new file named `run_my_pipeline.py`.

**Step 2: Add the Code**

.. code-block:: python
   :emphasize-lines: 5,10,14

   from tauro import ContextLoader, PipelineExecutor
   import os

   # Define the path to your Tauro project. For this script, it's the current directory.
   project_path = os.getcwd()

   # 1. Load the 'dev' environment context
   # The ContextLoader finds and reads your `settings.json` and `config` directory.
   print("Loading context...")
   context = ContextLoader(project_path).load_from_env("dev")

   # 2. Initialize the executor with the context
   # The executor is now ready to run any pipeline from your project.
   executor = PipelineExecutor(context)

   # 3. Execute the 'load' pipeline
   print("Executing pipeline 'load'...")
   result = executor.execute("load")

   # 4. Inspect the results
   print("\n--- Execution Finished ---")
   if result.success:
       print(f"✅ Pipeline executed successfully!")
       print(f"   - Nodes run: {result.nodes_executed}")
       print(f"   - Time taken: {result.execution_time_seconds:.2f} seconds")
   else:
       print(f"❌ Pipeline failed on node: {result.failed_node}")
       print(f"   - Error: {result.error_message}")

**Step 3: Run the Script**

.. code-block:: bash

   python run_my_pipeline.py

**Expected Output:**

.. code-block:: text

   Loading context...
   Executing pipeline 'load'...
   
   --- Execution Finished ---
   ✅ Pipeline executed successfully!
      - Nodes run: ['load_raw_data']
      - Time taken: 1.45 seconds

Advanced Usage
--------------

**Executing with Parameters**

You can pass parameters like `start_date` and `end_date` directly to the `execute` method.

.. code-block:: python

   result = executor.execute(
       pipeline_name="transform",
       start_date="2024-01-01",
       end_date="2024-01-31"
   )

**Validating a Pipeline**

To perform a "dry run" that checks configuration and dependencies without executing code:

.. code-block:: python

   is_valid, message = executor.validate("aggregate")
   if is_valid:
       print("Pipeline is valid!")
   else:
       print(f"Pipeline validation failed: {message}")

**Executing a Single Node**

This is useful for testing or debugging a specific part of a larger pipeline.

.. code-block:: python

   # To run just the 'clean_data' node from the 'transform' pipeline:
   result = executor.execute_node("transform", "clean_data")

Next Steps
----------

Now that you understand the basics of using Tauro as a library, you can explore more advanced topics:

- **Orchestration**: See the :doc:`tutorials/airflow_integration` tutorial.
- **Custom Applications**: See the :doc:`tutorials/fastapi_integration` tutorial for building a REST API.
- **Testing**: Learn how to test your pipelines in :doc:`advanced/testing`.
- **API Reference**: For a detailed look at all classes and methods, see the :doc:`api/index`.
