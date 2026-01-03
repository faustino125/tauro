Library Usage
=============

While the CLI is great for interactive use, the Python library is the key to integrating Tauro into your applications, tests, and orchestration workflows.

This guide covers the fundamental components for programmatic pipeline execution.

Core Concepts
-------------

1.  **`ContextLoader`**: Loads a `Context` from a normalized set of configuration files. You hand it the absolute paths that describe your global settings, pipelines, nodes, inputs, and outputs.

2.  **`AppConfigManager`**: Reads the `settings_json.json` (or `settings_yml.json` / `settings_dsl.json`) file that Tauro’s CLI template generates and resolves every environment to a concrete set of file paths.

3.  **`PipelineExecutor`**: Orchestrates batch, streaming, and hybrid pipelines once the context is ready. It raises when a node fails and returns either `None` (batch), an execution ID (streaming), or a dictionary (hybrid).

Executing a Pipeline: A Practical Example
-----------------------------------------

Let’s replicate the CLI command ``tauro --env dev --pipeline load`` using the Python library.

Assume you are in the ``my_first_pipeline`` project created in the :doc:`getting_started` guide.

.. code-block:: python
   :emphasize-lines: 5,9,15

   from tauro import ContextLoader, PipelineExecutor
   from tauro.cli.config import AppConfigManager

   settings = AppConfigManager("settings_json.json")
   config_paths = settings.get_env_config("dev")
   context = ContextLoader().load_from_paths(config_paths, "dev")
   executor = PipelineExecutor(context)

   try:
       executor.run_pipeline("load")
       print("✅ Pipeline executed successfully")
   except Exception as exc:
       print(f"❌ Pipeline failed: {exc}")

.. note::
   ``run_pipeline`` returns ``None`` for batch and ML pipelines, so a successful run is simply indicated by the absence of an exception. Streaming pipelines return the execution ID string, and hybrid pipelines return a dictionary with ``batch_execution`` and ``streaming_execution_ids``.

Advanced Usage
--------------

**Executing with Parameters**

You can pass parameters like ``start_date`` and ``end_date`` directly:

.. code-block:: python

   executor.run_pipeline(
       pipeline_name="transform",
       start_date="2024-01-01",
       end_date="2024-01-31"
   )

**Validating a Pipeline**

Tauro exposes ``PipelineValidator`` if you want to check the structure of a pipeline without executing it:

.. code-block:: python

   from tauro import PipelineValidator

   validator = PipelineValidator()
   pipeline = context.get_pipeline("aggregate")
   validator.validate_pipeline_config(pipeline)
   print("Pipeline definition is valid")

**Executing a Single Node**

Pass ``node_name`` to ``run_pipeline`` to target one step of a pipeline:

.. code-block:: python

   executor.run_pipeline("transform", node_name="clean_data")

Next Steps
----------

Now that you understand the basics of using Tauro as a library, you can explore more advanced topics:

- **Orchestration**: See the :doc:`tutorials/airflow_integration` tutorial.
- **Custom Applications**: See the :doc:`tutorials/fastapi_integration` tutorial for building a REST API.
- **Testing**: Learn how to test your pipelines in :doc:`advanced/testing`.
- **API Reference**: For a detailed look at all classes and methods, see the :doc:`api/index`.
