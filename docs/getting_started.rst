Getting Started
===============

This guide provides a hands-on introduction to Tauro, helping you install it and run your first data pipeline in minutes.

What is Tauro?
--------------

Tauro is a Python framework for building and orchestrating data pipelines. It's designed to help you reliably process data for analytics, machine learning, and business intelligence, whether you're working with batch files, real-time streams, or both.

Tauro's core philosophy is to provide a unified, enterprise-grade foundation for your data workflows, while maintaining developer-friendly simplicity. It helps you focus on your business logic, not on boilerplate code.

Key use cases include:

- **Declarative ETL/ELT**: Define batch pipelines in simple YAML files.
- **Streaming Workflows**: Process real-time data from sources like Kafka.
- **MLOps Pipelines**: Build reproducible data workflows for training and inference.
- **Hybrid Processing**: Combine batch and streaming logic in a single framework.

Prerequisites
-------------

- Python 3.9+
- `pip` for package installation

**(Optional) For specific features:**

- Apache Spark 3.4+ (for Spark-based pipelines)
- An account with a supported provider (e.g., Databricks, MLflow) for MLOps integrations.

Installation
------------

We strongly recommend installing Tauro in a virtual environment to avoid conflicts with system-wide packages.

.. code-block:: bash

   # Create and activate a virtual environment (macOS/Linux)
   python3 -m venv venv
   source venv/bin/activate

   # Create and activate a virtual environment (Windows)
   python -m venv venv
   venv\Scripts\activate

Once your environment is active, you can install Tauro.

**1. Standard Installation**

This installs the core framework, which is sufficient for many use cases.

.. code-block:: bash

   pip install tauro

**2. Installation with Extras**

Tauro uses "extras" to install dependencies for specific features. This keeps the core installation lightweight.

.. code-block:: bash

   # To process data with Apache Spark
   pip install tauro[spark]

   # For MLOps integrations (like MLflow)
   pip install tauro[mlops]

   # To install all optional dependencies at once
   pip install tauro[all]

**3. Installing from Source**

If you want the latest development version, you can install from source.

.. code-block:: bash

   git clone https://github.com/faustino125/tauro.git
   cd tauro
   pip install -e .

Verify Installation
-------------------

.. code-block:: bash

   tauro --version

The output should be similar to:

.. code-block:: text

   Tauro version 0.1.3

Your First Pipeline (CLI)
--------------------------

The fastest way to start is with the Tauro CLI.

**Step 1: Create a Project from a Template**

Tauro includes project templates to get you started quickly. We'll use the `medallion_basic` template, which sets up a project for a three-layer Medallion architecture (Bronze, Silver, Gold).

.. code-block:: bash

   tauro --template medallion_basic --project-name my_first_pipeline
   cd my_first_pipeline

This command generates a directory with sample data, configuration, and placeholder pipeline scripts. The structure looks like this:

.. code-block:: text

   my_first_pipeline/
   ├── config/
   │   ├── base/
   │   │   ├── global_settings.yaml
   │   │   ├── pipelines.yaml
   │   │   ├── nodes.yaml
   │   │   ├── input.yaml
   │   │   └── output.yaml
   │   └── dev/
   ├── data/
   │   └── raw/
   │       └── sample_data.csv
   ├── notebooks/
   ├── pipelines/
   │   ├── __init__.py
   │   ├── load.py
   │   ├── transform.py
   │   └── aggregate.py
   └── settings.json

**Step 2: List the Available Pipelines**

Your project's pipelines are defined in `config/base/pipelines.yaml`. You can list them with the CLI.

.. code-block:: bash

   tauro --list-pipelines

The output shows the three pipelines defined in the template:

.. code-block:: text

   Available pipelines:
   - load: Ingests raw data into the Bronze layer.
   - transform: Cleans and enriches data, moving it from Bronze to Silver.
   - aggregate: Creates business-level aggregations, moving data from Silver to Gold.

**Step 3: Run the 'load' Pipeline**

Now, let's run the first pipeline, `load`. This pipeline reads the sample CSV file and writes it to a new location in the "Bronze" layer as a Delta Lake table.

.. code-block:: bash

   tauro --env dev --pipeline load

You will see log messages as Tauro executes the nodes defined for this pipeline.

**Step 4: Verify the Outcome**

After the pipeline finishes, check the `data/bronze` directory. You will find a new folder containing the output, a Delta table. This confirms your pipeline ran successfully.

This simple workflow is the foundation for all Tauro projects. You can now inspect the YAML files in the `config` directory and the Python files in the `pipelines` directory to see how it works.

Your First Pipeline (Library)
------------------------------

For integration with other Python applications or for more complex orchestration, you can use Tauro as a library.

**The Goal:** We will replicate the CLI command `tauro --env dev --pipeline load` using a Python script.

Create a Python script named `run_pipeline.py` in the root of your `my_first_pipeline` project.

.. code-block:: python
   :emphasize-lines: 7

   from tauro import PipelineExecutor, ContextLoader

   # Define the project's root directory.
   # Tauro needs to know where your `settings.json` and `config` directory are located.
   # For this script, it's the current directory.
   project_path = "." 

   # 1. Load the context
   # This loads the 'dev' environment configuration by finding and reading settings.json.
   # The `project_path` tells Tauro where to start looking.
   context = ContextLoader(project_path).load_from_env("dev")

   # 2. Create a pipeline executor
   # The executor is responsible for running pipelines using the loaded context.
   executor = PipelineExecutor(context)

   # 3. Execute the 'load' pipeline
   result = executor.execute("load")

   # 4. Print the results
   if result.success:
       print(f"✅ Pipeline '{result.pipeline_name}' completed successfully!")
       print(f"   Nodes executed: {result.nodes_executed}")
       print(f"   Execution time: {result.execution_time_seconds:.2f}s")
   else:
       print(f"❌ Pipeline failed: {result.error_message}")

**Run the Script**

.. code-block:: bash

   python run_pipeline.py

The output confirms the successful execution:

.. code-block:: text

   ✅ Pipeline 'load' completed successfully!
      Nodes executed: ['load_raw_data']
      Execution time: 1.23s

This library-based approach is ideal for embedding Tauro in a larger application, such as a FastAPI service or an Airflow DAG.


Next Steps
----------

Congratulations on running your first pipeline! Here’s what you can do next:

- **Explore the CLI**: Dive deeper into the command-line interface.
  - See the :doc:`cli_usage` guide for a full list of commands and options.

- **Learn the Library**: Understand how to use Tauro programmatically.
  - Read the :doc:`library_usage` guide for in-depth examples.

- **Understand Configuration**: Learn how to customize pipelines, nodes, and environments.
  - See the :doc:`configuration` guide for details on the YAML-based setup.

- **Follow a Tutorial**: Work through a real-world example.
  - The :doc:`tutorials/batch_etl` tutorial is a great place to start.

Troubleshooting and Help
------------------------

**"Command not found" error**

If your shell cannot find the `tauro` command, it's likely that the installation directory isn't in your system's `PATH`. You can either:

1.  **Run as a module (recommended)**:
    .. code-block:: bash

       python -m tauro --version

2.  **Add the directory to your PATH**:
    Find the directory with `pip show tauro` (look for `Location`) and add the `scripts` or `bin` subdirectory to your `PATH`.

**Configuration not found error**

Tauro needs to be run from your project's root directory (the one containing `settings.json`) to find its configuration. If you can't run it from there, you must specify the path to your configuration.

- **CLI**: Use the `--project-dir` flag:
  .. code-block:: bash

     tauro --project-dir /path/to/my_first_pipeline --env dev --pipeline load

- **Library**: Pass the path to `ContextLoader`:
  .. code-block:: python

     context = ContextLoader("/path/to/my_first_pipeline").load_from_env("dev")

**Get Community Support**

If you're stuck, the Tauro community is here to help:

- **GitHub Discussions**: For questions, ideas, and showing off what you've built.
- **GitHub Issues**: For bug reports and feature requests.
