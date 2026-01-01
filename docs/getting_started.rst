Getting Started
===============

This guide takes you from installation to running your first Tauro pipeline in 10 minutes.

What You'll Learn
-----------------

- Install Tauro
- Create your first project
- Run a sample pipeline
- Understand how configuration works
- Customize the pipeline for your own data

Prerequisites
--------------

- **Python 3.10 or higher** (check with ``python --version``)
- **pip** (usually comes with Python)

Installation
~~~~~~~~~~~~

**Step 1: Create a Virtual Environment** (recommended)

.. code-block:: bash

   # On Mac/Linux
   python3 -m venv tauro-env
   source tauro-env/bin/activate

   # On Windows
   python -m venv tauro-env
   tauro-env\Scripts\activate

**Step 2: Install Tauro**

.. code-block:: bash

   pip install tauro

   # Or if you want to process large datasets:
   pip install tauro[spark]

**That's it!** You're ready to go.

Create Your First Project
--------------------------

Tauro comes with project templates that set up everything you need. Let's create one:

.. code-block:: bash

   tauro --template medallion_basic --project-name my_first_project
   cd my_first_project
   ls

You'll see a structure like this:

.. code-block:: text

   my_first_project/
   ├── config/
   │   ├── pipelines.yaml      # What pipelines you have
   │   ├── nodes.yaml          # What each node does
   │   ├── inputs.yaml         # Where data comes from
   │   ├── outputs.yaml        # Where results go
   │   └── global.yaml         # General settings
   ├── src/
   │   └── nodes/              # Your custom code
   │       ├── extract.py
   │       ├── transform.py
   │       └── load.py
   ├── data/
   │   ├── input/              # Test data
   │   └── output/             # Results
   └── .env                    # Environment variables

This is the recommended project structure. It keeps configuration, code, and data separate and organized.

Run Your First Pipeline
------------------------

Now let's run the included pipeline:

.. code-block:: bash

   tauro --env dev --pipeline sample_pipeline

You should see:

.. code-block:: text

   ✓ Loading configuration...
   ✓ Validating pipeline...
   ✓ Starting execution...
   ✓ extract ..................... [1/3]
   ✓ transform ................... [2/3]
   ✓ load ....................... [3/3]
   ✓ Pipeline completed in 2.3 seconds

Congratulations! Your first pipeline ran successfully.

What Just Happened?
~~~~~~~~~~~~~~~~~~~

Tauro executed three steps:

1. **extract** - Read data from ``data/input/sample.csv``
2. **transform** - Cleaned and processed the data
3. **load** - Saved results to ``data/output/results.parquet``

Each step is defined in ``config/nodes.yaml`` and the logic is in ``src/nodes/``.

Understand the Configuration
-----------------------------

Let's look at what makes up a Tauro pipeline. Open ``config/pipelines.yaml``:

.. code-block:: yaml

   pipelines:
     sample_pipeline:
       nodes: [extract, transform, load]
       description: "A simple ETL pipeline"

This says: "The pipeline called 'sample_pipeline' runs three steps in order: extract, transform, then load."

Now look at ``config/nodes.yaml``:

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract.extract_data"
       description: "Read data from CSV"
       timeout: 300

     transform:
       function: "src.nodes.transform.clean_data"
       description: "Clean and process data"
       timeout: 600

     load:
       function: "src.nodes.load.save_results"
       description: "Save processed data"
       timeout: 300

Each node points to a Python function that does the actual work. Let's look at one:

Open ``src/nodes/extract.py``:

.. code-block:: python

   import pandas as pd

   def extract_data():
       """Read data from CSV file."""
       df = pd.read_csv("data/input/sample.csv")
       return df

That's it! The function reads data and returns it. Tauro handles the plumbing—passing the result to the next step.

Customize Your Pipeline
-----------------------

Let's modify the pipeline to make it your own.

**Edit your data source:**

Replace ``data/input/sample.csv`` with your own data file, or create a simple test file:

.. code-block:: bash

   echo "id,name,amount
   1,Alice,100
   2,Bob,200
   3,Charlie,150" > data/input/sample.csv

**Update the transform logic:**

Edit ``src/nodes/transform.py``:

.. code-block:: python

   import pandas as pd

   def clean_data(df):
       """Add a total column and filter."""
       df['amount_double'] = df['amount'] * 2
       df = df[df['amount'] > 100]  # Only rows with amount > 100
       return df

**Run the modified pipeline:**

.. code-block:: bash

   tauro --env dev --pipeline sample_pipeline

You'll see your custom logic executed!

Next: What's Next?
------------------

✅ You've learned:
   - How to install Tauro
   - How to create a project
   - How to run a pipeline
   - How configuration works
   - How to customize the code

📖 **Continue learning:**

- :doc:`cli_usage` - Learn all the CLI commands
- :doc:`guides/batch_etl` - Build a realistic ETL pipeline
- :doc:`guides/configuration` - Master configuration options
- :doc:`best_practices` - Learn how to do things right

💡 **Pro Tips:**

- Use ``tauro --list-pipelines`` to see all available pipelines
- Add ``--log-level DEBUG`` to see detailed execution logs
- Use ``--validate`` to check your configuration without running it

Got stuck? Check :doc:`guides/troubleshooting` for solutions to common problems.

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
