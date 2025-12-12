Getting Started
===============

Welcome to Tauro! This guide will help you get up and running with Tauro in minutes.

What is Tauro?
--------------

Tauro is a unified data pipeline framework that simplifies:

- **Batch Processing**: ETL pipelines with date-range processing
- **Streaming**: Real-time data processing with Kafka, Kinesis
- **Hybrid Workflows**: Combine batch and streaming
- **MLOps**: Experiment tracking and model management

Prerequisites
-------------

Before you begin, ensure you have:

- Python 3.9 or newer
- pip or poetry for package management
- (Optional) Apache Spark 3.4+ for large-scale processing

Installation
------------

Basic Installation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pip install tauro

With Optional Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # With Spark support
   pip install tauro[spark]

   # With API and monitoring
   pip install tauro[api,monitoring]

   # Everything
   pip install tauro[all]

From Source
~~~~~~~~~~~

.. code-block:: bash

   git clone https://github.com/faustino125/tauro.git
   cd tauro
   pip install -e .

Verify Installation
-------------------

.. code-block:: bash

   tauro --version

You should see:

.. code-block:: text

   Tauro version 0.1.3

Your First Pipeline (CLI)
--------------------------

Step 1: Create a Project
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --template medallion_basic --project-name my_first_pipeline
   cd my_first_pipeline

This creates a complete project structure:

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
   ├── notebooks/
   └── settings.json

Step 2: List Available Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --list-pipelines

Step 3: Run Your First Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline bronze_ingestion

Step 4: Check Results
~~~~~~~~~~~~~~~~~~~~~

Your processed data will be in the ``data/`` directory according to your configuration.

Your First Pipeline (Library)
------------------------------

Create a Python Script
~~~~~~~~~~~~~~~~~~~~~~

Create ``my_pipeline.py``:

.. code-block:: python

   from tauro import PipelineExecutor, ContextLoader

   # Load configuration
   context = ContextLoader().load_from_env("dev")

   # Create executor
   executor = PipelineExecutor(context)

   # Execute pipeline
   result = executor.execute("bronze_ingestion")

   # Check results
   if result.success:
       print(f"✅ Pipeline completed successfully!")
       print(f"   Nodes executed: {result.nodes_executed}")
       print(f"   Time: {result.execution_time_seconds}s")
   else:
       print(f"❌ Pipeline failed: {result.error_message}")

Run the Script
~~~~~~~~~~~~~~

.. code-block:: bash

   python my_pipeline.py

Next Steps
----------

Now that you have Tauro installed and working, you can:

1. **Learn CLI Usage**: See :doc:`cli_usage` for all CLI commands
2. **Use as Library**: See :doc:`library_usage` for programmatic usage
3. **Configure Pipelines**: See :doc:`configuration` for config options
4. **Follow Tutorials**: Check out :doc:`tutorials/batch_etl` and others
5. **Explore Examples**: See the ``examples/`` directory in the repository

Common Issues
-------------

Command Not Found
~~~~~~~~~~~~~~~~~

If ``tauro`` command is not found:

.. code-block:: bash

   # Use Python module syntax
   python -m tauro --help

   # Or add to PATH (Linux/Mac)
   export PATH="$HOME/.local/bin:$PATH"

   # Or add to PATH (Windows PowerShell)
   $env:PATH += ";$HOME\.local\bin"

Import Errors
~~~~~~~~~~~~~

If you get import errors:

.. code-block:: bash

   # Ensure tauro is installed
   pip show tauro

   # Reinstall if needed
   pip install --upgrade --force-reinstall tauro

Configuration Not Found
~~~~~~~~~~~~~~~~~~~~~~~

If configuration files are not found:

.. code-block:: python

   from tauro import ContextLoader

   # Use explicit path
   context = ContextLoader().load_from_config("./config/settings.yaml")

Get Help
--------

- **Documentation**: https://tauro.readthedocs.io
- **GitHub Issues**: https://github.com/faustino125/tauro/issues
- **Discussions**: https://github.com/faustino125/tauro/discussions
- **Email**: faustinolopezramos@gmail.com
