.. Tauro documentation master file

Tauro - Data Pipeline Framework
============================================

**Tauro** is a data pipeline framework designed to simplify the development and orchestration of batch, streaming, and hybrid data workflows. Built for data engineers and ML practitioners, Tauro provides enterprise-grade reliability with developer-friendly simplicity.

.. image:: https://img.shields.io/badge/python-3.9%2B-blue.svg
   :target: https://www.python.org/downloads/
   :alt: Python Version

.. image:: https://img.shields.io/badge/license-MIT-green.svg
   :target: https://github.com/faustino125/tauro/blob/main/LICENSE
   :alt: License

.. image:: https://img.shields.io/badge/pypi-v0.1.3-orange.svg
   :target: https://pypi.org/project/tauro/
   :alt: PyPI version

Key Features
------------

🔧 **Dual Mode: CLI + Library**
   Use Tauro as a command-line tool or integrate it programmatically into your Python projects.

📊 **Multi-Pipeline Support**
   - Batch Processing with date ranges and incremental loads
   - Real-time Streaming (Kafka, Kinesis, file-based)
   - Hybrid Workflows combining batch and streaming
   - ML/MLOps with experiment tracking and model registry

🏗️ **Enterprise-Ready**
   - Security: Path validation, input sanitization, secure module loading
   - Resilience: Automatic retries, circuit breakers, graceful degradation
   - Observability: Structured logging, metrics, and health checks
   - Multi-Environment: Configuration per environment (dev, staging, prod)

Quick Start
-----------

Installation
~~~~~~~~~~~~

.. code-block:: bash

   # Basic installation
   pip install tauro

   # With Spark support
   pip install tauro[spark]

   # Complete installation
   pip install tauro[all]

CLI Mode
~~~~~~~~

.. code-block:: bash

   # Create a project
   tauro --template medallion_basic --project-name my_project
   cd my_project

   # Run a pipeline
   tauro --env dev --pipeline sales_etl

Library Mode
~~~~~~~~~~~~

.. code-block:: python

   from tauro import PipelineExecutor, ContextLoader

   # Load context
   context = ContextLoader().load_from_env("dev")

   # Execute pipeline
   executor = PipelineExecutor(context)
   result = executor.execute("sales_etl")

   print(f"✅ Success: {result.nodes_executed} nodes")

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   getting_started
   installation
   cli_usage
   library_usage
   configuration
   best_practices

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/batch_etl
   tutorials/streaming
   tutorials/mlops
   tutorials/airflow_integration
   tutorials/fastapi_integration

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   advanced/architecture
   advanced/security
   advanced/performance
   advanced/testing
   advanced/troubleshooting

.. toctree::
   :maxdepth: 1
   :caption: Additional Resources

   migration_guide
   changelog
   contributing
   license

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
