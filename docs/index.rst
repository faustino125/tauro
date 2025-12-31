.. Tauro documentation master file

=============================
Tauro - Data Pipeline Framework
=============================

**Tauro** is a Python framework that makes it simple to build, test, and manage data pipelines. Whether you're processing data in batch, handling real-time streams, or training machine learning models, Tauro provides the tools you need with minimal overhead.

.. image:: https://img.shields.io/badge/python-3.10%2B-blue.svg
   :target: https://www.python.org/downloads/
   :alt: Python Version

.. image:: https://img.shields.io/badge/license-MIT-green.svg
   :target: https://github.com/faustino125/tauro/blob/main/LICENSE
   :alt: License

.. image:: https://img.shields.io/badge/pypi-v0.1.4-orange.svg
   :target: https://pypi.org/project/tauro/
   :alt: PyPI version

.. contents:: **Table of Contents**
   :local:
   :depth: 2

What Can You Do With Tauro?
============================

✅ **Process Data in Batches**
   - Daily or periodic ETL jobs that transform data from one format to another
   - Incremental loading with date ranges
   - Scheduled workflows

✅ **Handle Real-Time Data**
   - Stream data from Kafka, files, or other sources
   - Process events as they arrive
   - Write results to data lakes or warehouses

✅ **Build ML Pipelines**
   - Prepare data for training
   - Automatically track experiments and models
   - Reproduce results consistently

✅ **Use Flexible Deployment**
   - Run locally during development
   - Deploy to Databricks or Spark clusters for scale
   - Same code works everywhere

Why Choose Tauro?
==================

**Simple Configuration**
   Define your pipelines using clear YAML files—no complex code needed

**Works Locally & At Scale**
   Develop on your laptop, deploy to production without changes

**Safe by Default**
   Input validation, error handling, and automatic retries built-in

**Production Ready**
   Designed for real-world data engineering with monitoring and logging

**Multi-Cloud**
   Works with AWS, Azure, GCP, and Databricks

**Batteries Included**
   Handles batch processing, streaming, ML tracking, and feature management

Getting Started in 5 Minutes
=============================

**Installation**

.. code-block:: bash

   pip install tauro

**Create Your First Project**

.. code-block:: bash

   tauro --template medallion_basic --project-name my_project
   cd my_project

**Run a Pipeline**

.. code-block:: bash

   tauro --env dev --pipeline sales_etl

**That's it!** Your pipeline is running.

Next Steps
==========

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   getting_started
   how_tauro_works

.. toctree::
   :maxdepth: 2
   :caption: Using Tauro

   cli_usage
   configuration
   best_practices

.. toctree::
   :maxdepth: 2
   :caption: Learning by Example

   tutorials/batch_etl
   tutorials/streaming
   tutorials/mlops

.. toctree::
   :maxdepth: 2
   :caption: Troubleshooting

   guides/troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api/reference
   changelog
   license

Need Help?
==========

- 📖 **First time?** Start with :doc:`getting_started`
- 🔧 **Building pipelines?** Check out :doc:`guides/batch_etl`
- 📊 **Real-time data?** See :doc:`guides/streaming_pipelines`
- ❓ **Something not working?** Visit :doc:`guides/troubleshooting`
- 💬 **Have questions?** Open an issue on `GitHub <https://github.com/faustino125/tauro>`_

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
