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

✅ **Flexible Deployment Options**
   - Run locally during development
   - Deploy to Databricks or Spark clusters for scale
   - Same code works everywhere

Why Choose Tauro?
==================

**Simple Configuration** 
   Define your pipelines using clear YAML files—no complex code needed

**Local & Production Ready**
   Develop on your laptop, deploy to production without changes

**Production Safe**
   Input validation, error handling, and automatic retries built-in

**Enterprise Grade**
   Designed for real-world data engineering with monitoring and logging

**Multi-Cloud Support**
   Works with AWS, Azure, GCP, and Databricks

**Complete Solution**
   Handles batch processing, streaming, ML tracking, and feature management

Quick Start
===========

**1. Install**

.. code-block:: bash

   pip install tauro

**2. Create a Project**

.. code-block:: bash

   tauro --template medallion_basic --project-name my_project
   cd my_project

**3. Run Your First Pipeline**

.. code-block:: bash

   tauro --env dev --pipeline sample_pipeline

Done! Your pipeline is running.

Documentation Structure
=======================

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   getting_started
   how_tauro_works

.. toctree::
   :maxdepth: 2
   :caption: Using Tauro

   quick_reference
   cli_usage
   library_usage
   migration_guide
   configuration
   best_practices

.. toctree::
   :maxdepth: 2
   :caption: Integration & Advanced

   databricks_setup
   feature_store
   advanced/architecture
   advanced/security
   advanced/performance
   advanced/testing
   advanced/troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Learning by Example

   tutorials/batch_etl
   tutorials/streaming
   tutorials/mlops
   tutorials/airflow_integration
   tutorials/fastapi_integration

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/reference

.. toctree::
   :maxdepth: 1
   :caption: Reference

   glossary
   changelog
   contributing
   license

Getting Help
============

- **New to Tauro?** Start with :doc:`getting_started`
- **Build batch pipelines?** Check out :doc:`tutorials/batch_etl`
- **Real-time data?** See :doc:`tutorials/streaming`
- **ML pipelines?** Read :doc:`tutorials/mlops`
- **Something broken?** Visit :doc:`advanced/troubleshooting`
- **Questions?** Open an issue on `GitHub <https://github.com/faustino125/tauro>`_
