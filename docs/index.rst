=============================
Tauro – Data Pipeline Framework
=============================

Tauro is a production-ready Python framework for building, deploying, and managing data pipelines at scale. 
Whether you're processing batch data, handling real-time streams, or building machine learning workflows, 
Tauro provides a complete, unified solution.

**Requirements**: Python 3.10+ | **License**: MIT | **Status**: Production Ready

.. contents:: Contents
   :local:
   :depth: 2

Core Capabilities
==================

**Batch Processing**
   Define and execute daily or periodic ETL jobs with simple YAML configuration. 
   Automatically parallelize processing of millions of rows.

**Real-Time Streaming**
   Consume data from Kafka, cloud streams, or files and process events in real time. 
   Built-in backpressure handling ensures system stability.

**Machine Learning Pipelines**
   Prepare data, train models, and automatically track experiments with integrated MLflow support. 
   Ensure reproducible results across environments.

**Feature Management**
   Define, version, and manage features for ML pipelines with built-in governance. 
   Keep feature data fresh and consistent for production models.

**Multi-Environment Deployment**
   Develop locally and deploy to production without code changes. 
   Works with Databricks, Spark, Kubernetes, and cloud platforms.

Key Features
============

**Simple Configuration**
   Define pipelines using clear, readable YAML configuration. No complex code required.

**Write Once, Deploy Anywhere**
   Run the same pipeline locally for development and in production on Databricks or Spark clusters.

**Production-Ready**
   Built-in validation, error handling, automatic retries, and comprehensive logging out of the box.

**Enterprise-Grade Reliability**
   Designed for real-world data engineering with monitoring integration and observability.

**Multi-Cloud & Multi-Platform**
   Compatible with AWS, Azure, GCP, Databricks, and traditional Spark environments.

**Complete Solution**
   Covers batch processing, streaming, ML tracking, feature management, and deployment—all integrated.

Quick Start
===========

1. **Install Tauro:**

   .. code-block:: bash

      pip install tauro

2. **Create a project:**

   .. code-block:: bash

      tauro template --template medallion_basic --project-name my_project
      cd my_project

3. **Run your first pipeline:**

   .. code-block:: bash

      tauro run --env dev --pipeline sample_pipeline

Your pipeline is now running. See :doc:`getting_started` for a complete walkthrough.

Documentation
==============

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   getting_started
   how_tauro_works

.. toctree::
   :maxdepth: 2
   :caption: User Guides

   quick_reference
   cli_usage
   configuration
   best_practices
   library_usage

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
   :caption: Production & Operations

   databricks_setup
   feature_store
   advanced/performance
   advanced/security
   advanced/troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Reference

   glossary
   api/reference
   migration_guide
   changelog

Getting Help
============

**First time with Tauro?**
   Start with :doc:`installation` and :doc:`getting_started`.

**Building a specific type of pipeline?**
   See the tutorials: :doc:`tutorials/batch_etl`, :doc:`tutorials/streaming`, or :doc:`tutorials/mlops`.

**Deploying to production?**
   Read :doc:`databricks_setup`, :doc:`advanced/security`, and :doc:`advanced/performance`.

**Troubleshooting issues?**
   Check :doc:`advanced/troubleshooting` and :doc:`glossary`.

**Questions or issues?**
   Open an issue on `GitHub <https://github.com/faustino125/tauro/issues>`_.
