Tutorials
=========

.. toctree::
   :maxdepth: 2

   batch_etl
   streaming
   mlops
   airflow_integration
   fastapi_integration

Overview
--------

These tutorials provide hands-on examples of using Tauro for various use cases.

Batch ETL Tutorial
~~~~~~~~~~~~~~~~~~

Learn how to build a complete batch ETL pipeline with the Medallion architecture (Bronze → Silver → Gold).

Topics covered:

- Data ingestion from multiple sources
- Data cleaning and validation
- Business metric calculation
- Partitioning and optimization

:doc:`batch_etl`

Streaming Tutorial
~~~~~~~~~~~~~~~~~~

Build real-time streaming pipelines with Kafka and Kinesis.

Topics covered:

- Kafka integration
- Checkpointing and recovery
- Stream monitoring
- Error handling

:doc:`streaming`

MLOps Tutorial
~~~~~~~~~~~~~~

Implement ML workflows with experiment tracking and model management.

Topics covered:

- Experiment tracking
- Model registry
- Model deployment
- Pipeline automation

:doc:`mlops`

Airflow Integration
~~~~~~~~~~~~~~~~~~~

Orchestrate Tauro pipelines with Apache Airflow.

Topics covered:

- DAG creation
- Task dependencies
- Error handling
- Monitoring

:doc:`airflow_integration`

FastAPI Integration
~~~~~~~~~~~~~~~~~~~

Build REST APIs for pipeline execution.

Topics covered:

- API endpoints
- Async execution
- Status tracking
- Authentication

:doc:`fastapi_integration`

Prerequisites
-------------

For all tutorials, you should have:

- Tauro installed (``pip install tauro[all]``)
- Basic Python knowledge
- Familiarity with data processing concepts

Optional:

- Apache Spark knowledge (for optimization)
- Docker (for some tutorials)
- Cloud account (for deployment tutorials)

Getting Help
------------

If you get stuck:

1. Check the :doc:`../troubleshooting` guide
2. Review the :doc:`../api/index` documentation
3. Ask in `GitHub Discussions <https://github.com/faustino125/tauro/discussions>`_
4. Report issues on `GitHub Issues <https://github.com/faustino125/tauro/issues>`_
