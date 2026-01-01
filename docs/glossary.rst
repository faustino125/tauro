Glossary
========

Essential Tauro terminology and concepts.

Core Concepts
-------------

**Pipeline**
   A sequence of steps (nodes) that process data from start to finish. Pipelines are defined in ``config/pipelines.yaml``.
   
   Example: A "daily_sales" pipeline with steps: extract → transform → load

**Node (or Step)**
   A single unit of work in a pipeline. Each node runs a Python function and passes its output to the next node.
   
   Example: The "extract" node reads data from CSV

**Input**
   A data source that a pipeline reads from. Defined in ``config/inputs.yaml``. Can be CSV, Parquet, Delta Lake, databases, etc.

**Output**
   A destination where a pipeline writes results. Defined in ``config/outputs.yaml``. Can be files, databases, or cloud storage.

**Environment**
   A named execution context (dev, staging, prod). Each environment can have different configurations, data sources, and compute resources.

Configuration
-------------

**config/global.yaml**
   Global settings shared across all pipelines: paths, logging level, compute resources, timeouts.

**config/pipelines.yaml**
   Defines all available pipelines and which nodes they contain.

**config/nodes.yaml**
   Defines individual nodes: what Python function each node runs, timeouts, retries.

**config/inputs.yaml**
   Defines data sources: where to read data from (paths, databases, credentials).

**config/outputs.yaml**
   Defines output targets: where to write results.

Execution
---------

**Execution**
   The process of running a pipeline from start to finish. Each execution produces logs, metrics, and results.

**Execution Time**
   How long a pipeline took to run, measured in seconds.

**Node Execution**
   The number of individual nodes that ran during a pipeline execution.

**Success**
   A pipeline execution completed without errors. All nodes ran successfully and outputs were written.

**Failure**
   A pipeline execution stopped due to an error. The error is logged and no further nodes run.

Data Processing
---------------

**Extract**
   The first step of a typical pipeline: reading raw data from a source.

**Transform**
   Middle steps that clean, enrich, or reshape data. May include filtering, aggregation, joining, etc.

**Load**
   The final step: writing processed data to a destination.

**ETL**
   Extract-Transform-Load: a common pipeline pattern for moving and processing data.

**Batch Processing**
   Processing all data at once, typically scheduled for specific times (daily, hourly, etc.).

**Streaming**
   Processing data continuously as it arrives, with lower latency but different architecture.

Advanced Concepts
-----------------

**Feature Store**
   Pre-computed data features (aggregations, transformations) stored for use in ML models or analytics.

**Materialized**
   Features or data physically stored on disk/cloud for fast access.

**Virtualized**
   Features computed on-demand from source data when needed.

**Medallion Architecture**
   A layered data architecture:
   - **Bronze**: Raw data ingestion layer
   - **Silver**: Cleaned, validated data layer
   - **Gold**: Business-ready aggregations and metrics

**Unity Catalog**
   Databricks feature for centralized data governance and discovery. Recommended for enterprise Tauro deployments.

**Spark**
   Distributed computing framework for processing large datasets. Tauro supports Spark for scaling to big data.

Project Structure
-----------------

**Project**
   A Tauro project directory containing configuration, code, and data. Typical structure:
   
   .. code-block:: text
   
      my_project/
      ├── config/           # YAML configurations
      ├── src/nodes/        # Python functions
      ├── data/             # Test data
      ├── tests/            # Unit tests
      └── .env              # Secrets

**Template**
   A pre-built project structure that Tauro provides. Example: ``medallion_basic`` creates a Medallion architecture project.

Help & Support
--------------

**CLI**
   Command-Line Interface. Run Tauro from terminal: ``tauro --env dev --pipeline my_pipeline``

**Library**
   Python library. Import Tauro in code: ``import tauro``

**Configuration Validation**
   Checking that YAML configs are correct before running a pipeline.

**Logging**
   Records of what happened during pipeline execution. Useful for debugging failures.

**Error Handling**
   How Tauro responds to problems: stops pipeline, logs error, allows retry.

See Also
--------

- :doc:`getting_started` - Start here if new to Tauro
- :doc:`quick_reference` - Common commands and patterns
- :doc:`best_practices` - Recommended approaches
