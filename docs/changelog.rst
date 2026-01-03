Changelog
=========

All notable changes to Tauro will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
------------

Changed
~~~~~~~

- **MLOps**: Both ExperimentTracker and ModelRegistry now use lazy initialization for 
  directory structures. Tracking directories (experiments, runs, artifacts, metrics) and 
  registry directories (models, versions, artifacts, metadata) are only created when 
  actually needed (on first experiment/run creation or model registration), preventing 
  empty directory creation during initialization.
- **MLOps**: Enhanced logging for MLOps path resolution to improve debugging and visibility 
  of where MLOps directories are being created.
- **MLOps**: Disabled automatic initialization from environment variables when no execution 
  context is available. This prevents creating MLOps directories in unexpected locations 
  (e.g., current working directory). MLOps will only initialize when a proper execution 
  context is provided.
- **MLOps**: Improved pipeline-specific path resolution. When MLOpsContext is initialized 
  with a pipeline_name, it correctly resolves paths to pipeline-specific MLOps directories. 
  The executor now passes pipeline_name to MLOps initialization when available.
- **MLOps**: Added fail-safe mechanism to prevent MLOps directory creation without proper 
  context. If MLOps path resolution fails (missing output_path or environment), MLOps 
  initialization is gracefully skipped rather than creating directories in the working 
  directory. This ensures MLOps directories are only created in intended locations.

Removed
~~~~~~~

- Removed auto-created `experiment_tracking` directory that was being created in use case 
  root directories. MLOps directories should only be created in `data/{environment}/mlops/` 
  when actually needed.

Fixed
~~~~~

- Fixed MLOps directory creation happening in unexpected locations (use case root, layer 
  root, working directory) instead of in the correct data directory structure. This was 
  caused by fallback initialization paths that would create directories without proper 
  context. Now MLOps gracefully skips initialization if proper context is unavailable.

[0.1.3] - 2024-12-12
--------------------

Added
~~~~~

- Complete Read the Docs documentation
- Sphinx-based documentation system
- Configuration caching with TTL (5 minutes)
- CLI argument validation functions
- Comprehensive library usage examples
- Migration guide from CLI to Library
- API reference documentation
- Best practices guide

Changed
~~~~~~~

- Improved ConfigCache with automatic TTL management
- Enhanced error messages for better debugging
- Updated README with dual-mode usage (CLI + Library)
- Reorganized documentation structure

Fixed
~~~~~

- Configuration file discovery in nested directories
- Validation error handling in streaming pipelines
- Memory leak in long-running streaming jobs

Security
~~~~~~~~

- Added path validation for all file operations
- Implemented input sanitization
- Enhanced security in module loading

[0.1.2] - 2024-11-20
--------------------

Added
~~~~~

- MLOps integration with MLflow
- Model registry support
- Experiment tracking capabilities
- Streaming pipeline management
- Support for Kafka and Kinesis
- Checkpoint management for streaming

Changed
~~~~~~~

- Improved Spark integration
- Enhanced configuration management
- Better error handling in executors

Fixed
~~~~~

- Date range validation in CLI
- Partition handling in Delta format
- Memory management in large datasets

[0.1.1] - 2024-10-15
--------------------

Added
~~~~~

- Template generation system
- Medallion architecture templates
- Streaming pipeline templates
- Project scaffolding

Changed
~~~~~~~

- Improved CLI interface
- Better logging output
- Enhanced configuration discovery

Fixed
~~~~~

- Pipeline execution order
- Node dependency resolution
- Configuration inheritance

[0.1.0] - 2024-09-01
--------------------

Added
~~~~~

- Initial release
- Core CLI interface
- Pipeline execution engine
- Configuration management system
- Input/Output operations
- Support for multiple data formats:
  - CSV
  - Parquet
  - Delta
  - JDBC
- Environment-based configuration
- Batch processing capabilities
- Multi-format configuration (YAML, JSON, DSL)
- Spark integration
- Logging and monitoring

Features
~~~~~~~~

- **CLI**: Command-line interface for pipeline execution
- **Library**: Programmatic API for integration
- **Config**: Flexible configuration system
- **Exec**: Pipeline and node execution
- **I/O**: Multi-format data reading and writing
- **Spark**: Apache Spark integration

Upcoming
--------

[0.2.0] - Planned for Q1 2025
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Planned Features
^^^^^^^^^^^^^^^^

- [ ] Apache Iceberg support
- [ ] Apache Hudi support
- [ ] dbt integration
- [ ] Improved React-based UI
- [ ] AWS Glue integration
- [ ] Enhanced data catalog
- [ ] Lineage tracking
- [ ] Better observability

[0.3.0] - Planned for Q2 2025
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Planned Features
^^^^^^^^^^^^^^^^

- [ ] Azure Databricks support
- [ ] Google Cloud integration
- [ ] Great Expectations integration
- [ ] Auto-scaling capabilities
- [ ] Advanced ML pipeline features
- [ ] Real-time monitoring dashboard
- [ ] Cost optimization tools
- [ ] Multi-cloud support

Migration Guides
----------------

Upgrading from 0.1.2 to 0.1.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No breaking changes. Just upgrade:

.. code-block:: bash

   pip install --upgrade tauro

New features are backward compatible.

Upgrading from 0.1.1 to 0.1.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MLOps configuration is now separate:

.. code-block:: yaml

   # Before (0.1.1)
   tracking:
     enabled: true

   # After (0.1.2)
   mlops:
     tracking:
       enabled: true
       backend: "mlflow"

Upgrading from 0.1.0 to 0.1.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Template syntax changed:

.. code-block:: bash

   # Before (0.1.0)
   tauro --create-project my_project

  # After (0.1.1)
  tauro template --template medallion_basic --project-name my_project

Deprecation Notices
-------------------

Deprecated in 0.1.3
~~~~~~~~~~~~~~~~~~~

- ``TauroCLI`` class name (use ``UnifiedCLI`` instead)
  
  .. code-block:: python

     # Deprecated
     from tauro import TauroCLI
     
     # Use instead
     from tauro import UnifiedCLI

Removed in 0.1.3
~~~~~~~~~~~~~~~~

None

To Be Deprecated in 0.2.0
~~~~~~~~~~~~~~~~~~~~~~~~~

- Old configuration format (will be removed in 0.3.0)
- Legacy streaming API (will be removed in 0.3.0)

Known Issues
------------

Current Known Issues
~~~~~~~~~~~~~~~~~~~~

- Streaming pipelines may experience occasional checkpoint delays on Windows
- Large Parquet files (>5GB) may cause memory issues with default settings
- Some JDBC drivers require manual installation

Workarounds
~~~~~~~~~~~

For checkpoint delays:

.. code-block:: python

   # Increase checkpoint interval
   manager.run_streaming_pipeline(
       "pipeline",
       checkpoint_location="/path",
       checkpoint_interval="60 seconds"  # Default is 10 seconds
   )

For large Parquet files:

.. code-block:: yaml

   spark:
     config:
       spark.driver.memory: "8g"
       spark.executor.memory: "16g"

Contributing
------------

See `CONTRIBUTING.md <https://github.com/faustino125/tauro/blob/main/CONTRIBUTING.md>`_ for details on:

- Reporting bugs
- Suggesting enhancements
- Code contribution guidelines
- Development setup

Support
-------

- **GitHub Issues**: https://github.com/faustino125/tauro/issues
- **Discussions**: https://github.com/faustino125/tauro/discussions
- **Email**: faustinolopezramos@gmail.com
- **Documentation**: https://tauro.readthedocs.io

License
-------

MIT License - see `LICENSE <https://github.com/faustino125/tauro/blob/main/LICENSE>`_ for details.

Acknowledgments
---------------

Thanks to all contributors who have helped make Tauro better:

- Community contributors
- Bug reporters
- Documentation writers
- Early adopters and testers

Special thanks to the open-source projects that Tauro builds upon:

- Apache Spark
- MLflow
- FastAPI
- Pandas
- And many others
