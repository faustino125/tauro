CLI Usage
=========

This guide covers all command-line interface (CLI) usage for Tauro.

Overview
--------

Tauro provides a powerful CLI for:

- Creating new projects from templates
- Executing batch pipelines
- Managing streaming pipelines
- Validating configurations
- Listing available pipelines and templates

Basic Syntax
------------

.. code-block:: bash

   tauro [OPTIONS] [COMMAND]

Global Options
--------------

.. code-block:: text

   --version              Show version and exit
   --help                 Show help message and exit
   --verbose, -v          Enable verbose logging
   --quiet, -q            Suppress non-essential output
   --config PATH          Path to configuration file

Pipeline Execution
------------------

Execute a Pipeline
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline sales_etl

With Date Range
~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline sales_etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

Validate Only (Dry Run)
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline sales_etl --validate-only

Execute Specific Node
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline sales_etl --node load_data

With Verbose Output
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline sales_etl --verbose

Pipeline Management
-------------------

List All Pipelines
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --list-pipelines

Show Pipeline Information
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --pipeline-info sales_etl

Validate Pipeline Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --validate-pipeline sales_etl

Project Generation
------------------

List Available Templates
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --list-templates

Create Project from Template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --template medallion_basic --project-name my_project

With Specific Format
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --template medallion_basic \
     --project-name my_project \
     --format json

Available Templates
~~~~~~~~~~~~~~~~~~~

- ``medallion_basic``: Basic Bronze-Silver-Gold architecture
- ``medallion_advanced``: Advanced medallion with quality checks
- ``streaming_kafka``: Kafka streaming pipeline
- ``ml_training``: ML model training pipeline
- ``hybrid_batch_stream``: Hybrid batch and streaming

Streaming Commands
------------------

Start Streaming Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro stream run \
     --config ./config \
     --pipeline kafka_events

Check Streaming Status
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro stream status --execution-id abc123

Stop Streaming Pipeline
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro stream stop --execution-id abc123

List Running Streams
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro stream list

Configuration Commands
----------------------

Discover Configuration Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro config discover

Validate Configuration
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro config validate --env dev

Show Configuration
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro config show --env dev

Environment Management
----------------------

List Environments
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro env list

Show Environment Details
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro env show dev

Switch Environment
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro env set production

Advanced Options
----------------

Execution Options
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline etl \
     --dry-run              # Show what would execute
     --parallel             # Enable parallel execution
     --retry-count 3        # Number of retries on failure
     --timeout 3600         # Timeout in seconds

Output Options
~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline etl \
     --output-format json   # Output format (json, yaml, text)
     --log-file ./logs.txt  # Log to file
     --no-color             # Disable colored output

Performance Options
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   tauro --env dev --pipeline etl \
     --workers 4            # Number of worker threads
     --memory-limit 4G      # Memory limit
     --cache-enabled        # Enable caching

Examples
--------

Example 1: Daily ETL Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Process yesterday's data
   tauro --env production --pipeline daily_sales_etl

   # Process specific date
   tauro --env production --pipeline daily_sales_etl \
     --start-date 2024-03-15 \
     --end-date 2024-03-15

Example 2: Historical Backfill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Backfill entire year
   tauro --env production --pipeline sales_etl \
     --start-date 2024-01-01 \
     --end-date 2024-12-31 \
     --parallel \
     --workers 8

Example 3: Validate Before Production
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Step 1: Validate configuration
   tauro --env production --pipeline critical_etl --validate-only

   # Step 2: Test with dry run
   tauro --env production --pipeline critical_etl --dry-run

   # Step 3: Execute
   tauro --env production --pipeline critical_etl

Example 4: Streaming Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Start streaming
   EXEC_ID=$(tauro stream run --pipeline kafka_events --output-format json | jq -r '.execution_id')

   # Monitor
   watch -n 5 "tauro stream status --execution-id $EXEC_ID"

   # Stop when done
   tauro stream stop --execution-id $EXEC_ID

Example 5: Debugging Failed Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Run with maximum verbosity
   tauro --env dev --pipeline failing_pipeline \
     --verbose \
     --log-file ./debug.log \
     --dry-run

   # Execute specific failing node
   tauro --env dev --pipeline failing_pipeline \
     --node problematic_node \
     --verbose

Exit Codes
----------

Tauro uses standard exit codes:

- ``0``: Success
- ``1``: General error
- ``2``: Configuration error
- ``3``: Pipeline execution error
- ``4``: Validation error
- ``130``: User interrupted (Ctrl+C)

Shell Integration
-----------------

Bash Completion
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Add to ~/.bashrc
   eval "$(_TAURO_COMPLETE=bash_source tauro)"

Zsh Completion
~~~~~~~~~~~~~~

.. code-block:: bash

   # Add to ~/.zshrc
   eval "$(_TAURO_COMPLETE=zsh_source tauro)"

Aliases
~~~~~~~

.. code-block:: bash

   # Add to ~/.bashrc or ~/.zshrc
   alias tp='tauro --env production'
   alias td='tauro --env dev'
   alias tl='tauro --list-pipelines'

Tips and Best Practices
-----------------------

1. **Always validate first**

   .. code-block:: bash

      tauro --env prod --pipeline critical --validate-only

2. **Use verbose mode for debugging**

   .. code-block:: bash

      tauro --env dev --pipeline failing --verbose

3. **Save logs for production runs**

   .. code-block:: bash

      tauro --env prod --pipeline etl --log-file ./logs/$(date +%Y%m%d).log

4. **Use environment variables**

   .. code-block:: bash

      export TAURO_ENV=production
      export TAURO_CONFIG=./config
      tauro --pipeline etl

5. **Combine with other tools**

   .. code-block:: bash

      # With jq for JSON processing
      tauro --output-format json --pipeline etl | jq '.metrics'

      # With systemd for service
      systemctl start tauro-pipeline

Troubleshooting
---------------

Command Not Found
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Use Python module
   python -m tauro --help

   # Or add to PATH
   export PATH="$HOME/.local/bin:$PATH"

Configuration Not Found
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Use explicit config path
   tauro --config ./config/settings.yaml --env dev --pipeline etl

Permission Denied
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Check file permissions
   chmod +x $(which tauro)

   # Or use sudo (not recommended)
   sudo tauro --env prod --pipeline etl

Next Steps
----------

- Learn :doc:`library_usage` for programmatic usage
- Configure pipelines in :doc:`configuration`
- Follow :doc:`tutorials/batch_etl` for hands-on examples
- Check :doc:`advanced/troubleshooting` for common issues
