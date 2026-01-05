Command Line Usage
==================

This guide covers the most common Tauro CLI commands for running pipelines.

Basic Commands
--------------

**Run a pipeline:**

.. code-block:: bash

   tauro run --env dev --pipeline my_pipeline

**List all pipelines:**

.. code-block:: bash

   tauro config list-pipelines --env dev

**Check version:**

.. code-block:: bash

   tauro --version

**Show help:**

.. code-block:: bash

   tauro --help
   tauro run --help

Running Pipelines
-----------------

**Run with default environment (dev):**

.. code-block:: bash

   tauro run --pipeline extract_data

**Run against production:**

.. code-block:: bash

   tauro run --env prod --pipeline transform_data

**Validate configuration without running:**

.. code-block:: bash

   tauro run --env dev --pipeline my_pipeline --validate-only

**Run with detailed logging:**

.. code-block:: bash

   tauro run --env dev --pipeline my_pipeline --log-level DEBUG

Configuration
-------------

**List all configurations:**

.. code-block:: bash

   tauro config list-nodes --env dev
   tauro config list-pipelines --env dev

**Show configuration details:**

.. code-block:: bash

   tauro config show --env dev

Batch Pipelines
---------------

**Run a batch pipeline:**

.. code-block:: bash

   tauro run --env dev --pipeline daily_etl

**Run with date range:**

.. code-block:: bash

   tauro run --env prod --pipeline backfill --start-date 2024-01-01 --end-date 2024-01-31

Streaming Pipelines
-------------------

**Start a streaming pipeline:**

.. code-block:: bash

   tauro stream run --config config/streaming.yaml --pipeline live_stream

**List active streams:**

.. code-block:: bash

   tauro stream status --config config/streaming.yaml

**Stop a stream:**

.. code-block:: bash

   tauro stream stop --config config/streaming.yaml --execution-id [ID]

Common Options
--------------

+-----------------------+----------------------------------------+
| Option                | Description                            |
+=======================+========================================+
| ``--env ENVIRONMENT`` | Environment to use (dev, prod, etc)   |
+-----------------------+----------------------------------------+
| ``--pipeline NAME``   | Pipeline to run                        |
+-----------------------+----------------------------------------+
| ``--log-level LEVEL`` | Log level (DEBUG, INFO, WARNING, ERROR)|
+-----------------------+----------------------------------------+
| ``--validate-only``   | Check configuration without executing |
+-----------------------+----------------------------------------+
| ``--dry-run``         | Simulate execution without side effects|
+-----------------------+----------------------------------------+
| ``--base-path PATH``  | Path to project root                   |
+-----------------------+----------------------------------------+

Tips and Tricks
---------------

**Enable shell completion (Bash):**

.. code-block:: bash

   eval "$(_TAURO_COMPLETE=bash_source tauro)"

Add to your ``~/.bashrc`` to make it permanent.

**Create useful aliases:**

.. code-block:: bash

   alias tdev='tauro run --env dev --pipeline'
   alias tprod='tauro run --env prod --pipeline'
   alias tlist='tauro config list-pipelines --env dev'

**Save logs to a file:**

.. code-block:: bash

   tauro run --env prod --pipeline daily_etl --log-file ./logs/daily_etl.log

**Show all available commands:**

.. code-block:: bash

   tauro --help

Next Steps
----------

- See :doc:`configuration` for detailed configuration options
- Learn about :doc:`best_practices` for production use
- Follow a tutorial: :doc:`tutorials/batch_etl` or :doc:`tutorials/streaming`

