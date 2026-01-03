Using Tauro from the Command Line
Shell Integration
-----------------

**Tab Completion**

Enable shell completion to speed up CLI discovery:

- **Bash**: add the following to your ``~/.bashrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=bash_source tauro)"

- **Zsh**: add this to ``~/.zshrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=zsh_source tauro)"

**Useful Aliases**

Shorten common commands:

.. code-block:: bash

   alias t='tauro'
   alias tl='tauro config list-pipelines --env dev'
   alias trd='tauro run --env dev --pipeline'
   alias trp='tauro run --env prod --pipeline'

To gracefully stop a running stream, keep the execution ID handy and run:

.. code-block:: bash

   tauro stream stop --config config/streaming/settings.py --execution-id [EXECUTION_ID]

- :doc:`guides/configuration` - Learn how to configure pipelines safely.
- :doc:`guides/batch_etl` - Build a realistic batch pipeline from scratch.
- :doc:`tutorials/streaming` - Understand Tauro's streaming workflow end-to-end.
- :doc:`guides/troubleshooting` - Learn how to diagnose tricky failures.

Streaming Pipelines
-------------------

Streaming commands live under the ``stream`` namespace. All streaming commands require a configuration
module or DSL file.

**Starting a Streaming Pipeline**

.. code-block:: bash

   tauro stream run \
     --config config/streaming/settings.py \
     --pipeline live_dashboard_updates \
     --mode async

Supply ``--config`` (a DSL or Python module) along with the pipeline name. ``--mode`` controls whether
the command blocks (``sync``) or returns immediately (``async``). Use ``--model-version`` and
``--hyperparams`` to pass optional metadata to the nodes.

**Listing Active Streams**

.. code-block:: bash

   tauro stream status --config config/streaming/settings.py --format table

Omit ``--execution-id`` to list every active stream in the provided configuration.

**Checking the Status of a Stream**

.. code-block:: bash

   tauro stream status --config config/streaming/settings.py --execution-id abc123

**Stopping a Stream**

.. code-block:: bash

   tauro stream stop --config config/streaming/settings.py --execution-id abc123 --timeout 60

Advanced Execution & Debugging
------------------------------

**Controlling Verbosity**

Use ``--verbose`` for trace-level output and ``--quiet`` to suppress everything below ``ERROR``:

.. code-block:: bash

   tauro run --env dev --pipeline load --verbose

**Redirecting Logs to a File**

For auditing, save the log stream to disk:

.. code-block:: bash

   tauro run --env prod --pipeline aggregate --log-file ./logs/aggregate_$(date +%Y%m%d).log

**Overriding Configuration**

Tauro does not expose a general ``--param`` flag. Adjust your ``config/`` files or rely on environment
variables to tweak behavior for a given run. Use ``--base-path`` and ``--layer-name`` if you need to
point Tauro at a specific configuration tree.

Practical Scenarios
-------------------

**Scenario 1: Daily ETL Run**

.. code-block:: bash

   YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
   tauro run --env prod --pipeline daily_etl \
     --start-date $YESTERDAY \
     --end-date $YESTERDAY

**Scenario 2: Historical Data Backfill**

.. code-block:: bash

   tauro run --env prod --pipeline transform \
     --start-date 2023-01-01 \
     --end-date 2023-12-31 \
     --dry-run

   tauro run --env prod --pipeline transform \
     --start-date 2023-01-01 \
     --end-date 2023-12-31

**Scenario 3: Pre-deployment Validation**

.. code-block:: bash

   tauro config list-pipelines --env prod
   tauro run --env prod --pipeline critical_reporting --validate-only
   tauro run --env prod --pipeline critical_reporting --dry-run
   tauro run --env prod --pipeline critical_reporting

Shell Integration
-----------------

**Tab Completion**

- **Bash**: add the following to your ``~/.bashrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=bash_source tauro)"
- **Zsh**: add this to ``~/.zshrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=zsh_source tauro)"

**Useful Aliases**

.. code-block:: bash

   alias t='tauro'
   alias tl='tauro config list-pipelines --env dev'
   alias trd='tauro run --env dev --pipeline'
   alias trp='tauro run --env prod --pipeline'

To stop a running stream gracefully, keep the execution ID handy and run:

.. code-block:: bash

   tauro stream stop --config config/streaming/settings.py --execution-id [EXECUTION_ID]Using Tauro from the Command Line
==================================

The Tauro Command-Line Interface (CLI) is how you run and manage your pipelines. This guide covers the commands you'll use most frequently.

The Basic Command
-----------------

The CLI is now driven by the ``run`` subcommand. The simplest invocation looks like this:

.. code-block:: bash

   tauro run --env ENVIRONMENT --pipeline PIPELINE_NAME

This executes the chosen pipeline against the environment configuration you previously defined
in ``settings_*.json`` and the associated ``config/`` files. For example:

.. code-block:: bash

   tauro run --env dev --pipeline sales_etl
Real-World Examples
-------------------

**Example 1: Run Daily ETL**

.. code-block:: bash

   # Run today's data
   tauro run --env prod --pipeline daily_sales_etl

**Example 2: Catch Up on Historical Data**

.. code-block:: bash

   # Backfill the past month
   tauro run --env prod --pipeline sales_etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

**Example 3: Debug a Failing Pipeline**

.. code-block:: bash

   # First, see what's wrong
   tauro run --env dev --pipeline sales_etl --log-level DEBUG

   # Then, re-run just the failing step
   tauro run --env dev --pipeline sales_etl --node transform

**Example 4: Schedule with Cron**

.. code-block:: bash

   # Add this line to run at 9 AM every day
   0 9 * * * cd /home/user/my_project && tauro run --env prod --pipeline daily_etl >> logs/cron.log 2>&1

Common Issues & Solutions
-------------------------

| Problem | Solution |
|---------|----------|
| ``Pipeline not found`` | Run ``tauro config list-pipelines --env dev`` and make sure the name matches exactly. |
| ``Configuration is invalid`` | Run ``tauro run --env dev --pipeline my_pipeline --validate-only`` to catch missing keys or syntax problems. |
| ``Node fails`` | Run ``tauro run --env dev --pipeline my_pipeline --node failing_node --log-level DEBUG`` to capture stack traces. |
| ``Streaming job missing`` | ``tauro stream status --config config/streaming/settings.py`` lists active executions. |
| ``Logs disappear`` | ``tauro run ... --log-level DEBUG --log-file logs/run.log`` keeps a permanent record. |

Tips & Tricks
~~~~~~~~~~~~~

1. **Always validate before running critical pipelines:**

   .. code-block:: bash

      tauro run --env prod --pipeline complex_pipeline --validate-only

2. **Use short node names for easier debugging:**

   Rename ``clean_and_aggregate_customer_data`` to ``clean`` so you can target it quickly.

3. **Organize pipelines by role:**

   .. code-block:: text

      Pipelines:
      - daily_etl
      - weekly_summary
      - monthly_reconciliation
      - ml_training

4. **Use environment-specific aliases:**

   .. code-block:: bash

      alias trd='tauro run --env dev --pipeline'
      alias trp='tauro run --env prod --pipeline'

Next Steps
----------

- :doc:`guides/configuration` - Learn how to configure pipelines.
- :doc:`guides/batch_etl` - Build a realistic batch pipeline from scratch.
- :doc:`docs/tutorials/streaming` - Understand Tauro's streaming workflow end-to-end.
- :doc:`guides/troubleshooting` - Learn how to diagnose tricky failures.

Streaming Pipelines
-------------------

Tauro also supports long-running streaming pipelines. These commands are grouped under the ``stream`` subcommand.

**Starting a Streaming Pipeline**

.. code-block:: bash

   tauro stream run \
     --config config/streaming/settings.py \
     --pipeline live_dashboard_updates \
     --mode async

Supply ``--config`` (a DSL or Python module) along with the pipeline name. ``--mode`` controls whether the command blocks
(``sync``) or returns immediately (``async``). ``--model-version`` and ``--hyperparams`` pass extra parameters to the nodes.

**Listing Active Streams**

Omit ``--execution-id`` from the status command to list every active streaming pipeline:

.. code-block:: bash

   tauro stream status --config config/streaming/settings.py --format table

**Checking the Status of a Stream**

Use the execution ID to get metrics for a single run:

.. code-block:: bash

   tauro stream status --config config/streaming/settings.py --execution-id abc123

**Stopping a Stream**

Shut down a stream gracefully by providing the execution ID, the config, and an optional timeout:

.. code-block:: bash

   tauro stream stop --config config/streaming/settings.py --execution-id abc123 --timeout 60

Advanced Execution & Debugging
------------------------------

**Controlling Verbosity**

Use ``--verbose`` for detailed tracebacks and ``--quiet`` to suppress everything below ``ERROR``:

.. code-block:: bash

   tauro run --env dev --pipeline load --verbose

**Redirecting Logs to a File**

Save the full log stream for auditing:

.. code-block:: bash

   tauro run --env prod --pipeline aggregate --log-file ./logs/aggregate_$(date +%Y%m%d).log

**Overriding Configuration**

Tauro does not expose a general ``--param`` flag. Edit your ``config/`` files or use environment variables to tweak behavior.

Practical Scenarios
-------------------

**Scenario 1: Daily ETL Run**

Run yesterday's data automatically:

.. code-block:: bash

   YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
   tauro run --env prod --pipeline daily_etl \
     --start-date $YESTERDAY \
     --end-date $YESTERDAY

**Scenario 2: Historical Data Backfill**

Kick off a one-time catch-up job for a full year of data:

.. code-block:: bash

   tauro run --env prod --pipeline transform \
     --start-date 2023-01-01 \
     --end-date 2023-12-31

**Scenario 3: Pre-deployment Validation**

Validate, dry-run, then go-live:

.. code-block:: bash

   tauro run --env prod --pipeline critical_reporting --validate-only
   tauro run --env prod --pipeline critical_reporting --dry-run
   tauro run --env prod --pipeline critical_reporting

Shell Integration
-----------------

**Tab Completion**

Enable shell completion to speed up CLI discovery:

- **Bash**: add the following to your ``~/.bashrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=bash_source tauro)"

- **Zsh**: add this to ``~/.zshrc``

  .. code-block:: bash

     eval "$( _TAURO_COMPLETE=zsh_source tauro)"

**Useful Aliases**

Shorten common commands:

.. code-block:: bash

   alias t='tauro'
   alias tl='tauro config list-pipelines --env dev'
   alias trd='tauro run --env dev --pipeline'
   alias trp='tauro run --env prod --pipeline'

