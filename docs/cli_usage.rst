Using Tauro from the Command Line
==================================

The Tauro Command-Line Interface (CLI) is how you run and manage your pipelines. This guide covers the commands you'll use most frequently.

The Basic Command
-----------------

The most common Tauro command is:

.. code-block:: bash

   tauro --env ENVIRONMENT --pipeline PIPELINE_NAME

This runs a specific pipeline in a specific environment. For example:

.. code-block:: bash

   tauro --env dev --pipeline sales_etl

Breaking it down:
   - ``--env dev`` - Use the "dev" environment (could also be "staging" or "prod")
   - ``--pipeline sales_etl`` - Run the "sales_etl" pipeline

That's it! Tauro takes care of everything else.

Running Pipelines
-----------------

**Check What Pipelines You Have**

.. code-block:: bash

   tauro --list-pipelines

This shows all pipelines defined in your project.

**Run a Pipeline**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline

**Run with a Date Range**

If your pipeline processes data by date, specify which dates to process:

.. code-block:: bash

   tauro --env prod --pipeline daily_etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

This runs the pipeline for each day between these dates.

**Run Just One Step**

If a pipeline has multiple steps and you want to re-run just one, use:

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --node extract

This runs only the "extract" node, useful for debugging.

**Check Before Running**

To validate your configuration without actually running anything:

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --validate

This catches errors in your configuration early.

Debugging & Logging
-------------------

**See Detailed Logs**

Running a pipeline is quiet by default. To see what's happening:

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --log-level DEBUG

Log levels from least to most detailed:
   - ``ERROR`` - Only problems
   - ``WARNING`` - Problems and warnings
   - ``INFO`` - General progress (default)
   - ``DEBUG`` - Everything (useful for debugging)

**Save Logs to a File**

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --log-level DEBUG > pipeline.log 2>&1
   cat pipeline.log  # View the log

This is useful when running long pipelines or scheduling them with cron.

Project Management
-------------------

**Create a New Project from a Template**

To start a new project, Tauro provides templates:

.. code-block:: bash

   tauro --list-templates

Shows available templates. Then create your project:

.. code-block:: bash

   tauro --template medallion_basic --project-name my_analytics_project
   cd my_analytics_project

**Validate Your Configuration**

Before running anything in production, check your configuration:

.. code-block:: bash

   tauro --validate-config --env prod

This checks:
   - YAML/JSON syntax is correct
   - All required fields are present
   - File references point to real files
   - Environment variables are set

Real-World Examples
-------------------

**Example 1: Run Daily ETL**

.. code-block:: bash

   # Run today's data
   tauro --env prod --pipeline daily_sales_etl

**Example 2: Catch Up on Historical Data**

.. code-block:: bash

   # Backfill the past month
   tauro --env prod --pipeline sales_etl \
     --start-date 2024-01-01 \
     --end-date 2024-01-31

**Example 3: Debug a Failing Pipeline**

.. code-block:: bash

   # First, see what's wrong
   tauro --env dev --pipeline sales_etl --log-level DEBUG

   # Then, re-run just the failing step
   tauro --env dev --pipeline sales_etl --node transform

**Example 4: Schedule with Cron (Linux/Mac)**

.. code-block:: bash

   # Edit crontab
   crontab -e

   # Add this line to run at 9 AM every day
   0 9 * * * cd /home/user/my_project && tauro --env prod --pipeline daily_etl >> logs/cron.log 2>&1

Getting Help
~~~~~~~~~~~~

**See all available commands:**

.. code-block:: bash

   tauro --help

**Help for a specific command:**

.. code-block:: bash

   tauro --pipeline --help

**See version information:**

.. code-block:: bash

   tauro --version

Common Issues & Solutions
-------------------------

**"Pipeline not found"**

.. code-block:: bash

   # Check what pipelines exist
   tauro --list-pipelines

   # Your pipeline name in the command must exactly match the YAML file

**"Configuration is invalid"**

.. code-block:: bash

   # Validate to see what's wrong
   tauro --validate-config --env dev

   # Check for:
   # - YAML syntax errors
   # - Missing fields
   # - File references that don't exist

**"Pipeline is running very slowly"**

.. code-block:: bash

   # Get verbose logs to see where it's stuck
   tauro --env dev --pipeline my_pipeline --log-level DEBUG

**"Environment variables not found"**

Make sure your ``.env`` file exists and is loaded:

.. code-block:: bash

   # Create/edit .env
   echo "DATABASE_URL=postgresql://..." >> .env

   # Run pipeline (it will load .env automatically)
   tauro --env dev --pipeline my_pipeline

Tips & Tricks
~~~~~~~~~~~~~

1. **Always validate before running in production:**

   .. code-block:: bash

      tauro --env prod --pipeline important_pipeline --validate

2. **Use short node names for easier debugging:**

   Instead of ``clean_and_aggregate_customer_data``, use ``clean``

3. **Organize pipelines by type:**

   .. code-block:: text

      Pipelines:
      - daily_etl
      - weekly_summary
      - monthly_reconciliation
      - ml_training

4. **Use different environments consistently:**

   .. code-block:: bash

      # Development
      tauro --env dev --pipeline pipeline_name

      # Staging (for testing before production)
      tauro --env staging --pipeline pipeline_name

      # Production (the real thing)
      tauro --env prod --pipeline pipeline_name

Next Steps
----------

- :doc:`guides/configuration` - Learn how to configure pipelines
- :doc:`guides/batch_etl` - Build a realistic ETL example
- :doc:`guides/troubleshooting` - Solve common problems


Streaming Pipelines
-------------------

Tauro also supports long-running streaming pipelines. These commands are grouped under the `stream` subcommand.

**Starting a Streaming Pipeline**

.. code-block:: bash

   # Usage: tauro stream run --pipeline [PIPELINE_NAME]

   # Example:
   tauro stream run --pipeline live_dashboard_updates

**Listing Active Streams**

To see all currently running streaming pipelines managed by Tauro:

.. code-block:: bash

   tauro stream list

**Checking the Status of a Stream**

Each streaming run is assigned an `execution_id`. You can use this ID to check its status.

.. code-block:: bash

   tauro stream status --execution-id [EXECUTION_ID]

**Stopping a Stream**

To gracefully stop a running stream:

.. code-block:: bash

   tauro stream stop --execution-id [EXECUTION_ID]

Advanced Execution & Debugging
------------------------------

**Controlling Verbosity**

Use `--verbose` or `-v` for detailed logs, and `--quiet` or `-q` to suppress non-essential output.

.. code-block:: bash

   tauro --env dev --pipeline load --verbose

**Redirecting Logs to a File**

For production runs, it's best practice to save logs to a file.

.. code-block:: bash

   tauro --env prod --pipeline aggregate --log-file ./logs/aggregate_$(date +%Y%m%d).log

**Overriding Configuration**

You can override any configuration parameter from the command line using the `--param` option.

.. code-block:: bash

   # Override the output format for a specific run
   tauro --env dev --pipeline load --param "output.file_format=parquet"

Practical Scenarios
-------------------

**Scenario 1: Daily ETL Run**

A common use case is a daily job that processes the previous day's data.

.. code-block:: bash

   # You can automate getting yesterday's date in your script
   YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

   tauro --env prod --pipeline daily_etl \
     --start-date $YESTERDAY \
     --end-date $YESTERDAY

**Scenario 2: Historical Data Backfill**

When you deploy a new pipeline, you often need to process historical data.

.. code-block:: bash

   # Process all of 2023 in parallel for faster execution
   tauro --env prod --pipeline transform \
     --start-date 2023-01-01 \
     --end-date 2023-12-31 \
     --parallel

**Scenario 3: Pre-deployment Validation**

Before deploying to production, you should always validate your pipeline.

.. code-block:: bash

   # 1. Validate the configuration for the 'prod' environment
   tauro --validate-config --env prod

   # 2. Do a dry-run of the pipeline to check dependencies and node ordering
   tauro --env prod --pipeline critical_reporting --validate-only

   # 3. If both pass, you are ready to deploy and run.
   tauro --env prod --pipeline critical_reporting

Shell Integration
-----------------

**Tab Completion**

To make using the CLI even easier, you can enable tab completion for commands and arguments.

- **For Bash**, add this to your `~/.bashrc`:
  .. code-block:: bash

     eval "$(_TAURO_COMPLETE=bash_source tauro)"

- **For Zsh**, add this to your `~/.zshrc`:
  .. code-block:: bash

     eval "$(_TAURO_COMPLETE=zsh_source tauro)"

**Useful Aliases**

You can also create shell aliases to shorten common commands. Add these to your `~/.bashrc` or `~/.zshrc`.

.. code-block:: bash

   # General aliases
   alias t='tauro'
   alias tl='tauro --list-pipelines'

   # Environment-specific aliases
   alias td='tauro --env dev'
   alias tp='tauro --env prod'

   # Now you can run pipelines like this:
   # td --pipeline load
   # tp --pipeline aggregate
