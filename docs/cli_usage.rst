CLI Usage
=========

The Tauro Command-Line Interface (CLI) is your primary tool for creating, managing, and executing data pipelines. This guide covers the most common and useful commands.

For a complete list of all commands and options, you can always use the `--help` flag:

.. code-block:: bash

   tauro --help
   tauro --pipeline --help  # Help for a specific command

Core Commands
-------------

These are the commands you'll use most frequently.

**Executing a Pipeline**

This is the main command for running your batch pipelines. You need to specify the environment and the pipeline name.

.. code-block:: bash

   # Usage: tauro --env [ENVIRONMENT] --pipeline [PIPELINE_NAME]
   
   # Example: Run the 'load' pipeline in the 'dev' environment
   tauro --env dev --pipeline load

**Listing Available Pipelines**

To see all the pipelines defined in your project:

.. code-block:: bash

   tauro --list-pipelines

**Creating a New Project**

To start a new project, use a template.

.. code-block:: bash

   # Usage: tauro --template [TEMPLATE_NAME] --project-name [YOUR_PROJECT_NAME]

   # Example: Create a new project using the basic Medallion architecture
   tauro --template medallion_basic --project-name new_etl_project

Pipeline Execution Options
--------------------------

You can customize how your pipelines are executed with these options.

**Running with a Date Range**

For incremental or historical data processing, you can specify a date range.

.. code-block:: bash

   tauro --env dev --pipeline transform --start-date 2024-01-01 --end-date 2024-01-31

**Running a Single Node**

To debug or re-run a specific part of a pipeline, you can execute a single node.

.. code-block:: bash

   # First, find the node name in your `nodes.yaml` file
   # Then, run it with the --node flag
   tauro --env dev --pipeline transform --node clean_customer_data

**Performing a Dry Run (Validation)**

To validate your pipeline's configuration and dependency graph without actually running the code, use `--validate-only`. This is useful for catching errors before a long-running job.

.. code-block:: bash

   tauro --env dev --pipeline aggregate --validate-only

Project and Configuration
-------------------------

**Listing Available Templates**

To see all available project templates:

.. code-block:: bash

   tauro --list-templates

**Validating Configuration**

To parse and validate all your YAML configuration files for a specific environment:

.. code-block:: bash

   tauro --validate-config --env dev

This checks for correct syntax, references between files, and schema validation.


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
