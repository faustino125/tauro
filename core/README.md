# Tauro

Tauro helps you run data pipelines without needing to be a developer. Think of it as a “remote control” to:
- Run batch jobs (for files or tables that update on a schedule)
- Start and monitor streaming jobs (for real‑time data)
- Use a simple folder of configuration files to keep things organized
- Generate a ready‑to‑use project template (Medallion: Bronze → Silver → Gold)

This guide explains how to use Tauro from your terminal in clear, practical steps.

---

## What can I do with Tauro?

- Create a new project from a template with one command
- Run a pipeline for a specific environment (dev, pre_prod, prod)
- Run a single step (node) of a pipeline if you need to re‑run just part of it
- Start a streaming pipeline and check its status or stop it
- See which pipelines exist and view basic details
- Validate your setup before running

You do not need to write code to use these features. If you later want to customize pipeline logic, a developer can edit the generated sample files.

---

## Before you start

- You need Python 3.9 or later
- Open a terminal (Command Prompt/PowerShell on Windows, Terminal on macOS/Linux)
- Install required packages (you’ll get a ready “requirements.txt” in the template)

If Tauro is already installed in your environment, you can skip template generation and use your team’s existing project.

---

## Quick Start in 10 Minutes

Follow these steps to try Tauro with a new sample project.

1) Create a new project
- YAML format (default):
  ```
  tauro --template medallion_basic --project-name demo_project
  ```
- JSON format:
  ```
  tauro --template medallion_basic --project-name demo_project --format json
  ```

2) Go into your project and install requirements
```
cd demo_project
pip install -r requirements.txt
```

3) Run your first batch pipeline (Bronze ingestion)
- Development environment (“dev”):
  ```
  tauro --env dev --pipeline bronze_batch_ingestion
  ```

4) Run your first streaming pipeline (Bronze streaming)
- Start (async mode, runs in background):
  ```
  tauro --streaming --streaming-command run \
        --streaming-config ./settings_json.json \
        --streaming-pipeline bronze_streaming_ingestion \
        --streaming-mode async
  ```
- Check status (all running jobs):
  ```
  tauro --streaming --streaming-command status --streaming-config ./settings_json.json
  ```
- Stop a streaming job (replace <ID> with the execution id from status):
  ```
  tauro --streaming --streaming-command stop \
        --streaming-config ./settings_json.json \
        --execution-id <ID>
  ```

Tip: If you generated YAML instead of JSON, your settings file will be settings_yml.json. Use that in --streaming-config.

---

## Everyday tasks

Choose an environment
- Environments help you separate development, testing, and production.
- Supported: base, dev, pre_prod, prod
- Example:
  ```
  tauro --env pre_prod --pipeline silver_transform
  ```

Run only one step (node) of a pipeline
- Useful if a particular step failed and you want to re‑run just that part.
  ```
  tauro --env dev --pipeline gold_aggregation --node aggregate_sales
  ```

Preview without actually running (dry run)
- Shows what would happen, but makes no changes.
  ```
  tauro --env dev --pipeline bronze_batch_ingestion --dry-run
  ```

Validate your setup (no execution)
- Checks the configuration structure and paths.
  ```
  tauro --env dev --pipeline bronze_batch_ingestion --validate-only
  ```

See available pipelines
```
tauro --list-pipelines
```

Get basic info about a pipeline
```
tauro --pipeline-info gold_aggregation
```

Clear cached discovery results
```
tauro --clear-cache
```

---

## Understanding the configuration (plain English)

Your project has:
- One “settings” file at the project root (for example, settings_json.json)
  - This file points Tauro to the right config files for each environment
- A “config/” folder with the actual settings:
  - global_settings: general options (project name, defaults)
  - pipelines: list of pipeline names and which steps (nodes) they include
  - nodes: what each step does and in which order
  - input: where data comes from (files, tables, streams)
  - output: where results go (tables, folders, streams)

You don’t need to edit these to try Tauro, but your team may customize them later.

---

## Dates and time windows

Some pipelines work with date ranges.

- Use ISO format: YYYY-MM-DD
- Example:
  ```
  tauro --env dev --pipeline bronze_batch_ingestion \
        --start-date 2025-01-01 --end-date 2025-01-31
  ```
- Tauro checks that the start date is not after the end date.

---

## Logging (making output quieter or more detailed)

- Default level is INFO (balanced)
- Make it very detailed:
  ```
  tauro --env dev --pipeline bronze_batch_ingestion --verbose
  ```
- Show only errors:
  ```
  tauro --env dev --pipeline bronze_batch_ingestion --quiet
  ```
- Send logs to a custom file:
  ```
  tauro --env dev --pipeline bronze_batch_ingestion --log-file ./my_run.log
  ```

A default log file is also saved in logs/tauro.log.

---

## Streaming (simple view)

- Run: starts the streaming job (sync waits until it finishes, async continues in background)
- Status: tells you if your streaming job is running and its identifier
- Stop: stops the job safely

You always need to point to your settings file with --streaming-config.

Examples:
- Run async:
  ```
  tauro --streaming --streaming-command run \
        --streaming-config ./settings_json.json \
        --streaming-pipeline bronze_streaming_ingestion \
        --streaming-mode async
  ```
- Status (all):
  ```
  tauro --streaming --streaming-command status --streaming-config ./settings_json.json
  ```
- Stop by id:
  ```
  tauro --streaming --streaming-command stop \
        --streaming-config ./settings_json.json \
        --execution-id <ID>
  ```

---

## Tips and common fixes

- “Config not found”
  - Make sure you are inside your project folder (cd demo_project)
  - The settings file should be visible in your current folder: settings_json.json (or settings_yml.json)
  - Try:
    ```
    tauro --list-configs
    ```
- “Invalid date format”
  - Use YYYY-MM-DD, for example 2025-03-15
- “Import” or “module not found” in custom code (if your team customized nodes)
  - Make sure code files are inside your project (for example under pipelines/ or src/)
  - Ask a developer to check Python package setup if needed
- Want to see what Tauro would do without changes?
  - Use --dry-run

---

## Frequently Asked Questions

- Do I need admin rights?
  - No, you just need Python and the project files.
- Does Tauro change my original data?
  - Only if a pipeline writes to an output location. You can always use --dry-run to preview.
- Can I use Tauro on Windows/macOS/Linux?
  - Yes. Commands are the same. Paths and permissions may differ by system.

---

## Where to get help

- Check the README created inside your generated project (it includes next steps)
- Use:
  ```
  tauro --list-pipelines
  tauro --pipeline-info <name>
  ```
- If you still need help, share the error message and the log file (logs/tauro.log) with your data team.

You’re ready to go. Start with bronze_batch_ingestion in dev, then explore the rest!
