# Tauro CLI

Command-line interface for the Tauro data pipeline framework. This CLI lets you:

- Execute batch (and hybrid) pipelines
- Manage streaming pipelines (run, check status, stop)
- Discover and select configuration files by environment
- Generate a minimal Medallion-style project template

The CLI in this folder is composed of:
- `cli.py`: main entry point and argument parser (argparse)
- `streaming_cli.py`: streaming subcommands (click)
- `config.py`: configuration discovery and format loaders
- `execution.py`: context initialization and execution wrapper
- `core.py`: shared types, logging setup, validation, and utilities
- `template.py`: project template generator

Requirements: Python 3.9+, `loguru`. Optional: `pyyaml` (YAML configs), streaming stack (e.g., Kafka client), Spark/Delta if your pipelines use them.

---

## Quick start

1) Generate a starter project (Medallion: Bronze, Silver, Gold)
- YAML (default):
  tauro --template medallion_basic --project-name demo_project
- JSON:
  tauro --template medallion_basic --project-name demo_project --format json

2) Run a batch pipeline (after customizing configs and code)
- From the project root:
  tauro --env dev --pipeline bronze_batch_ingestion

3) Start a streaming pipeline (async)
- Using the main CLI:
  tauro --streaming --streaming-command run \
        --streaming-config ./settings_json.json \
        --streaming-pipeline bronze_streaming_ingestion \
        --streaming-mode async

4) Check status or stop a streaming execution
- Status (all):
  tauro --streaming --streaming-command status --streaming-config ./settings_json.json
- Stop (by ID):
  tauro --streaming --streaming-command stop \
        --streaming-config ./settings_json.json \
        --execution-id <your_execution_id>

---

## Configuration model

Tauro separates configuration into one index file (“settings”) and five section files:

- Index settings file (auto-discovered):
  - YAML: settings_yml.json
  - JSON: settings_json.json
  - DSL: settings_dsl.json (optional)
- Section files (per environment mapping, see Template):
  - global_settings.(yaml|json|dsl)
  - pipelines.(yaml|json|dsl)
  - nodes.(yaml|json|dsl)
  - input.(yaml|json|dsl)
  - output.(yaml|json|dsl)

Environments supported by the CLI: base, dev, pre_prod, prod.

Auto-discovery scans for: settings_yml.json, settings_json.json, settings_dsl.json, settings.json, config.json, tauro.json. Discovery selects the best match by path score or interactive selection.

Security: The CLI validates paths to avoid directory traversal, hidden paths, symlinks, or world-writable files.

---

## Main commands

All flags below are from `cli.py` unless stated otherwise.

- Execute a pipeline
  tauro --env <base|dev|pre_prod|prod> --pipeline <name> [--node <node_name>] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]

- Validate configuration only
  tauro --env dev --pipeline my_pipeline --validate-only

- List discovered configs
  tauro --list-configs

- List pipelines (from active config dir)
  tauro --list-pipelines

- Show pipeline info
  tauro --pipeline-info my_pipeline

- Clear config discovery cache
  tauro --clear-cache

### Logging

- Set level: --log-level DEBUG|INFO|WARNING|ERROR|CRITICAL (default INFO)
- Verbose (overrides level to DEBUG): --verbose
- Quiet (only ERROR): --quiet
- Log file path: --log-file ./path/to/log.log (default logs/tauro.log)

### Dates

- --start-date and --end-date must be ISO format YYYY-MM-DD
- The CLI validates start_date <= end_date

---

## Streaming commands

You can manage streaming pipelines directly from the main CLI using the `--streaming` family of flags. Internally, this delegates to `streaming_cli.py`.

- Run
  tauro --streaming --streaming-command run \
        --streaming-config <settings_file> \
        --streaming-pipeline <pipeline_name> \
        [--streaming-mode sync|async] \
        [--model-version <ver>] \
        [--hyperparams '{"key": "value"}']

- Status (single or all)
  tauro --streaming --streaming-command status --streaming-config <settings_file> [--execution-id <id>]

- Stop
  tauro --streaming --streaming-command stop --streaming-config <settings_file> --execution-id <id>

Notes:
- `--hyperparams` expects a valid JSON string.
- In async mode, `run` returns an execution_id. Use it with `status`/`stop`.
- Output can be table or JSON when using the click commands directly; via the main CLI the default is a table.

---

## Template generation

The template generator produces:
- A settings index file (settings_yml.json or settings_json.json)
- Config files under ./config for base/dev/pre_prod/prod
- Minimal package structure and example node functions
- A README, requirements, and .gitignore

Commands:
- List templates:
  tauro --list-templates
- Generate (YAML):
  tauro --template medallion_basic --project-name demo_project
- Generate (JSON):
  tauro --template medallion_basic --project-name demo_project --format json
- Interactive:
  tauro --template-interactive
- Without sample code:
  tauro --template medallion_basic --project-name demo_project --no-sample-code

Tip: The CLI accepts --format yaml|json. If your build includes DSL, you may generate/configure DSL at your own risk.

---

## Programmatic API (advanced)

- Config discovery/management:
  - `ConfigManager`
  - `ConfigDiscovery`
- Context initialization:
  - `ContextInitializer.initialize(env)` -> `Context`
- Execution:
  - `PipelineExecutor.execute(pipeline_name, node_name?, start_date?, end_date?, dry_run?)`
  - `PipelineExecutor.list_pipelines()`
  - `PipelineExecutor.get_pipeline_info(pipeline_name)`

Streaming (from `tauro.exec.executor`):
- `run_pipeline(pipeline_name, execution_mode, model_version?, hyperparams?)`
- `get_streaming_pipeline_status(execution_id)`
- `stop_streaming_pipeline(execution_id, timeout=60)`

---

## Exit codes

- 0: SUCCESS
- 1: GENERAL_ERROR
- 2: CONFIGURATION_ERROR
- 3: VALIDATION_ERROR
- 4: EXECUTION_ERROR
- 5: DEPENDENCY_ERROR
- 6: SECURITY_ERROR

---

## Development notes

- Logging is configured via `LoggerManager.setup` (console + file).
- Paths are validated strictly by `SecurityValidator`; avoid symlinks and hidden paths.
- Auto-discovery caches results for a short time; use `--clear-cache` to reset.
- The streaming subcommands are implemented with click but are callable from the main CLI.

---
