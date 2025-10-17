# Tauro CLI

Command-line interface for the Tauro data pipeline framework. This CLI provides a unified interface for executing batch and streaming pipelines, managing orchestration, generating project templates, and handling configuration.

The CLI is organized into subcommands for different operations:
- `run`: Direct pipeline execution
- `orchestrate`: Pipeline orchestration management (runs, schedules, etc.)
- `stream`: Streaming pipeline management
- `template`: Project template generation
- `config`: Configuration management and discovery

## Components

- `cli.py`: Main entry point with unified argument parser and subcommand dispatch
- `core.py`: Shared utilities, error handling, security validation, and logging setup
- `config.py`: Configuration discovery, loading (YAML/JSON/DSL), and management
- `execution.py`: Context initialization and pipeline execution wrapper
- `template.py`: Project template generator with Medallion architecture support

Requirements: Python 3.9+, `loguru`. Optional: `pyyaml` (YAML configs), streaming stack (e.g., Kafka client), Spark/Delta if your pipelines use them.

---

## Quick start

1) Generate a starter project (Medallion: Bronze, Silver, Gold)
```bash
tauro template --template medallion_basic --project-name demo_project
# Or with JSON configs:
tauro template --template medallion_basic --project-name demo_project --format json
```

2) Run a batch pipeline (after customizing configs and code)
```bash
tauro run --env dev --pipeline bronze_batch_ingestion
```

3) Start a streaming pipeline (async)
```bash
tauro stream run --config config/streaming.py --pipeline real_time_processing
```

4) Check status or stop a streaming execution
```bash
# Status (all):
tauro stream status --config config/streaming.py
# Stop (by ID):
tauro stream stop --config config/streaming.py --execution-id <your_execution_id>
```

---

## Configuration model

Tauro separates configuration into one index file ("settings") and five section files:

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

Environments supported: base, dev, sandbox, prod, and sandbox_<developer> variants.

Auto-discovery scans for: settings_yml.json, settings_json.json, settings_dsl.json, settings.json, config.json, tauro.json. Discovery selects the best match by path score or interactive selection.

Security: The CLI validates paths to avoid directory traversal, hidden paths, symlinks, or world-writable files.

---

## Main subcommands

### Pipeline Execution (`run`)

Execute pipelines directly without orchestration:

```bash
tauro run --env <base|dev|sandbox|prod|sandbox_<developer>> --pipeline <name> [--node <node_name>] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]
```

Options:
- `--validate-only`: Validate configuration without executing
- `--dry-run`: Log actions without executing
- `--verbose`/`--quiet`: Control logging verbosity

### Orchestration Management (`orchestrate`)

Manage scheduled pipeline runs and orchestration:

**Run Management:**
```bash
tauro orchestrate run-create --pipeline my_pipeline
tauro orchestrate run-start --run-id <id> [--retries N] [--max-concurrency N]
tauro orchestrate run-status --run-id <id>
tauro orchestrate run-list [--pipeline <name>]
tauro orchestrate run-tasks --run-id <id>
tauro orchestrate run-cancel --run-id <id>
```

**Schedule Management:**
```bash
tauro orchestrate schedule-add --pipeline my_pipeline --schedule-kind INTERVAL --expression "3600"
tauro orchestrate schedule-list [--pipeline <name>]
tauro orchestrate schedule-start [--poll-interval 1.0]
tauro orchestrate backfill --pipeline my_pipeline --count 10
```

**Database Management:**
```bash
tauro orchestrate db-stats
tauro orchestrate db-cleanup --days 30
tauro orchestrate db-vacuum
```

### Streaming Pipelines (`stream`)

Manage real-time streaming pipelines:

```bash
# Run streaming pipeline
tauro stream run --config <settings_file> --pipeline <pipeline_name> [--mode sync|async] [--model-version <ver>] [--hyperparams '{"key": "value"}']

# Check status
tauro stream status --config <settings_file> [--execution-id <id>]

# Stop pipeline
tauro stream stop --config <settings_file> --execution-id <id> [--timeout 60]
```

### Template Generation (`template`)

Generate project templates and boilerplate code:

```bash
# List available templates
tauro template --list-templates

# Generate project
tauro template --template medallion_basic --project-name my_project [--format yaml|json|dsl] [--no-sample-code]

# Interactive generation
tauro template --template-interactive
```

### Configuration Management (`config`)

Manage Tauro configuration and discovery:

```bash
# List discovered configs
tauro config list-configs

# List pipelines for environment
tauro config list-pipelines --env dev

# Show pipeline info
tauro config pipeline-info --pipeline my_pipeline --env dev

# Clear config cache
tauro config clear-cache
```

---

## Logging

- Set level: `--log-level DEBUG|INFO|WARNING|ERROR|CRITICAL` (default INFO)
- Verbose (overrides level to DEBUG): `--verbose`
- Quiet (only ERROR): `--quiet`
- Log file path: `--log-file ./path/to/log.log` (default logs/tauro.log)

---

## Template generation

The template generator produces:
- A settings index file (settings_yml.json or settings_json.json)
- Config files under ./config for base/dev/sandbox/prod
- Minimal package structure and example node functions
- A README, requirements.txt, and .gitignore

Supported templates:
- `medallion_basic`: Simple Medallion architecture with batch and streaming examples

---

## Programmatic API (advanced)

**Configuration Management:**
```python
from tauro.cli.config import ConfigManager, ConfigDiscovery
cm = ConfigManager(base_path="./", layer_name="my_layer", use_case="analytics")
```

**Context Initialization:**
```python
from tauro.cli.execution import ContextInitializer
ctx = ContextInitializer(cm).initialize("dev")
```

**Pipeline Execution:**
```python
from tauro.cli.execution import PipelineExecutor
executor = PipelineExecutor(ctx)
executor.execute("my_pipeline", start_date="2025-01-01", end_date="2025-12-31")
```

**Streaming Operations:**
```python
from tauro.cli.cli import run_cli_impl, status_cli_impl, stop_cli_impl
run_cli_impl(config="./config.py", pipeline="streaming_pipeline")
```

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
- Auto-discovery caches results for a short time; use `config clear-cache` to reset.
- The CLI supports both programmatic and command-line usage patterns.
- Environment normalization supports sandbox variants (sandbox_developer1, etc.) with fallback to base sandbox config.