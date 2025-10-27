# Tauro CLI

A comprehensive command-line interface for the Tauro data pipeline framework. Provides unified access to pipeline execution, orchestration, streaming management, configuration discovery, and project scaffolding. Designed for both development and production environments with strong security validation and flexible configuration management.

The CLI is organized into intuitive subcommands for different operations:
- `run`: Direct pipeline execution with validation and dry-run modes
- `orchestrate`: Pipeline orchestration, scheduling, and run management
- `stream`: Streaming pipeline management and monitoring
- `template`: Project template generation with production-ready architecture
- `config`: Configuration discovery, validation, and management

---

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Architecture](#architecture)
- [Configuration Model](#configuration-model)
- [Subcommands Reference](#subcommands-reference)
- [Pipeline Execution (`run`)](#pipeline-execution-run)
- [Orchestration Management (`orchestrate`)](#orchestration-management-orchestrate)
- [Streaming Pipelines (`stream`)](#streaming-pipelines-stream)
- [Template Generation (`template`)](#template-generation-template)
- [Configuration Management (`config`)](#configuration-management-config)
- [Logging and Debugging](#logging-and-debugging)
- [Security and Validation](#security-and-validation)
- [Environment Management](#environment-management)
- [Exit Codes](#exit-codes)
- [Programmatic API](#programmatic-api-advanced)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)
- [Development Notes](#development-notes)

---

## Quick Start

### 1. Generate a Starter Project

Generate a production-ready project using the Medallion architecture (Bronze, Silver, Gold layers):

```bash
# YAML-based configuration (recommended)
tauro template --template medallion_basic --project-name demo_project

# JSON-based configuration (alternative)
tauro template --template medallion_basic --project-name demo_project --format json

# DSL-based configuration (advanced)
tauro template --template medallion_basic --project-name demo_project --format dsl
```

This creates:
- Project directory structure
- Configuration files for base, dev, sandbox, and prod environments
- Sample node functions and pipelines
- README with setup instructions
- requirements.txt and .gitignore

### 2. Run a Batch Pipeline

Execute a pipeline for a specific date range:

```bash
# Run with defaults (today's date)
tauro run --env dev --pipeline bronze_batch_ingestion

# Specify custom date range
tauro run --env dev --pipeline silver_transformation --start-date 2025-01-01 --end-date 2025-01-31

# Run specific node only
tauro run --env dev --pipeline silver_transformation --node extract

# Dry-run mode (no execution)
tauro run --env dev --pipeline bronze_batch_ingestion --dry-run

# Validate configuration only
tauro run --env dev --pipeline bronze_batch_ingestion --validate-only
```

### 3. Start a Streaming Pipeline

Manage real-time streaming pipelines:

```bash
# Run streaming pipeline (async by default)
tauro stream run --config config/streaming.py --pipeline real_time_processing

# Run in synchronous mode (wait for completion)
tauro stream run --config config/streaming.py --pipeline real_time_processing --mode sync

# Check status of all running streams
tauro stream status --config config/streaming.py

# Check specific execution
tauro stream status --config config/streaming.py --execution-id exec-20250126-001

# Stop a streaming pipeline
tauro stream stop --config config/streaming.py --execution-id exec-20250126-001

# Stop with custom timeout
tauro stream stop --config config/streaming.py --execution-id exec-20250126-001 --timeout 120
```

### 4. Manage Orchestration

Schedule and manage pipeline runs:

```bash
# List all pipelines available in dev environment
tauro config list-pipelines --env dev

# Create a new run
tauro orchestrate run-create --pipeline my_pipeline

# Start an existing run
tauro orchestrate run-start --run-id run-12345

# Check run status
tauro orchestrate run-status --run-id run-12345

# List all runs for a pipeline
tauro orchestrate run-list --pipeline my_pipeline

# Schedule a pipeline (every 3600 seconds)
tauro orchestrate schedule-add --pipeline my_pipeline --schedule-kind INTERVAL --expression "3600"

# Backfill: create N runs for historical data
tauro orchestrate backfill --pipeline my_pipeline --count 10
```

---

## Installation

### Requirements

- Python 3.9 or higher
- Core dependencies:
  - `loguru` (logging)
  - `click` (CLI framework)

### Optional Dependencies

- **YAML support**: `pyyaml`
- **Streaming support**: Kafka client, Spark streaming dependencies
- **Spark/Delta**: If pipelines use these technologies
- **Development**: `pytest`, `pytest-cov`

### Installation Methods

**From source:**
```bash
cd core
pip install -e .
```

**For development:**
```bash
pip install -e ".[dev]"
```

---

## Architecture

### Component Structure

```
tauro.cli/
├── cli.py              # Main entry point, unified parser, subcommand dispatch
├── core.py             # Shared utilities, error handling, logging, security
├── config.py           # Configuration discovery, loading, management
├── execution.py        # Context initialization and execution wrapper
├── template.py         # Project template generation
└── test/               # Unit tests
```

### Data Flow

```
CLI Arguments
    ↓
Parse & Validate
    ↓
Discover/Load Configuration
    ↓
Initialize Context (Spark, etc.)
    ↓
Execute Subcommand
    ↓
Return Results/Status
```

### Security Layers

1. **Path Validation**: No symlinks, hidden paths, directory traversal
2. **Environment Normalization**: Validates environment names and variants
3. **Configuration Schema**: Validates structure and required fields
4. **Execution Safety**: Dry-run and validation modes

---

## Configuration Model

Tauro separates configuration into structured files for maintainability and environment-specific overrides:

### Configuration Index Files

Auto-discovered files (in order of preference):
- `settings_yml.json`: YAML configuration index
- `settings_json.json`: JSON configuration index
- `settings_dsl.json`: DSL configuration index (optional)
- `settings.json`, `config.json`, `tauro.json` (fallback)

### Configuration Section Files

For each environment (base, dev, sandbox, prod), Tauro loads:

```
config/
├── base/
│   ├── global_settings.yaml
│   ├── pipelines.yaml
│   ├── nodes.yaml
│   ├── input.yaml
│   └── output.yaml
├── dev/
│   ├── global_settings.yaml
│   ├── pipelines.yaml
│   └── (other sections inherit from base)
├── sandbox/
│   └── (environment-specific overrides)
└── prod/
    └── (environment-specific overrides)
```

### Supported Formats

- **YAML** (recommended for readability):
  ```yaml
  global_settings:
    start_date: "2025-01-01"
    end_date: "2025-12-31"
  ```

- **JSON** (for programmatic generation):
  ```json
  {
    "global_settings": {
      "start_date": "2025-01-01"
    }
  }
  ```

- **DSL** (simplified format):
  ```ini
  [global_settings]
  start_date = "2025-01-01"
  ```

### Environment Support

Supported environments:
- `base`: Default environment (always loaded first)
- `dev`: Development environment
- `sandbox`: Sandbox/testing environment
- `prod`: Production environment
- `sandbox_<developer>`: Developer-specific sandbox (e.g., `sandbox_alice`)

Environment fallback chain:
1. Specific environment config (e.g., dev)
2. Sandbox config (if applicable)
3. Base config (fallback)

### Auto-Discovery

Configuration discovery automatically scans for settings files and ranks them by:
- Path specificity
- Format type (YAML > JSON > DSL)
- File location

Use `tauro config list-configs` to see discovered files.

---

## Subcommands Reference

All subcommands support:
- `--verbose` / `-v`: Enable DEBUG logging
- `--quiet` / `-q`: Enable ERROR-only logging
- `--log-level LEVEL`: Set specific log level
- `--log-file PATH`: Write logs to file (default: logs/tauro.log)

---

## Pipeline Execution (`run`)

Direct pipeline execution without orchestration orchestration.

### Basic Usage

```bash
tauro run --env <environment> --pipeline <pipeline_name> [options]
```

### Options

| Option | Type | Description |
|--------|------|-------------|
| `--env` | STRING | Environment (base, dev, sandbox, prod, sandbox_<dev>) |
| `--pipeline` | STRING | Pipeline name from configuration |
| `--node` | STRING | Execute only specific node (optional) |
| `--start-date` | DATE | Start date (YYYY-MM-DD, optional) |
| `--end-date` | DATE | End date (YYYY-MM-DD, optional) |
| `--validate-only` | BOOL | Validate config without executing |
| `--dry-run` | BOOL | Log actions without executing |
| `--verbose` | BOOL | Enable DEBUG logging |

### Examples

```bash
# Run entire pipeline with date range
tauro run --env prod --pipeline daily_etl --start-date 2025-01-01 --end-date 2025-01-31

# Run single node
tauro run --env dev --pipeline daily_etl --node transform_layer

# Validate configuration
tauro run --env dev --pipeline daily_etl --validate-only

# Dry-run to see what would execute
tauro run --env dev --pipeline daily_etl --dry-run --verbose
```

### Output

Success returns exit code 0 with execution summary. Failure returns appropriate error code with detailed error message.

---

## Orchestration Management (`orchestrate`)

Manage scheduled pipeline runs and orchestration state.

### Run Management

**Create a new run:**
```bash
tauro orchestrate run-create --pipeline <name> [--retries N] [--max-concurrency N]
# Returns: run-<timestamp>
```

**Start an existing run:**
```bash
tauro orchestrate run-start --run-id <id> [--retries 3] [--max-concurrency 5]
```

**Check run status:**
```bash
tauro orchestrate run-status --run-id <id>
# Output: pending|running|completed|failed|cancelled
```

**List runs:**
```bash
tauro orchestrate run-list [--pipeline <name>] [--status running]
```

**List run tasks:**
```bash
tauro orchestrate run-tasks --run-id <id>
```

**Cancel a run:**
```bash
tauro orchestrate run-cancel --run-id <id>
```

### Schedule Management

**Add schedule:**
```bash
# Run every 3600 seconds (1 hour)
tauro orchestrate schedule-add --pipeline my_pipeline --schedule-kind INTERVAL --expression "3600"

# Run on cron schedule (daily at 2 AM)
tauro orchestrate schedule-add --pipeline my_pipeline --schedule-kind CRON --expression "0 2 * * *"
```

**List schedules:**
```bash
tauro orchestrate schedule-list [--pipeline <name>]
```

**Start scheduler:**
```bash
tauro orchestrate schedule-start [--poll-interval 1.0]
```

### Data Management

**Database statistics:**
```bash
tauro orchestrate db-stats
# Output: Total runs, tasks, schedules, etc.
```

**Cleanup old data:**
```bash
# Remove records older than 30 days
tauro orchestrate db-cleanup --days 30
```

**Optimize database:**
```bash
tauro orchestrate db-vacuum
```

---

## Streaming Pipelines (`stream`)

Manage real-time streaming pipelines with Kafka, Delta CDC, or custom sources.

### Run Streaming Pipeline

```bash
tauro stream run --config <config_file> --pipeline <name> [options]
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `--config` | PATH | Configuration file |
| `--pipeline` | STRING | Pipeline name |
| `--mode` | STRING | sync (wait) or async (background) |
| `--model-version` | STRING | ML model version (optional) |
| `--hyperparams` | JSON | Hyperparameters JSON string (optional) |

**Examples:**

```bash
# Async mode (returns immediately)
tauro stream run --config config/streaming.py --pipeline kafka_events

# Sync mode (wait for pipeline to finish)
tauro stream run --config config/streaming.py --pipeline kafka_events --mode sync

# With ML parameters
tauro stream run --config config/streaming.py --pipeline ml_stream \
  --model-version v1.0.0 \
  --hyperparams '{"learning_rate": 0.01}'
```

### Check Streaming Status

```bash
# Check all running streams
tauro stream status --config <config_file>

# Check specific execution
tauro stream status --config <config_file> --execution-id <id>
```

**Output:**
```
Execution ID    | Status    | Pipeline           | Start Time         | Duration
exec-001        | ACTIVE    | kafka_events       | 2025-01-26 10:00   | 00:45:30
exec-002        | COMPLETED | kafka_events       | 2025-01-26 09:00   | 01:20:15
```

### Stop Streaming Pipeline

```bash
# Graceful stop with 60-second timeout
tauro stream stop --config <config_file> --execution-id <id> [--timeout 60]

# Force stop immediately
tauro stream stop --config <config_file> --execution-id <id> --timeout 0
```

---

## Template Generation (`template`)

Generate production-ready project structures with all necessary boilerplate.

### List Available Templates

```bash
tauro template --list-templates
```

Available templates:
- `medallion_basic`: Medallion architecture (Bronze/Silver/Gold)

### Generate Project

```bash
tauro template --template <name> --project-name <name> [options]
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `--template` | STRING | Template type |
| `--project-name` | STRING | Project name |
| `--format` | STRING | yaml, json, or dsl (default: yaml) |
| `--no-sample-code` | BOOL | Skip sample functions |
| `--output-dir` | PATH | Output directory (default: current) |

### Examples

```bash
# Generate YAML-based project
tauro template --template medallion_basic --project-name my_project

# Generate JSON-based project
tauro template --template medallion_basic --project-name my_project --format json

# Generate without sample code
tauro template --template medallion_basic --project-name my_project --no-sample-code

# Output to specific directory
tauro template --template medallion_basic --project-name my_project --output-dir /opt/projects
```

### Generated Structure

```
my_project/
├── settings_yml.json           # Configuration index
├── config/
│   ├── base/
│   │   ├── global_settings.yaml
│   │   ├── pipelines.yaml
│   │   ├── nodes.yaml
│   │   ├── input.yaml
│   │   └── output.yaml
│   ├── dev/
│   ├── sandbox/
│   └── prod/
├── src/
│   ├── pipelines/
│   ├── nodes/
│   └── utils/
├── README.md
├── requirements.txt
├── .gitignore
└── setup.py
```

---

## Configuration Management (`config`)

Discover, validate, and manage Tauro configurations.

### List Discovered Configurations

```bash
tauro config list-configs
```

Shows all discovered settings files with their scores and paths.

### List Available Pipelines

```bash
tauro config list-pipelines --env <environment>
```

Shows all pipelines available in specified environment.

### Get Pipeline Information

```bash
tauro config pipeline-info --env <environment> --pipeline <name>
```

Displays:
- Pipeline definition
- Node dependencies
- Input/output specifications
- Metadata and tags

### Validate Configuration

```bash
tauro config validate --env <environment>
```

Validates all configuration files without executing.

### Clear Configuration Cache

```bash
tauro config clear-cache
```

Clears cached configuration (useful after manual edits).

---

## Logging and Debugging

### Log Levels

```bash
# DEBUG: Most verbose, all internal details
tauro run --env dev --pipeline test --log-level DEBUG

# INFO: Default, normal operation information
tauro run --env dev --pipeline test --log-level INFO

# WARNING: Only warnings and errors
tauro run --env dev --pipeline test --log-level WARNING

# ERROR: Only errors
tauro run --env dev --pipeline test --log-level ERROR

# CRITICAL: Only critical failures
tauro run --env dev --pipeline test --log-level CRITICAL
```

### Verbosity Shortcuts

```bash
# Shorthand for --log-level DEBUG
tauro run --env dev --pipeline test --verbose

# Shorthand for --log-level ERROR
tauro run --env dev --pipeline test --quiet
```

### Custom Log Output

```bash
# Log to file instead of console
tauro run --env dev --pipeline test --log-file ./my_run.log

# Log to both console and file
tauro run --env dev --pipeline test --log-file ./my_run.log --verbose
```

### Log Format

Default format includes:
- Timestamp
- Log level
- Component name
- Message
- Context data (when available)

---

## Security and Validation

### Path Validation

The CLI strictly validates all file paths:
- ❌ No absolute paths (`/etc/passwd`)
- ❌ No symlinks
- ❌ No hidden files (`.config`)
- ❌ No directory traversal (`../../../etc`)
- ✅ Relative paths only
- ✅ Readable by current user
- ✅ Not world-writable

### Configuration Validation

Before execution, configurations are validated for:
- Required fields presence
- Type correctness
- Value ranges
- Reference integrity (dependency nodes exist)
- Format compatibility (batch vs streaming)

### Execution Safety

- **Dry-run mode**: Logs all actions without executing
- **Validation mode**: Validates configuration without running
- **Staging environments**: Test in sandbox before prod

---

## Environment Management

### Environment Hierarchy

Environments follow a strict hierarchy:

```
prod
├── base (fallback)
sandbox
├── sandbox_alice (developer-specific)
├── sandbox_bob
└── base (fallback)
dev
├── base (fallback)
```

### Environment-Specific Configuration

Each environment can override:
- Global settings (start_date, spark config, etc.)
- Pipeline definitions
- Node configurations
- Input/output specifications

### Using Developer Sandboxes

```bash
# Create sandbox_alice directory in config/
mkdir -p config/sandbox_alice

# Override specific files for alice
cp config/sandbox/pipelines.yaml config/sandbox_alice/

# Run pipeline in alice's sandbox
tauro run --env sandbox_alice --pipeline my_pipeline
```

---

## Exit Codes

| Code | Meaning | Example |
|------|---------|---------|
| 0 | SUCCESS | Pipeline executed successfully |
| 1 | GENERAL_ERROR | Unexpected error |
| 2 | CONFIGURATION_ERROR | Invalid config file or settings |
| 3 | VALIDATION_ERROR | Validation failed (node dependencies, types, etc.) |
| 4 | EXECUTION_ERROR | Pipeline execution failed |
| 5 | DEPENDENCY_ERROR | Missing dependency or module import error |
| 6 | SECURITY_ERROR | Security validation failed (path, permissions, etc.) |

### Handling Exit Codes

In scripts:
```bash
#!/bin/bash
tauro run --env prod --pipeline my_pipeline
EXIT_CODE=$?

case $EXIT_CODE in
  0)
    echo "Success!"
    ;;
  4)
    echo "Pipeline execution failed - retrying..."
    sleep 60
    # retry logic
    ;;
  *)
    echo "Error: $EXIT_CODE"
    exit $EXIT_CODE
    ;;
esac
```

---

## Programmatic API (Advanced)

Use Tauro CLI components programmatically in Python:

### Configuration Management

```python
from tauro.cli.config import ConfigManager, ConfigDiscovery

# Discover and load configurations
discovery = ConfigDiscovery(base_path="./", layer_name="analytics")
config_files = discovery.discover()

# Create configuration manager
cm = ConfigManager(base_path="./", layer_name="my_layer", use_case="etl")
config = cm.load("dev")
```

### Context Initialization

```python
from tauro.cli.execution import ContextInitializer

initializer = ContextInitializer(config_manager)
context = initializer.initialize("dev")

# Access context properties
print(context.spark)
print(context.global_settings)
```

### Pipeline Execution

```python
from tauro.cli.execution import PipelineExecutor

executor = PipelineExecutor(context)
executor.execute(
    pipeline_name="my_pipeline",
    start_date="2025-01-01",
    end_date="2025-12-31"
)
```

### Streaming Operations

```python
from tauro.cli.cli import run_cli_impl, status_cli_impl, stop_cli_impl

# Start streaming pipeline
run_cli_impl(config="./config.py", pipeline="streaming_pipeline")

# Check status
status_cli_impl(config="./config.py")

# Stop pipeline
stop_cli_impl(config="./config.py", execution_id="exec-001")
```

---

## Advanced Usage

### Batch Processing Multiple Environments

```bash
#!/bin/bash
for env in dev sandbox prod; do
  echo "Running in $env..."
  tauro run --env $env --pipeline daily_etl --start-date 2025-01-01 --end-date 2025-01-31
done
```

### Scheduled Backfills

```bash
#!/bin/bash
# Backfill 30 days of data
tauro orchestrate backfill --pipeline my_pipeline --count 30

# Then start scheduler
tauro orchestrate schedule-start --poll-interval 1.0
```

### Parallel Streaming Pipelines

```bash
#!/bin/bash
# Run multiple streaming pipelines in parallel
tauro stream run --config config1.py --pipeline stream1 &
tauro stream run --config config2.py --pipeline stream2 &
tauro stream run --config config3.py --pipeline stream3 &

# Wait for all
wait
```

### Continuous Integration

```bash
#!/bin/bash
# CI/CD pipeline
set -e

# Validate
tauro config validate --env dev

# Run tests
tauro run --env dev --pipeline test_pipeline --validate-only

# Execute
tauro run --env dev --pipeline main_pipeline --start-date today

echo "All checks passed"
```

---

## Troubleshooting

### Configuration Not Found

```
Error: CONFIGURATION_ERROR: No valid configuration found
```

**Solution:**
```bash
# Check discovered configurations
tauro config list-configs

# Clear cache
tauro config clear-cache

# Verify settings file exists
ls -la settings_yml.json
```

### Pipeline Not Found

```
Error: EXECUTION_ERROR: Pipeline 'my_pipeline' not found
```

**Solution:**
```bash
# List available pipelines
tauro config list-pipelines --env dev

# Check spelling matches exactly
```

### Environment Not Found

```
Error: VALIDATION_ERROR: Environment 'staging' not recognized
```

**Solution:**
```bash
# Use valid environment
tauro run --env dev --pipeline my_pipeline

# Supported: base, dev, sandbox, prod, sandbox_<developer>
```

### Path Security Error

```
Error: SECURITY_ERROR: Path contains invalid characters or is not accessible
```

**Solution:**
- Use relative paths only
- Avoid symlinks
- Avoid hidden directories (starting with `.`)
- Ensure file is readable by current user
- Don't use absolute paths

### Out of Memory

```
Error: java.lang.OutOfMemoryError: Java heap space
```

**Solution:**
```bash
# Set Spark memory before running
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g

tauro run --env prod --pipeline my_pipeline
```

### Network/Connection Issues

```
Error: Connection refused / Network is unreachable
```

**Solution:**
```bash
# Check connectivity
ping your-spark-master

# Validate configuration
tauro run --env dev --pipeline test --validate-only --verbose

# Use dry-run to debug
tauro run --env dev --pipeline test --dry-run --verbose
```

---

## Development Notes

### Internal Architecture

- **Logging**: Configured via `LoggerManager.setup()` (console + optional file)
- **Path Validation**: All paths validated by `SecurityValidator`
- **Config Discovery**: Implements scoring and caching for performance
- **Environment Normalization**: Supports fallback chains and developer variants
- **Subcommand Dispatch**: Click-based with unified error handling

### Configuration Caching

The CLI caches configuration discovery results for performance:
- Cache duration: 5 minutes (by default)
- Use `tauro config clear-cache` to manually clear
- Cache stored in system temp directory

### Programmatic Usage Pattern

```python
# Recommended order for programmatic use
from tauro.cli.config import ConfigManager
from tauro.cli.execution import ContextInitializer, PipelineExecutor

# 1. Configure
config_manager = ConfigManager()
config = config_manager.load("dev")

# 2. Initialize context
initializer = ContextInitializer(config_manager)
context = initializer.initialize("dev")

# 3. Execute
executor = PipelineExecutor(context)
executor.execute("my_pipeline", "2025-01-01", "2025-01-31")
```

### Error Handling

All CLI errors include:
- Error code (for scripting)
- Human-readable message
- Context information
- Suggested fix when available

---

## Components Reference

### cli.py

Main entry point with:
- Unified argument parser
- Subcommand dispatch
- Error handling wrapper
- Exit code management

### core.py

Shared utilities:
- `LoggerManager`: Logging configuration
- `SecurityValidator`: Path and input validation
- `ErrorHandler`: Consistent error handling
- `Config` classes: Data structures

### config.py

Configuration handling:
- `ConfigManager`: Load and manage configurations
- `ConfigDiscovery`: Find configuration files
- `ConfigLoader`: Parse YAML/JSON/DSL files
- Format-specific parsers

### execution.py

Execution coordination:
- `ContextInitializer`: Build execution context
- `PipelineExecutor`: Execute pipelines
- `StreamingExecutor`: Coordinate streaming runs
- Integration with tauro.exec and tauro.streaming

### template.py

Project scaffolding:
- `TemplateGenerator`: Create project structures
- `MedallionGenerator`: Medallion architecture
- Format writers (YAML, JSON, DSL)

---

## License

Copyright (c) 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.

---
