# Tauro

Tauro is a simple CLI for running and managing data pipelines (batch and streaming). It runs locally or with Spark and provides a concise command set to list, inspect, validate and execute pipelines.

Quick highlights
- Run batch, streaming and hybrid pipelines
- Read/write common formats (Parquet, JSON, CSV, Delta, Avro, ORC)
- Built-in validation, safe path handling and structured logs
- Lightweight configuration model with environment-aware settings

Installation

- From PyPI (recommended):
```
pip install tauro
```

Minimum requirements
- Python 3.9+
- (Optional) Spark 3.4+ if using Spark-backed formats
- (Optional) delta-spark for Delta format outside Databricks

Quick start (under 5 minutes)

1. Generate a project template
```
tauro --template medallion_basic --project-name demo_project
```

2. Install project dependencies and open the project
```
cd demo_project
pip install -r requirements.txt
```

3. Run a batch pipeline
```
tauro --env dev --pipeline bronze_batch_ingestion
```

4. Start a streaming pipeline (async)
```
tauro --streaming --streaming-command run \
  --streaming-config ./settings.json \
  --streaming-pipeline bronze_streaming_ingestion \
  --streaming-mode async
```

Configuration (high level)
- Tauro uses a single configuration index (settings file) that points to environment-specific sections.
- Key concepts:
  - settings file (JSON or YAML) — entry point
  - environment (dev, prod, etc.) — select runtime values
  - pipelines and nodes — define DAGs and processing steps
- Keep per-node streaming options (checkpoint_location, trigger) when using streaming.

Common commands

- List pipelines
```
tauro --list-pipelines
```

- Show pipeline info
```
tauro --pipeline-info <pipeline_name>
```

- Execute a pipeline
```
tauro --env <dev|prod|...> --pipeline <name> [--node <node_name>] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]
```

- Validate configuration without running
```
tauro --env dev --pipeline my_pipeline --validate-only
```

Helpful flags
- Verbosity: `--verbose` `--quiet`
- Date range: use ISO format `YYYY-MM-DD` (start_date must be <= end_date)

Streaming commands

- Run
```
tauro --streaming --streaming-command run \
  --streaming-config <settings_file> \
  --streaming-pipeline <pipeline_name> \
  [--streaming-mode sync|async]
```

- Status
```
tauro --streaming --streaming-command status \
  --streaming-config <settings_file> \
  [--execution-id <id>]
```

- Stop
```
tauro --streaming --streaming-command stop \
  --streaming-config <settings_file> \
  --execution-id <id>
```

Best practices
- Always set a checkpoint_location for streaming nodes.
- Prefer atomic output formats (Delta, Parquet) for production.
- Use `dry-run` or `--validate-only` before running in production.
- Pin Spark and connector versions when running on a cluster.

Troubleshooting (quick)

- Command not found / --help not working:
  - Try: `python -m tauro --help`

- Spark session missing:
  - Ensure Spark is installed and the runtime provides a Spark session when using Spark formats.

- Date validation errors:
  - Use ISO format `YYYY-MM-DD` and ensure start_date ≤ end_date.

- Configuration not found or invalid:
  - Verify the settings file path and that the selected environment section exists.

- Security/path errors:
  - Avoid symlinks, hidden paths or locations outside the project workspace.

Exit codes
- 0: success
- 1: general error
- 2: configuration error
- 3: validation error
- 4: execution error
- 5: dependency error
- 6: security error

Need more help?
- Use `tauro --help` for a full list of commands and options.
- For complex issues, reproduce with `--log-level DEBUG` and include the generated log when asking
