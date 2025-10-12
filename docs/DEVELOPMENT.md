# Tauro Development Guide

This document is for developers working on Tauro’s CLI layer and related components. It explains the architecture, coding standards, how to extend the CLI, and how to test and ship changes.

If you are a non‑technical user looking for basic usage, see core/README.md.

---

## Architecture Overview

Tauro follows a clear separation of concerns:

- CLI (core/tauro/cli)
  - cli.py: Main entry, argparse flags, high-level orchestration.
  - streaming_cli.py: Streaming subcommands implemented with click.
  - config.py: Configuration discovery and parsing (YAML/JSON/DSL) plus AppConfigManager for environment path mapping.
  - execution.py: Context initialization and a safe wrapper around ExternalPipelineExecutor.
  - core.py: Shared types (ExitCode), errors, logging (LoggerManager), path safety (SecurityValidator), date helpers, in-memory cache (ConfigCache).
  - template.py: Minimal project template generator (Medallion: Bronze → Silver → Gold; batch + streaming).
- Core runtime (outside this folder)
  - tauro.config.contexts.Context: Domain object that loads/holds config.
  - tauro.exec.executor.PipelineExecutor: The execution engine used by our wrapper.

High‑level flow:

1) User invokes tauro with flags (argparse).
2) Logger configured, special modes handled (template, streaming, listing).
3) Config discovery picks the right settings file; working directory changes to the active config directory.
4) ContextInitializer uses AppConfigManager to map environment → concrete config file paths.
5) PipelineExecutor wrapper calls the external executor, handles path setup, and normalizes errors and logs.
6) On exit, working directory and path changes are restored; caches cleared.

---

## Repository Layout (CLI)

- core/tauro/cli/core.py
  - CLIConfig dataclass for normalized args.
  - ExitCode, TauroError hierarchy, LoggerManager, SecurityValidator, ConfigCache.
  - Date helpers: parse_iso_date, validate_date_range.

- core/tauro/cli/config.py
  - YAMLConfigLoader, JSONConfigLoader, DSLConfigLoader.
  - ConfigDiscovery: scans filesystem and caches results.
  - ConfigManager: picks active config dir/file and format; manages chdir/restore.
  - AppConfigManager: reads settings_* index and resolves environment paths.

- core/tauro/cli/execution.py
  - ContextInitializer: composes Context from paths.
  - PipelineExecutor wrapper: adds path setup, error capture, validation helpers.

- core/tauro/cli/cli.py
  - ArgumentParser: argparse flags.
  - ConfigValidator: cross‑field checks.
  - SpecialModeHandler: list, info, clear cache, etc.
  - TauroCLI: main run loop, streaming delegation, dry‑run, validate‑only.

- core/tauro/cli/streaming_cli.py
  - click group: streaming.
  - Commands: run, status, stop.
  - Helpers: _list_all_pipelines_status, simple table rendering utilities.

- core/tauro/cli/template.py
  - Single template type: medallion_basic (batch + streaming).
  - TemplateGenerator creates folder structure, config files, and basic sample code.

---

## Configuration Model

- Index (“settings”) file located at the project root; auto‑discovered using predefined names:
  - settings_yml.json (points to YAML section files),
  - settings_json.json (points to JSON section files),
  - settings_dsl.json (points to DSL section files).
- Section files (per environment mapping inside settings):
  - global_settings, pipelines, nodes, input, output.
- Environments: base, dev, sandbox, prod.

AppConfigManager reads the settings index and returns validated, absolute paths for each required config section per environment. SecurityValidator ensures paths do not escape the project and are safe to use.

---

## Error Handling and Exit Codes

Use TauroError and its subclasses to communicate expected failures:

- ConfigurationError (ExitCode.CONFIGURATION_ERROR)
- ValidationError (ExitCode.VALIDATION_ERROR)
- ExecutionError (ExitCode.EXECUTION_ERROR)
- SecurityError (ExitCode.SECURITY_ERROR)

All other unexpected exceptions are treated as GENERAL_ERROR.

Guidelines:
- Prefer fail‑fast validation (e.g., date parsing/ordering, streaming required flags).
- Wrap external integrations with try/except and raise domain‑specific errors.
- Log error details with logger.exception only for truly unexpected errors.

---

## Logging

LoggerManager.setup configures:
- Console logging (DEBUG if --verbose, ERROR if --quiet, otherwise selected level).
- File logging to logs/tauro.log by default (rotated and retained).

Best practices:
- Use logger.info for normal flow, logger.debug for detailed internals, logger.warning for recoverable issues, and logger.error for failures.
- Avoid printing directly; use logger or click.echo in click command paths.
- Do not leak secrets in logs.

---

## Security

SecurityValidator.validate_path(base_path, target_path):
- Ensures target is within base (no traversal), not hidden, not symlinked, not world‑writable, and owned by current user (POSIX).
- Always validate paths before chdir or reading/writing files.

When changing directories:
- ConfigManager.change_to_config_directory() moves to the active config dir (validated).
- Always call restore_original_directory() in finally blocks.

---

## Dates

The CLI provides:
- parse_iso_date: validates YYYY‑MM‑DD and normalizes string.
- validate_date_range: ensures start <= end.

Integrate at the CLI boundary to fail fast and pass canonical values downstream.

---

## Coding Standards

- Python 3.9+.
- Type hints required for new/modified public functions.
- Logging via loguru, no prints in library code.
- Follow the existing exception hierarchy.
- Keep CLI flags backward compatible when possible; if a breaking change is unavoidable, document it in the changelog.

Formatting and linting (recommended):
- black
- isort
- flake8 (or ruff)
- mypy (optional but recommended for core modules)

Example:
```bash
python -m pip install -U black isort flake8 mypy
black core/tauro/cli
isort core/tauro/cli
flake8 core/tauro/cli
mypy core/tauro/cli
```

---

## Development Environment

Recommended setup:
```bash
# 1) Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2) Install project with dev extras (if defined) or basic deps
pip install -r requirements.txt

# 3) Install development tools
pip install -U black isort flake8 pytest

# 4) Run unit tests
pytest -q
```

Suggested pre-commit:
```bash
pip install pre-commit
pre-commit install
# Configure .pre-commit-config.yaml with black/isort/flake8/ruff
```

---

## Execution Flow Details

Batch pipeline (cli.py):
1) Parse args → LoggerManager.setup.
2) Handle template or streaming modes early.
3) SpecialModeHandler handles list, info, cache clear.
4) Date normalization and validation.
5) ConfigManager resolves active settings file and chdir.
6) ContextInitializer builds Context from AppConfigManager.
7) PipelineExecutor.execute runs the pipeline; wrapper sets up sys.path and logs.
8) Cleanup (restore directory, clear cache).

Streaming pipeline (streaming_cli.py):
- run: builds Context from DSL or Python config (Context.from_dsl), creates executor (from tauro.exec.executor), and calls run_pipeline with mode async/sync.
- status: fetches either a single execution’s status or all, pretty‑prints table or JSON.
- stop: requests a graceful stop with timeout.

Note: The main CLI invokes click commands’ .callback in a controlled manner to reuse logic without a full click context.

---

## Extending the CLI

### Add a new argparse flag

1) cli.py → ArgumentParser.create(): add parser.add_argument(...).
2) core.py → CLIConfig: add a matching field if the value is part of normalized config.
3) cli.py → TauroCLI.parse_arguments(): map parsed to CLIConfig.
4) cli.py → ConfigValidator.validate(): add cross‑field checks if needed.
5) Integrate usage in SpecialModeHandler or execution flow.

Be mindful of:
- Interactions with --verbose/--quiet.
- Special modes that bypass full execution.
- Backward compatibility.

### Add a new streaming subcommand

1) streaming_cli.py: define a new @streaming.command() with click options.
2) If you want to expose it through the main CLI:
   - cli.py → TauroCLI._handle_streaming_command(): branch on streaming_command and call your click_command.callback with appropriate kwargs.
3) Implement error handling using ValidationError/ExecutionError; return proper ExitCode on sys.exit.

Example:
```python
@streaming.command()
@click.option("--config", "-c", required=True)
@click.option("--execution-id", "-e", required=True)
def metrics(config: str, execution_id: str):
    try:
        context = _load_context_from_dsl(config)
        from tauro.exec.executor import PipelineExecutor
        executor = PipelineExecutor(context)
        data = executor.get_metrics(execution_id)  # implement in external exec
        click.echo(json.dumps(data, indent=2))
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(ExitCode.EXECUTION_ERROR.value)
```

Then wire it in cli.py:
```python
elif command == "metrics":
    return cmd_metrics.callback(config=parsed_args.streaming_config,
                                execution_id=parsed_args.execution_id)
```

### Add a new config format

- Implement a loader with the `ConfigLoaderProtocol` (load_config, get_format_name).
- Register it in ConfigManager.LOADERS and CONFIG_FILES (if needed).
- Update ConfigFormat enum if the format is first‑class.
- Ensure ConfigDiscovery looks for a settings_* index name that routes to your new format.

### Update or extend the template

- Only one template is shipped (Medallion Basic) to keep UX simple.
- To add new files or defaults, modify MedallionBasicTemplate methods in template.py.
- Ensure generated settings_* filename matches ConfigDiscovery’s expectations.

---

## Testing

Unit tests (suggestions):
- core.py
  - parse_iso_date and validate_date_range with valid/invalid cases.
  - SecurityValidator.validate_path positive/negative tests (use tmp dirs).
- config.py
  - ConfigDiscovery.discover against a temporary directory tree.
  - JSONConfigLoader/YAMLConfigLoader/DSLConfigLoader with sample files.
  - ConfigManager.detect format and change directory behavior.
  - AppConfigManager.get_env_config merges base + env and validates paths.
- execution.py
  - ContextInitializer with mocked AppConfigManager returning test paths.
  - PipelineExecutor.execute with a stub ExternalPipelineExecutor.
- streaming_cli.py
  - _list_all_pipelines_status normalization.
  - _fmt_ts and _fmt_seconds formatting.
- template.py
  - TemplateGenerator outputs expected files and folders.

Integration tests:
- Use the generated template to run a “dry run” pipeline and validate the CLI flow.

Mocking:
- Patch tauro.exec.executor.PipelineExecutor in tests to avoid real execution.
- Use tmp_path fixtures to isolate filesystem effects.

Example with pytest:
```python
def test_date_validation():
    from tauro.cli.core import parse_iso_date, validate_date_range, ValidationError
    assert parse_iso_date("2025-01-01") == "2025-01-01"
    with pytest.raises(ValidationError):
        parse_iso_date("2025/01/01")
    with pytest.raises(ValidationError):
        validate_date_range("2025-02-01", "2025-01-01")
```

---

## Performance and Reliability Tips

- Nodes should be idempotent where possible; rerunning should not corrupt outputs.
- Prefer deterministic outputs for testability.
- If using Spark:
  - Avoid collecting large data to the driver.
  - Use broadcast joins when appropriate.
  - Partition wisely for writes; configure vacuum where supported.
- For streaming:
  - Always set a checkpoint location and a suitable trigger.
  - Avoid heavy per‑record logging; prefer metrics.

---

## Debugging

- Use --verbose to get DEBUG logs and stack traces when handled.
- For unexpected exceptions, logger.exception is used sparingly to avoid noisy logs.
- When troubleshooting imports:
  - PathManager adds config_dir, src, lib, and parent/src into sys.path.
  - Use LoggerManager at DEBUG level to see added paths.
  - Check that package folders have __init__.py.

---

## Versioning and Releases

Recommended:
- Semantic Versioning (MAJOR.MINOR.PATCH).
- Update release notes/CHANGELOG for user‑visible changes.
- Keep template changes backward compatible where possible (avoid renaming settings_* index files).

---

## Common Pitfalls

- Skipping validate_path before chdir or reading config files.
- Adding argparse flags without updating CLIConfig/ConfigValidator.
- Calling click commands directly without passing required options to .callback.
- Assuming file ownership checks pass on non‑POSIX systems; guard platform specifics as in SecurityValidator.

---

## Glossary

- Context: In‑memory representation of configuration across global/pipelines/nodes/input/output.
- Node: A single step within a pipeline; typically a function in project code.
- Pipeline: An ordered (or DAG) set of nodes with inputs/outputs and dependencies.
- Settings index: The top‑level file that maps environments to config section files.

---

## Appendix: Exit Codes

- 0 SUCCESS
- 1 GENERAL_ERROR
- 2 CONFIGURATION_ERROR
- 3 VALIDATION_ERROR
- 4 EXECUTION_ERROR
- 5 DEPENDENCY_ERROR
- 6 SECURITY_ERROR

---
