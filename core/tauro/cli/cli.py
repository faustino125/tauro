"""Copyright (c) 2025 Tauro. All rights reserved.

This software is the proprietary intellectual property of Tauro and is
protected by copyright laws and international treaties. Any unauthorized
reproduction, distribution, modification, or other use of this software
is strictly prohibited without the express prior written consent of Tauro.

Licensed under a proprietary license with restricted commercial use. See
the license agreement for details.
"""

import argparse
import sys
from pathlib import Path
import traceback
from datetime import datetime
from typing import List, Optional, Union

from loguru import logger  # type: ignore

from tauro.cli.config import ConfigDiscovery, ConfigManager
from tauro.cli.core import (
    CLIConfig,
    ConfigCache,
    ExitCode,
    LoggerManager,
    TauroError,
    ValidationError,
    parse_iso_date,
    validate_date_range,
)
from tauro.cli.execution import ContextInitializer, PipelineExecutor
from tauro.cli.template import handle_template_command


class ArgumentParser:
    """Handles command-line argument parsing."""

    @staticmethod
    def create() -> argparse.ArgumentParser:
        """Create configured argument parser."""
        parser = argparse.ArgumentParser(
            prog="tauro",
            description="Tauro - Scalable Data Pipeline Execution Framework",
            epilog="""
            Examples:
            # Pipeline execution
            tauro --env dev --pipeline data_processing

            # Template generation
            tauro --template medallion_basic --project-name my_project
            tauro --template-interactive
            tauro --list-templates

            # Streaming commands
            tauro --streaming --streaming-command run --streaming-config config/streaming.py --streaming-pipeline real_time_processing
            tauro --streaming --streaming-command status --streaming-config config/streaming.py
            tauro --streaming --streaming-command stop --streaming-config config/streaming.py --execution-id pipeline-12345
            """,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        # Environment and pipeline
        parser.add_argument(
            "--env",
            choices=["base", "dev", "pre_prod", "prod"],
            help="Execution environment",
        )
        parser.add_argument("--pipeline", help="Pipeline name to execute")
        parser.add_argument("--node", help="Specific node to execute (optional)")

        # Date range
        parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
        parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

        # Configuration discovery
        parser.add_argument("--base-path", help="Base path for config discovery")
        parser.add_argument("--layer-name", help="Layer name for config discovery")
        parser.add_argument("--use-case", dest="use_case_name", help="Use case name")
        parser.add_argument(
            "--config-type",
            choices=["yaml", "json", "dsl"],
            help="Preferred configuration type",
        )
        parser.add_argument(
            "--interactive", action="store_true", help="Interactive config selection"
        )

        # Information commands
        parser.add_argument(
            "--list-configs", action="store_true", help="List discovered configs"
        )
        parser.add_argument(
            "--list-pipelines", action="store_true", help="List available pipelines"
        )
        parser.add_argument("--pipeline-info", help="Show pipeline information")
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear configuration discovery cache",
        )

        # Logging
        parser.add_argument(
            "--log-level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        parser.add_argument("--log-file", help="Custom log file path")
        parser.add_argument(
            "--verbose", action="store_true", help="Enable verbose output (DEBUG)"
        )
        parser.add_argument(
            "--quiet", action="store_true", help="Reduce output (ERROR only)"
        )

        # Template commands
        parser.add_argument("--template", help="Template type to generate")
        parser.add_argument("--project-name", help="Project name for template")
        parser.add_argument(
            "--output-path", help="Output path for generated template files"
        )
        parser.add_argument(
            "--format",
            choices=["yaml", "json", "dsl"],
            default="yaml",
            help="Config format for generated template",
        )
        parser.add_argument(
            "--no-sample-code",
            action="store_true",
            help="Do not include sample code in generated template",
        )
        parser.add_argument(
            "--list-templates", action="store_true", help="List available templates"
        )

        # Streaming toggle to delegate to streaming CLI if needed
        parser.add_argument(
            "--streaming", action="store_true", help="Use streaming subcommands"
        )
        parser.add_argument(
            "--streaming-command",
            choices=["run", "status", "stop"],
            help="Streaming command to execute",
        )
        parser.add_argument("--streaming-config", help="Streaming config path")
        parser.add_argument("--streaming-pipeline", help="Streaming pipeline name")
        parser.add_argument("--execution-id", help="Streaming execution id")
        parser.add_argument(
            "--streaming-mode",
            choices=["sync", "async"],
            default="async",
            help="Streaming execution mode",
        )
        parser.add_argument("--model-version", help="Model version for ML pipelines")
        parser.add_argument("--hyperparams", help="Hyperparameters as JSON string")

        # Dry-run / validate-only
        parser.add_argument(
            "--validate-only",
            action="store_true",
            help="Validate configuration without executing the pipeline",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Log actions without executing the pipeline",
        )

        return parser


def _normalize_and_validate_dates(parsed_args: argparse.Namespace) -> None:
    """Normalize and validate CLI date arguments in-place."""
    try:
        if getattr(parsed_args, "start_date", None):
            parsed_args.start_date = parse_iso_date(parsed_args.start_date)
        if getattr(parsed_args, "end_date", None):
            parsed_args.end_date = parse_iso_date(parsed_args.end_date)
        validate_date_range(parsed_args.start_date, parsed_args.end_date)
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Error validating dates: {e}")


class ConfigValidator:
    """Validates CLI configuration."""

    @staticmethod
    def validate(config: CLIConfig) -> None:
        """Validate CLI configuration for consistency."""
        if config.verbose and config.quiet:
            raise ValidationError("Cannot use both --verbose and --quiet")

        ConfigValidator._validate_streaming(config)
        ConfigValidator._validate_pipeline_execution(config)
        ConfigValidator._validate_dates(config)

    @staticmethod
    def _validate_streaming(config: CLIConfig) -> None:
        """Validate streaming-related configuration."""
        if config.streaming:
            if not config.streaming_config:
                raise ValidationError(
                    "--streaming-config required for streaming commands"
                )
            if config.streaming_command == "run" and not config.streaming_pipeline:
                raise ValidationError("--streaming-pipeline required for 'run' command")
            if config.streaming_command == "stop" and not config.execution_id:
                raise ValidationError("--execution-id required for this command")

    @staticmethod
    def _validate_pipeline_execution(config: CLIConfig) -> None:
        """Validate pipeline execution configuration."""
        special_modes = [
            config.list_configs,
            config.list_pipelines,
            bool(config.pipeline_info),
            config.clear_cache,
            config.streaming,  # Streaming handled separately
        ]
        if not any(special_modes):
            if not config.env:
                raise ValidationError("--env required for pipeline execution")
            if not config.pipeline:
                raise ValidationError("--pipeline required for pipeline execution")

    @staticmethod
    def _validate_dates(config: CLIConfig) -> None:
        """Validate date range configuration."""
        if config.start_date or config.end_date:
            sd = parse_iso_date(config.start_date) if config.start_date else None
            ed = parse_iso_date(config.end_date) if config.end_date else None
            validate_date_range(sd, ed)


class SpecialModeHandler:
    """Handles special CLI modes that don't require full pipeline execution."""

    def __init__(self):
        """Initialize special-mode handler state."""
        self.config_manager: Optional[ConfigManager] = None

    def handle(self, parsed_args) -> Optional[int]:
        """Handle special modes, return exit code if handled."""
        if getattr(parsed_args, "clear_cache", False):
            ConfigCache.clear()
            logger.info("Configuration cache cleared")
            return ExitCode.SUCCESS.value

        if getattr(parsed_args, "list_configs", False):
            discovery = ConfigDiscovery(getattr(parsed_args, "base_path", None))
            discovery.list_all()
            return ExitCode.SUCCESS.value

        try:
            self.config_manager = ConfigManager(
                base_path=getattr(parsed_args, "base_path", None),
                layer_name=getattr(parsed_args, "layer_name", None),
                use_case=getattr(parsed_args, "use_case_name", None),
                config_type=getattr(parsed_args, "config_type", None),
                interactive=getattr(parsed_args, "interactive", False),
            )
            self.config_manager.change_to_config_directory()
        except Exception as e:
            logger.error(f"Config initialization failed: {e}")
            return ExitCode.CONFIGURATION_ERROR.value

        if getattr(parsed_args, "list_pipelines", False):
            return self._handle_list_pipelines()

        if getattr(parsed_args, "pipeline_info", None):
            return self._handle_pipeline_info(parsed_args.pipeline_info)

        return None

    def _handle_list_pipelines(self) -> int:
        """List all available pipelines."""
        try:
            context_init = ContextInitializer(self.config_manager)
            context = context_init.initialize("dev")  # Use dev for listing

            executor = PipelineExecutor(
                context, self.config_manager.get_config_directory()
            )
            pipelines = executor.list_pipelines()

            if pipelines:
                logger.info("Available pipelines:")
                for pipeline in sorted(pipelines):
                    logger.info(f"  - {pipeline}")
            else:
                logger.warning("No pipelines found")

            return ExitCode.SUCCESS.value
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            return ExitCode.CONFIGURATION_ERROR.value

    def _handle_pipeline_info(self, pipeline_name: str) -> int:
        """Show information about specific pipeline."""
        try:
            context_init = ContextInitializer(self.config_manager)
            context = context_init.initialize("dev")

            executor = PipelineExecutor(
                context, self.config_manager.get_config_directory()
            )
            info = executor.get_pipeline_info(pipeline_name)

            logger.info(f"Pipeline: {pipeline_name}")
            logger.info(f"  Exists: {info['exists']}")
            logger.info(f"  Description: {info['description']}")
            if info["nodes"]:
                logger.info(f"  Nodes: {', '.join(info['nodes'])}")
            else:
                logger.info("  Nodes: None found")

            return ExitCode.SUCCESS.value
        except Exception as e:
            logger.error(f"Failed to get pipeline info: {e}")
            return ExitCode.CONFIGURATION_ERROR.value


class TauroCLI:
    """Main CLI application class."""

    def __init__(self):
        self.config: Optional[CLIConfig] = None
        self.config_manager: Optional[ConfigManager] = None

    def parse_arguments(
        self,
        args: Optional[List[str]] = None,
        parsed_args: Optional[argparse.Namespace] = None,
    ) -> CLIConfig:
        """Parse command line arguments into configuration object."""
        if parsed_args is None:
            parser = ArgumentParser.create()
            parsed = parser.parse_args(args)
        else:
            parsed = parsed_args

        base_path = (
            Path(parsed.base_path) if getattr(parsed, "base_path", None) else None
        )
        log_file = Path(parsed.log_file) if getattr(parsed, "log_file", None) else None
        output_path = (
            Path(parsed.output_path)
            if hasattr(parsed, "output_path") and parsed.output_path
            else None
        )
        streaming_config = (
            Path(parsed.streaming_config)
            if getattr(parsed, "streaming_config", None)
            else None
        )

        try:
            start_date = (
                parse_iso_date(parsed.start_date)
                if getattr(parsed, "start_date", None)
                else None
            )
        except Exception:
            start_date = parsed.start_date
        try:
            end_date = (
                parse_iso_date(parsed.end_date)
                if getattr(parsed, "end_date", None)
                else None
            )
        except Exception:
            end_date = parsed.end_date

        return CLIConfig(
            env=parsed.env or "",
            pipeline=parsed.pipeline or "",
            node=parsed.node,
            start_date=start_date,
            end_date=end_date,
            base_path=base_path,
            layer_name=parsed.layer_name,
            use_case_name=parsed.use_case_name,
            config_type=parsed.config_type,
            interactive=parsed.interactive,
            list_configs=parsed.list_configs,
            list_pipelines=parsed.list_pipelines,
            pipeline_info=parsed.pipeline_info,
            clear_cache=getattr(parsed, "clear_cache", False),
            log_level=parsed.log_level,
            log_file=log_file,
            validate_only=parsed.validate_only,
            dry_run=parsed.dry_run,
            verbose=parsed.verbose,
            quiet=parsed.quiet,
            streaming=parsed.streaming,
            streaming_command=parsed.streaming_command,
            streaming_config=streaming_config,
            streaming_pipeline=parsed.streaming_pipeline,
            execution_id=parsed.execution_id,
            streaming_mode=parsed.streaming_mode,
            model_version=parsed.model_version,
            hyperparams=parsed.hyperparams,
            output_path=output_path,
        )

    def run(self, args: Optional[List[str]] = None) -> int:
        """Main entry point for CLI execution."""
        parsed_args = None
        try:
            parsed_args = self._parse_and_setup_logger(args)

            result = self._handle_quick_commands(parsed_args)
            if result is not None:
                return result

            special_result = self._handle_special_modes(parsed_args)
            if special_result is not None:
                return special_result

            _normalize_and_validate_dates(parsed_args)

            self.config = self.parse_arguments(parsed_args=parsed_args)

            ConfigValidator.validate(self.config)

            logger.info("Starting Tauro CLI execution")
            logger.info(f"Environment: {self.config.env.upper()}")
            logger.info(f"Pipeline: {self.config.pipeline}")

            self._init_config_manager()
            context_init = ContextInitializer(self.config_manager)

            if self.config.validate_only:
                return self._handle_validate_only(context_init)
            return self._execute_pipeline(context_init)

        except TauroError as e:
            logger.error(f"Tauro error: {e}")
            if self.config and self.config.verbose:
                logger.debug(traceback.format_exc())
            return e.exit_code.value

        except KeyboardInterrupt:
            logger.warning("Execution interrupted by user")
            return ExitCode.GENERAL_ERROR.value

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if (self.config and self.config.verbose) or (
                parsed_args and getattr(parsed_args, "verbose", False)
            ):
                logger.debug(traceback.format_exc())
            return ExitCode.GENERAL_ERROR.value

        finally:
            if self.config_manager:
                try:
                    self.config_manager.restore_original_directory()
                except Exception:
                    pass
            ConfigCache.clear()

    def _parse_and_setup_logger(self, args: Optional[List[str]]) -> argparse.Namespace:
        """Parse arguments and setup logger."""
        parser = ArgumentParser.create()
        parsed_args = parser.parse_args(args)
        log_file = Path(parsed_args.log_file) if parsed_args.log_file else None
        LoggerManager.setup(
            level=parsed_args.log_level,
            log_file=log_file,
            verbose=parsed_args.verbose,
            quiet=parsed_args.quiet,
        )
        return parsed_args

    def _handle_quick_commands(self, parsed_args) -> Optional[int]:
        """Handle template and streaming commands."""
        if parsed_args.template or parsed_args.list_templates:
            return handle_template_command(parsed_args)
        if parsed_args.streaming:
            return self._handle_streaming_command(parsed_args)
        return None

    def _handle_special_modes(self, parsed_args) -> Optional[int]:
        """Handle special CLI modes."""
        special_handler = SpecialModeHandler()
        return special_handler.handle(parsed_args)

    def _init_config_manager(self):
        """Initialize configuration manager if not already set."""
        if not self.config_manager:
            self.config_manager = ConfigManager(
                base_path=self.config.base_path,
                layer_name=self.config.layer_name,
                use_case=self.config.use_case_name,
                config_type=self.config.config_type,
                interactive=self.config.interactive,
            )
            self.config_manager.change_to_config_directory()

    def _handle_streaming_command(self, parsed_args) -> int:
        """Execute streaming pipeline commands."""
        try:
            from tauro.cli.streaming_cli import (
                run_cli_impl,
                status_cli_impl,
                stop_cli_impl,
            )

            command = parsed_args.streaming_command or "run"

            if command == "run":
                return self._handle_streaming_run(run_cli_impl, parsed_args)
            elif command == "status":
                return self._handle_streaming_status(status_cli_impl, parsed_args)
            elif command == "stop":
                return self._handle_streaming_stop(stop_cli_impl, parsed_args)
            else:
                raise ValidationError(f"Unknown streaming command: {command}")

        except SystemExit:
            raise
        except Exception as e:
            logger.error(f"Streaming command failed: {e}")
            if getattr(parsed_args, "verbose", False):
                logger.debug(traceback.format_exc())
            return ExitCode.EXECUTION_ERROR.value

    def _handle_streaming_run(self, run_impl, parsed_args) -> int:
        """Handle streaming 'run' command."""
        if not parsed_args.streaming_pipeline:
            raise ValidationError("--streaming-pipeline required for 'run' command")
        streaming_config = (
            Path(parsed_args.streaming_config) if parsed_args.streaming_config else None
        )
        result = run_impl(
            config=streaming_config,
            pipeline=parsed_args.streaming_pipeline,
            mode=parsed_args.streaming_mode,
            model_version=parsed_args.model_version,
            hyperparams=parsed_args.hyperparams,
        )
        return result if isinstance(result, int) else ExitCode.SUCCESS.value

    def _handle_streaming_status(self, status_impl, parsed_args) -> int:
        """Handle streaming 'status' command."""
        streaming_config = (
            Path(parsed_args.streaming_config) if parsed_args.streaming_config else None
        )
        result = status_impl(
            config=streaming_config,
            execution_id=parsed_args.execution_id,
            format="table",
        )
        return result if isinstance(result, int) else ExitCode.SUCCESS.value

    def _handle_streaming_stop(self, stop_impl, parsed_args) -> int:
        """Handle streaming 'stop' command."""
        if not parsed_args.execution_id:
            raise ValidationError("--execution-id required for 'stop' command")
        streaming_config = (
            Path(parsed_args.streaming_config) if parsed_args.streaming_config else None
        )
        result = stop_impl(
            config=streaming_config,
            execution_id=parsed_args.execution_id,
            timeout=60,
        )
        return result if isinstance(result, int) else ExitCode.SUCCESS.value

    def _extract_exit_code(self, se: SystemExit) -> int:
        """Extract exit code from SystemExit exception."""
        try:
            code = int(getattr(se, "code", ExitCode.GENERAL_ERROR.value))
        except Exception:
            code = ExitCode.GENERAL_ERROR.value
        return code

    def _handle_validate_only(self, context_init: ContextInitializer) -> int:
        """Handle validation-only mode."""
        logger.info("Validating configuration...")
        context = context_init.initialize(self.config.env)
        logger.success("Configuration validation successful")

        executor = PipelineExecutor(context, self.config_manager.get_config_directory())
        summary = executor.get_execution_summary()

        logger.info("Execution Summary:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")

        return ExitCode.SUCCESS.value

    def _execute_pipeline(self, context_init: ContextInitializer) -> int:
        """Execute the specified pipeline."""
        context = context_init.initialize(self.config.env)

        executor = PipelineExecutor(context, self.config_manager.get_config_directory())

        if not executor.validate_pipeline(self.config.pipeline):
            available = executor.list_pipelines()
            if available:
                logger.error(f"Pipeline '{self.config.pipeline}' not found")
                logger.info(f"Available: {', '.join(available)}")
            else:
                logger.warning("Could not validate pipeline existence")

        if self.config.node and not executor.validate_node(
            self.config.pipeline, self.config.node
        ):
            logger.warning(
                f"Node '{self.config.node}' may not exist in pipeline '{self.config.pipeline}'"
            )

        executor.execute(
            pipeline_name=self.config.pipeline,
            node_name=self.config.node,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            dry_run=self.config.dry_run,
        )

        logger.success("Tauro CLI execution completed successfully")
        return ExitCode.SUCCESS.value


def main() -> int:
    """Main entry point for Tauro CLI application."""
    cli = TauroCLI()
    return cli.run()


if __name__ == "__main__":
    sys.exit(main())
