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
import traceback
from datetime import datetime
from typing import List, Optional

from loguru import logger  # type: ignore

from tauro.cli.config import ConfigDiscovery, ConfigManager
from tauro.cli.core import (
    CLIConfig,
    ConfigCache,
    ExitCode,
    LoggerManager,
    TauroError,
    ValidationError,
)
from tauro.cli.execution import ContextInitializer, PipelineExecutor


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
  tauro --env dev --pipeline data_processing
  tauro --env prod --pipeline etl --node transform_data
  tauro --env dev --pipeline test --layer-name golden_layer
  tauro --env dev --pipeline clustering --use-case clustering_analysis
  tauro --list-configs
  tauro --env dev --pipeline demo --interactive
  tauro --env dev --pipeline test --config-type yaml --dry-run
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

        # Logging
        parser.add_argument(
            "--log-level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Logging level",
        )
        parser.add_argument("--log-file", help="Custom log file path")
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="Verbose output"
        )
        parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode")

        # Execution modes
        parser.add_argument(
            "--validate-only", action="store_true", help="Validate config only"
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Show execution plan"
        )
        parser.add_argument(
            "--clear-cache", action="store_true", help="Clear config cache"
        )

        # Version
        parser.add_argument("--version", action="version", version="Tauro CLI v2.1.0")

        return parser


class ConfigValidator:
    """Validates CLI configuration."""

    @staticmethod
    def validate(config: CLIConfig) -> None:
        """Validate CLI configuration for consistency."""
        if config.verbose and config.quiet:
            raise ValidationError("Cannot use both --verbose and --quiet")

        # Check if this is a special mode that doesn't need full config
        special_modes = [
            config.list_configs,
            hasattr(config, "list_pipelines") and getattr(config, "list_pipelines"),
            hasattr(config, "clear_cache") and getattr(config, "clear_cache"),
            hasattr(config, "pipeline_info") and getattr(config, "pipeline_info"),
        ]

        if not any(special_modes):
            if not config.env:
                raise ValidationError("--env required for pipeline execution")
            if not config.pipeline:
                raise ValidationError("--pipeline required for pipeline execution")

        # Validate date range
        if config.start_date and config.end_date:
            try:
                start = datetime.strptime(config.start_date, "%Y-%m-%d")
                end = datetime.strptime(config.end_date, "%Y-%m-%d")
                if start > end:
                    raise ValidationError("Start date must be before end date")
            except ValueError:
                raise ValidationError("Invalid date format. Use YYYY-MM-DD")


class SpecialModeHandler:
    """Handles special CLI modes that don't require full pipeline execution."""

    def __init__(self):
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

        # Initialize config manager for pipeline-related commands
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

    def parse_arguments(self, args: Optional[List[str]] = None) -> CLIConfig:
        """Parse command line arguments into configuration object."""
        parser = ArgumentParser.create()
        parsed = parser.parse_args(args)

        return CLIConfig(
            env=parsed.env or "",
            pipeline=parsed.pipeline or "",
            node=parsed.node,
            start_date=getattr(parsed, "start_date", None),
            end_date=getattr(parsed, "end_date", None),
            base_path=getattr(parsed, "base_path", None),
            layer_name=getattr(parsed, "layer_name", None),
            use_case_name=getattr(parsed, "use_case_name", None),
            config_type=getattr(parsed, "config_type", None),
            interactive=getattr(parsed, "interactive", False),
            list_configs=getattr(parsed, "list_configs", False),
            log_level=parsed.log_level,
            log_file=getattr(parsed, "log_file", None),
            validate_only=parsed.validate_only,
            dry_run=parsed.dry_run,
            verbose=parsed.verbose,
            quiet=parsed.quiet,
        )

    def run(self, args: Optional[List[str]] = None) -> int:
        """Main entry point for CLI execution."""
        parsed_args = None

        try:
            # Parse arguments for logger setup
            parser = ArgumentParser.create()
            parsed_args = parser.parse_args(args)

            LoggerManager.setup(
                level=getattr(parsed_args, "log_level", "INFO"),
                log_file=getattr(parsed_args, "log_file", None),
                verbose=getattr(parsed_args, "verbose", False),
                quiet=getattr(parsed_args, "quiet", False),
            )

            # Handle special modes first
            special_handler = SpecialModeHandler()
            special_result = special_handler.handle(parsed_args)
            if special_result is not None:
                return special_result

            # Parse and validate full configuration
            self.config = self.parse_arguments(args)
            ConfigValidator.validate(self.config)

            logger.info("Starting Tauro CLI execution")
            logger.info(f"Environment: {self.config.env.upper()}")
            logger.info(f"Pipeline: {self.config.pipeline}")

            # Initialize configuration manager
            if not self.config_manager:
                self.config_manager = ConfigManager(
                    base_path=self.config.base_path,
                    layer_name=self.config.layer_name,
                    use_case=self.config.use_case_name,
                    config_type=self.config.config_type,
                    interactive=self.config.interactive,
                )
                self.config_manager.change_to_config_directory()

            # Initialize context
            context_init = ContextInitializer(self.config_manager)

            if self.config.validate_only:
                return self._handle_validate_only(context_init)

            # Execute pipeline
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
                self.config_manager.restore_original_directory()
            ConfigCache.clear()

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

        # Validate pipeline exists
        if not executor.validate_pipeline(self.config.pipeline):
            available = executor.list_pipelines()
            if available:
                logger.error(f"Pipeline '{self.config.pipeline}' not found")
                logger.info(f"Available: {', '.join(available)}")
            else:
                logger.warning("Could not validate pipeline existence")

        # Validate node exists if specified
        if self.config.node and not executor.validate_node(
            self.config.pipeline, self.config.node
        ):
            logger.warning(
                f"Node '{self.config.node}' may not exist in pipeline '{self.config.pipeline}'"
            )

        # Execute pipeline
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
