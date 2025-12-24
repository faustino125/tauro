"""
Tests for CLI argument validation functionality.

Tests cover:
- Stream run argument validation
- Stream status argument validation
- Stream stop argument validation
- Run command argument validation
- Template argument validation
- Config argument validation
- Error handling and user-friendly messages
"""
import pytest
import argparse
import tempfile
from pathlib import Path
from core.cli.cli import (
    validate_stream_run_arguments,
    validate_stream_status_arguments,
    validate_stream_stop_arguments,
    validate_run_arguments,
    validate_template_arguments,
    validate_config_arguments,
)
from core.cli.core import ValidationError


class TestStreamRunValidation:
    """Test stream run subcommand argument validation."""

    def test_valid_stream_run_arguments(self):
        """Test that valid arguments don't raise errors."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            hyperparams=None,
            model_version=None,
        )
        # Should not raise
        validate_stream_run_arguments(args)

    def test_valid_stream_run_with_hyperparams(self):
        """Test stream run with valid JSON hyperparams."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            hyperparams='{"param1": "value1"}',
            model_version=None,
        )
        # Should not raise
        validate_stream_run_arguments(args)

    def test_valid_stream_run_with_model_version(self):
        """Test stream run with model version."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="ml_pipeline",
            mode="async",
            hyperparams=None,
            model_version="v1.0.0",
        )
        # Should not raise
        validate_stream_run_arguments(args)

    def test_invalid_mode(self):
        """Test that invalid mode raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="invalid_mode",
            hyperparams=None,
            model_version=None,
        )
        with pytest.raises(ValidationError, match="Invalid mode"):
            validate_stream_run_arguments(args)

    def test_invalid_hyperparams_json(self):
        """Test that malformed JSON hyperparams raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            hyperparams='{"invalid": json}',  # Missing quotes
            model_version=None,
        )
        with pytest.raises(ValidationError, match="Invalid hyperparams JSON"):
            validate_stream_run_arguments(args)

    def test_sync_mode(self):
        """Test that sync mode is valid."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="sync",
            hyperparams=None,
            model_version=None,
        )
        # Should not raise
        validate_stream_run_arguments(args)

    def test_valid_log_level(self):
        """Test that valid log levels are accepted."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            args = argparse.Namespace(
                config="config.py",
                pipeline="test_pipeline",
                mode="async",
                hyperparams=None,
                model_version=None,
                log_level=level,
            )
            # Should not raise
            validate_stream_run_arguments(args)

    def test_invalid_log_level(self):
        """Test that invalid log level raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            hyperparams=None,
            model_version=None,
            log_level="INVALID",
        )
        with pytest.raises(ValidationError, match="Invalid log-level"):
            validate_stream_run_arguments(args)


class TestStreamStatusValidation:
    """Test stream status subcommand argument validation."""

    def test_valid_status_arguments(self):
        """Test that valid arguments don't raise errors."""
        args = argparse.Namespace(
            config="config.py",
            execution_id=None,
            format="table",
        )
        # Should not raise
        validate_stream_status_arguments(args)

    def test_valid_status_with_execution_id(self):
        """Test status with specific execution ID."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            format="table",
        )
        # Should not raise
        validate_stream_status_arguments(args)

    def test_valid_status_json_format(self):
        """Test status with JSON format."""
        args = argparse.Namespace(
            config="config.py",
            execution_id=None,
            format="json",
        )
        # Should not raise
        validate_stream_status_arguments(args)

    def test_invalid_format(self):
        """Test that invalid format raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            execution_id=None,
            format="csv",
        )
        with pytest.raises(ValidationError, match="Invalid format"):
            validate_stream_status_arguments(args)


class TestStreamStopValidation:
    """Test stream stop subcommand argument validation."""

    def test_valid_stop_arguments(self):
        """Test that valid arguments don't raise errors."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            timeout=60,
        )
        # Should not raise
        validate_stream_stop_arguments(args)

    def test_valid_stop_with_custom_timeout(self):
        """Test stop with custom timeout."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            timeout=120,
        )
        # Should not raise
        validate_stream_stop_arguments(args)

    def test_invalid_zero_timeout(self):
        """Test that zero timeout raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            timeout=0,
        )
        with pytest.raises(ValidationError, match="Timeout must be positive"):
            validate_stream_stop_arguments(args)

    def test_invalid_negative_timeout(self):
        """Test that negative timeout raises ValidationError."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            timeout=-30,
        )
        with pytest.raises(ValidationError, match="Timeout must be positive"):
            validate_stream_stop_arguments(args)

    def test_very_long_timeout_warning(self, caplog):
        """Test that very long timeout generates warning."""
        args = argparse.Namespace(
            config="config.py",
            execution_id="exec_12345",
            timeout=7200,  # 2 hours
        )
        # Should not raise, but may log warning
        validate_stream_stop_arguments(args)


class TestRunValidation:
    """Test run subcommand argument validation."""

    def test_valid_run_arguments(self):
        """Test that valid run arguments don't raise errors."""
        args = argparse.Namespace(
            env="dev",
            pipeline="data_pipeline",
            start_date=None,
            end_date=None,
            validate_only=False,
            dry_run=False,
        )
        # Should not raise
        validate_run_arguments(args)

    def test_missing_env(self):
        """Test that missing env raises ValidationError."""
        args = argparse.Namespace(
            env=None,
            pipeline="data_pipeline",
            start_date=None,
            end_date=None,
            validate_only=False,
            dry_run=False,
        )
        with pytest.raises(ValidationError, match="--env is required"):
            validate_run_arguments(args)

    def test_missing_pipeline(self):
        """Test that missing pipeline raises ValidationError."""
        args = argparse.Namespace(
            env="dev",
            pipeline=None,
            start_date=None,
            end_date=None,
            validate_only=False,
            dry_run=False,
        )
        with pytest.raises(ValidationError, match="--pipeline is required"):
            validate_run_arguments(args)

    def test_valid_with_dates(self):
        """Test run with valid date range."""
        args = argparse.Namespace(
            env="dev",
            pipeline="data_pipeline",
            start_date="2025-01-01",
            end_date="2025-12-31",
            validate_only=False,
            dry_run=False,
        )
        # Should not raise
        validate_run_arguments(args)

    def test_conflicting_validate_and_dry_run(self, caplog):
        """Test warning when both validate-only and dry-run are set."""
        args = argparse.Namespace(
            env="dev",
            pipeline="data_pipeline",
            start_date=None,
            end_date=None,
            validate_only=True,
            dry_run=True,
        )
        # Should not raise, but may log warning
        validate_run_arguments(args)


class TestTemplateValidation:
    """Test template subcommand argument validation."""

    def test_valid_template_generation(self, tmp_path):
        """Test valid template generation arguments."""
        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name="my_project",
            output_path=str(tmp_path / "my_project"),
            format="yaml",
        )
        # Should not raise
        validate_template_arguments(args)

    def test_list_templates_no_other_args_required(self):
        """Test that list-templates doesn't require other arguments."""
        args = argparse.Namespace(
            list_templates=True,
            template=None,
            project_name=None,
        )
        # Should not raise
        validate_template_arguments(args)

    def test_missing_template(self):
        """Test that missing template raises ValidationError."""
        args = argparse.Namespace(
            list_templates=False,
            template=None,
            project_name="my_project",
            output_path=str(Path(tempfile.gettempdir()) / "my_project"),
            format="yaml",
        )
        with pytest.raises(ValidationError, match="--template is required"):
            validate_template_arguments(args)

    def test_missing_project_name(self):
        """Test that missing project name raises ValidationError."""
        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name=None,
            output_path=str(Path(tempfile.gettempdir()) / "my_project"),
            format="yaml",
        )
        with pytest.raises(ValidationError, match="--project-name is required"):
            validate_template_arguments(args)

    def test_invalid_project_name_with_spaces(self):
        """Test that project name with spaces raises ValidationError."""
        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name="my project with spaces",
            output_path=str(Path(tempfile.gettempdir()) / "my_project"),
            format="yaml",
        )
        with pytest.raises(ValidationError, match="Project name must contain only"):
            validate_template_arguments(args)

    def test_invalid_project_name_with_special_chars(self):
        """Test that project name with special chars raises ValidationError."""
        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name="my-project!",
            output_path=str(Path(tempfile.gettempdir()) / "my_project"),
            format="yaml",
        )
        with pytest.raises(ValidationError, match="Project name must contain only"):
            validate_template_arguments(args)

    def test_valid_project_names(self):
        """Test various valid project names."""
        valid_names = [
            "my_project",
            "my-project",
            "MyProject",
            "project123",
            "project_v2",
            "pipeline-ml-v1",
        ]
        for name in valid_names:
            args = argparse.Namespace(
                list_templates=False,
                template="medallion_basic",
                project_name=name,
                output_path=str(Path(tempfile.gettempdir()) / name),
                format="yaml",
            )
            # Should not raise
            validate_template_arguments(args)

    def test_invalid_format(self):
        """Test that invalid format raises ValidationError."""
        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name="my_project",
            output_path=str(Path(tempfile.gettempdir()) / "my_project"),
            format="xml",
        )
        with pytest.raises(ValidationError, match="Invalid format"):
            validate_template_arguments(args)

    def test_existing_output_path(self, tmp_path):
        """Test that existing non-empty output path raises ValidationError."""
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()
        (existing_dir / "file.txt").write_text("content")

        args = argparse.Namespace(
            list_templates=False,
            template="medallion_basic",
            project_name="my_project",
            output_path=str(existing_dir),
            format="yaml",
        )
        with pytest.raises(ValidationError, match="already exists and is not empty"):
            validate_template_arguments(args)


class TestConfigValidation:
    """Test config subcommand argument validation."""

    def test_valid_config_command(self):
        """Test that valid config command doesn't raise errors."""
        args = argparse.Namespace(
            config_command="list-configs",
        )
        # Should not raise
        validate_config_arguments(args)

    def test_valid_list_pipelines(self):
        """Test list-pipelines config command."""
        args = argparse.Namespace(
            config_command="list-pipelines",
        )
        # Should not raise
        validate_config_arguments(args)

    def test_missing_config_command(self):
        """Test that missing config command raises ValidationError."""
        args = argparse.Namespace(
            config_command=None,
        )
        with pytest.raises(ValidationError, match="A config subcommand is required"):
            validate_config_arguments(args)
