"""
Tests for streaming pipeline execution functionality.

Tests cover:
- Stream run basic execution
- Stream status tracking
- Stream stop operations
- Error handling in streaming operations
- Execution ID tracking
- Format output validation
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import argparse
from core.cli.cli import run_cli_impl, status_cli_impl, stop_cli_impl
from core.cli.core import ExitCode, ValidationError


class TestStreamRunExecution:
    """Test stream run command execution."""

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_run_success(self, mock_executor_class, mock_load_context):
        """Test successful stream run execution."""
        # Mock context
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        # Mock executor
        mock_executor = MagicMock()
        mock_executor.run_streaming_pipeline.return_value = "exec_12345"
        mock_executor_class.return_value = mock_executor

        # Execute
        result = run_cli_impl(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            model_version=None,
            hyperparams=None,
        )

        # Verify
        assert result == ExitCode.SUCCESS.value
        mock_executor.run_streaming_pipeline.assert_called_once()

    @patch("core.cli.cli._load_context_from_dsl")
    def test_stream_run_context_load_failure(self, mock_load_context):
        """Test stream run with context loading failure."""
        # Mock failure
        mock_load_context.side_effect = Exception("Config not found")

        # Execute
        result = run_cli_impl(
            config="missing.py",
            pipeline="test_pipeline",
            mode="async",
            model_version=None,
            hyperparams=None,
        )

        # Should return error code
        assert result == ExitCode.GENERAL_ERROR.value

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_run_with_hyperparams(self, mock_executor_class, mock_load_context):
        """Test stream run with hyperparameters."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor.run_streaming_pipeline.return_value = "exec_12345"
        mock_executor_class.return_value = mock_executor

        # Execute with hyperparams
        hyperparams_json = '{"learning_rate": 0.01, "epochs": 100}'
        result = run_cli_impl(
            config="config.py",
            pipeline="ml_pipeline",
            mode="async",
            model_version=None,
            hyperparams=hyperparams_json,
        )

        # Verify hyperparams were parsed
        assert result == ExitCode.SUCCESS.value
        call_kwargs = mock_executor.run_streaming_pipeline.call_args[1]
        assert call_kwargs["hyperparams"] == {"learning_rate": 0.01, "epochs": 100}

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_run_invalid_hyperparams_json(self, mock_executor_class, mock_load_context):
        """Test stream run with malformed hyperparams JSON."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        # Execute with invalid JSON
        result = run_cli_impl(
            config="config.py",
            pipeline="ml_pipeline",
            mode="async",
            model_version=None,
            hyperparams='{"invalid": json}',  # Invalid JSON
        )

        # Should return validation error
        assert result == ExitCode.VALIDATION_ERROR.value


class TestStreamStatusExecution:
    """Test stream status command execution."""

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_status_single_pipeline(self, mock_executor_class, mock_load_context):
        """Test getting status of single pipeline."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_status = {
            "execution_id": "exec_12345",
            "pipeline_name": "test_pipeline",
            "state": "running",
        }
        mock_executor.get_streaming_pipeline_status.return_value = mock_status
        mock_executor_class.return_value = mock_executor

        # Execute
        result = status_cli_impl(
            config="config.py",
            execution_id="exec_12345",
            format="table",
        )

        # Verify
        assert result == ExitCode.SUCCESS.value
        mock_executor.get_streaming_pipeline_status.assert_called_once_with("exec_12345")

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    @patch("core.cli.cli._list_all_pipelines_status")
    def test_stream_status_all_pipelines(
        self, mock_list_all, mock_executor_class, mock_load_context
    ):
        """Test getting status of all pipelines."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        mock_status_list = [
            {"execution_id": "exec_1", "state": "running"},
            {"execution_id": "exec_2", "state": "stopped"},
        ]
        mock_list_all.return_value = mock_status_list

        # Execute without execution_id
        result = status_cli_impl(
            config="config.py",
            execution_id=None,
            format="table",
        )

        # Verify
        assert result == ExitCode.SUCCESS.value

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_status_not_found(self, mock_executor_class, mock_load_context):
        """Test status for non-existent pipeline."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor.get_streaming_pipeline_status.return_value = None
        mock_executor_class.return_value = mock_executor

        # Execute
        result = status_cli_impl(
            config="config.py",
            execution_id="nonexistent_exec",
            format="table",
        )

        # Should return validation error
        assert result == ExitCode.VALIDATION_ERROR.value

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_status_json_format(self, mock_executor_class, mock_load_context):
        """Test status output in JSON format."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_status = {
            "execution_id": "exec_12345",
            "pipeline_name": "test_pipeline",
            "state": "running",
        }
        mock_executor.get_streaming_pipeline_status.return_value = mock_status
        mock_executor_class.return_value = mock_executor

        # Execute with JSON format
        result = status_cli_impl(
            config="config.py",
            execution_id="exec_12345",
            format="json",
        )

        # Should succeed
        assert result == ExitCode.SUCCESS.value


class TestStreamStopExecution:
    """Test stream stop command execution."""

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_stop_success(self, mock_executor_class, mock_load_context):
        """Test successful pipeline stop."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor.stop_streaming_pipeline.return_value = True
        mock_executor_class.return_value = mock_executor

        # Execute
        result = stop_cli_impl(
            config="config.py",
            execution_id="exec_12345",
            timeout=60,
        )

        # Verify
        assert result == ExitCode.SUCCESS.value
        mock_executor.stop_streaming_pipeline.assert_called_once_with("exec_12345", 60)

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_stop_timeout(self, mock_executor_class, mock_load_context):
        """Test pipeline stop timeout."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor.stop_streaming_pipeline.return_value = False
        mock_executor_class.return_value = mock_executor

        # Execute
        result = stop_cli_impl(
            config="config.py",
            execution_id="exec_12345",
            timeout=30,
        )

        # Should return execution error
        assert result == ExitCode.EXECUTION_ERROR.value

    @patch("core.cli.cli._load_context_from_dsl")
    def test_stream_stop_context_failure(self, mock_load_context):
        """Test stop with context loading failure."""
        # Mock failure
        mock_load_context.side_effect = Exception("Config error")

        # Execute
        result = stop_cli_impl(
            config="missing.py",
            execution_id="exec_12345",
            timeout=60,
        )

        # Should return error code
        assert result == ExitCode.GENERAL_ERROR.value


class TestStreamExecutionErrorHandling:
    """Test error handling in streaming operations."""

    @patch("core.cli.cli._load_context_from_dsl")
    def test_stream_run_validation_error_in_context(self, mock_load_context):
        """Test validation error during context loading."""
        from core.cli.core import ValidationError

        # Mock validation error
        mock_load_context.side_effect = ValidationError("Invalid config")

        # Execute
        result = run_cli_impl(
            config="bad_config.py",
            pipeline="test_pipeline",
            mode="async",
            model_version=None,
            hyperparams=None,
        )

        # Should handle gracefully
        assert result == ExitCode.GENERAL_ERROR.value

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_stream_run_executor_exception(self, mock_executor_class, mock_load_context):
        """Test executor throwing exception."""
        # Mock context and executor
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        mock_executor = MagicMock()
        mock_executor.run_streaming_pipeline.side_effect = RuntimeError("Executor error")
        mock_executor_class.return_value = mock_executor

        # Execute
        result = run_cli_impl(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            model_version=None,
            hyperparams=None,
        )

        # Should handle gracefully
        assert result == ExitCode.GENERAL_ERROR.value


class TestStreamExecutionIntegration:
    """Integration tests for streaming operations."""

    @patch("core.cli.cli._load_context_from_dsl")
    @patch("core.cli.exec.executor.PipelineExecutor")
    def test_run_and_check_status_workflow(self, mock_executor_class, mock_load_context):
        """Test typical workflow: run pipeline, then check status."""
        # Mock context
        mock_context = MagicMock()
        mock_load_context.return_value = mock_context

        # Mock executor
        mock_executor = MagicMock()
        execution_id = "exec_workflow_12345"

        # First call: run
        mock_executor.run_streaming_pipeline.return_value = execution_id

        # Second call: status
        mock_executor.get_streaming_pipeline_status.return_value = {
            "execution_id": execution_id,
            "pipeline_name": "test_pipeline",
            "state": "completed",
        }

        mock_executor_class.return_value = mock_executor

        # Run pipeline
        run_result = run_cli_impl(
            config="config.py",
            pipeline="test_pipeline",
            mode="async",
            model_version=None,
            hyperparams=None,
        )
        assert run_result == ExitCode.SUCCESS.value

        # Check status
        status_result = status_cli_impl(
            config="config.py",
            execution_id=execution_id,
            format="table",
        )
        assert status_result == ExitCode.SUCCESS.value
