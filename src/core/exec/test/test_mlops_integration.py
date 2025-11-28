"""
Tests for MLOps-Executor integration.
"""

import tempfile
from pathlib import Path
import pytest  # type: ignore

from core.exec.mlops_integration import (
    MLOpsExecutorIntegration,
    MLInfoConfigLoader,
)
from core.mlops.config import MLOpsContext
from core.mlops.experiment_tracking import RunStatus


class TestMLOpsExecutorIntegration:
    """Tests for MLOpsExecutorIntegration."""

    @pytest.fixture
    def mlops_context(self):
        """Create MLOps context for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield MLOpsContext(backend_type="local", storage_path=tmpdir)

    @pytest.fixture
    def integration(self, mlops_context):
        """Create integration instance."""
        return MLOpsExecutorIntegration(mlops_context=mlops_context, auto_init=False)

    def test_create_experiment(self, integration):
        """Test experiment creation."""
        exp_id = integration.create_pipeline_experiment(
            pipeline_name="test_pipeline",
            pipeline_type="BATCH",
            description="Test experiment",
        )

        assert exp_id is not None
        assert integration.active_experiment_id == exp_id

    def test_start_run(self, integration):
        """Test starting a run."""
        exp_id = integration.create_pipeline_experiment(
            pipeline_name="test_pipeline",
            pipeline_type="BATCH",
        )

        run_id = integration.start_pipeline_run(
            experiment_id=exp_id,
            pipeline_name="test_pipeline",
            hyperparams={"lr": 0.01},
        )

        assert run_id is not None
        assert integration.active_run_id == run_id
        assert integration.pipeline_runs["test_pipeline"] == run_id

    def test_log_node_execution(self, integration):
        """Test logging node execution."""
        exp_id = integration.create_pipeline_experiment(
            pipeline_name="test_pipeline",
            pipeline_type="BATCH",
        )

        run_id = integration.start_pipeline_run(
            experiment_id=exp_id,
            pipeline_name="test_pipeline",
        )

        # Should not raise
        integration.log_node_execution(
            run_id=run_id,
            node_name="feature_engineering",
            status="completed",
            duration_seconds=10.5,
            metrics={"features_created": 25},
        )

    def test_log_artifact(self, integration):
        """Test logging artifact."""
        with tempfile.TemporaryDirectory() as tmpdir:
            exp_id = integration.create_pipeline_experiment(
                pipeline_name="test_pipeline",
                pipeline_type="BATCH",
            )

            run_id = integration.start_pipeline_run(
                experiment_id=exp_id,
                pipeline_name="test_pipeline",
            )

            # Create dummy artifact
            artifact_path = Path(tmpdir) / "model.pkl"
            artifact_path.write_text("dummy model")

            artifact_uri = integration.log_artifact(
                run_id=run_id,
                artifact_path=str(artifact_path),
                artifact_type="model",
            )

            assert artifact_uri is not None
            assert run_id in integration.node_artifacts
            assert artifact_uri in integration.node_artifacts[run_id]

    def test_end_run(self, integration):
        """Test ending a run."""
        exp_id = integration.create_pipeline_experiment(
            pipeline_name="test_pipeline",
            pipeline_type="BATCH",
        )

        run_id = integration.start_pipeline_run(
            experiment_id=exp_id,
            pipeline_name="test_pipeline",
        )

        integration.end_pipeline_run(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            summary={"total_nodes": 5},
        )

        assert integration.active_run_id is None

    def test_register_model_from_run(self, integration):
        """Test registering model from run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            exp_id = integration.create_pipeline_experiment(
                pipeline_name="test_pipeline",
                pipeline_type="BATCH",
            )

            run_id = integration.start_pipeline_run(
                experiment_id=exp_id,
                pipeline_name="test_pipeline",
            )

            # Create dummy model
            model_path = Path(tmpdir) / "model.pkl"
            model_path.write_text("dummy model")

            # Should not raise
            integration.register_model_from_run(
                run_id=run_id,
                model_name="test_model",
                artifact_path=str(model_path),
                artifact_type="sklearn",
                framework="scikit-learn",
                hyperparams={"n_estimators": 100},
                metrics={"accuracy": 0.95},
            )

    def test_get_run_comparison(self, integration):
        """Test getting run comparison."""
        exp_id = integration.create_pipeline_experiment(
            pipeline_name="test_pipeline",
            pipeline_type="BATCH",
        )

        run1_id = integration.start_pipeline_run(
            experiment_id=exp_id,
            pipeline_name="test_pipeline",
            hyperparams={"lr": 0.01},
        )

        integration.log_pipeline_metrics(run1_id, {"accuracy": 0.90})
        integration.end_pipeline_run(run1_id, RunStatus.COMPLETED)

        run2_id = integration.start_pipeline_run(
            experiment_id=exp_id,
            pipeline_name="test_pipeline",
            hyperparams={"lr": 0.001},
        )

        integration.log_pipeline_metrics(run2_id, {"accuracy": 0.95})
        integration.end_pipeline_run(run2_id, RunStatus.COMPLETED)

        # Compare
        comparison_df = integration.get_run_comparison(exp_id)
        assert comparison_df is not None
        assert len(comparison_df) == 2


class TestMLInfoConfigLoader:
    """Tests for MLInfoConfigLoader."""

    def test_load_from_yaml(self):
        """Test loading ML info from YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_file = Path(tmpdir) / "ml_config.yml"
            yaml_file.write_text(
                """
experiment:
  name: test_exp
model:
  name: test_model
hyperparams:
  lr: 0.01
  batch_size: 32
"""
            )

            ml_info = MLInfoConfigLoader.load_ml_info_from_file(str(yaml_file))
            assert ml_info["experiment"]["name"] == "test_exp"
            assert ml_info["hyperparams"]["lr"] == pytest.approx(0.01)

    def test_load_from_json(self):
        """Test loading ML info from JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_file = Path(tmpdir) / "ml_config.json"
            json_file.write_text(
                """
{
  "experiment": {"name": "test_exp"},
  "hyperparams": {"lr": 0.01}
}
"""
            )

            ml_info = MLInfoConfigLoader.load_ml_info_from_file(str(json_file))
            assert ml_info["experiment"]["name"] == "test_exp"
            assert ml_info["hyperparams"]["lr"] == pytest.approx(0.01)

    def test_merge_ml_info(self):
        """Test merging ML info."""
        base = {
            "hyperparams": {"a": 1, "b": 2},
            "tags": {"team": "ds"},
        }
        override = {
            "hyperparams": {"b": 20, "c": 3},
            "tags": {"project": "ml"},
        }

        merged = MLInfoConfigLoader.merge_ml_info(base, override)

        assert merged["hyperparams"]["a"] == 1
        assert merged["hyperparams"]["b"] == 20  # Override
        assert merged["hyperparams"]["c"] == 3
        assert merged["tags"]["team"] == "ds"
        assert merged["tags"]["project"] == "ml"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
