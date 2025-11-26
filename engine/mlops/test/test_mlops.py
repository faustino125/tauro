"""
Unit tests for MLOps layer.
"""

import tempfile
from pathlib import Path
import pytest  # type: ignore

from engine.mlops.config import MLOpsContext
from engine.mlops.model_registry import ModelStage
from engine.mlops.experiment_tracking import RunStatus
from engine.mlops.exceptions import (
    ModelNotFoundError,
    RunNotActiveError,
    InvalidMetricError,
    ArtifactNotFoundError,
)


class TestLocalStorageBackend:
    """Tests for LocalStorageBackend."""

    @pytest.fixture
    def storage(self):
        """Create temp storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from engine.mlops.storage import LocalStorageBackend

            yield LocalStorageBackend(tmpdir)

    def test_write_and_read_json(self, storage):
        """Test JSON write/read."""
        data = {"key": "value", "number": 42}
        storage.write_json(data, "test.json")

        result = storage.read_json("test.json")
        assert result == data

    def test_write_and_read_dataframe(self, storage):
        """Test DataFrame write/read."""
        import pandas as pd  # type: ignore

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        storage.write_dataframe(df, "test.parquet")

        result = storage.read_dataframe("test.parquet")
        pd.testing.assert_frame_equal(df, result)

    def test_exists(self, storage):
        """Test path existence check."""
        storage.write_json({"test": True}, "test.json")
        assert storage.exists("test.json")
        assert not storage.exists("nonexistent.json")

    def test_list_paths(self, storage):
        """Test path listing."""
        storage.write_json({"a": 1}, "dir1/file1.json")
        storage.write_json({"b": 2}, "dir1/file2.json")
        storage.write_json({"c": 3}, "dir2/file3.json")

        paths = storage.list_paths("dir1")
        assert len(paths) > 0
        assert any("file1" in p for p in paths)


class TestModelRegistry:
    """Tests for ModelRegistry."""

    @pytest.fixture
    def registry(self):
        """Create registry with temp storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = MLOpsContext(backend_type="local", storage_path=tmpdir)
            yield ctx.model_registry

    @pytest.fixture
    def dummy_model(self):
        """Create dummy model artifact."""
        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = Path(tmpdir) / "model"
            model_dir.mkdir()
            (model_dir / "data.pkl").write_text("model data")
            yield str(model_dir)

    def test_register_nonexistent_artifact(self, registry):
        """Test that registering non-existent artifact raises ArtifactNotFoundError."""
        with pytest.raises(ArtifactNotFoundError):
            registry.register_model(
                name="test_model",
                artifact_path="/nonexistent/model.pkl",
                artifact_type="sklearn",
                framework="scikit-learn",
            )

    def test_get_nonexistent_model(self, registry):
        """Test that getting non-existent model raises ModelNotFoundError."""
        with pytest.raises(ModelNotFoundError) as exc_info:
            registry.get_model_version("nonexistent_model")

        assert "nonexistent_model" in str(exc_info.value)

    def test_register_model(self, registry, dummy_model):
        """Test model registration."""
        model_v1 = registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
            hyperparameters={"param1": 10},
            metrics={"accuracy": 0.95},
        )

        assert model_v1.metadata.name == "test_model"
        assert model_v1.version == 1
        assert model_v1.metadata.metrics["accuracy"] == pytest.approx(0.95)

    def test_register_multiple_versions(self, registry, dummy_model):
        """Test registering multiple versions of same model."""
        v1 = registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        v2 = registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        assert v1.version == 1
        assert v2.version == 2
        assert v1.model_id == v2.model_id

    def test_get_model_version(self, registry, dummy_model):
        """Test retrieving model version."""
        registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        retrieved = registry.get_model_version("test_model", version=1)
        assert retrieved.metadata.name == "test_model"
        assert retrieved.version == 1

    def test_get_latest_version(self, registry, dummy_model):
        """Test getting latest version without specifying version."""
        registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        latest = registry.get_model_version("test_model")
        assert latest.version == 2

    def test_list_models(self, registry, dummy_model):
        """Test listing models."""
        registry.register_model(
            name="model1",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        registry.register_model(
            name="model2",
            artifact_path=dummy_model,
            artifact_type="xgboost",
            framework="xgboost",
        )

        models = registry.list_models()
        assert len(models) == 2
        names = [m["name"] for m in models]
        assert "model1" in names
        assert "model2" in names

    def test_promote_model(self, registry, dummy_model):
        """Test model promotion."""
        registry.register_model(
            name="test_model",
            artifact_path=dummy_model,
            artifact_type="sklearn",
            framework="scikit-learn",
        )

        promoted = registry.promote_model("test_model", 1, ModelStage.PRODUCTION)

        assert promoted.metadata.stage == ModelStage.PRODUCTION

        retrieved = registry.get_model_version("test_model", 1)
        assert retrieved.metadata.stage == ModelStage.PRODUCTION


class TestExperimentTracker:
    """Tests for ExperimentTracker."""

    @pytest.fixture
    def tracker(self):
        """Create tracker with temp storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx = MLOpsContext(backend_type="local", storage_path=tmpdir)
            yield ctx.experiment_tracker

    def test_log_invalid_metric_type(self, tracker):
        """Test that logging non-numeric metric raises InvalidMetricError."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        with pytest.raises(InvalidMetricError) as exc_info:
            tracker.log_metric(run.run_id, "accuracy", "not_a_number")

        assert "Expected int or float" in str(exc_info.value)

    def test_log_nan_metric(self, tracker):
        """Test that logging NaN raises InvalidMetricError."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        with pytest.raises(InvalidMetricError) as exc_info:
            tracker.log_metric(run.run_id, "loss", float("nan"))

        assert "NaN" in str(exc_info.value)

    def test_log_artifact_nonexistent(self, tracker):
        """Test that logging non-existent artifact raises ArtifactNotFoundError."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        with pytest.raises(ArtifactNotFoundError):
            tracker.log_artifact(run.run_id, "/nonexistent/path/model.pkl")

    def test_get_inactive_run_raises_error(self, tracker):
        """Test that accessing inactive run via log_metric raises RunNotActiveError."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)
        tracker.end_run(run.run_id)

        # Try to log metric on ended run
        with pytest.raises(RunNotActiveError):
            tracker.log_metric(run.run_id, "accuracy", 0.95)

    def test_create_experiment(self, tracker):
        """Test experiment creation."""
        exp = tracker.create_experiment(
            name="test_exp",
            description="Test experiment",
            tags={"tag1": "value1"},
        )

        assert exp.name == "test_exp"
        assert exp.description == "Test experiment"
        assert exp.tags["tag1"] == "value1"

    def test_start_run(self, tracker):
        """Test starting run."""
        exp = tracker.create_experiment("test_exp")

        run = tracker.start_run(
            exp.experiment_id,
            name="run1",
            parameters={"lr": 0.01},
        )

        assert run.name == "run1"
        assert run.parameters["lr"] == pytest.approx(0.01)
        assert run.status == RunStatus.RUNNING

    def test_log_metric(self, tracker):
        """Test logging metrics."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        tracker.log_metric(run.run_id, "loss", 0.5, step=1)
        tracker.log_metric(run.run_id, "loss", 0.4, step=2)

        run = tracker.get_run(run.run_id)
        assert len(run.metrics["loss"]) == 2
        assert run.metrics["loss"][0].value == pytest.approx(0.5)
        assert run.metrics["loss"][1].value == pytest.approx(0.4)

    def test_log_parameter(self, tracker):
        """Test logging parameters."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        tracker.log_parameter(run.run_id, "n_estimators", 100)

        run = tracker.get_run(run.run_id)
        assert run.parameters["n_estimators"] == 100

    def test_log_artifact(self, tracker):
        """Test logging artifacts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            exp = tracker.create_experiment("test_exp")
            run = tracker.start_run(exp.experiment_id)

            artifact_path = Path(tmpdir) / "model.pkl"
            artifact_path.write_text("model data")

            uri = tracker.log_artifact(run.run_id, str(artifact_path))
            assert uri is not None

    def test_end_run(self, tracker):
        """Test ending run."""
        exp = tracker.create_experiment("test_exp")
        run = tracker.start_run(exp.experiment_id)

        completed_run = tracker.end_run(run.run_id, RunStatus.COMPLETED)

        assert completed_run.status == RunStatus.COMPLETED
        assert completed_run.end_time is not None
        assert completed_run.duration_seconds is not None

    def test_list_runs(self, tracker):
        """Test listing runs."""
        exp = tracker.create_experiment("test_exp")

        run1 = tracker.start_run(exp.experiment_id, name="run1")
        tracker.end_run(run1.run_id)

        run2 = tracker.start_run(exp.experiment_id, name="run2")
        tracker.end_run(run2.run_id)

        runs = tracker.list_runs(exp.experiment_id)
        assert len(runs) == 2

    def test_compare_runs(self, tracker):
        """Test comparing runs."""
        exp = tracker.create_experiment("test_exp")

        run1 = tracker.start_run(exp.experiment_id, name="run1")
        tracker.log_metric(run1.run_id, "accuracy", 0.90)
        tracker.log_parameter(run1.run_id, "lr", 0.01)
        tracker.end_run(run1.run_id)

        run2 = tracker.start_run(exp.experiment_id, name="run2")
        tracker.log_metric(run2.run_id, "accuracy", 0.95)
        tracker.log_parameter(run2.run_id, "lr", 0.001)
        tracker.end_run(run2.run_id)

        comparison = tracker.compare_runs([run1.run_id, run2.run_id])
        assert len(comparison) == 2
        assert "metric_accuracy" in comparison.columns
        assert "param_lr" in comparison.columns

    def test_search_runs(self, tracker):
        """Test searching runs by metrics."""
        exp = tracker.create_experiment("test_exp")

        run1 = tracker.start_run(exp.experiment_id)
        tracker.log_metric(run1.run_id, "accuracy", 0.80)
        tracker.end_run(run1.run_id)

        run2 = tracker.start_run(exp.experiment_id)
        tracker.log_metric(run2.run_id, "accuracy", 0.95)
        tracker.end_run(run2.run_id)

        # Search for accuracy > 0.90
        matching = tracker.search_runs(exp.experiment_id, metric_filter={"accuracy": (">", 0.90)})

        assert len(matching) == 1
        assert matching[0] == run2.run_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
