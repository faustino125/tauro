"""
Tests for MLOps validators module.

Validates input validation, security checks, and error handling.
"""
import pytest
import math
from pathlib import Path
import tempfile

from core.mlops.validators import (
    PathValidator,
    NameValidator,
    MetricValidator,
    ParameterValidator,
    MetadataValidator,
    FrameworkValidator,
    ArtifactValidator,
    ValidationError,
    validate_model_name,
    validate_experiment_name,
    validate_run_name,
    validate_framework,
    validate_artifact_type,
    validate_metric_value,
    validate_parameters,
    validate_tags,
    validate_description,
)


class TestPathValidator:
    """Tests for PathValidator."""

    def test_validate_path_relative(self):
        """Test that relative paths are accepted."""
        path = PathValidator.validate_path("models/v1/model.pkl")
        assert path == Path("models/v1/model.pkl")

    def test_validate_path_rejects_absolute(self):
        """Test that absolute paths are rejected."""
        # Test Unix-style absolute path
        if Path("/etc/passwd").is_absolute():
            with pytest.raises(ValidationError, match="Absolute paths not allowed"):
                PathValidator.validate_path("/etc/passwd")

        # Test Windows-style absolute path
        if Path("C:\\Windows\\System32").is_absolute():
            with pytest.raises(ValidationError, match="Absolute paths not allowed"):
                PathValidator.validate_path("C:\\Windows\\System32")

    def test_validate_path_rejects_parent_traversal(self):
        """Test that parent directory traversal is rejected."""
        with pytest.raises(ValidationError, match="Directory traversal not allowed"):
            PathValidator.validate_path("../../../etc/passwd")

        with pytest.raises(ValidationError, match="Directory traversal not allowed"):
            PathValidator.validate_path("models/../../secrets")

    def test_validate_path_rejects_tilde(self):
        """Test that tilde expansion is rejected."""
        with pytest.raises(ValidationError, match="Directory traversal not allowed"):
            PathValidator.validate_path("~/secrets/api_key.txt")

    def test_validate_path_with_base_path(self):
        """Test path validation with base path restriction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Valid path within base
            valid = PathValidator.validate_path("models/v1", base_path=base)
            assert valid.is_absolute()
            assert base in valid.parents or valid == base / "models/v1"

    def test_validate_path_outside_base_rejected(self):
        """Test that paths outside base directory are rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            with pytest.raises(ValidationError, match="Directory traversal not allowed"):
                PathValidator.validate_path("../../outside", base_path=base)

    def test_validate_file_exists(self):
        """Test file existence validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            existing_file = Path(tmpdir) / "test.txt"
            existing_file.write_text("test")

            result = PathValidator.validate_file_exists(existing_file)
            assert result == existing_file

    def test_validate_file_exists_raises_on_missing(self):
        """Test that missing file raises ValidationError."""
        with pytest.raises(ValidationError, match="does not exist"):
            PathValidator.validate_file_exists(Path("/nonexistent/file.txt"))

    def test_validate_empty_path(self):
        """Test that empty path is rejected."""
        with pytest.raises(ValidationError, match="non-empty string"):
            PathValidator.validate_path("")

    def test_validate_none_path(self):
        """Test that None path is rejected."""
        with pytest.raises(ValidationError, match="non-empty string"):
            PathValidator.validate_path(None)


class TestNameValidator:
    """Tests for NameValidator."""

    def test_validate_name_valid(self):
        """Test valid names are accepted."""
        valid_names = [
            "model_v1",
            "experiment-2024",
            "my.model.v2",
            "Model123",
            "a",  # Single char
            "model_name_with_many_underscores",
        ]

        for name in valid_names:
            result = NameValidator.validate_name(name, "Model")
            assert result == name

    def test_validate_name_rejects_empty(self):
        """Test that empty name is rejected."""
        with pytest.raises(ValidationError, match="non-empty string"):
            NameValidator.validate_name("", "Model")

    def test_validate_name_rejects_too_long(self):
        """Test that names exceeding 255 chars are rejected."""
        long_name = "a" * 256
        with pytest.raises(ValidationError, match="must be 1-255 characters"):
            NameValidator.validate_name(long_name, "Model")

    def test_validate_name_rejects_invalid_chars(self):
        """Test that invalid characters are rejected."""
        invalid_names = [
            "model/name",  # Slash
            "model\\name",  # Backslash
            "model name",  # Space
            "model@v1",  # Special char
            "model#1",  # Hash
            "model$v1",  # Dollar
        ]

        for name in invalid_names:
            with pytest.raises(ValidationError, match="invalid characters"):
                NameValidator.validate_name(name, "Model")

    def test_validate_name_must_start_with_alnum(self):
        """Test that names must start with alphanumeric."""
        with pytest.raises(ValidationError, match="must start with letter or number"):
            NameValidator.validate_name("-model", "Model")

        with pytest.raises(ValidationError, match="must start with letter or number"):
            NameValidator.validate_name("_model", "Model")

        with pytest.raises(ValidationError, match="must start with letter or number"):
            NameValidator.validate_name(".model", "Model")

    def test_validate_name_different_types(self):
        """Test validation with different entity types."""
        for entity_type in ["Model", "Experiment", "Run", "Pipeline"]:
            with pytest.raises(ValidationError, match=entity_type):
                NameValidator.validate_name("", entity_type)


class TestMetricValidator:
    """Tests for MetricValidator."""

    def test_validate_metric_value_valid(self):
        """Test valid metric values are accepted."""
        assert MetricValidator.validate_metric_value("acc", 0.95) == 0.95
        assert MetricValidator.validate_metric_value("loss", 0.0) == 0.0
        assert MetricValidator.validate_metric_value("count", 100) == 100.0
        assert MetricValidator.validate_metric_value("neg", -0.5) == -0.5

    def test_validate_metric_value_rejects_non_numeric(self):
        """Test that non-numeric values are rejected."""
        from core.mlops.exceptions import InvalidMetricError

        with pytest.raises(InvalidMetricError, match="int or float"):
            MetricValidator.validate_metric_value("acc", "0.95")

        with pytest.raises(InvalidMetricError, match="int or float"):
            MetricValidator.validate_metric_value("acc", None)

        with pytest.raises(InvalidMetricError, match="int or float"):
            MetricValidator.validate_metric_value("acc", [0.95])

    def test_validate_metric_value_rejects_nan(self):
        """Test that NaN is rejected."""
        from core.mlops.exceptions import InvalidMetricError

        with pytest.raises(InvalidMetricError, match="cannot be NaN"):
            MetricValidator.validate_metric_value("loss", float("nan"))

    def test_validate_metric_value_warns_on_inf(self):
        """Test that Inf generates warning but is allowed."""
        import warnings

        # Should not raise, but may log warning
        result = MetricValidator.validate_metric_value("loss", float("inf"))
        assert math.isinf(result)

        result = MetricValidator.validate_metric_value("loss", float("-inf"))
        assert math.isinf(result)

    def test_validate_step(self):
        """Test step validation."""
        assert MetricValidator.validate_step(0) == 0
        assert MetricValidator.validate_step(100) == 100

    def test_validate_step_rejects_negative(self):
        """Test that negative step is rejected."""
        with pytest.raises(ValidationError, match="non-negative"):
            MetricValidator.validate_step(-1)

    def test_validate_step_rejects_non_int(self):
        """Test that non-integer step is rejected."""
        with pytest.raises(ValidationError, match="must be integer"):
            MetricValidator.validate_step(1.5)


class TestParameterValidator:
    """Tests for ParameterValidator."""

    def test_validate_parameters_valid(self):
        """Test valid parameters are accepted."""
        params = {
            "lr": 0.01,
            "n_estimators": 100,
            "max_depth": 10,
            "use_gpu": True,
            "optimizer": "adam",
            "regularization": None,
        }

        result = ParameterValidator.validate_parameters(params)
        assert result == params

    def test_validate_parameters_none(self):
        """Test that None parameters is accepted."""
        result = ParameterValidator.validate_parameters(None)
        assert result is None

    def test_validate_parameters_empty_dict(self):
        """Test that empty dict is accepted."""
        result = ParameterValidator.validate_parameters({})
        assert result == {}

    def test_validate_parameters_rejects_non_dict(self):
        """Test that non-dict parameters are rejected."""
        from core.mlops.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="must be dict"):
            ParameterValidator.validate_parameters([1, 2, 3])

        with pytest.raises(InvalidParameterError, match="must be dict"):
            ParameterValidator.validate_parameters("params")

    def test_validate_parameters_rejects_too_many(self):
        """Test that too many parameters are rejected."""
        from core.mlops.exceptions import InvalidParameterError

        too_many = {f"param_{i}": i for i in range(1001)}

        with pytest.raises(InvalidParameterError, match="Too many parameters"):
            ParameterValidator.validate_parameters(too_many)

    def test_validate_parameters_rejects_unsupported_types(self):
        """Test that unsupported parameter types are rejected."""
        from core.mlops.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="not supported"):
            ParameterValidator.validate_parameters({"list_param": [1, 2, 3]})

        with pytest.raises(InvalidParameterError, match="not supported"):
            ParameterValidator.validate_parameters({"dict_param": {"nested": "value"}})

    def test_validate_parameters_rejects_nan(self):
        """Test that NaN parameter values are rejected."""
        from core.mlops.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="cannot be NaN"):
            ParameterValidator.validate_parameters({"lr": float("nan")})

    def test_validate_parameters_warns_on_inf(self):
        """Test that Inf parameter values generate warning."""
        # Should not raise but may log warning
        result = ParameterValidator.validate_parameters({"threshold": float("inf")})
        assert math.isinf(result["threshold"])


class TestMetadataValidator:
    """Tests for MetadataValidator."""

    def test_validate_tags_valid(self):
        """Test valid tags are accepted."""
        tags = {
            "version": "v1.0",
            "environment": "production",
            "team": "data-science",
        }

        result = MetadataValidator.validate_tags(tags)
        assert result == tags

    def test_validate_tags_none(self):
        """Test that None tags is accepted."""
        result = MetadataValidator.validate_tags(None)
        assert result is None

    def test_validate_tags_empty_dict(self):
        """Test that empty dict is accepted."""
        result = MetadataValidator.validate_tags({})
        assert result == {}

    def test_validate_tags_rejects_non_dict(self):
        """Test that non-dict tags are rejected."""
        with pytest.raises(ValidationError, match="must be dict"):
            MetadataValidator.validate_tags("tags")

    def test_validate_tags_rejects_too_many(self):
        """Test that too many tags are rejected."""
        too_many = {f"tag_{i}": f"value_{i}" for i in range(101)}

        with pytest.raises(ValidationError, match="Too many tags"):
            MetadataValidator.validate_tags(too_many)

    def test_validate_tags_rejects_non_string_key(self):
        """Test that non-string tag keys are rejected."""
        with pytest.raises(ValidationError, match="Tag key must be string"):
            MetadataValidator.validate_tags({123: "value"})

    def test_validate_tags_rejects_non_string_value(self):
        """Test that non-string tag values are rejected."""
        with pytest.raises(ValidationError, match="Tag value must be string"):
            MetadataValidator.validate_tags({"key": 123})

    def test_validate_description_valid(self):
        """Test valid descriptions are accepted."""
        desc = "This is a test model for classification."
        result = MetadataValidator.validate_description(desc)
        assert result == desc

    def test_validate_description_none(self):
        """Test that None description is accepted."""
        result = MetadataValidator.validate_description(None)
        assert result is None

    def test_validate_description_empty(self):
        """Test that empty description is accepted."""
        result = MetadataValidator.validate_description("")
        assert result == ""

    def test_validate_description_rejects_too_long(self):
        """Test that descriptions over 10000 chars are rejected."""
        long_desc = "x" * 10001

        with pytest.raises(ValidationError, match="too long"):
            MetadataValidator.validate_description(long_desc)

    def test_validate_description_rejects_non_string(self):
        """Test that non-string descriptions are rejected."""
        with pytest.raises(ValidationError, match="must be string"):
            MetadataValidator.validate_description(123)


class TestFrameworkValidator:
    """Tests for FrameworkValidator."""

    def test_validate_framework_valid(self):
        """Test valid frameworks are accepted."""
        valid_frameworks = [
            "scikit-learn",
            "sklearn",
            "xgboost",
            "lightgbm",
            "catboost",
            "pytorch",
            "tensorflow",
            "keras",
        ]

        for framework in valid_frameworks:
            result = FrameworkValidator.validate_framework(framework)
            assert result == framework.lower()

    def test_validate_framework_case_insensitive(self):
        """Test that framework validation is case-insensitive."""
        assert FrameworkValidator.validate_framework("SKLEARN") == "sklearn"
        assert FrameworkValidator.validate_framework("PyTorch") == "pytorch"
        assert FrameworkValidator.validate_framework("TensorFlow") == "tensorflow"

    def test_validate_framework_custom_allowed(self):
        """Test that 'custom' framework is allowed by default."""
        result = FrameworkValidator.validate_framework("custom")
        assert result == "custom"

    def test_validate_framework_custom_disallowed(self):
        """Test that 'custom' can be disallowed."""
        with pytest.raises(ValidationError, match="not allowed"):
            FrameworkValidator.validate_framework("custom", allow_custom=False)

    def test_validate_framework_rejects_unsupported(self):
        """Test that unsupported frameworks are rejected."""
        with pytest.raises(ValidationError, match="Unsupported framework"):
            FrameworkValidator.validate_framework("unknown_framework")

    def test_add_supported_framework(self):
        """Test adding new supported framework."""
        FrameworkValidator.add_supported_framework("my-custom-framework")

        result = FrameworkValidator.validate_framework("my-custom-framework")
        assert result == "my-custom-framework"


class TestArtifactValidator:
    """Tests for ArtifactValidator."""

    def test_validate_artifact_type_valid(self):
        """Test valid artifact types are accepted."""
        valid_types = [
            "sklearn",
            "pytorch",
            "tensorflow",
            "onnx",
            "pickle",
            "joblib",
            "h5",
        ]

        for artifact_type in valid_types:
            result = ArtifactValidator.validate_artifact_type(artifact_type)
            assert result == artifact_type.lower()

    def test_validate_artifact_type_case_insensitive(self):
        """Test that artifact type validation is case-insensitive."""
        assert ArtifactValidator.validate_artifact_type("SKLEARN") == "sklearn"
        assert ArtifactValidator.validate_artifact_type("PyTorch") == "pytorch"

    def test_validate_artifact_type_rejects_invalid_chars(self):
        """Test that invalid characters are rejected."""
        with pytest.raises(ValidationError, match="invalid characters"):
            ArtifactValidator.validate_artifact_type("model/type")

        with pytest.raises(ValidationError, match="invalid characters"):
            ArtifactValidator.validate_artifact_type("model type")

    def test_validate_artifact_type_rejects_empty(self):
        """Test that empty artifact type is rejected."""
        with pytest.raises(ValidationError, match="non-empty string"):
            ArtifactValidator.validate_artifact_type("")


class TestConvenienceFunctions:
    """Tests for convenience wrapper functions."""

    def test_validate_model_name(self):
        """Test validate_model_name wrapper."""
        assert validate_model_name("my_model") == "my_model"

        with pytest.raises(ValidationError, match="Model"):
            validate_model_name("")

    def test_validate_experiment_name(self):
        """Test validate_experiment_name wrapper."""
        assert validate_experiment_name("my_experiment") == "my_experiment"

        with pytest.raises(ValidationError, match="Experiment"):
            validate_experiment_name("")

    def test_validate_run_name(self):
        """Test validate_run_name wrapper."""
        assert validate_run_name("run_001") == "run_001"

        with pytest.raises(ValidationError, match="Run"):
            validate_run_name("")

    def test_validate_framework_wrapper(self):
        """Test validate_framework wrapper."""
        assert validate_framework("sklearn") == "sklearn"

        with pytest.raises(ValidationError):
            validate_framework("unknown")

    def test_validate_artifact_type_wrapper(self):
        """Test validate_artifact_type wrapper."""
        assert validate_artifact_type("sklearn") == "sklearn"

        with pytest.raises(ValidationError):
            validate_artifact_type("")

    def test_validate_metric_value_wrapper(self):
        """Test validate_metric_value wrapper."""
        assert validate_metric_value("acc", 0.95) == 0.95

        from core.mlops.exceptions import InvalidMetricError

        with pytest.raises(InvalidMetricError):
            validate_metric_value("acc", "invalid")

    def test_validate_parameters_wrapper(self):
        """Test validate_parameters wrapper."""
        params = {"lr": 0.01}
        assert validate_parameters(params) == params
        assert validate_parameters(None) is None

    def test_validate_tags_wrapper(self):
        """Test validate_tags wrapper."""
        tags = {"version": "v1"}
        assert validate_tags(tags) == tags
        assert validate_tags(None) is None

    def test_validate_description_wrapper(self):
        """Test validate_description wrapper."""
        desc = "Test description"
        assert validate_description(desc) == desc
        assert validate_description(None) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
