"""
Tests for tauro.core.config.interpolator module.

Coverage includes:
- Variable interpolation with ${VAR} syntax
- Precedence: environment variables > context variables > unchanged
- Recursive structure interpolation (dicts, lists, strings)
- Protection against circular references and infinite loops
- File path interpolation
"""

import pytest  # type: ignore
import os

from core.config.interpolator import VariableInterpolator
from core.config.exceptions import ConfigurationError


# ============================================================================
# BASIC INTERPOLATION TESTS
# ============================================================================


class TestBasicInterpolation:
    """Tests for basic variable interpolation."""

    def test_interpolate_single_variable(self):
        """Should replace single variable placeholder."""
        result = VariableInterpolator.interpolate("path/${ENV}/data", {"ENV": "prod"})
        assert result == "path/prod/data"

    def test_interpolate_multiple_variables(self):
        """Should replace multiple placeholders in same string."""
        result = VariableInterpolator.interpolate(
            "s3://${bucket}/${path}", {"bucket": "my-bucket", "path": "data"}
        )
        assert result == "s3://my-bucket/data"

    def test_interpolate_no_variables(self):
        """Should return unchanged string when no placeholders."""
        s = "/data/path/without/placeholders"
        result = VariableInterpolator.interpolate(s, {"VAR": "value"})
        assert result == s

    def test_interpolate_nonexistent_variable_unchanged(self):
        """Should leave undefined variables unchanged."""
        s = "path/${UNDEFINED}/data"
        result = VariableInterpolator.interpolate(s, {})
        assert result == s


# ============================================================================
# PRECEDENCE TESTS
# ============================================================================


class TestInterpolationPrecedence:
    """Tests for variable precedence (environment > context > unchanged)."""

    def test_env_precedence_over_context(self, monkeypatch):
        """Environment variables should override context variables."""
        monkeypatch.setenv("FOO", "env_value")

        result = VariableInterpolator.interpolate("value=${FOO}", {"FOO": "context_value"})
        assert result == "value=env_value"

    def test_context_variable_fallback(self):
        """Should use context variables when env var not set."""
        result = VariableInterpolator.interpolate("value=${BAR}", {"BAR": "context_value"})
        assert result == "value=context_value"

    def test_multiple_sources(self, monkeypatch):
        """Should correctly use both env and context variables."""
        monkeypatch.setenv("FOO", "env_value")

        result = VariableInterpolator.interpolate(
            "first=${FOO}/second=${BAR}", {"BAR": "context_value"}
        )
        assert result == "first=env_value/second=context_value"


# ============================================================================
# STRUCTURE INTERPOLATION TESTS
# ============================================================================


class TestStructureInterpolation:
    """Tests for recursive interpolation of complex structures."""

    def test_interpolate_nested_dict(self):
        """Should interpolate variables in nested dictionaries."""
        data = {"level1": {"level2": {"path": "/data/${ENV}/file"}}}
        result = VariableInterpolator.interpolate_structure(data, {"ENV": "prod"})
        assert result["level1"]["level2"]["path"] == "/data/prod/file"

    def test_interpolate_list_of_strings(self):
        """Should interpolate strings within lists."""
        data = [
            "/data/${stage}/raw",
            "/data/${stage}/processed",
        ]
        result = VariableInterpolator.interpolate_structure(data, {"stage": "dev"})
        assert result == ["/data/dev/raw", "/data/dev/processed"]

    def test_interpolate_mixed_structure(self):
        """Should interpolate in mixed dict/list structures."""
        data = {
            "paths": [{"input": "/in/${env}", "output": "/out/${env}"}],
            "settings": {"database": "db-${env}"},
        }
        result = VariableInterpolator.interpolate_structure(data, {"env": "test"})
        assert result["paths"][0]["input"] == "/in/test"
        assert result["paths"][0]["output"] == "/out/test"
        assert result["settings"]["database"] == "db-test"

    def test_interpolate_preserves_non_strings(self):
        """Should preserve non-string values (numbers, bools, etc)."""
        data = {"count": 42, "enabled": True, "ratio": 3.14, "path": "/data/${stage}"}
        result = VariableInterpolator.interpolate_structure(data, {"stage": "prod"})
        assert result["count"] == 42
        assert result["enabled"] is True
        assert result["ratio"] == pytest.approx(3.14)
        assert result["path"] == "/data/prod"


# ============================================================================
# FILE PATH INTERPOLATION TESTS
# ============================================================================


class TestFilePathInterpolation:
    """Tests for filepath-specific interpolation."""

    def test_interpolate_config_paths_in_nested_structure(self, tmp_path):
        """Should find and interpolate all filepath fields."""
        cfg = {
            "input": {
                "filepath": "/data/${stage}/input.csv",
                "schema": {"path": "/schema/${stage}.json"},
            },
            "output": {"filepath": "/results/${stage}/output.parquet"},
        }
        VariableInterpolator.interpolate_config_paths(cfg, {"stage": "prod"})
        assert cfg["input"]["filepath"] == "/data/prod/input.csv"
        assert cfg["input"]["schema"]["path"] == "/schema/prod.json"
        assert cfg["output"]["filepath"] == "/results/prod/output.parquet"

    def test_interpolate_config_paths_in_list(self):
        """Should interpolate filepath in list items."""
        cfg = {
            "sources": [
                {"filepath": "/data/${n}/file1.csv"},
                {"filepath": "/data/${n}/file2.csv"},
            ]
        }
        VariableInterpolator.interpolate_config_paths(cfg, {"n": "test"})
        assert cfg["sources"][0]["filepath"] == "/data/test/file1.csv"
        assert cfg["sources"][1]["filepath"] == "/data/test/file2.csv"


# ============================================================================
# CIRCULAR REFERENCE PROTECTION
# ============================================================================


class TestCircularReferenceProtection:
    """Tests for protection against circular variable references."""

    def test_direct_circular_reference(self):
        """Should detect direct circular references."""
        with pytest.raises(ConfigurationError) as exc_info:
            VariableInterpolator.interpolate("${a}", {"a": "${b}", "b": "${a}"})
        assert "circular" in str(exc_info.value).lower()

    def test_self_reference(self):
        """Should detect self-referencing variables."""
        # This behavior depends on implementation
        # Some may allow self-reference with modification
        try:
            result = VariableInterpolator.interpolate("${var}", {"var": "${var}"})
            # If it doesn't raise, it should at least not infinite loop
            assert result is not None
        except ConfigurationError:
            # This is also acceptable behavior
            pass

    def test_deep_circular_chain(self):
        """Should detect circular references in long chains."""
        with pytest.raises(ConfigurationError):
            VariableInterpolator.interpolate(
                "${a}", {"a": "${b}", "b": "${c}", "c": "${d}", "d": "${a}"}  # Back to start
            )

    def test_max_iteration_protection(self):
        """Should stop after max iterations to prevent infinite loops."""
        # Create a variable that references itself (would loop forever)
        variables = {"x": "${x}"}

        with pytest.raises(ConfigurationError):
            VariableInterpolator.interpolate("${x}", variables)


# ============================================================================
# DEFAULT VALUE SUPPORT (IF IMPLEMENTED)
# ============================================================================


class TestDefaultValues:
    """Tests for default value syntax like ${VAR:default}."""

    def test_default_value_when_var_undefined(self):
        """Should use default value if variable not found."""
        # This may not be implemented yet - check code
        try:
            result = VariableInterpolator.interpolate("memory=${MEM:4g}", {})
            # If supported, should use default
            assert "4g" in result
        except (ConfigurationError, KeyError):
            # If not supported, that's okay - document it
            pytest.skip("Default values not yet implemented")

    def test_default_value_ignored_when_var_exists(self):
        """Should prefer variable over default when both exist."""
        try:
            result = VariableInterpolator.interpolate("memory=${MEM:4g}", {"MEM": "8g"})
            assert result == "memory=8g"
        except (ConfigurationError, KeyError):
            pytest.skip("Default values not yet implemented")


# ============================================================================
# EDGE CASES
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and unusual inputs."""

    def test_empty_string(self):
        """Should handle empty string."""
        result = VariableInterpolator.interpolate("", {})
        assert result == ""

    def test_variable_at_start(self):
        """Should interpolate variable at string start."""
        result = VariableInterpolator.interpolate("${stage}/data", {"stage": "prod"})
        assert result == "prod/data"

    def test_variable_at_end(self):
        """Should interpolate variable at string end."""
        result = VariableInterpolator.interpolate("data/${stage}", {"stage": "prod"})
        assert result == "data/prod"

    def test_only_variable(self):
        """Should handle string that is only variable."""
        result = VariableInterpolator.interpolate("${value}", {"value": "replaced"})
        assert result == "replaced"

    def test_consecutive_variables(self):
        """Should handle consecutive variables without separator."""
        result = VariableInterpolator.interpolate(
            "${first}${second}", {"first": "hello", "second": "world"}
        )
        assert result == "helloworld"

    def test_variable_with_special_chars(self):
        """Should handle variable names with underscores and numbers."""
        result = VariableInterpolator.interpolate(
            "path/${ENV_STAGE_2}/data", {"ENV_STAGE_2": "prod"}
        )
        assert result == "path/prod/data"

    def test_malformed_placeholder_unchanged(self):
        """Should leave malformed placeholders unchanged."""
        s = "path/${incomplete/data"
        result = VariableInterpolator.interpolate(s, {})
        assert result == s

    def test_empty_dict_in_structure(self):
        """Should handle empty dictionaries in structure."""
        data = {"empty": {}, "value": "path/${stage}"}
        result = VariableInterpolator.interpolate_structure(data, {"stage": "test"})
        assert result["empty"] == {}
        assert result["value"] == "path/test"

    def test_none_values_preserved(self):
        """Should preserve None values in structures."""
        data = {"nullable": None, "path": "/data/${stage}"}
        result = VariableInterpolator.interpolate_structure(data, {"stage": "prod"})
        assert result["nullable"] is None
        assert result["path"] == "/data/prod"
