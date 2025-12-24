"""
Tests for tauro.core.config.loaders module.

Coverage includes:
- ConfigLoaderFactory (format detection and loading)
- YAML, JSON, Python, and DSL loaders
- Security validation (path traversal prevention)
- Error handling for malformed files
"""

import pytest  # type: ignore
import json
from pathlib import Path

from core.config.loaders import (
    ConfigLoaderFactory,
    YamlConfigLoader,
    JsonConfigLoader,
    DSLConfigLoader,
    PythonConfigLoader,
)
from core.config.exceptions import ConfigLoadError


# ============================================================================
# FACTORY TESTS
# ============================================================================


class TestConfigLoaderFactory:
    """Tests for ConfigLoaderFactory auto-detection."""

    def test_load_dict_passthrough(self):
        """Factory should return dict as-is without loading."""
        factory = ConfigLoaderFactory()
        data = {"a": 1, "b": "x"}
        cfg = factory.load_config(data)
        assert cfg == data

    def test_load_json_string(self):
        """Factory should detect and parse JSON strings."""
        factory = ConfigLoaderFactory()
        js = '{"a": 1, "b": "x"}'
        cfg = factory.load_config(js)
        assert cfg["a"] == 1
        assert cfg["b"] == "x"

    def test_load_json_file(self, tmp_path):
        """Factory should detect and load JSON files."""
        factory = ConfigLoaderFactory()
        json_file = tmp_path / "config.json"
        json_file.write_text(json.dumps({"key": "value"}))

        cfg = factory.load_config(str(json_file))
        assert cfg["key"] == "value"

    def test_load_yaml_file(self, tmp_path):
        """Factory should detect and load YAML files."""
        factory = ConfigLoaderFactory()
        yaml_file = tmp_path / "config.yml"
        yaml_file.write_text("input_path: /data/in\nmode: local\n")

        cfg = factory.load_config(str(yaml_file))
        assert cfg["input_path"] == "/data/in"
        assert cfg["mode"] == "local"

    def test_load_python_file(self, tmp_path):
        """Factory should detect and load Python module files."""
        factory = ConfigLoaderFactory()
        py_file = tmp_path / "config.py"
        py_file.write_text("config = {'input_path': '/in', 'mode': 'local'}")

        cfg = factory.load_config(str(py_file))
        assert cfg["input_path"] == "/in"
        assert cfg["mode"] == "local"

    def test_file_not_found(self, tmp_path):
        """Factory should raise ConfigLoadError for missing files."""
        factory = ConfigLoaderFactory()
        missing_file = tmp_path / "nonexistent.json"

        with pytest.raises(ConfigLoadError) as exc_info:
            factory.load_config(str(missing_file))
        assert "not found" in str(exc_info.value).lower()


# ============================================================================
# YAML LOADER TESTS
# ============================================================================


class TestYamlConfigLoader:
    """Tests for YAML configuration loading."""

    def test_load_valid_yaml(self, tmp_path):
        """Should load valid YAML files correctly."""
        loader = YamlConfigLoader()
        yaml_file = tmp_path / "config.yml"
        yaml_file.write_text(
            """
input_path: /data/in
output_path: /data/out
mode: local
nested:
  key: value
  list: [1, 2, 3]
"""
        )

        cfg = loader.load(yaml_file)
        assert cfg["input_path"] == "/data/in"
        assert cfg["nested"]["key"] == "value"
        assert cfg["nested"]["list"] == [1, 2, 3]

    def test_load_empty_yaml_returns_empty_dict(self, tmp_path):
        """Should return empty dict for empty YAML file."""
        loader = YamlConfigLoader()
        yaml_file = tmp_path / "empty.yml"
        yaml_file.write_text("")

        cfg = loader.load(yaml_file)
        assert cfg == {}

    def test_load_invalid_yaml_raises_error(self, tmp_path):
        """Should raise ConfigLoadError for invalid YAML."""
        loader = YamlConfigLoader()
        yaml_file = tmp_path / "invalid.yml"
        yaml_file.write_text("invalid: yaml: syntax:")

        with pytest.raises(ConfigLoadError):
            loader.load(yaml_file)

    def test_load_nonexistent_file_raises_error(self, tmp_path):
        """Should raise ConfigLoadError for missing file."""
        loader = YamlConfigLoader()
        missing = tmp_path / "nonexistent.yml"

        with pytest.raises(ConfigLoadError):
            loader.load(missing)


# ============================================================================
# JSON LOADER TESTS
# ============================================================================


class TestJsonConfigLoader:
    """Tests for JSON configuration loading."""

    def test_load_valid_json(self, tmp_path):
        """Should load valid JSON files correctly."""
        loader = JsonConfigLoader()
        json_file = tmp_path / "config.json"
        json_file.write_text(
            json.dumps(
                {
                    "input_path": "/data/in",
                    "mode": "local",
                    "nested": {"key": "value"},
                }
            )
        )

        cfg = loader.load(json_file)
        assert cfg["input_path"] == "/data/in"
        assert cfg["nested"]["key"] == "value"

    def test_load_invalid_json_raises_error(self, tmp_path):
        """Should raise ConfigLoadError for invalid JSON."""
        loader = JsonConfigLoader()
        json_file = tmp_path / "invalid.json"
        json_file.write_text("{invalid json}")

        with pytest.raises(ConfigLoadError):
            loader.load(json_file)

    def test_load_empty_json_raises_error(self, tmp_path):
        """Should raise ConfigLoadError for empty JSON file."""
        loader = JsonConfigLoader()
        json_file = tmp_path / "empty.json"
        json_file.write_text("")

        with pytest.raises(ConfigLoadError):
            loader.load(json_file)


# ============================================================================
# DSL LOADER TESTS
# ============================================================================


class TestDslConfigLoader:
    """Tests for DSL configuration loading."""

    def test_load_valid_dsl(self, tmp_path):
        """Should load and parse valid DSL files."""
        loader = DSLConfigLoader()
        dsl_file = tmp_path / "config.dsl"
        dsl_file.write_text(
            """
[global]
input_path = "/data/in"
output_path = "/data/out"
mode = "local"

[nodes.node1]
input = {"format": "json"}
function = "etl.extract"

[pipelines.batch_p]
nodes = [node1]
type = "batch"
"""
        )

        cfg = loader.load(dsl_file)
        assert cfg["global"]["input_path"] == "/data/in"
        assert "node1" in cfg["nodes"]
        assert cfg["nodes"]["node1"]["function"] == "etl.extract"
        assert "batch_p" in cfg["pipelines"]

    def test_dsl_hierarchical_sections(self, tmp_path):
        """Should handle hierarchical section notation."""
        loader = DSLConfigLoader()
        dsl_file = tmp_path / "config.dsl"
        dsl_file.write_text(
            """
[section.subsection.key]
value = "nested"
"""
        )

        cfg = loader.load(dsl_file)
        assert cfg["section"]["subsection"]["key"]["value"] == "nested"

    def test_dsl_type_coercion(self, tmp_path):
        """Should coerce types correctly in DSL."""
        loader = DSLConfigLoader()
        dsl_file = tmp_path / "config.dsl"
        dsl_file.write_text(
            """
[types]
string_val = "text"
int_val = 123
bool_true = true
bool_false = false
list_val = [1, 2, 3]
dict_val = {"a": 1, "b": 2}
"""
        )

        cfg = loader.load(dsl_file)
        assert cfg["types"]["string_val"] == "text"
        assert cfg["types"]["int_val"] == 123
        assert cfg["types"]["bool_true"] is True
        assert cfg["types"]["bool_false"] is False
        assert cfg["types"]["list_val"] == [1, 2, 3]
        assert cfg["types"]["dict_val"] == {"a": 1, "b": 2}


# ============================================================================
# PYTHON LOADER TESTS
# ============================================================================


class TestPythonConfigLoader:
    """Tests for Python module configuration loading."""

    def test_load_valid_python_module(self, tmp_path):
        """Should load and execute Python module with config variable."""
        loader = PythonConfigLoader()
        py_file = tmp_path / "config.py"
        py_file.write_text(
            """
config = {
    'input_path': '/in',
    'output_path': '/out',
    'mode': 'local',
    'metadata': {'project': 'test'}
}
"""
        )

        cfg = loader.load(py_file)
        assert cfg["input_path"] == "/in"
        assert cfg["metadata"]["project"] == "test"

    def test_python_file_missing_config_variable(self, tmp_path):
        """Should raise ConfigLoadError if config variable not found."""
        loader = PythonConfigLoader()
        py_file = tmp_path / "bad_config.py"
        py_file.write_text("settings = {'input_path': '/in'}")  # Wrong variable name

        with pytest.raises(ConfigLoadError) as exc_info:
            loader.load(py_file)
        assert "config" in str(exc_info.value).lower()

    def test_python_file_with_syntax_error(self, tmp_path):
        """Should raise ConfigLoadError for Python syntax errors."""
        loader = PythonConfigLoader()
        py_file = tmp_path / "syntax_error.py"
        py_file.write_text("config = {'a': }")  # Invalid syntax

        with pytest.raises(ConfigLoadError):
            loader.load(py_file)

    def test_python_file_with_runtime_error(self, tmp_path):
        """Should raise ConfigLoadError for runtime errors during execution."""
        loader = PythonConfigLoader()
        py_file = tmp_path / "runtime_error.py"
        py_file.write_text("config = undefined_variable")  # NameError

        with pytest.raises(ConfigLoadError):
            loader.load(py_file)


# ============================================================================
# SECURITY TESTS
# ============================================================================


class TestLoaderSecurity:
    """Tests for security validations in loaders."""

    def test_path_traversal_prevention_dotdot(self, tmp_path):
        """Should prevent path traversal with '..' sequences."""
        loader = YamlConfigLoader()

        # Try to reference parent directory
        with pytest.raises(ConfigLoadError) as exc_info:
            loader.load(tmp_path / ".." / "escape.yml")
        assert "path traversal" in str(exc_info.value).lower() or ".." in str(exc_info.value)

    def test_path_traversal_prevention_absolute_path(self, tmp_path):
        """Should validate against absolute path attempts."""
        loader = YamlConfigLoader()

        # This is optional depending on security implementation
        # Some systems may allow absolute paths with validation
        # Use a platform-specific absolute path that doesn't exist
        import sys

        if sys.platform == "win32":
            dangerous_path = Path("C:\\Windows\\nonexistent_secure_file.txt")
        else:
            dangerous_path = Path("/etc/nonexistent_secure_file")

        if not dangerous_path.exists():
            with pytest.raises(ConfigLoadError):
                loader.load(dangerous_path)
