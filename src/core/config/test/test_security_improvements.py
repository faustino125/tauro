"""
Tests for security improvements in Phase 1:
1. Path traversal protection
2. Infinite loop protection in interpolation
3. Thread safety in SparkSessionFactory
"""
import os
import pytest
import tempfile
from types import SimpleNamespace

from core.config.loaders import (
    ConfigLoaderFactory,
    YamlConfigLoader,
    JsonConfigLoader,
    DSLConfigLoader,
)
from core.config.interpolator import VariableInterpolator
from core.config.exceptions import ConfigLoadError
from core.config.session import SparkSessionFactory


# ============================================================================
# Path Traversal Protection Tests
# ============================================================================


def test_path_traversal_yaml_loader_blocked():
    """Test that path traversal attempts are blocked in YAML loader"""
    loader = YamlConfigLoader()

    # Try to access parent directory
    with pytest.raises(ConfigLoadError) as exc:
        loader.load("../../../etc/passwd")

    assert "Path traversal not allowed" in str(exc.value)


def test_path_traversal_json_loader_blocked():
    """Test that path traversal attempts are blocked in JSON loader"""
    loader = JsonConfigLoader()

    with pytest.raises(ConfigLoadError) as exc:
        loader.load("../../config.json")

    assert "Path traversal not allowed" in str(exc.value)


def test_path_traversal_dsl_loader_blocked():
    """Test that path traversal attempts are blocked in DSL loader"""
    loader = DSLConfigLoader()

    with pytest.raises(ConfigLoadError) as exc:
        loader.load("../sensitive/data.dsl")

    assert "Path traversal not allowed" in str(exc.value)


def test_valid_path_without_traversal_works():
    """Test that valid paths without .. still work"""
    loader = ConfigLoaderFactory()

    # Create a temporary valid config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write('{"test": "value"}')
        temp_path = f.name

    try:
        config = loader.load_config(temp_path)
        assert config == {"test": "value"}
    finally:
        os.unlink(temp_path)


def test_nonexistent_file_error_message():
    """Test that nonexistent files give clear error messages"""
    loader = YamlConfigLoader()

    with pytest.raises(ConfigLoadError) as exc:
        loader.load("/nonexistent/path/config.yaml")

    assert "not found" in str(exc.value).lower()


def test_directory_instead_of_file_rejected():
    """Test that directories are rejected when a file is expected"""
    loader = YamlConfigLoader()

    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(ConfigLoadError) as exc:
            loader.load(tmpdir)

        assert "must be a file" in str(exc.value).lower() or "not found" in str(exc.value).lower()


# ============================================================================
# Infinite Loop Protection Tests
# ============================================================================


def test_circular_reference_detection_direct():
    """Test detection of direct circular references in interpolation"""
    # Create a circular reference scenario
    variables = {"var_a": "${var_b}", "var_b": "${var_a}"}

    with pytest.raises(ConfigLoadError) as exc:
        VariableInterpolator.interpolate("${var_a}", variables)

    assert "Maximum interpolation depth exceeded" in str(exc.value) or "Circular" in str(exc.value)


def test_max_iterations_protection():
    """Test that excessive iterations are prevented"""
    # Create a deeply nested variable chain that should trigger max iterations
    string_with_many_vars = "${a}" * 150  # More than MAX_ITERATIONS
    variables = {"a": "x"}

    # This should either succeed quickly or raise an error, but not hang
    try:
        result = VariableInterpolator.interpolate(string_with_many_vars, variables)
        # If it succeeds, verify it processed correctly
        assert "x" in result
    except ConfigLoadError as e:
        # Or it should raise max iterations or circular reference error
        error_msg = str(e)
        assert "Maximum interpolation" in error_msg or "Circular reference" in error_msg


def test_max_depth_protection_in_structure():
    """Test depth protection in nested structure interpolation"""
    # Create a very deeply nested structure
    deep_structure = {"level": 0}
    current = deep_structure
    for i in range(15):  # More than MAX_INTERPOLATION_DEPTH
        current["child"] = {"level": i + 1, "value": "${test}"}
        current = current["child"]

    variables = {"test": "value"}

    with pytest.raises(ConfigLoadError) as exc:
        VariableInterpolator.interpolate_structure(deep_structure, variables)

    assert "depth exceeded" in str(exc.value).lower()


def test_normal_nested_interpolation_still_works():
    """Test that normal nested interpolation still works correctly"""
    variables = {"base": "/data", "env": "prod", "full_path": "${base}/${env}/files"}

    result = VariableInterpolator.interpolate("${full_path}", variables)
    assert result == "/data/prod/files"


def test_env_var_interpolation_with_protection():
    """Test that environment variable interpolation works with new protections"""
    os.environ["TEST_VAR_SECURITY"] = "secret_value"

    try:
        result = VariableInterpolator.interpolate("${TEST_VAR_SECURITY}/path", {})
        assert result == "secret_value/path"
    finally:
        del os.environ["TEST_VAR_SECURITY"]


def test_interpolate_structure_with_lists():
    """Test that list interpolation works with depth protection"""
    structure = ["${var1}", {"nested": "${var2}"}, ["${var3}", "${var4}"]]

    variables = {"var1": "value1", "var2": "value2", "var3": "value3", "var4": "value4"}

    result = VariableInterpolator.interpolate_structure(structure, variables, copy=True)

    assert result[0] == "value1"
    assert result[1]["nested"] == "value2"
    assert result[2][0] == "value3"
    assert result[2][1] == "value4"


# ============================================================================
# Thread Safety Tests
# ============================================================================


@pytest.fixture(autouse=True)
def reset_spark_session():
    """Reset Spark session before and after each test"""
    SparkSessionFactory.reset_session()
    yield
    SparkSessionFactory.reset_session()


def test_spark_session_singleton_consistency(monkeypatch):
    """Test that SparkSessionFactory returns the same session instance"""
    # Mock the session creation to return fake sessions
    call_count = [0]

    def fake_create_session(*args, **kwargs):
        call_count[0] += 1
        return SimpleNamespace(id=call_count[0], name=f"session-{call_count[0]}")

    monkeypatch.setattr(SparkSessionFactory, "create_session", fake_create_session)

    # Get session multiple times
    session1 = SparkSessionFactory.get_session("local")
    session2 = SparkSessionFactory.get_session("local")
    session3 = SparkSessionFactory.get_session("local")

    # Should all be the same instance
    assert session1 is session2
    assert session2 is session3

    # Should only have called create once
    assert call_count[0] == 1


def test_spark_session_reset_allows_new_session(monkeypatch):
    """Test that reset_session allows creating a new session"""
    call_count = [0]

    def fake_create_session(*args, **kwargs):
        call_count[0] += 1
        return SimpleNamespace(id=call_count[0])

    monkeypatch.setattr(SparkSessionFactory, "create_session", fake_create_session)

    # Get first session
    session1 = SparkSessionFactory.get_session("local")
    assert call_count[0] == 1

    # Reset
    SparkSessionFactory.reset_session()

    # Get new session
    session2 = SparkSessionFactory.get_session("local")
    assert call_count[0] == 2

    # Should be different instances
    assert session1.id != session2.id


def test_concurrent_session_access_thread_safety(monkeypatch):
    """Test that concurrent access to session is thread-safe"""
    import threading

    call_count = [0]
    sessions = []

    def fake_create_session(*args, **kwargs):
        call_count[0] += 1
        # Simulate some work
        import time

        time.sleep(0.01)
        return SimpleNamespace(id=call_count[0])

    monkeypatch.setattr(SparkSessionFactory, "create_session", fake_create_session)

    def get_session_thread():
        session = SparkSessionFactory.get_session("local")
        sessions.append(session)

    # Create multiple threads trying to get session simultaneously
    threads = [threading.Thread(target=get_session_thread) for _ in range(10)]

    # Start all threads
    for t in threads:
        t.start()

    # Wait for all to complete
    for t in threads:
        t.join()

    # Should only have created one session despite concurrent access
    assert call_count[0] == 1

    # All threads should have gotten the same session
    assert all(s.id == sessions[0].id for s in sessions)


# ============================================================================
# Integration Tests
# ============================================================================


def test_secure_config_loading_end_to_end():
    """Test that secure config loading works end-to-end"""
    factory = ConfigLoaderFactory()

    # Create a valid temporary config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write('{"key": "value", "path": "${HOME}/data"}')
        temp_path = f.name

    try:
        config = factory.load_config(temp_path)
        assert config["key"] == "value"
        assert "path" in config
    finally:
        os.unlink(temp_path)


def test_interpolation_with_depth_limit_warning():
    """Test that reasonable nesting still works but excessive depth is caught"""
    # This should work (reasonable depth)
    structure = {"a": {"b": {"c": {"d": "${value}"}}}}
    variables = {"value": "test"}

    result = VariableInterpolator.interpolate_structure(structure, variables, copy=True)
    assert result["a"]["b"]["c"]["d"] == "test"

    # But this should fail (too deep)
    deep = {}
    current = deep
    for _ in range(12):  # Exceeds MAX_INTERPOLATION_DEPTH
        current["level"] = {}
        current = current["level"]
    current["value"] = "${test}"

    with pytest.raises(ConfigLoadError):
        VariableInterpolator.interpolate_structure(deep, variables)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
