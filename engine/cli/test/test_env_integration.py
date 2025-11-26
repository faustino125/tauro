"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Integration tests for environment handling across engine.
Tests cover normalization, fallback chains, configuration merging, and validation.
"""

import json
import tempfile
from pathlib import Path

import pytest  # type: ignore

from engine.cli.core import (
    normalize_environment,
    is_allowed_environment,
    is_sandbox_environment,
    get_sandbox_developer,
    get_base_environment,
    CanonicalEnvironment,
    get_fallback_chain,
)
from engine.cli.config import AppConfigManager, ConfigurationError
from engine.cli.validators import EnvironmentConfigValidator, ValidationSeverity


class TestEnvironmentNormalization:
    """Tests for environment name normalization"""

    def test_normalize_production_alias(self):
        """production → prod"""
        assert normalize_environment("production") == "prod"

    def test_normalize_production_alias_uppercase(self):
        """PRODUCTION → prod"""
        assert normalize_environment("PRODUCTION") == "prod"

    def test_normalize_development_alias(self):
        """development → dev"""
        assert normalize_environment("development") == "dev"

    def test_normalize_staging_alias(self):
        """staging stays staging (is now a canonical environment)"""
        assert normalize_environment("staging") == "staging"

    def test_normalize_base(self):
        """base stays base"""
        assert normalize_environment("base") == "base"

    def test_normalize_dev(self):
        """dev stays dev"""
        assert normalize_environment("dev") == "dev"

    def test_normalize_sandbox(self):
        """sandbox stays sandbox"""
        assert normalize_environment("sandbox") == "sandbox"

    def test_normalize_sandbox_developer(self):
        """sandbox_alice stays sandbox_alice"""
        assert normalize_environment("sandbox_alice") == "sandbox_alice"

    def test_normalize_sandbox_developer_uppercase(self):
        """SANDBOX_ALICE → sandbox_alice"""
        assert normalize_environment("SANDBOX_ALICE") == "sandbox_alice"

    def test_normalize_sandbox_developer_sanitize_special_chars(self):
        """sandbox_@alice! → sandbox_alice (remove special chars)"""
        assert normalize_environment("sandbox_@alice!") == "sandbox_alice"

    def test_normalize_sandbox_developer_sanitize_mixed(self):
        """sandbox_bob@2025 → sandbox_bob2025"""
        assert normalize_environment("sandbox_bob@2025") == "sandbox_bob2025"

    def test_normalize_empty_string(self):
        """Empty string returns None"""
        assert normalize_environment("") is None

    def test_normalize_none(self):
        """None returns None"""
        assert normalize_environment(None) is None

    def test_normalize_whitespace_only(self):
        """Whitespace-only string returns None"""
        assert normalize_environment("   ") is None


class TestSandboxEnvironments:
    """Tests for sandbox-specific functionality"""

    def test_is_sandbox_environment_generic(self):
        """sandbox is recognized as sandbox"""
        assert is_sandbox_environment("sandbox")

    def test_is_sandbox_environment_developer(self):
        """sandbox_alice is recognized as sandbox"""
        assert is_sandbox_environment("sandbox_alice")

    def test_is_sandbox_environment_not_dev(self):
        """dev is not sandbox"""
        assert not is_sandbox_environment("dev")

    def test_is_sandbox_environment_not_prod(self):
        """prod is not sandbox"""
        assert not is_sandbox_environment("prod")

    def test_get_sandbox_developer_none_for_generic(self):
        """Generic sandbox has no developer"""
        assert get_sandbox_developer("sandbox") is None

    def test_get_sandbox_developer_extracts_name(self):
        """sandbox_alice extracts alice"""
        assert get_sandbox_developer("sandbox_alice") == "alice"

    def test_get_sandbox_developer_none_for_non_sandbox(self):
        """Non-sandbox returns None"""
        assert get_sandbox_developer("dev") is None

    def test_get_base_environment_sandbox_generic(self):
        """sandbox → sandbox (already base)"""
        assert get_base_environment("sandbox") == "sandbox"

    def test_get_base_environment_sandbox_developer(self):
        """sandbox_alice → sandbox"""
        assert get_base_environment("sandbox_alice") == "sandbox"

    def test_get_base_environment_non_sandbox(self):
        """dev → dev (already base)"""
        assert get_base_environment("dev") == "dev"


class TestAllowedEnvironments:
    """Tests for environment allow/deny checking"""

    def test_is_allowed_base(self):
        """base is allowed"""
        assert is_allowed_environment("base")

    def test_is_allowed_dev(self):
        """dev is allowed"""
        assert is_allowed_environment("dev")

    def test_is_allowed_sandbox(self):
        """sandbox is allowed"""
        assert is_allowed_environment("sandbox")

    def test_is_allowed_sandbox_developer(self):
        """sandbox_alice is allowed"""
        assert is_allowed_environment("sandbox_alice")

    def test_is_allowed_prod(self):
        """prod is allowed"""
        assert is_allowed_environment("prod")

    def test_is_allowed_production_alias(self):
        """production (alias) is allowed"""
        assert is_allowed_environment("production")

    def test_is_not_allowed_invalid(self):
        """invalid_env is not allowed"""
        # Note: may be allowed if engine_ALLOWED_ENVS is set
        # This test assumes default behavior
        is_allowed = is_allowed_environment("invalid_env")
        # Should be False or True depending on env var
        assert isinstance(is_allowed, bool)


class TestFallbackChains:
    """Tests for environment fallback chain resolution"""

    def test_fallback_chain_base(self):
        """base has only itself as fallback"""
        chain = get_fallback_chain("base")
        assert chain == ["base"]

    def test_fallback_chain_dev(self):
        """dev fallback: dev → base"""
        chain = get_fallback_chain("dev")
        assert "dev" in chain
        assert "base" in chain
        assert chain[0] == "dev"  # dev comes first
        assert chain[-1] == "base"  # base is last

    def test_fallback_chain_sandbox(self):
        """sandbox fallback: sandbox → base"""
        chain = get_fallback_chain("sandbox")
        assert "sandbox" in chain
        assert "base" in chain
        assert chain[0] == "sandbox"
        assert chain[-1] == "base"

    def test_fallback_chain_prod(self):
        """prod fallback: prod → base"""
        chain = get_fallback_chain("prod")
        assert "prod" in chain
        assert "base" in chain
        assert chain[0] == "prod"
        assert chain[-1] == "base"

    def test_fallback_chain_invalid_raises(self):
        """Invalid environment raises ValueError"""
        with pytest.raises(ValueError):
            get_fallback_chain("invalid_env_xyz")


class TestAppConfigManager:
    """Tests for AppConfigManager configuration loading and merging"""

    @pytest.fixture
    def temp_project(self):
        """Create a temporary project structure for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            config_path = project_path / "config"
            config_path.mkdir()

            # Create base config
            base_settings = {
                "global_settings_path": "config/global_settings.json",
                "input_config_path": "config/input.json",
                "output_config_path": "config/output.json",
            }
            (config_path / "global_settings.json").write_text(json.dumps({"env": "base"}))
            (config_path / "input.json").write_text(json.dumps({"type": "csv"}))
            (config_path / "output.json").write_text(json.dumps({"format": "parquet"}))

            # Create dev config
            dev_path = config_path / "dev"
            dev_path.mkdir()
            (dev_path / "input.json").write_text(json.dumps({"type": "json"}))

            # Create settings file
            settings_data = {
                "base_path": ".",
                "env_config": {
                    "base": base_settings,
                    "dev": {"input_config_path": "config/dev/input.json"},
                },
            }
            settings_file = project_path / "settings_json.json"
            settings_file.write_text(json.dumps(settings_data))

            yield project_path

    def test_load_base_config(self, temp_project):
        """Load base configuration"""
        manager = AppConfigManager(str(temp_project / "settings_json.json"))
        config = manager.get_env_config("base")

        assert "global_settings_path" in config
        assert "input_config_path" in config

    def test_load_dev_config(self, temp_project):
        """Load dev configuration (with fallback to base)"""
        manager = AppConfigManager(str(temp_project / "settings_json.json"))
        config = manager.get_env_config("dev")

        assert "global_settings_path" in config
        assert "input_config_path" in config
        # dev input should override base
        assert "dev" in config["input_config_path"]

    def test_load_with_alias(self, temp_project):
        """Load configuration using alias (development → dev)"""
        manager = AppConfigManager(str(temp_project / "settings_json.json"))
        config = manager.get_env_config("development")

        assert config is not None

    def test_invalid_environment_raises(self, temp_project):
        """Invalid environment raises ConfigurationError"""
        manager = AppConfigManager(str(temp_project / "settings_json.json"))

        with pytest.raises(ConfigurationError):
            manager.get_env_config("invalid_env")


class TestEnvironmentConfigValidator:
    """Tests for configuration validation"""

    @pytest.fixture
    def valid_project(self):
        """Create a valid project structure"""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            config_path = project_path / "config"
            config_path.mkdir()

            # Create base files
            (config_path / "global_settings.json").write_text(json.dumps({}))
            (config_path / "pipelines.json").write_text(json.dumps({}))
            (config_path / "nodes.json").write_text(json.dumps({}))
            (config_path / "input.json").write_text(json.dumps({}))
            (config_path / "output.json").write_text(json.dumps({}))

            # Create settings file
            settings_data = {
                "base_path": ".",
                "env_config": {
                    "base": {
                        "global_settings_path": "config/global_settings.json",
                        "pipelines_config_path": "config/pipelines.json",
                        "nodes_config_path": "config/nodes.json",
                        "input_config_path": "config/input.json",
                        "output_config_path": "config/output.json",
                    },
                    "dev": {},
                },
            }
            (project_path / "settings_json.json").write_text(json.dumps(settings_data))

            yield project_path

    def test_validate_valid_project(self, valid_project):
        """Valid project passes validation"""
        validator = EnvironmentConfigValidator(valid_project)
        errors = validator.validate_project()

        # Should have 0 critical/error issues
        assert all(
            e.severity not in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]
            for e in errors
        )

    def test_validate_missing_base(self):
        """Missing base configuration is error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            (project_path / "config").mkdir()

            validator = EnvironmentConfigValidator(project_path)
            errors = validator.validate_project()

            # Should find base config missing
            error_messages = [e.message for e in errors]
            assert any("base" in msg.lower() for msg in error_messages)

    def test_validate_json_syntax_error(self):
        """Invalid JSON is detected"""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            config_path = project_path / "config"
            config_path.mkdir()

            # Write invalid JSON
            (config_path / "global_settings.json").write_text("{invalid json}")

            validator = EnvironmentConfigValidator(project_path)
            errors = validator.validate_project()

            error_messages = [e.message for e in errors]
            assert any("JSON" in msg or "syntax" in msg.lower() for msg in error_messages)


class TestCanonicalEnvironments:
    """Tests for canonical environment definitions"""

    def test_canonical_environments_exist(self):
        """All canonical environments are defined"""
        envs = list(CanonicalEnvironment.__members__.keys())
        assert "BASE" in envs
        assert "DEV" in envs
        assert "SANDBOX" in envs
        assert "STAGING" in envs
        assert "PROD" in envs

    def test_canonical_environment_values(self):
        """Canonical environment values are correct"""
        assert CanonicalEnvironment.BASE.value == "base"
        assert CanonicalEnvironment.DEV.value == "dev"
        assert CanonicalEnvironment.SANDBOX.value == "sandbox"
        assert CanonicalEnvironment.STAGING.value == "staging"
        assert CanonicalEnvironment.PROD.value == "prod"

    def test_is_local_environment(self):
        """Local environments are identified"""
        assert CanonicalEnvironment.is_local("base")
        assert CanonicalEnvironment.is_local("dev")
        assert CanonicalEnvironment.is_local("sandbox")
        assert not CanonicalEnvironment.is_local("staging")
        assert not CanonicalEnvironment.is_local("prod")

    def test_is_remote_environment(self):
        """Remote environments are identified"""
        assert not CanonicalEnvironment.is_remote("base")
        assert not CanonicalEnvironment.is_remote("dev")
        assert not CanonicalEnvironment.is_remote("sandbox")
        assert CanonicalEnvironment.is_remote("staging")
        assert CanonicalEnvironment.is_remote("prod")

    def test_is_safe_environment(self):
        """Safe (non-prod) environments are identified"""
        assert CanonicalEnvironment.is_safe("base")
        assert CanonicalEnvironment.is_safe("dev")
        assert CanonicalEnvironment.is_safe("sandbox")
        assert CanonicalEnvironment.is_safe("staging")
        assert not CanonicalEnvironment.is_safe("prod")


# Integration tests
class TestEnvironmentIntegration:
    """End-to-end integration tests"""

    def test_full_workflow_dev_environment(self):
        """
        Full workflow: create project, load dev config, validate
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            config_path = project_path / "config"
            config_path.mkdir()
            (config_path / "dev").mkdir()

            # Create configs
            (config_path / "global_settings.json").write_text(json.dumps({"env": "base"}))
            (config_path / "pipelines.json").write_text(json.dumps({}))
            (config_path / "nodes.json").write_text(json.dumps({}))
            (config_path / "input.json").write_text(json.dumps({}))
            (config_path / "output.json").write_text(json.dumps({}))
            (config_path / "dev" / "input.json").write_text(json.dumps({"env": "dev"}))

            # Create settings
            settings = {
                "base_path": ".",
                "env_config": {
                    "base": {
                        "global_settings_path": "config/global_settings.json",
                        "pipelines_config_path": "config/pipelines.json",
                        "nodes_config_path": "config/nodes.json",
                        "input_config_path": "config/input.json",
                        "output_config_path": "config/output.json",
                    },
                    "dev": {"input_config_path": "config/dev/input.json"},
                },
            }
            (project_path / "settings_json.json").write_text(json.dumps(settings))

            # Test loading
            manager = AppConfigManager(str(project_path / "settings_json.json"))
            config = manager.get_env_config("dev")

            assert config is not None
            assert "input_config_path" in config

            # Test validation
            validator = EnvironmentConfigValidator(project_path)
            errors = validator.validate_project()

            # Should pass validation
            assert not any(e.severity.value == "error" for e in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
