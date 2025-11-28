"""
Unit tests for template generation functionality.

Tests cover:
- Template factory and creation
- Configuration generation for different formats
- Directory structure creation
- Error handling and edge cases
"""

import json
import tempfile
from pathlib import Path

import pytest  # type: ignore
from loguru import logger

from core.cli.core import ConfigFormat, ExitCode
from core.cli.template import (
    TemplateType,
    TemplateError,
    BaseTemplate,
    MedallionBasicTemplate,
    TemplateFactory,
    TemplateGenerator,
)

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class TestTemplateType:
    """Tests for TemplateType enum."""

    def test_template_type_medallion_basic(self):
        """Test MEDALLION_BASIC template type exists."""
        assert TemplateType.MEDALLION_BASIC.value == "medallion_basic"

    def test_template_type_all_values(self):
        """Test all available template types."""
        types = list(TemplateType)
        assert len(types) >= 1
        assert TemplateType.MEDALLION_BASIC in types


class TestTemplateError:
    """Tests for TemplateError exception."""

    def test_template_error_creation(self):
        """Test creating a TemplateError."""
        error = TemplateError("Test error message")
        assert str(error) == "Test error message"
        assert error.exit_code == ExitCode.CONFIGURATION_ERROR

    def test_template_error_inherits_from_tauro_error(self):
        """Test TemplateError inherits from TauroError."""
        from core.cli.core import TauroError

        error = TemplateError("Test")
        assert isinstance(error, TauroError)


class TestBaseTemplate:
    """Tests for BaseTemplate abstract base class."""

    def test_cannot_instantiate_base_template(self):
        """Test that BaseTemplate cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseTemplate("test_project")

    def test_medallion_basic_template_instantiation(self):
        """Test creating MedallionBasicTemplate instance."""
        template = MedallionBasicTemplate("my_project", ConfigFormat.YAML)
        assert template.project_name == "my_project"
        assert template.config_format == ConfigFormat.YAML

    def test_template_get_file_extension_yaml(self):
        """Test get_file_extension returns .yaml for YAML format."""
        template = MedallionBasicTemplate("test", ConfigFormat.YAML)
        assert template._get_file_extension() == ".yaml"

    def test_template_get_file_extension_json(self):
        """Test get_file_extension returns .json for JSON format."""
        template = MedallionBasicTemplate("test", ConfigFormat.JSON)
        assert template._get_file_extension() == ".json"

    def test_template_get_file_extension_dsl(self):
        """Test get_file_extension returns .dsl for DSL format."""
        template = MedallionBasicTemplate("test", ConfigFormat.DSL)
        assert template._get_file_extension() == ".dsl"

    def test_template_get_template_type(self):
        """Test getting template type."""
        template = MedallionBasicTemplate("test")
        assert template.get_template_type() == TemplateType.MEDALLION_BASIC

    def test_common_global_settings(self):
        """Test get_common_global_settings returns expected fields."""
        template = MedallionBasicTemplate("my_project")
        settings = template.get_common_global_settings()

        assert settings["project_name"] == "my_project"
        assert settings["version"] == "1.0.0"
        assert settings["architecture"] == "medallion"
        assert settings["layers"] == ["bronze", "silver", "gold"]
        assert "created_at" in settings


class TestMedallionBasicTemplate:
    """Tests for MedallionBasicTemplate."""

    def test_generate_global_settings(self):
        """Test generating global settings configuration."""
        template = MedallionBasicTemplate("test_project")
        settings = template.generate_global_settings()

        assert settings["project_name"] == "test_project"
        assert "data_root" in settings
        assert "bronze_path" in settings
        assert "silver_path" in settings
        assert "gold_path" in settings

    def test_generate_pipelines_config(self):
        """Test generating pipelines configuration."""
        template = MedallionBasicTemplate("test")
        pipelines = template.generate_pipelines_config()

        assert "load" in pipelines
        assert "transform" in pipelines
        assert "aggregate" in pipelines

    def test_pipelines_have_descriptions(self):
        """Test that all pipelines have descriptions."""
        template = MedallionBasicTemplate("test")
        pipelines = template.generate_pipelines_config()

        for pipeline_name, pipeline_config in pipelines.items():
            assert "description" in pipeline_config
            assert "type" in pipeline_config
            assert "nodes" in pipeline_config

    def test_generate_nodes_config(self):
        """Test generating nodes configuration."""
        template = MedallionBasicTemplate("test")
        nodes = template.generate_nodes_config()

        assert "load_raw_data" in nodes
        assert "clean_data" in nodes
        assert "calculate_metrics" in nodes

    def test_nodes_have_required_fields(self):
        """Test that all nodes have required configuration fields."""
        template = MedallionBasicTemplate("test")
        nodes = template.generate_nodes_config()

        for node_name, node_config in nodes.items():
            assert "description" in node_config
            assert "module" in node_config
            assert "function" in node_config
            if "dependencies" in node_config:
                assert isinstance(node_config["dependencies"], list)

    def test_generate_input_config(self):
        """Test generating input configuration."""
        template = MedallionBasicTemplate("test")
        input_config = template.generate_input_config()

        assert "raw_data_source" in input_config
        assert input_config["raw_data_source"]["format"] == "csv"

    def test_generate_output_config(self):
        """Test generating output configuration."""
        template = MedallionBasicTemplate("test")
        output_config = template.generate_output_config()

        assert "bronze_data" in output_config
        assert "silver_data" in output_config
        assert "gold_metrics" in output_config
        assert output_config["bronze_data"]["format"] == "delta"

    def test_generate_settings_json(self):
        """Test generating settings.json index file."""
        template = MedallionBasicTemplate("test")
        settings = template.generate_settings_json()

        assert "base_path" in settings
        assert "env_config" in settings
        assert "base" in settings["env_config"]
        assert "dev" in settings["env_config"]
        assert "sandbox" in settings["env_config"]
        assert "prod" in settings["env_config"]


class TestTemplateFactory:
    """Tests for TemplateFactory."""

    def test_create_medallion_basic_template(self):
        """Test creating MedallionBasicTemplate via factory."""
        template = TemplateFactory.create_template(
            TemplateType.MEDALLION_BASIC, "my_project", ConfigFormat.YAML
        )
        assert isinstance(template, MedallionBasicTemplate)
        assert template.project_name == "my_project"

    def test_list_available_templates(self):
        """Test listing available templates."""
        templates = TemplateFactory.list_available_templates()

        assert len(templates) >= 1
        assert any(t["type"] == "medallion_basic" for t in templates)

        template = templates[0]
        assert "type" in template
        assert "name" in template
        assert "description" in template


class TestTemplateGeneratorExecution:
    """Tests for TemplateGenerator project generation."""

    def test_generate_project_creates_directory(self):
        """Test that generate_project creates output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            assert output_path.exists()
            assert output_path.is_dir()

    def test_generate_project_creates_config_directory(self):
        """Test that config directory structure is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            assert (output_path / "config").exists()
            assert (output_path / "config" / "dev").exists()
            assert (output_path / "config" / "sandbox").exists()

    def test_generate_project_creates_pipelines_directory(self):
        """Test that pipelines directory is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            assert (output_path / "pipelines").exists()

    def test_generate_project_creates_data_directories(self):
        """Test that data layer directories are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            assert (output_path / "data" / "bronze").exists()
            assert (output_path / "data" / "silver").exists()
            assert (output_path / "data" / "gold").exists()

    @pytest.mark.skipif(not HAS_YAML, reason="PyYAML not installed")
    def test_generate_project_yaml_format(self):
        """Test generating project with YAML format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            # Check YAML files exist
            yaml_files = list(output_path.glob("config/**/*.yaml"))
            assert len(yaml_files) > 0

    def test_generate_project_json_format(self):
        """Test generating project with JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            # Check JSON files exist
            json_files = list(output_path.glob("config/**/*.json"))
            assert len(json_files) > 0

    def test_generate_project_with_sample_code(self):
        """Test generating project with sample code."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=True,
            )

            # Check sample code files exist
            assert (output_path / "pipelines" / "load.py").exists()
            assert (output_path / "pipelines" / "transform.py").exists()
            assert (output_path / "pipelines" / "aggregate.py").exists()

    def test_generate_project_creates_readme(self):
        """Test that README.md is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            readme_path = output_path / "README.md"
            assert readme_path.exists()

            # Check README contains project name
            content = readme_path.read_text()
            assert "test_project" in content

    def test_generate_project_creates_requirements(self):
        """Test that requirements.txt is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            req_path = output_path / "requirements.txt"
            assert req_path.exists()

            # Check requirements contain tauro
            content = req_path.read_text()
            assert "tauro" in content

    def test_generate_project_without_yaml_library(self, monkeypatch):
        """Test that missing PyYAML is caught early."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            # Mock HAS_YAML to False
            import core.cli.template as template_module

            original_has_yaml = template_module.HAS_YAML
            monkeypatch.setattr(template_module, "HAS_YAML", False)

            try:
                with pytest.raises(TemplateError) as exc_info:
                    generator.generate_project(
                        TemplateType.MEDALLION_BASIC,
                        "test_project",
                        create_sample_code=False,
                    )
                assert "PyYAML" in str(exc_info.value)
            finally:
                monkeypatch.setattr(template_module, "HAS_YAML", original_has_yaml)


class TestTemplateConfigGeneration:
    """Tests for template configuration file generation."""

    @pytest.mark.skipif(not HAS_YAML, reason="PyYAML not installed")
    def test_yaml_config_is_valid(self):
        """Test that generated YAML configuration is valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            # Load and parse a YAML file
            yaml_file = output_path / "config" / "global_settings.yaml"
            assert yaml_file.exists()

            with open(yaml_file) as f:
                config = yaml.safe_load(f)

            assert config is not None
            assert "project_name" in config

    def test_json_config_is_valid(self):
        """Test that generated JSON configuration is valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            # Load and parse a JSON file
            json_file = output_path / "config" / "global_settings.json"
            assert json_file.exists()

            with open(json_file) as f:
                config = json.load(f)

            assert config is not None
            assert "project_name" in config

    @pytest.mark.skipif(not HAS_YAML, reason="PyYAML not installed")
    def test_settings_index_file_exists(self):
        """Test that settings index file is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "test_project",
                create_sample_code=False,
            )

            settings_file = output_path / "settings_yml.json"
            assert settings_file.exists()

            with open(settings_file) as f:
                settings = json.load(f)

            assert "env_config" in settings


class TestTemplateIntegration:
    """Integration tests for template generation."""

    @pytest.mark.skipif(not HAS_YAML, reason="PyYAML not installed")
    def test_end_to_end_yaml_project(self):
        """Test complete project generation with YAML format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "e2e_project"
            generator = TemplateGenerator(output_path, ConfigFormat.YAML)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "e2e_project",
                create_sample_code=True,
                developer_sandboxes=["alice", "bob"],
            )

            # Verify directory structure
            assert output_path.exists()
            assert (output_path / "config").exists()
            assert (output_path / "config" / "sandbox_alice").exists()
            assert (output_path / "config" / "sandbox_bob").exists()
            assert (output_path / "pipelines").exists()
            assert (output_path / "data").exists()

            # Verify files exist
            assert (output_path / "README.md").exists()
            assert (output_path / "requirements.txt").exists()
            assert (output_path / "pyproject.toml").exists()

            # Verify sample code
            assert (output_path / "pipelines" / "load.py").exists()

    def test_end_to_end_json_project(self):
        """Test complete project generation with JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "e2e_json_project"
            generator = TemplateGenerator(output_path, ConfigFormat.JSON)

            generator.generate_project(
                TemplateType.MEDALLION_BASIC,
                "e2e_json_project",
                create_sample_code=True,
            )

            # Verify basic structure
            assert output_path.exists()
            assert (output_path / "config").exists()
            assert (output_path / "pipelines").exists()
            assert (output_path / "data").exists()

            # Verify JSON config files
            global_settings = output_path / "config" / "global_settings.json"
            assert global_settings.exists()

            with open(global_settings) as f:
                config = json.load(f)
            assert config["project_name"] == "e2e_json_project"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
