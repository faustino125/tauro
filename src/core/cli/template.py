"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from core.cli.core import ConfigFormat, ExitCode, TauroError

try:
    import yaml  # type: ignore

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class TemplateType(Enum):
    """Available template types (reduced to a single, simple Medallion template)."""

    MEDALLION_BASIC = "medallion_basic"


class TemplateError(TauroError):
    """Exception for template-related errors."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.CONFIGURATION_ERROR)


class BaseTemplate(ABC):
    """Abstract base class for configuration templates."""

    def __init__(self, project_name: str, config_format: ConfigFormat = ConfigFormat.YAML):
        self.project_name = project_name
        self.config_format = config_format
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @abstractmethod
    def get_template_type(self) -> TemplateType:
        """Get the template type."""
        pass

    @abstractmethod
    def generate_global_settings(self) -> Dict[str, Any]:
        """Generate global settings configuration."""
        pass

    @abstractmethod
    def generate_pipelines_config(self) -> Dict[str, Any]:
        """Generate pipelines configuration."""
        pass

    @abstractmethod
    def generate_nodes_config(self) -> Dict[str, Any]:
        """Generate nodes configuration."""
        pass

    @abstractmethod
    def generate_input_config(self) -> Dict[str, Any]:
        """Generate input configuration."""
        pass

    @abstractmethod
    def generate_output_config(self) -> Dict[str, Any]:
        """Generate output configuration."""
        pass

    def generate_settings_json(self) -> Dict[str, Any]:
        """Generate the main settings.json file with environment mappings."""
        file_ext = self._get_file_extension()

        # Include 'sandbox' to align with CLI environment choices
        return {
            "base_path": ".",
            "env_config": {
                "base": {
                    "global_settings_path": f"config/global_settings{file_ext}",
                    "pipelines_config_path": f"config/pipelines{file_ext}",
                    "nodes_config_path": f"config/nodes{file_ext}",
                    "input_config_path": f"config/input{file_ext}",
                    "output_config_path": f"config/output{file_ext}",
                },
                "dev": {
                    "global_settings_path": f"config/dev/global_settings{file_ext}",
                    "input_config_path": f"config/dev/input{file_ext}",
                    "output_config_path": f"config/dev/output{file_ext}",
                },
                "sandbox": {
                    "global_settings_path": f"config/sandbox/global_settings{file_ext}",
                    "input_config_path": f"config/sandbox/input{file_ext}",
                    "output_config_path": f"config/sandbox/output{file_ext}",
                },
                "prod": {
                    "global_settings_path": f"config/prod/global_settings{file_ext}",
                    "input_config_path": f"config/prod/input{file_ext}",
                    "output_config_path": f"config/prod/output{file_ext}",
                },
            },
        }

    def _get_file_extension(self) -> str:
        """Get file extension based on config format."""
        if self.config_format == ConfigFormat.YAML:
            return ".yaml"
        elif self.config_format == ConfigFormat.JSON:
            return ".json"
        else:
            return ".dsl"

    def get_common_global_settings(self) -> Dict[str, Any]:
        """Get common global settings for all templates."""
        return {
            "project_name": self.project_name,
            "version": "1.0.0",
            "created_at": self.timestamp,
            "template_type": self.get_template_type().value,
            "architecture": "medallion",
            "layers": ["bronze", "silver", "gold"],
            "mode": "local",  # change to 'databricks' or 'spark' if needed
            "max_parallel_nodes": 4,
            "fail_on_error": True,
        }


_TEMPLATE_CANCELLED_MSG = "Template generation cancelled"


class MedallionBasicTemplate(BaseTemplate):
    """Simple Medallion template supporting batch and streaming pipelines."""

    def get_template_type(self) -> TemplateType:
        return TemplateType.MEDALLION_BASIC

    def generate_global_settings(self) -> Dict[str, Any]:
        base_settings = self.get_common_global_settings()
        base_settings.update(
            {
                "default_start_date": "2025-01-01",
                "default_end_date": "2025-12-31",
                "data_root": "data",
                "bronze_path": "data/bronze",
                "silver_path": "data/silver",
                "gold_path": "data/gold",
                "spark_master": "local[*]",
                "spark_config": {
                    "spark.sql.shuffle.partitions": "4",
                    "spark.default.parallelism": "4",
                    "spark.sql.adaptive.enabled": "true",
                },
                "validation": {
                    "check_required_columns": True,
                    "check_empty_dataframes": True,
                    "fail_on_validation_error": True,
                },
                "logging": {
                    "level": "INFO",
                    "format": "structured",
                },
                "max_retries": 3,
                "retry_wait_seconds": 5,
            }
        )
        return base_settings

    def generate_pipelines_config(self) -> Dict[str, Any]:
        """Simplified pipeline configuration with clear dependencies."""
        return {
            "load": {
                "description": "Load raw data from source to Bronze layer",
                "type": "batch",
                "nodes": ["load_raw_data"],
                "inputs": ["raw_data_source"],
                "outputs": ["bronze_data"],
            },
            "transform": {
                "description": "Transform Bronze to Silver (data quality, enrichment)",
                "type": "batch",
                "nodes": ["validate_data", "clean_data"],
                "inputs": ["bronze_data"],
                "outputs": ["silver_data"],
            },
            "aggregate": {
                "description": "Aggregate Silver to Gold (business metrics)",
                "type": "batch",
                "nodes": ["calculate_metrics"],
                "inputs": ["silver_data"],
                "outputs": ["gold_metrics"],
            },
        }

    def generate_nodes_config(self) -> Dict[str, Any]:
        """Minimal, focused node definitions with clear data flow."""
        return {
            # Bronze: Load raw data
            "load_raw_data": {
                "description": "Load raw data from source files",
                "module": "pipelines.load",
                "function": "load_raw_data",
                "input": ["raw_data_source"],
                "output": ["bronze_data"],
                "dependencies": [],
            },
            # Silver: Validate
            "validate_data": {
                "description": "Validate data quality and schema",
                "module": "pipelines.transform",
                "function": "validate_data",
                "input": ["bronze_data"],
                "output": ["validated_data"],
                "dependencies": ["load_raw_data"],
            },
            # Silver: Clean
            "clean_data": {
                "description": "Clean and standardize data",
                "module": "pipelines.transform",
                "function": "clean_data",
                "input": ["validated_data"],
                "output": ["silver_data"],
                "dependencies": ["validate_data"],
            },
            # Gold: Calculate metrics
            "calculate_metrics": {
                "description": "Calculate business metrics and aggregations",
                "module": "pipelines.aggregate",
                "function": "calculate_metrics",
                "input": ["silver_data"],
                "output": ["gold_metrics"],
                "dependencies": ["clean_data"],
            },
        }

    def generate_input_config(self) -> Dict[str, Any]:
        """Input configuration with format validation."""
        return {
            "raw_data_source": {
                "description": "Raw data from CSV source",
                "format": "csv",
                "filepath": "data/raw/input.csv",
                "options": {
                    "header": True,
                    "inferSchema": True,
                    "encoding": "utf-8",
                },
                "validation": {
                    "required_columns": ["id", "name", "value"],
                    "check_empty": True,
                },
            },
        }

    def generate_output_config(self) -> Dict[str, Any]:
        """Output configuration with multiple format support."""
        return {
            # Bronze layer
            "bronze_data": {
                "description": "Raw data ingested to Bronze",
                "format": "delta",
                "filepath": "data/bronze",
                "write_mode": "append",
                "partitioned_by": [],
                "vacuum": True,
            },
            # Silver layer
            "validated_data": {
                "description": "Intermediate validated data",
                "format": "delta",
                "filepath": "data/silver/validated",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "silver_data": {
                "description": "Cleaned data in Silver layer",
                "format": "delta",
                "filepath": "data/silver",
                "write_mode": "overwrite",
                "partitioned_by": [],
                "vacuum": True,
                "optimize": True,
            },
            # Gold layer
            "gold_metrics": {
                "description": "Business metrics in Gold layer",
                "format": "delta",
                "filepath": "data/gold",
                "write_mode": "overwrite",
                "partitioned_by": [],
                "vacuum": True,
                "optimize": True,
            },
        }


class TemplateFactory:
    """Factory for creating configuration templates (single option)."""

    TEMPLATES = {
        TemplateType.MEDALLION_BASIC: MedallionBasicTemplate,
    }

    @classmethod
    def create_template(
        cls,
        template_type: TemplateType,
        project_name: str,
        config_format: ConfigFormat = ConfigFormat.YAML,
    ) -> BaseTemplate:
        """Create a template instance."""
        if template_type not in cls.TEMPLATES:
            available = list(cls.TEMPLATES.keys())
            raise TemplateError(
                f"Template type '{template_type.value}' not supported. Available: {[t.value for t in available]}"
            )

        template_class = cls.TEMPLATES[template_type]
        return template_class(project_name, config_format)

    @classmethod
    def list_available_templates(cls) -> List[Dict[str, str]]:
        """List the single available template with a clear description."""
        return [
            {
                "type": TemplateType.MEDALLION_BASIC.value,
                "name": "Medallion (Batch + Streaming)",
                "description": "Simple Medallion architecture with batch and streaming examples (Bronze, Silver, Gold).",
            }
        ]


class TemplateGenerator:
    """Generates complete project templates with directory structure."""

    def __init__(self, output_path: Path, config_format: ConfigFormat = ConfigFormat.YAML):
        self.output_path = Path(output_path)
        self.config_format = config_format
        self._file_extension = self._get_file_extension()

    def _get_file_extension(self) -> str:
        """Get file extension based on config format."""
        if self.config_format == ConfigFormat.YAML:
            return ".yaml"
        elif self.config_format == ConfigFormat.JSON:
            return ".json"
        else:
            return ".dsl"

    def _settings_filename(self) -> str:
        """Return settings file name aligned with ConfigDiscovery/ConfigManager expectations."""
        if self.config_format == ConfigFormat.YAML:
            return "settings_yml.json"
        elif self.config_format == ConfigFormat.JSON:
            return "settings_json.json"
        else:
            return "settings_dsl.json"

    def generate_project(
        self,
        template_type: TemplateType,
        project_name: str,
        create_sample_code: bool = True,
        developer_sandboxes: Optional[List[str]] = None,
    ) -> None:
        """Generate complete project structure from template."""
        logger.info(f"Generating {template_type.value} template for project '{project_name}'")

        # Create template instance
        template = TemplateFactory.create_template(template_type, project_name, self.config_format)

        # Create directory structure
        self._create_directory_structure(developer_sandboxes)

        # Generate configuration files
        self._generate_config_files(template, developer_sandboxes)

        # Generate sample code if requested
        if create_sample_code:
            self._generate_sample_code()

        # Generate additional project files
        self._generate_project_files(template)

        logger.success(f"Project '{project_name}' generated successfully at {self.output_path}")

    def _create_directory_structure(self, developer_sandboxes: Optional[List[str]] = None) -> None:
        """Create project directory structure."""
        directories = [
            "config",
            "config/dev",
            "config/sandbox",
            "config/prod",
            "pipelines",
            "src",
            "tests",
            "tests/unit",
            "tests/integration",
            "docs",
            "notebooks",
            "data",
            "data/raw",
            "data/bronze",
            "data/silver",
            "data/gold",
            "data/checkpoints",
            "logs",
        ]

        # Add developer sandbox directories
        if developer_sandboxes:
            for dev in developer_sandboxes:
                directories.append(f"config/sandbox_{dev}")

        for directory in directories:
            dir_path = self.output_path / directory
            dir_path.mkdir(parents=True, exist_ok=True)

            # Create __init__.py files for Python packages
            if (
                directory.startswith("pipelines")
                or directory == "src"
                or directory.startswith("tests")
            ):
                (dir_path / "__init__.py").touch()

    def _generate_config_files(
        self, template: BaseTemplate, developer_sandboxes: Optional[List[str]] = None
    ) -> None:
        """Generate all configuration files."""
        configs = {
            "global_settings": template.generate_global_settings(),
            "pipelines": template.generate_pipelines_config(),
            "nodes": template.generate_nodes_config(),
            "input": template.generate_input_config(),
            "output": template.generate_output_config(),
        }

        # Generate main settings file
        settings_file = self.output_path / self._settings_filename()
        self._write_json_file(settings_file, template.generate_settings_json())

        # Generate configuration files for each environment
        environments = ["base", "dev", "sandbox", "prod"]

        # Add developer sandbox environments
        if developer_sandboxes:
            environments.extend([f"sandbox_{dev}" for dev in developer_sandboxes])

        for env in environments:
            config_dir = self.output_path / "config" / (env if env != "base" else "")

            for config_name, config_data in configs.items():
                # Only generate pipelines/nodes for base environment
                if env != "base" and config_name in ["pipelines", "nodes"]:
                    continue

                file_path = config_dir / f"{config_name}{self._file_extension}"
                self._write_config_file(file_path, config_data)

    def _write_config_file(self, file_path: Path, config_data: Dict[str, Any]) -> None:
        """Write configuration file in the specified format."""
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if self.config_format == ConfigFormat.YAML:
            self._write_yaml_file(file_path, config_data)
        elif self.config_format == ConfigFormat.JSON:
            self._write_json_file(file_path, config_data)
        else:
            self._write_dsl_file(file_path, config_data)

    def _write_yaml_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write YAML file."""
        if not HAS_YAML:
            raise TemplateError("PyYAML not available. Install with: pip install PyYAML")

        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, indent=2)

    def _write_json_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write JSON file."""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _write_dsl_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write DSL file using [section] and [parent.child] headers."""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"# Configuration file generated on {datetime.now()}\n\n")
            for top_key, top_val in data.items():
                if isinstance(top_val, dict):
                    self._write_dsl_sections(f, [top_key], top_val)
                else:
                    f.write(f"{top_key} = {self._fmt_dsl_value(top_val)}\n")

    def _write_dsl_sections(self, f, path: List[str], obj: Dict[str, Any]) -> None:
        """Write a section [a.b.c] and its scalar keys, then recurse for sub-dicts."""
        section_name = ".".join(path)
        f.write(f"[{section_name}]\n")
        nested_items: List[tuple[str, Any]] = []
        for k, v in obj.items():
            if isinstance(v, dict):
                nested_items.append((k, v))
            else:
                f.write(f"{k} = {self._fmt_dsl_value(v)}\n")
        f.write("\n")
        for k, v in nested_items:
            self._write_dsl_sections(f, path + [k], v)

    def _fmt_dsl_value(self, v: Any) -> str:
        """Format values for DSL (strings with quotes, bool lowercase, lists with formatted elements)."""
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, list):
            items = ", ".join(self._fmt_dsl_value(x) for x in v)
            return f"[{items}]"
        if isinstance(v, dict):
            return json.dumps(v, ensure_ascii=False)
        s = str(v).replace('"', '\\"')
        return f'"{s}"'

    def _generate_sample_code(self) -> None:
        """Generate functional sample Python code with Tauro integration."""
        # Load module
        load_code = '''"""Load layer: Read raw data from sources."""
from typing import Any, Dict, Optional
from loguru import logger


def load_raw_data(
    raw_data_source: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Any:
    """
    Load raw data from source.
    
    Args:
        raw_data_source: DataFrame from input config
        start_date: Start date for filtering (ISO format)
        end_date: End date for filtering (ISO format)
    
    Returns:
        DataFrame with loaded raw data
    """
    logger.info(f"Loading raw data from {start_date} to {end_date}")
    
    # Tauro automatically loads data via InputLoader
    # This function receives the loaded DataFrame
    if raw_data_source is None:
        logger.warning("No data provided")
        return None
    
    # Optional: Add filtering logic
    # if start_date and end_date:
    #     raw_data_source = raw_data_source.filter(
    #         (col("date") >= start_date) & (col("date") <= end_date)
    #     )
    
    row_count = raw_data_source.count() if hasattr(raw_data_source, "count") else 0
    logger.info(f"Loaded {row_count} rows")
    
    return raw_data_source
'''
        load_file = self.output_path / "pipelines" / "load.py"
        self._write_text_file(load_file, load_code)

        # Transform module
        transform_code = '''"""Transform layer: Data validation and cleaning."""
from typing import Any, Optional
from loguru import logger


def validate_data(
    bronze_data: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Any:
    """
    Validate data quality and schema.
    
    Args:
        bronze_data: DataFrame from Bronze layer
        start_date: Start date (not used here, included for CLI compatibility)
        end_date: End date (not used here, included for CLI compatibility)
    
    Returns:
        Validated DataFrame
    """
    logger.info("Validating data quality")
    
    if bronze_data is None:
        raise ValueError("bronze_data cannot be None")
    
    # Check for required columns
    required_cols = {"id", "name", "value"}
    if hasattr(bronze_data, "columns"):
        available_cols = set(bronze_data.columns)
        missing_cols = required_cols - available_cols
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}")
    
    # Check for null values in critical columns
    if hasattr(bronze_data, "filter"):
        null_count = bronze_data.filter("id IS NULL").count()
        if null_count > 0:
            logger.warning(f"Found {null_count} rows with null IDs")
            # Optionally filter out nulls: bronze_data = bronze_data.filter("id IS NOT NULL")
    
    logger.info("Data validation complete")
    return bronze_data


def clean_data(
    validated_data: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Any:
    """
    Clean and standardize data.
    
    Args:
        validated_data: Validated DataFrame
        start_date: Start date for filtering
        end_date: End date for filtering
    
    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning data from {start_date} to {end_date}")
    
    if validated_data is None:
        raise ValueError("validated_data cannot be None")
    
    # Example transformations:
    # - Trim whitespace from string columns
    # - Convert data types
    # - Handle missing values
    
    # For PySpark/Pandas compatibility, keep it generic
    # validated_data = validated_data.withColumn("name", trim(col("name")))
    
    logger.info("Data cleaning complete")
    return validated_data
'''
        transform_file = self.output_path / "pipelines" / "transform.py"
        self._write_text_file(transform_file, transform_code)

        # Aggregate module
        aggregate_code = '''"""Aggregate layer: Business metrics and aggregations."""
from typing import Any, Optional
from loguru import logger


def calculate_metrics(
    silver_data: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Any:
    """
    Calculate business metrics and aggregations.
    
    Args:
        silver_data: Cleaned DataFrame from Silver layer
        start_date: Start date for metrics
        end_date: End date for metrics
    
    Returns:
        Aggregated metrics DataFrame
    """
    logger.info(f"Calculating metrics from {start_date} to {end_date}")
    
    if silver_data is None:
        raise ValueError("silver_data cannot be None")
    
    # Example aggregations:
    # - Count by category
    # - Sum values
    # - Calculate ratios
    # 
    # For PySpark:
    # from pyspark.sql.functions import col, count, sum, avg
    # metrics = silver_data.groupBy("name").agg(
    #     count("id").alias("count"),
    #     sum("value").alias("total_value"),
    #     avg("value").alias("avg_value")
    # )
    
    logger.info("Metrics calculation complete")
    return silver_data
'''
        aggregate_file = self.output_path / "pipelines" / "aggregate.py"
        self._write_text_file(aggregate_file, aggregate_code)

    def _generate_project_files(self, template: BaseTemplate) -> None:
        """Generate additional project files."""
        # README.md
        readme_content = f"""# {template.project_name}

Generated using **Tauro** {template.get_template_type().value} template.

## What's included

A production-ready data pipeline with:

- **Medallion Architecture** (Bronze → Silver → Gold)
  - Bronze: Raw data ingestion
  - Silver: Data validation and cleaning
  - Gold: Business metrics and aggregations

- **Tauro Features**
  - Multi-format I/O support (CSV, Delta, Parquet, JSON, etc.)
  - Automatic data validation
  - Structured logging
  - Environment-aware configuration (dev, sandbox, prod)
  - Dependency management
  - Parallel node execution

- **Production-Ready**
  - Type hints throughout
  - Error handling
  - Logging integration
  - Security path validation

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure your environment

Edit the configuration files under `./config/` and update:
- `settings_*.json` - Environment mapping
- `global_settings.yaml` - Global settings
- `pipelines.yaml` - Pipeline definitions
- `nodes.yaml` - Node definitions
- `input.yaml` - Input data sources
- `output.yaml` - Output configurations

### 3. Run a pipeline

```bash
# Show available pipelines
tauro config list-pipelines --env dev

# Run the complete pipeline
tauro run --env dev --pipeline load

# Run a specific node
tauro run --env dev --pipeline load --node load_raw_data

# Run with date range
tauro run --env dev --pipeline transform --start-date 2025-01-01 --end-date 2025-01-31
```

## Project structure

```
{template.project_name}/
├── config/                  # Configuration files by environment
│   ├── global_settings.yaml
│   ├── pipelines.yaml
│   ├── nodes.yaml
│   ├── input.yaml
│   ├── output.yaml
│   ├── dev/                # Dev environment overrides
│   ├── sandbox/            # Sandbox environment overrides
│   └── prod/               # Prod environment overrides
├── pipelines/              # Pipeline implementations
│   ├── load.py
│   ├── transform.py
│   └── aggregate.py
├── data/                   # Data directories
│   ├── raw/                # Raw input data
│   ├── bronze/             # Bronze layer
│   ├── silver/             # Silver layer
│   └── gold/               # Gold layer
├── tests/                  # Test suites
├── docs/                   # Documentation
├── notebooks/              # Jupyter notebooks
├── logs/                   # Execution logs
└── requirements.txt        # Python dependencies
```

## Pipeline flow

```
raw_data_source
    ↓
[load_raw_data] → bronze_data
    ↓
[validate_data] → validated_data
    ↓
[clean_data] → silver_data
    ↓
[calculate_metrics] → gold_metrics
```

## Common tasks

### List available pipelines

```bash
tauro config list-pipelines --env dev
```

### Validate configuration

```bash
tauro run --env dev --pipeline load --validate-only
```

### Run with debug logging

```bash
tauro run --env dev --pipeline load --log-level DEBUG
```

### Dry run (don't execute)

```bash
tauro run --env dev --pipeline load --dry-run
```

## Tauro features used

- ✅ Configuration discovery and loading (YAML/JSON/DSL)
- ✅ Multi-environment support (base, dev, sandbox, prod)
- ✅ Input validation and data loading
- ✅ Automatic output management with format support
- ✅ Dependency resolution and execution
- ✅ Structured logging with loguru
- ✅ Security path validation
- ✅ Error handling and retries

## Next steps

1. Implement your business logic in `pipelines/`
2. Update input/output configurations for your data sources
3. Add tests under `tests/`
4. Create additional pipelines as needed
5. Deploy to your target environment

## For more information

- See `config/` for detailed configuration examples
- Check `pipelines/` for implementation templates
- Review Tauro documentation for advanced features

Generated on: {template.timestamp}
"""
        readme_file = self.output_path / "README.md"
        self._write_text_file(readme_file, readme_content)

        # requirements.txt with focused dependencies
        requirements = """# Tauro framework
tauro>=0.1.0

# Data processing (Spark optional)
pyspark>=3.4.0
delta-spark>=2.4.0

# Data manipulation
pandas>=1.5.0
numpy>=1.23.0

# Utilities
loguru>=0.7.0
pyyaml>=6.0
python-dateutil>=2.8.2

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0

# Optional: Kafka for streaming
# kafka-python>=2.0.2
"""
        requirements_file = self.output_path / "requirements.txt"
        self._write_text_file(requirements_file, requirements)

        # pyproject.toml for modern Python packaging
        pyproject_content = f"""[build-system]
requires = ["setuptools>=65", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{template.project_name.lower().replace(' ', '_')}"
version = "0.1.0"
description = "Data pipeline built with Tauro"
requires-python = ">=3.9"
dependencies = [
    "tauro>=0.1.0",
    "pyspark>=3.4.0",
    "delta-spark>=2.4.0",
    "pandas>=1.5.0",
    "numpy>=1.23.0",
    "loguru>=0.7.0",
    "pyyaml>=6.0",
    "python-dateutil>=2.8.2",
]

[project.optional-dependencies]
dev = ["pytest>=7.4.0", "pytest-cov>=4.1.0", "black>=23.0", "flake8>=6.0"]
streaming = ["kafka-python>=2.0.2"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
addopts = "--cov=pipelines --cov-report=term-missing"
"""
        pyproject_file = self.output_path / "pyproject.toml"
        self._write_text_file(pyproject_file, pyproject_content)

        # .gitignore
        gitignore = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Data and artifacts
data/raw/*
data/bronze/*
data/silver/*
data/gold/*
data/checkpoints/*
!data/raw/.gitkeep

# Logs
logs/*.log

# Notebooks
.ipynb_checkpoints/
*.ipynb

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Test coverage
.coverage
htmlcov/
.pytest_cache/

# Spark
metastore_db/
spark-warehouse/
*.jar

# Environment variables
.env
.env.local
"""
        gitignore_file = self.output_path / ".gitignore"
        self._write_text_file(gitignore_file, gitignore)

        # Create sample data file
        sample_data = """id,name,value
1,Item A,100
2,Item B,200
3,Item C,150
4,Item D,300
5,Item E,250
"""
        sample_data_file = self.output_path / "data" / "raw" / "input.csv"
        self._write_text_file(sample_data_file, sample_data)

    def _write_text_file(self, file_path: Path, content: str) -> None:
        """Write text file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)


class TemplateCommand:
    """Handles the --template command functionality."""

    def __init__(self):
        self.generator = None

    def handle_template_command(
        self,
        template_type: Optional[str] = None,
        project_name: Optional[str] = None,
        output_path: Optional[str] = None,
        config_format: str = "yaml",
        create_sample_code: bool = True,
        list_templates: bool = False,
        interactive: bool = False,
        sandbox_developers: Optional[List[str]] = None,
    ) -> int:
        """Handle template generation command."""
        try:
            if list_templates:
                return self._list_templates()

            if interactive:
                return self._interactive_generation()

            if not template_type or not project_name:
                logger.error("Template type and project name are required")
                logger.info("Use --list-templates to see available templates")
                return ExitCode.VALIDATION_ERROR.value

            return self._generate_template(
                template_type,
                project_name,
                output_path,
                config_format,
                create_sample_code,
                sandbox_developers,
            )

        except TemplateError as e:
            logger.error(f"Template error: {e}")
            return e.exit_code.value
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return ExitCode.GENERAL_ERROR.value

    def _list_templates(self) -> int:
        """List all available templates."""
        templates = TemplateFactory.list_available_templates()

        logger.info("Available template types:")
        for template in templates:
            logger.info(f"  {template['type']:20} - {template['name']}")
            logger.info(f"  {' ' * 20}   {template['description']}")
            logger.info("")

        logger.info("Usage:")
        logger.info("  tauro template --template <type> --project-name <name> [options]")
        logger.info("")
        logger.info("Examples:")
        logger.info("  tauro template --template medallion_basic --project-name my_pipeline")
        logger.info(
            "  tauro template --template medallion_basic --project-name my_pipeline --format json"
        )

        return ExitCode.SUCCESS.value

    def _interactive_generation(self) -> int:
        """Interactive template generation."""
        try:
            # Select template type
            templates = TemplateFactory.list_available_templates()

            print("\nAvailable templates:")
            for i, template in enumerate(templates, 1):
                print(f"  {i}. {template['name']} - {template['description']}")

            while True:
                try:
                    choice = input(f"\nSelect template (1-{len(templates)}): ").strip()
                    if choice.isdigit():
                        index = int(choice) - 1
                        if 0 <= index < len(templates):
                            selected_template = templates[index]
                            break
                    # Invalid selection -> prompt again
                    print("Invalid selection. Please try again or press Ctrl+C to cancel.")
                    continue
                except (KeyboardInterrupt, EOFError):
                    logger.info(_TEMPLATE_CANCELLED_MSG)
                    return ExitCode.GENERAL_ERROR.value

            # Get project name
            project_name = input("Enter project name: ").strip()
            if not project_name:
                logger.error("Project name cannot be empty")
                return ExitCode.VALIDATION_ERROR.value

            # Get output path
            default_output = f"./{project_name}"
            output_path = input(f"Output path (default: {default_output}): ").strip()
            if not output_path:
                output_path = default_output

            # Get config format
            formats = ["yaml", "json", "dsl"]
            print(f"\nConfig formats: {', '.join(formats)}")
            config_format = input("Config format (default: yaml): ").strip().lower()
            if not config_format:
                config_format = "yaml"
            elif config_format not in formats:
                logger.error(f"Invalid format. Use one of: {', '.join(formats)}")
                return ExitCode.VALIDATION_ERROR.value

            # Generate sample code
            create_code = input("Generate sample code? (Y/n): ").strip().lower()
            create_sample_code = create_code != "n"

            return self._generate_template(
                selected_template["type"],
                project_name,
                output_path,
                config_format,
                create_sample_code,
            )

        except Exception as e:
            logger.error(f"Interactive generation failed: {e}")
            return ExitCode.GENERAL_ERROR.value

    def _generate_template(
        self,
        template_type: str,
        project_name: str,
        output_path: Optional[str],
        config_format: str,
        create_sample_code: bool,
        sandbox_developers: Optional[List[str]] = None,
    ) -> int:
        """Generate template with specified parameters."""
        try:
            # Validate template type
            try:
                template_enum = TemplateType(template_type)
            except ValueError:
                available = [t.value for t in TemplateType]
                logger.error(f"Invalid template type: {template_type}")
                logger.info(f"Available types: {', '.join(available)}")
                return ExitCode.VALIDATION_ERROR.value

            # Validate config format
            try:
                format_enum = ConfigFormat(config_format)
            except ValueError:
                available = [f.value for f in ConfigFormat]
                logger.error(f"Invalid config format: {config_format}")
                logger.info(f"Available formats: {', '.join(available)}")
                return ExitCode.VALIDATION_ERROR.value

            # Set default output path
            if not output_path:
                output_path = f"./{project_name}"

            output_dir = Path(output_path)

            # Check if directory exists
            if output_dir.exists() and any(output_dir.iterdir()):
                logger.warning(f"Directory {output_dir} already exists and is not empty")
                logger.info(_TEMPLATE_CANCELLED_MSG)
                return ExitCode.VALIDATION_ERROR.value

            # Generate template
            self.generator = TemplateGenerator(output_dir, format_enum)
            self.generator.generate_project(
                template_enum, project_name, create_sample_code, sandbox_developers
            )

            # Show success message with next steps
            self._show_success_message(project_name, output_dir, template_enum)

            return ExitCode.SUCCESS.value

        except Exception as e:
            logger.error(f"Template generation failed: {e}")
            return ExitCode.GENERAL_ERROR.value

    def _show_success_message(
        self, project_name: str, output_dir: Path, template_type: TemplateType
    ) -> None:
        """Show success message with next steps."""
        logger.success(f"✅ Project '{project_name}' created successfully!")
        logger.info(f"📁 Location: {output_dir.absolute()}")
        logger.info(f"🏗️  Template: {template_type.value}")

        logger.info("\n📋 Next steps:")
        logger.info(f"1️⃣  cd {output_dir}")
        logger.info("2️⃣  pip install -r requirements.txt")
        logger.info("3️⃣  Review and customize config files in ./config")
        logger.info("4️⃣  Implement pipeline functions in ./pipelines")
        logger.info("5️⃣  Update input/output config with your data sources")

        logger.info("\n🚀 Quick start commands:")
        logger.info("   # List pipelines")
        logger.info("   tauro config list-pipelines --env dev")
        logger.info("")
        logger.info("   # Run the load pipeline")
        logger.info("   tauro run --env dev --pipeline load")
        logger.info("")
        logger.info("   # Run with specific date range")
        logger.info(
            "   tauro run --env dev --pipeline load --start-date 2025-01-01 --end-date 2025-01-31"
        )
        logger.info("")
        logger.info("   # Run with debug logging")
        logger.info("   tauro run --env dev --pipeline load --log-level DEBUG")

        logger.info("\n� Features already configured:")
        logger.info("   ✓ Multi-environment support (dev, sandbox, prod)")
        logger.info("   ✓ Input/output validation")
        logger.info("   ✓ Automatic data type handling (CSV, Delta, Parquet)")
        logger.info("   ✓ Structured logging")
        logger.info("   ✓ Error handling and retries")
        logger.info("   ✓ Dependency management")

        logger.info("\n💡 Tips:")
        logger.info("   • Check README.md for detailed documentation")
        logger.info("   • Review config/global_settings.yaml for environment settings")
        logger.info("   • See pipelines/ for implementation examples")
        logger.info("   • Run 'tauro --help' for all available commands")


# Integration with CLI system
def add_template_arguments(parser) -> None:
    """Add template-related arguments to CLI parser."""
    template_group = parser.add_argument_group("Template Generation")

    template_group.add_argument(
        "--template",
        help="Generate project template (use --list-templates to see options)",
    )

    template_group.add_argument("--project-name", help="Name for the generated project")

    template_group.add_argument(
        "--output-path",
        help="Output directory for generated project (default: ./<project-name>)",
    )

    template_group.add_argument(
        "--format",
        choices=["yaml", "json", "dsl"],
        default="yaml",
        help="Configuration file format (default: yaml)",
    )

    template_group.add_argument(
        "--no-sample-code",
        action="store_true",
        help="Skip generation of sample code files",
    )

    template_group.add_argument(
        "--list-templates", action="store_true", help="List available template types"
    )

    template_group.add_argument(
        "--template-interactive",
        action="store_true",
        help="Interactive template generation",
    )


def handle_template_command(parsed_args) -> int:
    """Handle template command execution from CLI."""
    template_cmd = TemplateCommand()

    return template_cmd.handle_template_command(
        template_type=parsed_args.template,
        project_name=parsed_args.project_name,
        output_path=parsed_args.output_path,
        config_format=parsed_args.format,
        create_sample_code=not parsed_args.no_sample_code,
        list_templates=parsed_args.list_templates,
        sandbox_developers=getattr(parsed_args, "sandbox_developers", None),
    )
