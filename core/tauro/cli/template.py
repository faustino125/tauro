from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List, Optional
from enum import Enum
import json
from datetime import datetime
from loguru import logger  # type: ignore

from .core import ConfigFormat, TauroError, ExitCode

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class TemplateType(Enum):
    """Available template types."""

    MEDALLION_BASIC = "medallion_basic"
    MEDALLION_ML = "medallion_ml"
    ML_TRAINING = "ml_training"
    ML_INFERENCE = "ml_inference"
    ETL_BASIC = "etl_basic"
    STREAMING = "streaming"


class DataLayer(Enum):
    """Data architecture layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"  # For ML models and features


class TemplateError(TauroError):
    """Exception for template-related errors."""

    def __init__(self, message: str):
        super().__init__(message, ExitCode.CONFIGURATION_ERROR)


class BaseTemplate(ABC):
    """Abstract base class for configuration templates."""

    def __init__(
        self, project_name: str, config_format: ConfigFormat = ConfigFormat.YAML
    ):
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
        """Generate the main settings.json file."""
        file_ext = self._get_file_extension()

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
            "mode": "databricks",
            "max_parallel_nodes": 4,
            "fail_on_error": True,
            "layer": "standard",
        }


class MedallionBasicTemplate(BaseTemplate):
    """Template for basic Medallion architecture (Bronze, Silver, Gold)."""

    def get_template_type(self) -> TemplateType:
        return TemplateType.MEDALLION_BASIC

    def generate_global_settings(self) -> Dict[str, Any]:
        base_settings = self.get_common_global_settings()
        base_settings.update(
            {
                "input_path": "/mnt/data/raw",
                "output_path": "/mnt/data/processed",
                "architecture": "medallion",
                "layers": ["bronze", "silver", "gold"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            }
        )
        return base_settings

    def generate_pipelines_config(self) -> Dict[str, Any]:
        return {
            "data_preparation": {
                "description": "Prepare data for ML training",
                "nodes": ["load_features", "split_data", "validate_splits"],
                "inputs": ["feature_store"],
                "outputs": ["train_data", "test_data", "validation_data"],
            },
            "model_training": {
                "description": "Train and tune ML models",
                "nodes": [
                    "train_baseline",
                    "hyperparameter_tuning",
                    "train_best_model",
                    "cross_validate",
                ],
                "inputs": ["train_data", "validation_data"],
                "outputs": ["baseline_model", "tuned_model", "best_model"],
            },
            "model_evaluation": {
                "description": "Evaluate and validate models",
                "nodes": [
                    "evaluate_models",
                    "generate_reports",
                    "validate_performance",
                ],
                "inputs": ["test_data", "best_model"],
                "outputs": ["evaluation_metrics", "model_report"],
            },
            "model_deployment": {
                "description": "Register and deploy models",
                "nodes": ["register_model", "create_endpoints", "deploy_model"],
                "inputs": ["best_model", "evaluation_metrics"],
                "outputs": ["registered_model", "model_endpoint"],
            },
        }

    def generate_nodes_config(self) -> Dict[str, Any]:
        return {
            # Data Preparation Nodes
            "load_features": {
                "description": "Load feature store data",
                "module": "pipelines.ml.data_prep",
                "function": "load_features",
                "input": ["feature_store"],
                "output": ["raw_features"],
                "dependencies": [],
            },
            "split_data": {
                "description": "Split data into train/validation/test sets",
                "module": "pipelines.ml.data_prep",
                "function": "split_data",
                "input": ["raw_features"],
                "output": ["train_data", "test_data", "validation_data"],
                "dependencies": ["load_features"],
            },
            "validate_splits": {
                "description": "Validate data splits quality",
                "module": "pipelines.ml.validation",
                "function": "validate_splits",
                "input": ["train_data", "test_data", "validation_data"],
                "output": [],
                "dependencies": ["split_data"],
            },
            # Training Nodes
            "train_baseline": {
                "description": "Train baseline model",
                "module": "pipelines.ml.training",
                "function": "train_baseline_model",
                "input": ["train_data"],
                "output": ["baseline_model"],
                "model_artifacts": [
                    {
                        "name": "baseline_model",
                        "type": "sklearn",
                        "path": "models/baseline",
                    }
                ],
                "dependencies": ["validate_splits"],
            },
            "hyperparameter_tuning": {
                "description": "Perform hyperparameter tuning",
                "module": "pipelines.ml.tuning",
                "function": "hyperparameter_tuning",
                "input": ["train_data", "validation_data"],
                "output": ["tuned_hyperparams"],
                "dependencies": ["train_baseline"],
            },
            "train_best_model": {
                "description": "Train model with best hyperparameters",
                "module": "pipelines.ml.training",
                "function": "train_best_model",
                "input": ["train_data", "tuned_hyperparams"],
                "output": ["best_model"],
                "model_artifacts": [
                    {"name": "best_model", "type": "sklearn", "path": "models/best"}
                ],
                "dependencies": ["hyperparameter_tuning"],
            },
            "cross_validate": {
                "description": "Perform cross-validation",
                "module": "pipelines.ml.validation",
                "function": "cross_validate_model",
                "input": ["train_data", "best_model"],
                "output": ["cv_scores"],
                "dependencies": ["train_best_model"],
            },
            # Evaluation Nodes
            "evaluate_models": {
                "description": "Evaluate model performance",
                "module": "pipelines.ml.evaluation",
                "function": "evaluate_models",
                "input": ["test_data", "best_model"],
                "output": ["evaluation_metrics"],
                "dependencies": ["cross_validate"],
            },
            "generate_reports": {
                "description": "Generate model evaluation reports",
                "module": "pipelines.ml.reporting",
                "function": "generate_reports",
                "input": ["evaluation_metrics", "cv_scores"],
                "output": ["model_report"],
                "dependencies": ["evaluate_models"],
            },
            "validate_performance": {
                "description": "Validate model meets performance thresholds",
                "module": "pipelines.ml.validation",
                "function": "validate_performance",
                "input": ["evaluation_metrics"],
                "output": [],
                "dependencies": ["generate_reports"],
            },
            # Deployment Nodes
            "register_model": {
                "description": "Register model in model registry",
                "module": "pipelines.ml.deployment",
                "function": "register_model",
                "input": ["best_model", "evaluation_metrics"],
                "output": ["registered_model"],
                "dependencies": ["validate_performance"],
            },
            "create_endpoints": {
                "description": "Create model serving endpoints",
                "module": "pipelines.ml.deployment",
                "function": "create_endpoints",
                "input": ["registered_model"],
                "output": ["model_endpoint"],
                "dependencies": ["register_model"],
            },
            "deploy_model": {
                "description": "Deploy model to production",
                "module": "pipelines.ml.deployment",
                "function": "deploy_model",
                "input": ["model_endpoint"],
                "output": [],
                "dependencies": ["create_endpoints"],
            },
        }

    def generate_input_config(self) -> Dict[str, Any]:
        return {
            "feature_store": {
                "description": "Feature store with ML-ready features",
                "format": "delta",
                "filepath": "/mnt/data/features/feature_store",
            },
            "raw_features": {
                "description": "Raw features from feature store",
                "format": "delta",
                "filepath": "/mnt/data/features/raw_features",
            },
            "train_data": {
                "description": "Training dataset",
                "format": "delta",
                "filepath": "/mnt/data/ml/train_data",
            },
            "test_data": {
                "description": "Test dataset",
                "format": "delta",
                "filepath": "/mnt/data/ml/test_data",
            },
            "validation_data": {
                "description": "Validation dataset",
                "format": "delta",
                "filepath": "/mnt/data/ml/validation_data",
            },
            "baseline_model": {
                "description": "Baseline model",
                "format": "pickle",
                "filepath": "/mnt/models/baseline/model.pkl",
            },
            "tuned_hyperparams": {
                "description": "Tuned hyperparameters",
                "format": "json",
                "filepath": "/mnt/models/hyperparams/best_params.json",
            },
            "best_model": {
                "description": "Best trained model",
                "format": "pickle",
                "filepath": "/mnt/models/best/model.pkl",
            },
            "cv_scores": {
                "description": "Cross-validation scores",
                "format": "json",
                "filepath": "/mnt/models/evaluation/cv_scores.json",
            },
            "evaluation_metrics": {
                "description": "Model evaluation metrics",
                "format": "json",
                "filepath": "/mnt/models/evaluation/metrics.json",
            },
            "model_report": {
                "description": "Model evaluation report",
                "format": "json",
                "filepath": "/mnt/models/reports/model_report.json",
            },
            "registered_model": {
                "description": "Registered model reference",
                "format": "json",
                "filepath": "/mnt/models/registry/registered_model.json",
            },
        }

    def generate_output_config(self) -> Dict[str, Any]:
        return {
            "train_data": {
                "description": "Training data output",
                "format": "delta",
                "table_name": "train_data",
                "schema": "ml_training",
                "sub_folder": "datasets",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "test_data": {
                "description": "Test data output",
                "format": "delta",
                "table_name": "test_data",
                "schema": "ml_training",
                "sub_folder": "datasets",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "validation_data": {
                "description": "Validation data output",
                "format": "delta",
                "table_name": "validation_data",
                "schema": "ml_training",
                "sub_folder": "datasets",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "baseline_model": {
                "description": "Baseline model output",
                "format": "pickle",
                "table_name": "baseline_model",
                "schema": "ml_models",
                "sub_folder": "baseline",
                "write_mode": "overwrite",
            },
            "tuned_hyperparams": {
                "description": "Tuned hyperparameters output",
                "format": "json",
                "table_name": "hyperparams",
                "schema": "ml_tuning",
                "sub_folder": "hyperparams",
                "write_mode": "overwrite",
            },
            "best_model": {
                "description": "Best model output",
                "format": "pickle",
                "table_name": "best_model",
                "schema": "ml_models",
                "sub_folder": "production",
                "write_mode": "overwrite",
            },
            "cv_scores": {
                "description": "Cross-validation scores output",
                "format": "json",
                "table_name": "cv_scores",
                "schema": "ml_evaluation",
                "sub_folder": "validation",
                "write_mode": "overwrite",
            },
            "evaluation_metrics": {
                "description": "Evaluation metrics output",
                "format": "unity_catalog",
                "catalog_name": "ml_monitoring",
                "table_name": "evaluation_metrics",
                "schema": "ml_evaluation",
                "sub_folder": "metrics",
                "write_mode": "append",
                "partition_col": "date",
                "vacuum": True,
            },
            "model_report": {
                "description": "Model report output",
                "format": "json",
                "table_name": "model_reports",
                "schema": "ml_evaluation",
                "sub_folder": "reports",
                "write_mode": "overwrite",
            },
            "registered_model": {
                "description": "Registered model output",
                "format": "unity_catalog",
                "catalog_name": "ml_registry",
                "table_name": "registered_models",
                "schema": "ml_registry",
                "sub_folder": "models",
                "write_mode": "append",
                "vacuum": True,
            },
            "model_endpoint": {
                "description": "Model endpoint output",
                "format": "json",
                "table_name": "model_endpoints",
                "schema": "ml_deployment",
                "sub_folder": "endpoints",
                "write_mode": "overwrite",
            },
        }


class TemplateFactory:
    """Factory for creating configuration templates."""

    TEMPLATES = {TemplateType.MEDALLION_BASIC: MedallionBasicTemplate}

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
        """List all available templates with descriptions."""
        return [
            {
                "type": TemplateType.MEDALLION_BASIC.value,
                "name": "Medallion Basic",
                "description": "Basic Medallion architecture with Bronze, Silver, and Gold layers",
            },
            {
                "type": TemplateType.MEDALLION_ML.value,
                "name": "Medallion ML",
                "description": "ML-optimized Medallion architecture with Platinum layer for models",
            },
            {
                "type": TemplateType.ML_TRAINING.value,
                "name": "ML Training",
                "description": "Specialized ML model training and evaluation pipeline",
            },
        ]


class TemplateGenerator:
    """Generates complete project templates with directory structure."""

    def __init__(
        self, output_path: Path, config_format: ConfigFormat = ConfigFormat.YAML
    ):
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

    def generate_project(
        self,
        template_type: TemplateType,
        project_name: str,
        create_sample_code: bool = True,
    ) -> None:
        """Generate complete project structure from template."""
        logger.info(
            f"Generating {template_type.value} template for project '{project_name}'"
        )

        # Create template instance
        template = TemplateFactory.create_template(
            template_type, project_name, self.config_format
        )

        # Create directory structure
        self._create_directory_structure(template_type)

        # Generate configuration files
        self._generate_config_files(template)

        # Generate sample code if requested
        if create_sample_code:
            self._generate_sample_code(template)

        # Generate additional project files
        self._generate_project_files(template)

        logger.success(
            f"Project '{project_name}' generated successfully at {self.output_path}"
        )

    def _create_directory_structure(self, template_type: TemplateType) -> None:
        """Create project directory structure."""
        directories = [
            "config",
            "config/dev",
            "config/prod",
            "pipelines",
            "pipelines/bronze",
            "pipelines/silver",
            "pipelines/gold",
            "src",
            "tests",
            "docs",
            "notebooks",
            "data",
            "data/raw",
            "data/processed",
            "logs",
        ]

        # Add ML-specific directories
        if template_type in [TemplateType.MEDALLION_ML, TemplateType.ML_TRAINING]:
            directories.extend(
                [
                    "pipelines/platinum",
                    "pipelines/ml",
                    "models",
                    "experiments",
                    "features",
                ]
            )

        for directory in directories:
            dir_path = self.output_path / directory
            dir_path.mkdir(parents=True, exist_ok=True)

            # Create __init__.py files for Python packages
            if directory.startswith("pipelines") or directory == "src":
                (dir_path / "__init__.py").touch()

    def _generate_config_files(self, template: BaseTemplate) -> None:
        """Generate all configuration files."""
        configs = {
            "global_settings": template.generate_global_settings(),
            "pipelines": template.generate_pipelines_config(),
            "nodes": template.generate_nodes_config(),
            "input": template.generate_input_config(),
            "output": template.generate_output_config(),
        }

        # Generate main settings file
        settings_file = self.output_path / f"settings_{self.config_format.value}.json"
        self._write_json_file(settings_file, template.generate_settings_json())

        # Generate configuration files for each environment
        for env in ["base", "dev", "prod"]:
            config_dir = self.output_path / "config" / (env if env != "base" else "")

            for config_name, config_data in configs.items():
                if env != "base" and config_name in ["pipelines", "nodes"]:
                    continue  # Only generate these for base environment

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
            raise TemplateError(
                "PyYAML not available. Install with: pip install PyYAML"
            )

        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, indent=2)

    def _write_json_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write JSON file."""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _write_dsl_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write DSL file."""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"# Configuration file generated on {datetime.now()}\n\n")
            self._write_dsl_dict(f, data, 0)

    def _write_dsl_dict(self, f, data: Dict[str, Any], indent: int) -> None:
        """Recursively write dictionary to DSL format."""
        for key, value in data.items():
            if isinstance(value, dict):
                f.write("  " * indent + f"[{key}]\n")
                self._write_dsl_dict(f, value, indent + 1)
            elif isinstance(value, list):
                f.write("  " * indent + f"{key} = {value}\n")
            else:
                f.write("  " * indent + f"{key} = {value}\n")

    def _generate_sample_code(self, template: BaseTemplate) -> None:
        """Generate sample Python code for pipeline functions."""
        template_type = template.get_template_type()

        if template_type == TemplateType.MEDALLION_BASIC:
            self._generate_medallion_sample_code()
        elif template_type == TemplateType.MEDALLION_ML:
            self._generate_medallion_ml_sample_code()
        elif template_type == TemplateType.ML_TRAINING:
            self._generate_ml_training_sample_code()

    def _generate_medallion_sample_code(self) -> None:
        """Generate sample code for Medallion architecture."""
        # Bronze layer sample
        bronze_code = '''"""Bronze layer data ingestion functions."""
from pyspark.sql import DataFrame
from loguru import logger


def ingest_sales_data(start_date: str, end_date: str) -> DataFrame:
    """Ingest raw sales data into Bronze layer."""
    logger.info(f"Ingesting sales data from {start_date} to {end_date}")
    # Implementation here
    pass


def ingest_customer_data(start_date: str, end_date: str) -> DataFrame:
    """Ingest raw customer data into Bronze layer."""
    logger.info(f"Ingesting customer data from {start_date} to {end_date}")
    # Implementation here
    pass
'''

        bronze_file = self.output_path / "pipelines" / "bronze" / "ingestion.py"
        self._write_text_file(bronze_file, bronze_code)

        # Silver layer sample
        silver_code = '''"""Silver layer data cleaning and transformation functions."""
from pyspark.sql import DataFrame
from loguru import logger


def clean_sales_data(sales_bronze: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Clean and standardize sales data for Silver layer."""
    logger.info(f"Cleaning sales data from {start_date} to {end_date}")
    # Implementation here
    pass


def clean_customer_data(customer_bronze: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Clean and standardize customer data for Silver layer."""
    logger.info(f"Cleaning customer data from {start_date} to {end_date}")
    # Implementation here
    pass
'''

        silver_file = self.output_path / "pipelines" / "silver" / "cleaning.py"
        self._write_text_file(silver_file, silver_code)

    def _generate_medallion_ml_sample_code(self) -> None:
        """Generate sample code for ML Medallion architecture."""
        # Feature engineering sample
        features_code = '''"""Gold layer feature engineering for ML."""
from pyspark.sql import DataFrame
from loguru import logger


def create_customer_features(master_dataset: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Engineer customer-level features for ML models."""
    logger.info(f"Creating customer features from {start_date} to {end_date}")
    # Implementation here
    pass


def create_temporal_features(master_dataset: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Engineer time-based features for ML models."""
    logger.info(f"Creating temporal features from {start_date} to {end_date}")
    # Implementation here
    pass
'''

        features_file = self.output_path / "pipelines" / "gold" / "features.py"
        self._write_text_file(features_file, features_code)

        # ML training sample
        ml_code = '''"""Platinum layer ML model training."""
from pyspark.sql import DataFrame
from loguru import logger
import pickle


def train_churn_model(training_data: DataFrame, start_date: str, end_date: str) -> bytes:
    """Train customer churn prediction model."""
    logger.info(f"Training churn model with data from {start_date} to {end_date}")
    # Implementation here
    pass


def train_recommendation_model(training_data: DataFrame, start_date: str, end_date: str) -> bytes:
    """Train product recommendation model."""
    logger.info(f"Training recommendation model with data from {start_date} to {end_date}")
    # Implementation here
    pass
'''

        ml_file = self.output_path / "pipelines" / "platinum" / "training.py"
        self._write_text_file(ml_file, ml_code)

    def _generate_ml_training_sample_code(self) -> None:
        """Generate sample code for ML training pipeline."""
        training_code = '''"""ML model training pipeline functions."""
from pyspark.sql import DataFrame
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from loguru import logger
import pickle


def split_data(features: DataFrame, start_date: str, end_date: str) -> tuple:
    """Split data into train, validation, and test sets."""
    logger.info(f"Splitting data from {start_date} to {end_date}")
    # Implementation here
    pass


def train_baseline_model(train_data: DataFrame, start_date: str, end_date: str) -> bytes:
    """Train a baseline model."""
    logger.info(f"Training baseline model with data from {start_date} to {end_date}")
    # Implementation here
    pass


def hyperparameter_tuning(train_data: DataFrame, validation_data: DataFrame, 
                         start_date: str, end_date: str) -> dict:
    """Perform hyperparameter tuning."""
    logger.info(f"Tuning hyperparameters from {start_date} to {end_date}")
    # Implementation here
    pass
'''

        training_file = self.output_path / "pipelines" / "ml" / "training.py"
        training_file.parent.mkdir(parents=True, exist_ok=True)
        self._write_text_file(training_file, training_code)

    def _generate_project_files(self, template: BaseTemplate) -> None:
        """Generate additional project files."""
        # README.md
        readme_content = f"""# {template.project_name}

Generated using Tauro {template.get_template_type().value} template.

## Project Structure

- `config/`: Configuration files for different environments
- `pipelines/`: Data pipeline implementation
- `src/`: Source code utilities
- `tests/`: Unit and integration tests
- `notebooks/`: Jupyter notebooks for exploration
- `data/`: Data storage (local development)
- `logs/`: Application logs

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run a pipeline:
   ```bash
   tauro --env dev --pipeline bronze_ingestion
   ```

## Architecture

This project follows the {template.get_template_type().value} architecture pattern.

Generated on: {template.timestamp}
"""

        readme_file = self.output_path / "README.md"
        self._write_text_file(readme_file, readme_content)

        # requirements.txt
        requirements = """# Tauro framework dependencies
tauro-framework>=0.1.0

# Data processing
pyspark>=3.4.0
pandas>=1.5.0
polars>=0.19.0

# Delta Lake
delta-spark>=2.4.0

# ML libraries (if using ML templates)
scikit-learn>=1.3.0
mlflow>=2.7.0

# Utilities
loguru>=0.7.0
pyyaml>=6.0
"""

        requirements_file = self.output_path / "requirements.txt"
        self._write_text_file(requirements_file, requirements)

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

# Data
data/raw/*
data/processed/*
!data/raw/.gitkeep
!data/processed/.gitkeep

# Logs
logs/*.log

# Models
models/*.pkl
models/*.joblib

# Notebooks checkpoints
.ipynb_checkpoints/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Spark
metastore_db/
spark-warehouse/

# Databricks
.databricks/
"""

        gitignore_file = self.output_path / ".gitignore"
        self._write_text_file(gitignore_file, gitignore)

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
        logger.info("  tauro --template <type> --project-name <name> [options]")
        logger.info("")
        logger.info("Examples:")
        logger.info("  tauro --template medallion_basic --project-name my_pipeline")
        logger.info(
            "  tauro --template medallion_ml --project-name ml_project --format json"
        )
        logger.info(
            "  tauro --template ml_training --project-name model_training --output ./projects"
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
                    print("Invalid selection. Try again.")
                except (KeyboardInterrupt, EOFError):
                    logger.warning("Template generation cancelled")
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
                logger.warning(
                    f"Directory {output_dir} already exists and is not empty"
                )
                try:
                    response = input("Continue anyway? (y/N): ").strip().lower()
                    if response != "y":
                        logger.info("Template generation cancelled")
                        return ExitCode.SUCCESS.value
                except (KeyboardInterrupt, EOFError):
                    logger.info("Template generation cancelled")
                    return ExitCode.SUCCESS.value

            # Generate template
            self.generator = TemplateGenerator(output_dir, format_enum)
            self.generator.generate_project(
                template_enum, project_name, create_sample_code
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
        logger.info(f"1. cd {output_dir}")
        logger.info("2. pip install -r requirements.txt")
        logger.info("3. Review and customize configuration files")
        logger.info("4. Implement pipeline functions in the pipelines/ directory")

        # Template-specific guidance
        if template_type == TemplateType.MEDALLION_BASIC:
            logger.info("\n🔄 Medallion Architecture:")
            logger.info("   • Bronze: Raw data ingestion")
            logger.info("   • Silver: Cleaned and transformed data")
            logger.info("   • Gold: Business-ready aggregations")

            logger.info("\n🚀 Example usage:")
            logger.info("   tauro --env dev --pipeline bronze_ingestion")
            logger.info("   tauro --env dev --pipeline silver_transformation")
            logger.info("   tauro --env dev --pipeline gold_aggregation")

        elif template_type == TemplateType.MEDALLION_ML:
            logger.info("\n🤖 ML Medallion Architecture:")
            logger.info("   • Bronze: Raw data ingestion")
            logger.info("   • Silver: ML preprocessing")
            logger.info("   • Gold: Feature engineering")
            logger.info("   • Platinum: ML models and inference")

            logger.info("\n🚀 Example usage:")
            logger.info("   tauro --env dev --pipeline bronze_ingestion")
            logger.info("   tauro --env dev --pipeline gold_feature_engineering")
            logger.info("   tauro --env dev --pipeline platinum_ml_training")

        elif template_type == TemplateType.ML_TRAINING:
            logger.info("\n🎯 ML Training Pipeline:")
            logger.info("   • Data preparation and splitting")
            logger.info("   • Model training and tuning")
            logger.info("   • Evaluation and validation")
            logger.info("   • Model registration and deployment")

            logger.info("\n🚀 Example usage:")
            logger.info("   tauro --env dev --pipeline data_preparation")
            logger.info("   tauro --env dev --pipeline model_training")
            logger.info("   tauro --env dev --pipeline model_evaluation")


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
        interactive=parsed_args.template_interactive,
    )


# Example usage and testing
if __name__ == "__main__":
    import sys

    # Example: Generate Medallion ML template
    try:
        generator = TemplateGenerator(Path("./test_project"), ConfigFormat.YAML)
        generator.generate_project(
            TemplateType.MEDALLION_ML, "ml_analytics_project", create_sample_code=True
        )
        print("✅ Template generated successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    def generate_pipelines_config(self) -> Dict[str, Any]:
        return {
            "bronze_ingestion": {
                "description": "Ingest raw data into Bronze layer",
                "nodes": [
                    "ingest_sales_data",
                    "ingest_customer_data",
                    "validate_bronze_data",
                ],
                "inputs": ["sales_raw", "customer_raw"],
                "outputs": ["sales_bronze", "customer_bronze"],
            },
            "silver_transformation": {
                "description": "Clean and transform data for Silver layer",
                "nodes": [
                    "clean_sales_data",
                    "clean_customer_data",
                    "join_sales_customer",
                ],
                "inputs": ["sales_bronze", "customer_bronze"],
                "outputs": ["sales_silver", "customer_silver", "sales_customer_silver"],
            },
            "gold_aggregation": {
                "description": "Create business-ready aggregations for Gold layer",
                "nodes": [
                    "aggregate_monthly_sales",
                    "create_customer_metrics",
                    "build_kpis",
                ],
                "inputs": ["sales_customer_silver"],
                "outputs": [
                    "monthly_sales_gold",
                    "customer_metrics_gold",
                    "business_kpis_gold",
                ],
            },
        }

    def generate_nodes_config(self) -> Dict[str, Any]:
        return {
            # Bronze Layer Nodes
            "ingest_sales_data": {
                "description": "Ingest raw sales data",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_sales_data",
                "input": ["sales_raw"],
                "output": ["sales_bronze"],
                "dependencies": [],
            },
            "ingest_customer_data": {
                "description": "Ingest raw customer data",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_customer_data",
                "input": ["customer_raw"],
                "output": ["customer_bronze"],
                "dependencies": [],
            },
            "validate_bronze_data": {
                "description": "Validate Bronze layer data quality",
                "module": "pipelines.bronze.validation",
                "function": "validate_data_quality",
                "input": ["sales_bronze", "customer_bronze"],
                "output": [],
                "dependencies": ["ingest_sales_data", "ingest_customer_data"],
            },
            # Silver Layer Nodes
            "clean_sales_data": {
                "description": "Clean and standardize sales data",
                "module": "pipelines.silver.cleaning",
                "function": "clean_sales_data",
                "input": ["sales_bronze"],
                "output": ["sales_silver"],
                "dependencies": ["validate_bronze_data"],
            },
            "clean_customer_data": {
                "description": "Clean and standardize customer data",
                "module": "pipelines.silver.cleaning",
                "function": "clean_customer_data",
                "input": ["customer_bronze"],
                "output": ["customer_silver"],
                "dependencies": ["validate_bronze_data"],
            },
            "join_sales_customer": {
                "description": "Join sales and customer data",
                "module": "pipelines.silver.transformation",
                "function": "join_sales_customer",
                "input": ["sales_silver", "customer_silver"],
                "output": ["sales_customer_silver"],
                "dependencies": ["clean_sales_data", "clean_customer_data"],
            },
            # Gold Layer Nodes
            "aggregate_monthly_sales": {
                "description": "Create monthly sales aggregations",
                "module": "pipelines.gold.aggregation",
                "function": "aggregate_monthly_sales",
                "input": ["sales_customer_silver"],
                "output": ["monthly_sales_gold"],
                "dependencies": ["join_sales_customer"],
            },
            "create_customer_metrics": {
                "description": "Calculate customer lifetime value and metrics",
                "module": "pipelines.gold.metrics",
                "function": "create_customer_metrics",
                "input": ["sales_customer_silver"],
                "output": ["customer_metrics_gold"],
                "dependencies": ["join_sales_customer"],
            },
            "build_kpis": {
                "description": "Build business KPIs and dashboards",
                "module": "pipelines.gold.kpis",
                "function": "build_business_kpis",
                "input": ["monthly_sales_gold", "customer_metrics_gold"],
                "output": ["business_kpis_gold"],
                "dependencies": ["aggregate_monthly_sales", "create_customer_metrics"],
            },
        }

    def generate_input_config(self) -> Dict[str, Any]:
        return {
            "sales_raw": {
                "description": "Raw sales transaction data",
                "format": "csv",
                "filepath": "${input_path}/sales/sales_data.csv",
                "options": {"header": "true", "inferSchema": "true"},
            },
            "customer_raw": {
                "description": "Raw customer master data",
                "format": "parquet",
                "filepath": "${input_path}/customers/customer_data.parquet",
            },
            "sales_bronze": {
                "description": "Bronze layer sales data",
                "format": "delta",
                "filepath": "/mnt/data/bronze/sales",
            },
            "customer_bronze": {
                "description": "Bronze layer customer data",
                "format": "delta",
                "filepath": "/mnt/data/bronze/customers",
            },
            "sales_silver": {
                "description": "Silver layer cleaned sales data",
                "format": "delta",
                "filepath": "/mnt/data/silver/sales",
            },
            "customer_silver": {
                "description": "Silver layer cleaned customer data",
                "format": "delta",
                "filepath": "/mnt/data/silver/customers",
            },
            "sales_customer_silver": {
                "description": "Silver layer joined sales and customer data",
                "format": "delta",
                "filepath": "/mnt/data/silver/sales_customer",
            },
        }

    def generate_output_config(self) -> Dict[str, Any]:
        return {
            "sales_bronze": {
                "description": "Bronze layer sales output",
                "format": "delta",
                "table_name": "sales",
                "schema": "bronze",
                "sub_folder": "sales",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "customer_bronze": {
                "description": "Bronze layer customer output",
                "format": "delta",
                "table_name": "customers",
                "schema": "bronze",
                "sub_folder": "customers",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "sales_silver": {
                "description": "Silver layer sales output",
                "format": "delta",
                "table_name": "sales_clean",
                "schema": "silver",
                "sub_folder": "sales",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "customer_silver": {
                "description": "Silver layer customer output",
                "format": "delta",
                "table_name": "customers_clean",
                "schema": "silver",
                "sub_folder": "customers",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "sales_customer_silver": {
                "description": "Silver layer joined data output",
                "format": "delta",
                "table_name": "sales_customer",
                "schema": "silver",
                "sub_folder": "joined",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "monthly_sales_gold": {
                "description": "Gold layer monthly sales aggregation",
                "format": "unity_catalog",
                "catalog_name": "main",
                "table_name": "monthly_sales",
                "schema": "gold",
                "sub_folder": "aggregations",
                "write_mode": "overwrite",
                "partition_col": "month",
                "vacuum": True,
            },
            "customer_metrics_gold": {
                "description": "Gold layer customer metrics",
                "format": "unity_catalog",
                "catalog_name": "main",
                "table_name": "customer_metrics",
                "schema": "gold",
                "sub_folder": "metrics",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "business_kpis_gold": {
                "description": "Gold layer business KPIs",
                "format": "unity_catalog",
                "catalog_name": "main",
                "table_name": "business_kpis",
                "schema": "gold",
                "sub_folder": "kpis",
                "write_mode": "overwrite",
                "vacuum": True,
            },
        }


class MedallionMLTemplate(BaseTemplate):
    """Template for ML-optimized Medallion architecture with Platinum layer."""

    def get_template_type(self) -> TemplateType:
        return TemplateType.MEDALLION_ML

    def generate_global_settings(self) -> Dict[str, Any]:
        base_settings = self.get_common_global_settings()
        base_settings.update(
            {
                "input_path": "/mnt/data/raw",
                "output_path": "/mnt/data/processed",
                "model_registry_path": "/mnt/models",
                "feature_store_path": "/mnt/features",
                "architecture": "medallion_ml",
                "layers": ["bronze", "silver", "gold", "platinum"],
                "layer": "ml",
                "default_model_version": "latest",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            }
        )
        return base_settings

    def generate_pipelines_config(self) -> Dict[str, Any]:
        return {
            "bronze_ingestion": {
                "description": "Ingest raw data into Bronze layer",
                "nodes": [
                    "ingest_transactions",
                    "ingest_customers",
                    "ingest_products",
                    "validate_bronze",
                ],
                "inputs": ["transactions_raw", "customers_raw", "products_raw"],
                "outputs": [
                    "transactions_bronze",
                    "customers_bronze",
                    "products_bronze",
                ],
            },
            "silver_preprocessing": {
                "description": "Clean and preprocess data for ML",
                "nodes": [
                    "clean_transactions",
                    "clean_customers",
                    "clean_products",
                    "join_datasets",
                    "detect_outliers",
                ],
                "inputs": [
                    "transactions_bronze",
                    "customers_bronze",
                    "products_bronze",
                ],
                "outputs": [
                    "transactions_silver",
                    "customers_silver",
                    "products_silver",
                    "master_dataset_silver",
                ],
            },
            "gold_feature_engineering": {
                "description": "Engineer features for ML models",
                "nodes": [
                    "create_customer_features",
                    "create_product_features",
                    "create_temporal_features",
                    "aggregate_features",
                ],
                "inputs": ["master_dataset_silver"],
                "outputs": [
                    "customer_features_gold",
                    "product_features_gold",
                    "temporal_features_gold",
                    "feature_store_gold",
                ],
            },
            "platinum_ml_training": {
                "description": "Train and evaluate ML models",
                "nodes": [
                    "prepare_training_data",
                    "train_churn_model",
                    "train_recommendation_model",
                    "evaluate_models",
                    "register_models",
                ],
                "inputs": ["feature_store_gold"],
                "outputs": [
                    "training_data_platinum",
                    "churn_model_platinum",
                    "recommendation_model_platinum",
                ],
            },
            "platinum_ml_inference": {
                "description": "Generate predictions and recommendations",
                "nodes": [
                    "batch_scoring",
                    "real_time_features",
                    "generate_recommendations",
                    "model_monitoring",
                ],
                "inputs": [
                    "feature_store_gold",
                    "churn_model_platinum",
                    "recommendation_model_platinum",
                ],
                "outputs": [
                    "predictions_platinum",
                    "recommendations_platinum",
                    "model_metrics_platinum",
                ],
            },
        }

    def generate_nodes_config(self) -> Dict[str, Any]:
        return {
            # Bronze Layer Nodes
            "ingest_transactions": {
                "description": "Ingest transaction data",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_transactions",
                "input": ["transactions_raw"],
                "output": ["transactions_bronze"],
                "dependencies": [],
            },
            "ingest_customers": {
                "description": "Ingest customer data",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_customers",
                "input": ["customers_raw"],
                "output": ["customers_bronze"],
                "dependencies": [],
            },
            "ingest_products": {
                "description": "Ingest product data",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_products",
                "input": ["products_raw"],
                "output": ["products_bronze"],
                "dependencies": [],
            },
            "validate_bronze": {
                "description": "Validate bronze data quality",
                "module": "pipelines.bronze.validation",
                "function": "validate_data_quality",
                "input": ["transactions_bronze", "customers_bronze", "products_bronze"],
                "output": [],
                "dependencies": [
                    "ingest_transactions",
                    "ingest_customers",
                    "ingest_products",
                ],
            },
            # Silver Layer Nodes
            "clean_transactions": {
                "description": "Clean transaction data",
                "module": "pipelines.silver.cleaning",
                "function": "clean_transactions",
                "input": ["transactions_bronze"],
                "output": ["transactions_silver"],
                "dependencies": ["validate_bronze"],
            },
            "clean_customers": {
                "description": "Clean customer data",
                "module": "pipelines.silver.cleaning",
                "function": "clean_customers",
                "input": ["customers_bronze"],
                "output": ["customers_silver"],
                "dependencies": ["validate_bronze"],
            },
            "clean_products": {
                "description": "Clean product data",
                "module": "pipelines.silver.cleaning",
                "function": "clean_products",
                "input": ["products_bronze"],
                "output": ["products_silver"],
                "dependencies": ["validate_bronze"],
            },
            "join_datasets": {
                "description": "Join all datasets for ML",
                "module": "pipelines.silver.transformation",
                "function": "join_all_datasets",
                "input": ["transactions_silver", "customers_silver", "products_silver"],
                "output": ["master_dataset_silver"],
                "dependencies": [
                    "clean_transactions",
                    "clean_customers",
                    "clean_products",
                ],
            },
            "detect_outliers": {
                "description": "Detect and handle outliers",
                "module": "pipelines.silver.outliers",
                "function": "detect_outliers",
                "input": ["master_dataset_silver"],
                "output": [],
                "dependencies": ["join_datasets"],
            },
            # Gold Layer Nodes (Feature Engineering)
            "create_customer_features": {
                "description": "Engineer customer-level features",
                "module": "pipelines.gold.features",
                "function": "create_customer_features",
                "input": ["master_dataset_silver"],
                "output": ["customer_features_gold"],
                "dependencies": ["detect_outliers"],
            },
            "create_product_features": {
                "description": "Engineer product-level features",
                "module": "pipelines.gold.features",
                "function": "create_product_features",
                "input": ["master_dataset_silver"],
                "output": ["product_features_gold"],
                "dependencies": ["detect_outliers"],
            },
            "create_temporal_features": {
                "description": "Engineer time-based features",
                "module": "pipelines.gold.features",
                "function": "create_temporal_features",
                "input": ["master_dataset_silver"],
                "output": ["temporal_features_gold"],
                "dependencies": ["detect_outliers"],
            },
            "aggregate_features": {
                "description": "Aggregate all features into feature store",
                "module": "pipelines.gold.features",
                "function": "aggregate_features",
                "input": [
                    "customer_features_gold",
                    "product_features_gold",
                    "temporal_features_gold",
                ],
                "output": ["feature_store_gold"],
                "dependencies": [
                    "create_customer_features",
                    "create_product_features",
                    "create_temporal_features",
                ],
            },
            # Platinum Layer Nodes (ML)
            "prepare_training_data": {
                "description": "Prepare data for model training",
                "module": "pipelines.platinum.training",
                "function": "prepare_training_data",
                "input": ["feature_store_gold"],
                "output": ["training_data_platinum"],
                "dependencies": ["aggregate_features"],
            },
            "train_churn_model": {
                "description": "Train customer churn prediction model",
                "module": "pipelines.platinum.training",
                "function": "train_churn_model",
                "input": ["training_data_platinum"],
                "output": ["churn_model_platinum"],
                "model_artifacts": [
                    {"name": "churn_model", "type": "sklearn", "path": "models/churn"}
                ],
                "dependencies": ["prepare_training_data"],
            },
            "train_recommendation_model": {
                "description": "Train product recommendation model",
                "module": "pipelines.platinum.training",
                "function": "train_recommendation_model",
                "input": ["training_data_platinum"],
                "output": ["recommendation_model_platinum"],
                "model_artifacts": [
                    {
                        "name": "recommendation_model",
                        "type": "collaborative_filtering",
                        "path": "models/recommendations",
                    }
                ],
                "dependencies": ["prepare_training_data"],
            },
            "evaluate_models": {
                "description": "Evaluate model performance",
                "module": "pipelines.platinum.evaluation",
                "function": "evaluate_models",
                "input": ["churn_model_platinum", "recommendation_model_platinum"],
                "output": [],
                "dependencies": ["train_churn_model", "train_recommendation_model"],
            },
            "register_models": {
                "description": "Register models in model registry",
                "module": "pipelines.platinum.registry",
                "function": "register_models",
                "input": ["churn_model_platinum", "recommendation_model_platinum"],
                "output": [],
                "dependencies": ["evaluate_models"],
            },
            # Inference Nodes
            "batch_scoring": {
                "description": "Generate batch predictions",
                "module": "pipelines.platinum.inference",
                "function": "batch_scoring",
                "input": ["feature_store_gold", "churn_model_platinum"],
                "output": ["predictions_platinum"],
                "dependencies": ["register_models"],
            },
            "real_time_features": {
                "description": "Compute real-time features",
                "module": "pipelines.platinum.inference",
                "function": "compute_real_time_features",
                "input": ["feature_store_gold"],
                "output": [],
                "dependencies": ["register_models"],
            },
            "generate_recommendations": {
                "description": "Generate product recommendations",
                "module": "pipelines.platinum.inference",
                "function": "generate_recommendations",
                "input": ["feature_store_gold", "recommendation_model_platinum"],
                "output": ["recommendations_platinum"],
                "dependencies": ["register_models"],
            },
            "model_monitoring": {
                "description": "Monitor model performance and drift",
                "module": "pipelines.platinum.monitoring",
                "function": "monitor_models",
                "input": ["predictions_platinum", "recommendations_platinum"],
                "output": ["model_metrics_platinum"],
                "dependencies": ["batch_scoring", "generate_recommendations"],
            },
        }

    def generate_input_config(self) -> Dict[str, Any]:
        return {
            # Raw Data Sources
            "transactions_raw": {
                "description": "Raw transaction data",
                "format": "csv",
                "filepath": "${input_path}/transactions/transactions.csv",
                "options": {"header": "true", "inferSchema": "true"},
            },
            "customers_raw": {
                "description": "Raw customer data",
                "format": "parquet",
                "filepath": "${input_path}/customers/customers.parquet",
            },
            "products_raw": {
                "description": "Raw product data",
                "format": "json",
                "filepath": "${input_path}/products/products.json",
            },
            # Bronze Layer
            "transactions_bronze": {
                "description": "Bronze transactions",
                "format": "delta",
                "filepath": "/mnt/data/bronze/transactions",
            },
            "customers_bronze": {
                "description": "Bronze customers",
                "format": "delta",
                "filepath": "/mnt/data/bronze/customers",
            },
            "products_bronze": {
                "description": "Bronze products",
                "format": "delta",
                "filepath": "/mnt/data/bronze/products",
            },
            # Silver Layer
            "transactions_silver": {
                "description": "Clean transactions",
                "format": "delta",
                "filepath": "/mnt/data/silver/transactions",
            },
            "customers_silver": {
                "description": "Clean customers",
                "format": "delta",
                "filepath": "/mnt/data/silver/customers",
            },
            "products_silver": {
                "description": "Clean products",
                "format": "delta",
                "filepath": "/mnt/data/silver/products",
            },
            "master_dataset_silver": {
                "description": "Joined master dataset",
                "format": "delta",
                "filepath": "/mnt/data/silver/master_dataset",
            },
            # Gold Layer (Features)
            "customer_features_gold": {
                "description": "Customer features",
                "format": "delta",
                "filepath": "/mnt/data/gold/features/customer_features",
            },
            "product_features_gold": {
                "description": "Product features",
                "format": "delta",
                "filepath": "/mnt/data/gold/features/product_features",
            },
            "temporal_features_gold": {
                "description": "Temporal features",
                "format": "delta",
                "filepath": "/mnt/data/gold/features/temporal_features",
            },
            "feature_store_gold": {
                "description": "Consolidated feature store",
                "format": "delta",
                "filepath": "/mnt/data/gold/feature_store",
            },
            # Platinum Layer (ML)
            "training_data_platinum": {
                "description": "ML training dataset",
                "format": "delta",
                "filepath": "/mnt/data/platinum/training_data",
            },
            "churn_model_platinum": {
                "description": "Trained churn model",
                "format": "pickle",
                "filepath": "/mnt/models/churn/model.pkl",
            },
            "recommendation_model_platinum": {
                "description": "Trained recommendation model",
                "format": "pickle",
                "filepath": "/mnt/models/recommendations/model.pkl",
            },
        }

    def generate_output_config(self) -> Dict[str, Any]:
        return {
            # Bronze Layer Outputs
            "transactions_bronze": {
                "description": "Bronze transactions output",
                "format": "delta",
                "table_name": "transactions",
                "schema": "bronze",
                "sub_folder": "transactions",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "customers_bronze": {
                "description": "Bronze customers output",
                "format": "delta",
                "table_name": "customers",
                "schema": "bronze",
                "sub_folder": "customers",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "products_bronze": {
                "description": "Bronze products output",
                "format": "delta",
                "table_name": "products",
                "schema": "bronze",
                "sub_folder": "products",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            # Silver Layer Outputs
            "transactions_silver": {
                "description": "Silver transactions output",
                "format": "delta",
                "table_name": "transactions_clean",
                "schema": "silver",
                "sub_folder": "transactions",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "customers_silver": {
                "description": "Silver customers output",
                "format": "delta",
                "table_name": "customers_clean",
                "schema": "silver",
                "sub_folder": "customers",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "products_silver": {
                "description": "Silver products output",
                "format": "delta",
                "table_name": "products_clean",
                "schema": "silver",
                "sub_folder": "products",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "master_dataset_silver": {
                "description": "Silver master dataset output",
                "format": "delta",
                "table_name": "master_dataset",
                "schema": "silver",
                "sub_folder": "master",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            # Gold Layer Outputs (Features)
            "customer_features_gold": {
                "description": "Customer features output",
                "format": "unity_catalog",
                "catalog_name": "feature_store",
                "table_name": "customer_features",
                "schema": "gold",
                "sub_folder": "features",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "product_features_gold": {
                "description": "Product features output",
                "format": "unity_catalog",
                "catalog_name": "feature_store",
                "table_name": "product_features",
                "schema": "gold",
                "sub_folder": "features",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "temporal_features_gold": {
                "description": "Temporal features output",
                "format": "unity_catalog",
                "catalog_name": "feature_store",
                "table_name": "temporal_features",
                "schema": "gold",
                "sub_folder": "features",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "feature_store_gold": {
                "description": "Feature store output",
                "format": "unity_catalog",
                "catalog_name": "feature_store",
                "table_name": "feature_store",
                "schema": "gold",
                "sub_folder": "store",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            # Platinum Layer Outputs (ML)
            "training_data_platinum": {
                "description": "Training data output",
                "format": "delta",
                "table_name": "training_data",
                "schema": "platinum",
                "sub_folder": "training",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "churn_model_platinum": {
                "description": "Churn model output",
                "format": "pickle",
                "table_name": "churn_model",
                "schema": "platinum",
                "sub_folder": "models",
                "write_mode": "overwrite",
            },
            "recommendation_model_platinum": {
                "description": "Recommendation model output",
                "format": "pickle",
                "table_name": "recommendation_model",
                "schema": "platinum",
                "sub_folder": "models",
                "write_mode": "overwrite",
            },
            "predictions_platinum": {
                "description": "Model predictions output",
                "format": "unity_catalog",
                "catalog_name": "ml_results",
                "table_name": "predictions",
                "schema": "platinum",
                "sub_folder": "predictions",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "recommendations_platinum": {
                "description": "Product recommendations output",
                "format": "unity_catalog",
                "catalog_name": "ml_results",
                "table_name": "recommendations",
                "schema": "platinum",
                "sub_folder": "recommendations",
                "write_mode": "overwrite",
                "partition_col": "date",
                "vacuum": True,
            },
            "model_metrics_platinum": {
                "description": "Model monitoring metrics output",
                "format": "unity_catalog",
                "catalog_name": "ml_monitoring",
                "table_name": "model_metrics",
                "schema": "platinum",
                "sub_folder": "monitoring",
                "write_mode": "append",
                "partition_col": "date",
                "vacuum": True,
            },
        }


class MLTrainingTemplate(BaseTemplate):
    """Template specifically for ML model training pipelines."""

    def get_template_type(self) -> TemplateType:
        return TemplateType.ML_TRAINING

    def generate_global_settings(self) -> Dict[str, Any]:
        base_settings = self.get_common_global_settings()
        base_settings.update(
            {
                "input_path": "/mnt/data/features",
                "output_path": "/mnt/models",
                "model_registry_path": "/mnt/models/registry",
                "experiment_tracking_path": "/mnt/experiments",
                "layer": "ml",
                "architecture": "ml_training",
                "default_model_version": "v1.0.0",
                "ml_framework": "sklearn",
                "hyperparameter_tuning": True,
                "cross_validation_folds": 5,
                "test_size": 0.2,
                "random_state": 42,
            }
        )
        return base_settings
