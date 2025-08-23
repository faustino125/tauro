import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from .core import ConfigFormat, ExitCode, TauroError

try:
    import yaml  # type: ignore

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class TemplateType(Enum):
    """Available template types."""

    MEDALLION_BASIC = "medallion_basic"


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
                "paths": {
                    "bronze": "/mnt/data/bronze",
                    "silver": "/mnt/data/silver", 
                    "gold": "/mnt/data/gold"
                }
            }
        )
        return base_settings

    def generate_pipelines_config(self) -> Dict[str, Any]:
        return {
            "bronze_ingestion": {
                "description": "Ingest raw data into Bronze layer",
                "nodes": ["ingest_sales_data", "ingest_customer_data"],
                "type": "batch",
                "inputs": ["raw_data_source"],
                "outputs": ["bronze_sales", "bronze_customers"],
            },
            "silver_processing": {
                "description": "Clean and transform data for Silver layer",
                "nodes": ["clean_sales_data", "clean_customer_data"],
                "type": "batch",
                "inputs": ["bronze_sales", "bronze_customers"],
                "outputs": ["silver_sales", "silver_customers"],
            },
            "gold_aggregation": {
                "description": "Create business-ready aggregations for Gold layer",
                "nodes": ["sales_summary"],
                "type": "batch",
                "inputs": ["silver_sales", "silver_customers"],
                "outputs": ["gold_sales_summary"],
            },
            "real_time_processing": {
                "description": "Real-time streaming data processing",
                "nodes": ["read_stream", "transform_stream", "write_stream"],
                "type": "streaming",
                "inputs": ["kafka_topic"],
                "outputs": ["delta_sink"],
            },
        }

    def generate_nodes_config(self) -> Dict[str, Any]:
        return {
            # Bronze Layer Nodes (Batch Ingestion)
            "ingest_sales_data": {
                "description": "Ingest raw sales data into Bronze layer",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_sales_data",
                "input": ["raw_data_source"],
                "output": ["bronze_sales"],
                "dependencies": [],
            },
            "ingest_customer_data": {
                "description": "Ingest raw customer data into Bronze layer",
                "module": "pipelines.bronze.ingestion",
                "function": "ingest_customer_data",
                "input": ["raw_data_source"],
                "output": ["bronze_customers"],
                "dependencies": [],
            },
            # Silver Layer Nodes (Batch Processing)
            "clean_sales_data": {
                "description": "Clean and standardize sales data for Silver layer",
                "module": "pipelines.silver.cleaning",
                "function": "clean_sales_data",
                "input": ["bronze_sales"],
                "output": ["silver_sales"],
                "dependencies": ["ingest_sales_data"],
            },
            "clean_customer_data": {
                "description": "Clean and standardize customer data for Silver layer",
                "module": "pipelines.silver.cleaning",
                "function": "clean_customer_data",
                "input": ["bronze_customers"],
                "output": ["silver_customers"],
                "dependencies": ["ingest_customer_data"],
            },
            # Gold Layer Nodes (Batch Aggregation)
            "sales_summary": {
                "description": "Create business-ready sales summary for Gold layer",
                "module": "pipelines.gold.aggregation",
                "function": "sales_summary",
                "input": ["silver_sales", "silver_customers"],
                "output": ["gold_sales_summary"],
                "dependencies": ["clean_sales_data", "clean_customer_data"],
            },
            # Streaming Nodes
            "read_stream": {
                "description": "Read data from streaming source",
                "module": "pipelines.streaming.processing",
                "function": "read_stream",
                "input": ["kafka_topic"],
                "output": ["raw_stream"],
                "dependencies": [],
                "type": "streaming",
            },
            "transform_stream": {
                "description": "Transform streaming data in real-time",
                "module": "pipelines.streaming.processing",
                "function": "transform_stream",
                "input": ["raw_stream"],
                "output": ["transformed_stream"],
                "dependencies": ["read_stream"],
                "type": "streaming",
            },
            "write_stream": {
                "description": "Write processed stream to sink",
                "module": "pipelines.streaming.processing",
                "function": "write_stream",
                "input": ["transformed_stream"],
                "output": ["delta_sink"],
                "dependencies": ["transform_stream"],
                "type": "streaming",
            },
        }

    def generate_input_config(self) -> Dict[str, Any]:
        return {
            # Raw data sources
            "raw_data_source": {
                "description": "Raw data source (CSV files, databases, etc.)",
                "format": "csv",
                "filepath": "/mnt/data/raw",
            },
            # Bronze layer outputs (used as inputs for Silver)
            "bronze_sales": {
                "description": "Raw sales data in Bronze layer",
                "format": "delta",
                "filepath": "/mnt/data/bronze/sales",
            },
            "bronze_customers": {
                "description": "Raw customer data in Bronze layer",
                "format": "delta",
                "filepath": "/mnt/data/bronze/customers",
            },
            # Silver layer outputs (used as inputs for Gold)
            "silver_sales": {
                "description": "Cleaned sales data in Silver layer",
                "format": "delta",
                "filepath": "/mnt/data/silver/sales",
            },
            "silver_customers": {
                "description": "Cleaned customer data in Silver layer",
                "format": "delta",
                "filepath": "/mnt/data/silver/customers",
            },
            # Streaming source
            "kafka_topic": {
                "description": "Kafka topic for real-time data streaming",
                "format": "kafka",
                "connection": {
                    "bootstrap.servers": "localhost:9092",
                    "topic": "real_time_events",
                    "group.id": "tauro_consumer_group",
                },
            },
        }

    def generate_output_config(self) -> Dict[str, Any]:
        return {
            # Bronze layer outputs
            "bronze_sales": {
                "description": "Bronze layer sales data output",
                "format": "delta",
                "table_name": "bronze_sales",
                "schema": "bronze",
                "sub_folder": "sales",
                "write_mode": "append",
                "vacuum": True,
            },
            "bronze_customers": {
                "description": "Bronze layer customer data output",
                "format": "delta",
                "table_name": "bronze_customers",
                "schema": "bronze",
                "sub_folder": "customers",
                "write_mode": "append",
                "vacuum": True,
            },
            # Silver layer outputs
            "silver_sales": {
                "description": "Silver layer cleaned sales data output",
                "format": "delta",
                "table_name": "silver_sales",
                "schema": "silver",
                "sub_folder": "sales",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            "silver_customers": {
                "description": "Silver layer cleaned customer data output",
                "format": "delta",
                "table_name": "silver_customers",
                "schema": "silver",
                "sub_folder": "customers",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            # Gold layer outputs
            "gold_sales_summary": {
                "description": "Gold layer business-ready sales summary",
                "format": "delta",
                "table_name": "gold_sales_summary",
                "schema": "gold",
                "sub_folder": "summaries",
                "write_mode": "overwrite",
                "vacuum": True,
            },
            # Streaming output
            "delta_sink": {
                "description": "Real-time processed data sink",
                "format": "delta",
                "table_name": "real_time_data",
                "schema": "streaming",
                "sub_folder": "real_time",
                "write_mode": "append",
                "checkpoint_location": "/mnt/checkpoints/real_time_processing",
            },
        }





class TemplateFactory:
    """Factory for creating configuration templates."""

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
        """List all available templates with descriptions."""
        return [
            {
                "type": TemplateType.MEDALLION_BASIC.value,
                "name": "Medallion Basic",
                "description": "Basic Medallion architecture with Bronze, Silver, Gold layers and streaming support",
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
            "pipelines/streaming",
            "src",
            "tests",
            "docs",
            "notebooks",
            "data",
            "data/raw",
            "data/processed",
            "logs",
        ]

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
            self._generate_medallion_basic_sample_code()
        # Note: Other template types have been removed as per requirements

    def _generate_medallion_basic_sample_code(self) -> None:
        """Generate sample code for basic Medallion architecture with streaming support."""
        # Bronze layer sample
        bronze_code = '''"""Bronze layer data ingestion functions."""
from pyspark.sql import DataFrame, SparkSession
from loguru import logger


def ingest_sales_data(start_date: str, end_date: str) -> DataFrame:
    """Ingest raw sales data into Bronze layer."""
    logger.info(f"Ingesting sales data from {start_date} to {end_date}")
    spark = SparkSession.getActiveSession()
    
    # Example implementation - replace with actual data source
    # df = spark.read.format("csv").option("header", "true").load("/path/to/sales/data")
    # Add processing logic here
    logger.info("Sales data ingestion completed")
    # return df


def ingest_customer_data(start_date: str, end_date: str) -> DataFrame:
    """Ingest raw customer data into Bronze layer."""
    logger.info(f"Ingesting customer data from {start_date} to {end_date}")
    spark = SparkSession.getActiveSession()
    
    # Example implementation - replace with actual data source
    # df = spark.read.format("csv").option("header", "true").load("/path/to/customer/data")
    # Add processing logic here
    logger.info("Customer data ingestion completed")
    # return df
'''

        bronze_file = self.output_path / "pipelines" / "bronze" / "ingestion.py"
        self._write_text_file(bronze_file, bronze_code)

        # Silver layer sample
        silver_code = '''"""Silver layer data cleaning and transformation functions."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull
from loguru import logger


def clean_sales_data(bronze_sales: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Clean and standardize sales data for Silver layer."""
    logger.info(f"Cleaning sales data from {start_date} to {end_date}")
    
    # Example cleaning operations
    # cleaned_df = bronze_sales.filter(col("amount") > 0) \\
    #                         .fillna(0, subset=["quantity"]) \\
    #                         .dropna(subset=["customer_id", "product_id"])
    
    logger.info("Sales data cleaning completed")
    # return cleaned_df


def clean_customer_data(bronze_customers: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Clean and standardize customer data for Silver layer."""
    logger.info(f"Cleaning customer data from {start_date} to {end_date}")
    
    # Example cleaning operations
    # cleaned_df = bronze_customers.filter(col("customer_id").isNotNull()) \\
    #                             .dropDuplicates(["customer_id"]) \\
    #                             .fillna("Unknown", subset=["city", "state"])
    
    logger.info("Customer data cleaning completed")
    # return cleaned_df
'''

        silver_file = self.output_path / "pipelines" / "silver" / "cleaning.py"
        self._write_text_file(silver_file, silver_code)

        # Gold layer sample
        gold_code = '''"""Gold layer aggregation functions for business-ready data."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, avg, count, desc
from loguru import logger


def sales_summary(silver_sales: DataFrame, silver_customers: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Create business-ready sales summary for Gold layer."""
    logger.info(f"Creating sales summary from {start_date} to {end_date}")
    
    # Example aggregation operations
    # summary_df = silver_sales.join(silver_customers, "customer_id") \\
    #                         .groupBy("customer_id", "city", "state") \\
    #                         .agg(
    #                             sum("amount").alias("total_sales"),
    #                             avg("amount").alias("avg_order_value"),
    #                             count("order_id").alias("order_count")
    #                         ) \\
    #                         .orderBy(desc("total_sales"))
    
    logger.info("Sales summary creation completed")
    # return summary_df
'''

        gold_file = self.output_path / "pipelines" / "gold" / "aggregation.py"
        self._write_text_file(gold_file, gold_code)

        # Streaming sample
        streaming_code = '''"""Real-time streaming data processing functions."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from loguru import logger


def read_stream(kafka_topic: str, start_date: str, end_date: str) -> DataFrame:
    """Read data from streaming source (e.g., Kafka)."""
    logger.info(f"Starting stream reading from {kafka_topic}")
    spark = SparkSession.getActiveSession()
    
    # Example Kafka stream reading
    # stream_df = spark.readStream \\
    #                 .format("kafka") \\
    #                 .option("kafka.bootstrap.servers", "localhost:9092") \\
    #                 .option("subscribe", kafka_topic) \\
    #                 .option("startingOffsets", "latest") \\
    #                 .load()
    
    logger.info("Stream reading started")
    # return stream_df


def transform_stream(raw_stream: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Transform streaming data in real-time."""
    logger.info("Transforming streaming data")
    
    # Example schema for JSON data
    # schema = StructType([
    #     StructField("event_id", StringType(), True),
    #     StructField("user_id", StringType(), True),
    #     StructField("event_type", StringType(), True),
    #     StructField("timestamp", TimestampType(), True),
    #     StructField("value", IntegerType(), True)
    # ])
    
    # Example transformation
    # transformed_df = raw_stream.select(
    #     from_json(col("value").cast("string"), schema).alias("data")
    # ).select("data.*") \\
    #  .withColumn("processed_timestamp", current_timestamp()) \\
    #  .filter(col("event_type").isNotNull())
    
    logger.info("Stream transformation completed")
    # return transformed_df


def write_stream(transformed_stream: DataFrame, start_date: str, end_date: str) -> None:
    """Write processed stream to sink (e.g., Delta table)."""
    logger.info("Writing stream to sink")
    
    # Example stream writing to Delta
    # query = transformed_stream.writeStream \\
    #                          .format("delta") \\
    #                          .outputMode("append") \\
    #                          .option("checkpointLocation", "/mnt/checkpoints/real_time_processing") \\
    #                          .option("path", "/mnt/data/streaming/real_time_data") \\
    #                          .start()
    
    logger.info("Stream writing started")
    # return query  # Return the streaming query for monitoring
'''

        streaming_file = self.output_path / "pipelines" / "streaming" / "processing.py"
        streaming_file.parent.mkdir(parents=True, exist_ok=True)
        self._write_text_file(streaming_file, streaming_code)



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
            logger.info("\n🔄 Medallion Architecture with Streaming:")
            logger.info("   • Bronze: Raw data ingestion (batch)")
            logger.info("   • Silver: Cleaned and transformed data (batch)")
            logger.info("   • Gold: Business-ready aggregations (batch)")
            logger.info("   • Streaming: Real-time processing pipeline")

            logger.info("\n🚀 Example usage:")
            logger.info("   # Batch pipelines:")
            logger.info("   tauro --env dev --pipeline bronze_ingestion")
            logger.info("   tauro --env dev --pipeline silver_processing")
            logger.info("   tauro --env dev --pipeline gold_aggregation")
            logger.info("   # Streaming pipeline:")
            logger.info("   tauro --env dev --pipeline real_time_processing")


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
