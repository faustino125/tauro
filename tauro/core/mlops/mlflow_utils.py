"""
MLflow Configuration and Helper Utilities for Tauro.

Facilita la configuración y uso de MLflow en pipelines de Tauro.

Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger


class MLflowConfig:
    """
    Configuración centralizada de MLflow para Tauro.
    
    Soporta:
    - Variables de entorno
    - Configuración en YAML/JSON
    - Valores por defecto sensatos
    - Auto-detección de Databricks
    
    Example:
        >>> config = MLflowConfig.from_env()
        >>> config = MLflowConfig.from_yaml("mlflow_config.yml")
        >>> config = MLflowConfig(
        ...     tracking_uri="http://localhost:5000",
        ...     experiment_name="my_pipeline",
        ... )
    """
    
    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        experiment_name: str = "tauro_pipeline",
        artifact_location: Optional[str] = None,
        enable_autolog: bool = True,
        nested_runs: bool = True,
        registry_uri: Optional[str] = None,
        default_tags: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize MLflow configuration.
        
        Args:
            tracking_uri: MLflow tracking server URI (None = local)
            experiment_name: Default experiment name
            artifact_location: Artifact storage location
            enable_autolog: Enable auto-logging for ML frameworks
            nested_runs: Use nested runs for pipeline nodes
            registry_uri: Model registry URI (None = same as tracking)
            default_tags: Default tags for all runs
        """
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        self.artifact_location = artifact_location
        self.enable_autolog = enable_autolog
        self.nested_runs = nested_runs
        self.registry_uri = registry_uri or tracking_uri
        self.default_tags = default_tags or {}
        
        # Auto-detect Databricks
        self.is_databricks = self._detect_databricks()
        if self.is_databricks:
            logger.info("Databricks environment detected")
    
    def _detect_databricks(self) -> bool:
        """Detect if running on Databricks."""
        return (
            os.getenv("DATABRICKS_RUNTIME_VERSION") is not None
            or os.getenv("SPARK_HOME", "").find("databricks") >= 0
        )
    
    @classmethod
    def from_env(cls, prefix: str = "MLFLOW_") -> "MLflowConfig":
        """
        Create configuration from environment variables.
        
        Environment variables:
        - MLFLOW_TRACKING_URI: Tracking server URI
        - MLFLOW_EXPERIMENT_NAME: Experiment name
        - MLFLOW_ARTIFACT_LOCATION: Artifact location
        - MLFLOW_ENABLE_AUTOLOG: Enable autologging (true/false)
        - MLFLOW_NESTED_RUNS: Use nested runs (true/false)
        - MLFLOW_REGISTRY_URI: Model registry URI
        
        Args:
            prefix: Prefix for environment variables
            
        Returns:
            MLflowConfig instance
            
        Example:
            >>> os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5000"
            >>> config = MLflowConfig.from_env()
        """
        return cls(
            tracking_uri=os.getenv(f"{prefix}TRACKING_URI"),
            experiment_name=os.getenv(f"{prefix}EXPERIMENT_NAME", "tauro_pipeline"),
            artifact_location=os.getenv(f"{prefix}ARTIFACT_LOCATION"),
            enable_autolog=os.getenv(f"{prefix}ENABLE_AUTOLOG", "true").lower() == "true",
            nested_runs=os.getenv(f"{prefix}NESTED_RUNS", "true").lower() == "true",
            registry_uri=os.getenv(f"{prefix}REGISTRY_URI"),
        )
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "MLflowConfig":
        """
        Load configuration from YAML file.
        
        YAML structure:
        ```yaml
        mlflow:
          tracking_uri: http://localhost:5000
          experiment_name: my_pipeline
          artifact_location: /path/to/artifacts
          enable_autolog: true
          nested_runs: true
        ```
        
        Args:
            config_path: Path to YAML file
            
        Returns:
            MLflowConfig instance
        """
        import yaml
        
        with open(config_path) as f:
            data = yaml.safe_load(f)
        
        mlflow_config = data.get("mlflow", {})
        
        return cls(
            tracking_uri=mlflow_config.get("tracking_uri"),
            experiment_name=mlflow_config.get("experiment_name", "tauro_pipeline"),
            artifact_location=mlflow_config.get("artifact_location"),
            enable_autolog=mlflow_config.get("enable_autolog", True),
            nested_runs=mlflow_config.get("nested_runs", True),
            registry_uri=mlflow_config.get("registry_uri"),
            default_tags=mlflow_config.get("tags", {}),
        )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "MLflowConfig":
        """
        Create configuration from dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            MLflowConfig instance
        """
        return cls(
            tracking_uri=config_dict.get("tracking_uri"),
            experiment_name=config_dict.get("experiment_name", "tauro_pipeline"),
            artifact_location=config_dict.get("artifact_location"),
            enable_autolog=config_dict.get("enable_autolog", True),
            nested_runs=config_dict.get("nested_runs", True),
            registry_uri=config_dict.get("registry_uri"),
            default_tags=config_dict.get("tags", {}),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "tracking_uri": self.tracking_uri,
            "experiment_name": self.experiment_name,
            "artifact_location": self.artifact_location,
            "enable_autolog": self.enable_autolog,
            "nested_runs": self.nested_runs,
            "registry_uri": self.registry_uri,
            "tags": self.default_tags,
            "is_databricks": self.is_databricks,
        }
    
    def apply(self) -> None:
        """Apply configuration to MLflow environment."""
        try:
            import mlflow
            
            if self.tracking_uri:
                mlflow.set_tracking_uri(self.tracking_uri)
                logger.info(f"Set MLflow tracking URI: {self.tracking_uri}")
            
            if self.registry_uri:
                mlflow.set_registry_uri(self.registry_uri)
                logger.info(f"Set MLflow registry URI: {self.registry_uri}")
            
            logger.info("MLflow configuration applied successfully")
            
        except ImportError:
            logger.warning("MLflow not installed, configuration not applied")


class MLflowHelper:
    """
    Helper utilities para trabajar con MLflow en Tauro.
    
    Proporciona funciones de conveniencia para:
    - Logging de DataFrames
    - Logging de plots
    - Comparación de runs
    - Búsqueda de mejores modelos
    """
    
    @staticmethod
    def log_dataframe_sample(
        df: Any,
        name: str = "data_sample",
        n_rows: int = 100,
        artifact_path: Optional[str] = None,
    ) -> None:
        """
        Log muestra de DataFrame como artifact.
        
        Args:
            df: pandas o Spark DataFrame
            name: Nombre del artifact
            n_rows: Número de filas a incluir
            artifact_path: Path en artifacts store
        """
        try:
            import mlflow
            import tempfile
            
            # Convert to pandas if needed
            if hasattr(df, "toPandas"):
                df = df.limit(n_rows).toPandas()
            else:
                df = df.head(n_rows)
            
            # Save to temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                df.to_csv(f.name, index=False)
                temp_path = f.name
            
            # Log artifact
            mlflow.log_artifact(temp_path, artifact_path or "data")
            
            # Cleanup
            Path(temp_path).unlink(missing_ok=True)
            
            logger.debug(f"Logged DataFrame sample: {name}")
            
        except Exception as e:
            logger.warning(f"Could not log DataFrame sample: {e}")
    
    @staticmethod
    def log_plot(
        fig: Any,
        name: str,
        artifact_path: Optional[str] = None,
    ) -> None:
        """
        Log matplotlib/plotly figure como artifact.
        
        Args:
            fig: matplotlib.figure.Figure o plotly.graph_objects.Figure
            name: Nombre del artifact
            artifact_path: Path en artifacts store
        """
        try:
            import mlflow
            import tempfile
            
            # Detect figure type and save
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
                temp_path = f.name
            
            if hasattr(fig, "savefig"):
                # Matplotlib
                fig.savefig(temp_path, bbox_inches="tight", dpi=150)
            elif hasattr(fig, "write_image"):
                # Plotly
                fig.write_image(temp_path)
            else:
                logger.warning(f"Unknown figure type: {type(fig)}")
                return
            
            # Log artifact
            mlflow.log_artifact(temp_path, artifact_path or "plots")
            
            # Cleanup
            Path(temp_path).unlink(missing_ok=True)
            
            logger.debug(f"Logged plot: {name}")
            
        except Exception as e:
            logger.warning(f"Could not log plot: {e}")
    
    @staticmethod
    def log_dict_as_json(
        data: Dict[str, Any],
        name: str,
        artifact_path: Optional[str] = None,
    ) -> None:
        """
        Log dictionary como JSON artifact.
        
        Args:
            data: Dictionary to log
            name: Nombre del artifact
            artifact_path: Path en artifacts store
        """
        try:
            import mlflow
            import json
            import tempfile
            
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(data, f, indent=2)
                temp_path = f.name
            
            mlflow.log_artifact(temp_path, artifact_path or "config")
            
            Path(temp_path).unlink(missing_ok=True)
            
            logger.debug(f"Logged JSON: {name}")
            
        except Exception as e:
            logger.warning(f"Could not log JSON: {e}")
    
    @staticmethod
    def get_best_run(
        experiment_name: str,
        metric: str,
        mode: str = "max",
    ) -> Optional[Any]:
        """
        Get best run from experiment by metric.
        
        Args:
            experiment_name: Experiment name
            metric: Metric to optimize
            mode: 'max' or 'min'
            
        Returns:
            Best run object or None
        """
        try:
            import mlflow
            from mlflow.tracking import MlflowClient
            
            client = MlflowClient()
            experiment = client.get_experiment_by_name(experiment_name)
            
            if not experiment:
                logger.warning(f"Experiment not found: {experiment_name}")
                return None
            
            # Search runs
            runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string="",
                order_by=[f"metrics.{metric} {'DESC' if mode == 'max' else 'ASC'}"],
                max_results=1,
            )
            
            if runs:
                return runs[0]
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not get best run: {e}")
            return None
    
    @staticmethod
    def log_system_metrics() -> None:
        """Log system metrics (CPU, memory, etc.)."""
        try:
            import mlflow
            import psutil
            
            mlflow.log_metric("system_cpu_percent", psutil.cpu_percent())
            mlflow.log_metric("system_memory_percent", psutil.virtual_memory().percent)
            
            logger.debug("Logged system metrics")
            
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"Could not log system metrics: {e}")


def setup_mlflow_for_tauro(
    config: Optional[MLflowConfig] = None,
    config_path: Optional[str] = None,
) -> MLflowConfig:
    """
    Setup completo de MLflow para Tauro.
    
    Args:
        config: MLflowConfig instance
        config_path: Path to YAML config file
        
    Returns:
        Applied MLflowConfig
        
    Example:
        >>> config = setup_mlflow_for_tauro(config_path="mlflow_config.yml")
        >>> # O con variables de entorno:
        >>> config = setup_mlflow_for_tauro()
    """
    if config is None:
        if config_path and Path(config_path).exists():
            config = MLflowConfig.from_yaml(config_path)
        else:
            config = MLflowConfig.from_env()
    
    config.apply()
    return config
