"""
Shared pytest fixtures and configuration for tauro.core.config tests.

This module provides:
- Fake Spark session mock (avoids launching JVM)
- Common test configurations (minimal, comprehensive)
- Fixture factories for different test scenarios
- Context builders for different pipeline types
"""

import pytest  # type: ignore
import tempfile
from types import SimpleNamespace
from pathlib import Path
from typing import Dict, Any, Tuple

from core.config.session import SparkSessionFactory


# ============================================================================
# SPARK SESSION MOCKING
# ============================================================================


@pytest.fixture(autouse=True)
def fake_spark_session(monkeypatch):
    """
    Auto-used fixture that replaces SparkSessionFactory with a mock.

    This prevents tests from attempting to launch the Spark JVM while maintaining
    the SparkSessionFactory API contract. All tests automatically use this.

    Yields:
        None (automatically applied to all tests)
    """
    # Clear any previous session
    SparkSessionFactory.reset_session()

    # Replace get_session with a mock that returns a fake Spark-like object
    fake_spark = SimpleNamespace(
        name="fake-spark-session",
        version="3.0.0",
        conf=SimpleNamespace(get=lambda k, default=None: default),
        sql=lambda query: SimpleNamespace(show=lambda n=20: None),
        createDataFrame=lambda data, schema=None: SimpleNamespace(show=lambda n=20: None),
    )

    monkeypatch.setattr(
        SparkSessionFactory,
        "get_session",
        lambda *args, **kwargs: fake_spark,
    )

    # Replace reset_session to avoid side effects
    monkeypatch.setattr(
        SparkSessionFactory,
        "reset_session",
        lambda: setattr(SparkSessionFactory, "_session", None),
    )

    yield

    # Cleanup
    SparkSessionFactory.reset_session()


# ============================================================================
# MINIMAL CONFIGURATION FIXTURES
# ============================================================================


@pytest.fixture
def minimal_global_settings() -> Dict[str, Any]:
    """Minimal global settings with only required keys."""
    return {
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
    }


@pytest.fixture
def minimal_pipelines_config() -> Dict[str, Any]:
    """Single minimal batch pipeline."""
    return {
        "batch_pipeline": {
            "type": "batch",
            "nodes": ["extract"],
        }
    }


@pytest.fixture
def minimal_nodes_config() -> Dict[str, Any]:
    """Single minimal node with extract function."""
    return {
        "extract": {
            "input": ["raw_data"],
            "function": "etl.extract",
        }
    }


@pytest.fixture
def minimal_input_config() -> Dict[str, Any]:
    """Single input source."""
    return {
        "raw_data": {
            "format": "parquet",
            "filepath": "/data/in/raw.parquet",
        }
    }


@pytest.fixture
def minimal_output_config() -> Dict[str, Any]:
    """Single output destination."""
    return {
        "processed_data": {
            "format": "parquet",
            "filepath": "/data/out/processed.parquet",
        }
    }


@pytest.fixture
def minimal_configs(
    minimal_global_settings,
    minimal_pipelines_config,
    minimal_nodes_config,
    minimal_input_config,
    minimal_output_config,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    """
    Combine all minimal fixtures into a single 5-tuple.

    Returns:
        (global_settings, pipelines_config, nodes_config, input_config, output_config)
    """
    return (
        minimal_global_settings,
        minimal_pipelines_config,
        minimal_nodes_config,
        minimal_input_config,
        minimal_output_config,
    )


# ============================================================================
# COMPREHENSIVE CONFIGURATION FIXTURES
# ============================================================================


@pytest.fixture
def comprehensive_global_settings() -> Dict[str, Any]:
    """Global settings with optional fields and custom configurations."""
    return {
        "input_path": "/data/in",
        "output_path": "/data/out",
        "mode": "local",
        "layer": "batch",
        "project_name": "test_project",
        "variables": {
            "env_stage": "test",
            "model_version": "v1.0",
        },
        "spark_config": {
            "spark.sql.shuffle.partitions": "200",
            "spark.executor.memory": "4g",
        },
        "format_policy": {
            "supported_inputs": ["parquet", "csv", "json"],
            "supported_outputs": ["parquet", "delta"],
        },
    }


@pytest.fixture
def comprehensive_pipelines_config() -> Dict[str, Any]:
    """Multiple pipelines with different types and configurations."""
    return {
        "batch_etl": {
            "type": "batch",
            "nodes": ["extract", "transform", "load"],
            "spark_config": {
                "spark.sql.shuffle.partitions": "300",
            },
        },
        "ml_pipeline": {
            "type": "ml",
            "nodes": ["preprocess", "train", "evaluate"],
            "model_version": "v2.0",
        },
        "streaming_pipeline": {
            "type": "streaming",
            "nodes": ["ingest", "process"],
            "checkpoint_dir": "/checkpoints",
        },
    }


@pytest.fixture
def comprehensive_nodes_config() -> Dict[str, Any]:
    """Multiple nodes with various configurations and dependencies."""
    return {
        "extract": {
            "input": ["raw_source"],
            "function": "etl.extract",
            "output": ["raw_extracted"],
        },
        "transform": {
            "dependencies": ["extract"],
            "input": ["raw_extracted"],
            "function": "etl.transform",
            "output": ["transformed"],
        },
        "load": {
            "dependencies": ["transform"],
            "input": ["transformed"],
            "function": "etl.load",
            "output": ["target"],
        },
        "preprocess": {
            "input": ["training_data"],
            "function": "ml.preprocess",
        },
        "train": {
            "dependencies": ["preprocess"],
            "function": "ml.train",
            "model": {
                "type": "spark_ml",
            },
            "hyperparams": {
                "max_iter": 100,
                "learning_rate": 0.01,
            },
        },
        "evaluate": {
            "dependencies": ["train"],
            "function": "ml.evaluate",
        },
        "ingest": {
            "input": ["kafka_source"],
            "function": "streaming.ingest",
        },
        "process": {
            "dependencies": ["ingest"],
            "function": "streaming.process",
        },
    }


@pytest.fixture
def comprehensive_input_config() -> Dict[str, Any]:
    """Multiple input sources with different formats."""
    return {
        "raw_source": {
            "format": "parquet",
            "filepath": "/data/in/raw.parquet",
        },
        "training_data": {
            "format": "csv",
            "filepath": "/data/in/train.csv",
        },
        "kafka_source": {
            "format": "kafka",
            "bootstrap_servers": "localhost:9092",
            "topic": "input_topic",
        },
    }


@pytest.fixture
def comprehensive_output_config() -> Dict[str, Any]:
    """Multiple output destinations with different formats."""
    return {
        "target": {
            "format": "delta",
            "schema": "curated",
            "table_name": "sales",
        },
        "model_artifact": {
            "format": "pickle",
            "filepath": "/models/model-${model_version}.pkl",
        },
        "metrics": {
            "format": "json",
            "filepath": "/output/metrics.json",
        },
    }


@pytest.fixture
def comprehensive_configs(
    comprehensive_global_settings,
    comprehensive_pipelines_config,
    comprehensive_nodes_config,
    comprehensive_input_config,
    comprehensive_output_config,
) -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    """
    Combine all comprehensive fixtures into a single 5-tuple.

    Returns:
        (global_settings, pipelines_config, nodes_config, input_config, output_config)
    """
    return (
        comprehensive_global_settings,
        comprehensive_pipelines_config,
        comprehensive_nodes_config,
        comprehensive_input_config,
        comprehensive_output_config,
    )


# ============================================================================
# SPECIALIZED CONFIGURATION FIXTURES
# ============================================================================


@pytest.fixture
def ml_configs() -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    """Configuration focused on ML pipelines."""
    global_settings = {
        "input_path": "/ml/in",
        "output_path": "/ml/out",
        "mode": "local",
        "layer": "ml",
    }
    pipelines = {
        "training": {
            "type": "ml",
            "nodes": ["prep", "train", "eval"],
        },
    }
    nodes = {
        "prep": {
            "input": ["raw"],
            "function": "ml.prep",
        },
        "train": {
            "dependencies": ["prep"],
            "function": "ml.train",
            "model": {"type": "spark_ml"},
            "hyperparams": {"max_depth": 10},
        },
        "eval": {
            "dependencies": ["train"],
            "function": "ml.eval",
        },
    }
    input_cfg = {"raw": {"format": "csv", "filepath": "/ml/in/data.csv"}}
    output_cfg = {"model": {"format": "pickle", "filepath": "/ml/out/model.pkl"}}

    return (global_settings, pipelines, nodes, input_cfg, output_cfg)


@pytest.fixture
def streaming_configs() -> Tuple[Dict, Dict, Dict, Dict, Dict]:
    """Configuration focused on streaming pipelines."""
    global_settings = {
        "input_path": "/stream/in",
        "output_path": "/stream/out",
        "mode": "local",
        "layer": "streaming",
    }
    pipelines = {
        "real_time": {
            "type": "streaming",
            "nodes": ["source", "process", "sink"],
            "checkpoint_dir": "/checkpoints",
        },
    }
    nodes = {
        "source": {
            "input": ["kafka_in"],
            "function": "stream.source",
        },
        "process": {
            "dependencies": ["source"],
            "function": "stream.process",
        },
        "sink": {
            "dependencies": ["process"],
            "output": ["kafka_out"],
            "function": "stream.sink",
        },
    }
    input_cfg = {
        "kafka_in": {
            "format": "kafka",
            "bootstrap_servers": "broker:9092",
            "subscribe": "input",
        }
    }
    output_cfg = {
        "kafka_out": {
            "format": "kafka",
            "bootstrap_servers": "broker:9092",
            "topic": "output",
        }
    }

    return (global_settings, pipelines, nodes, input_cfg, output_cfg)


# ============================================================================
# TEMPORARY FILE FIXTURES
# ============================================================================


@pytest.fixture
def yaml_config_files(tmp_path):
    """Create temporary YAML configuration files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    global_yml = config_dir / "global.yml"
    global_yml.write_text(
        """
input_path: /data/in
output_path: /data/out
mode: local
"""
    )

    pipelines_yml = config_dir / "pipelines.yml"
    pipelines_yml.write_text(
        """
batch_pipeline:
  type: batch
  nodes: [extract]
"""
    )

    nodes_yml = config_dir / "nodes.yml"
    nodes_yml.write_text(
        """
extract:
  input: [raw]
  function: etl.extract
"""
    )

    input_yml = config_dir / "input.yml"
    input_yml.write_text(
        """
raw:
  format: parquet
  filepath: /data/in/raw.parquet
"""
    )

    output_yml = config_dir / "output.yml"
    output_yml.write_text(
        """
processed:
  format: parquet
  filepath: /data/out/processed.parquet
"""
    )

    return {
        "global": global_yml,
        "pipelines": pipelines_yml,
        "nodes": nodes_yml,
        "input": input_yml,
        "output": output_yml,
    }


@pytest.fixture
def json_config_files(tmp_path):
    """Create temporary JSON configuration files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    import json

    global_json = config_dir / "global.json"
    global_json.write_text(
        json.dumps(
            {
                "input_path": "/data/in",
                "output_path": "/data/out",
                "mode": "local",
            }
        )
    )

    pipelines_json = config_dir / "pipelines.json"
    pipelines_json.write_text(
        json.dumps(
            {
                "batch_pipeline": {
                    "type": "batch",
                    "nodes": ["extract"],
                }
            }
        )
    )

    nodes_json = config_dir / "nodes.json"
    nodes_json.write_text(
        json.dumps(
            {
                "extract": {
                    "input": ["raw"],
                    "function": "etl.extract",
                }
            }
        )
    )

    input_json = config_dir / "input.json"
    input_json.write_text(
        json.dumps(
            {
                "raw": {
                    "format": "parquet",
                    "filepath": "/data/in/raw.parquet",
                }
            }
        )
    )

    output_json = config_dir / "output.json"
    output_json.write_text(
        json.dumps(
            {
                "processed": {
                    "format": "parquet",
                    "filepath": "/data/out/processed.parquet",
                }
            }
        )
    )

    return {
        "global": global_json,
        "pipelines": pipelines_json,
        "nodes": nodes_json,
        "input": input_json,
        "output": output_json,
    }


# ============================================================================
# UTILITY FUNCTIONS (NOT FIXTURES)
# ============================================================================


def create_config_with_env_vars(global_settings: Dict, env_vars: Dict) -> Dict:
    """
    Create a config with placeholder variables that will be interpolated.

    Args:
        global_settings: Base global settings dict
        env_vars: Environment variables to set as placeholders

    Returns:
        Modified global_settings with ${VAR} placeholders
    """
    for key, var_name in env_vars.items():
        if isinstance(global_settings.get(key), str):
            global_settings[key] = f"${{${var_name}}}"
    return global_settings


def create_config_with_circular_ref() -> Dict:
    """Create a configuration with circular variable references."""
    return {
        "variables": {
            "a": "${b}",
            "b": "${a}",
        }
    }
