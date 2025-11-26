import pytest  # type: ignore
from unittest.mock import MagicMock
from typing import Dict, Generator, Any


@pytest.fixture(scope="session")
def spark_session() -> Generator[Any, None, None]:
    """Creates a real SparkSession for integration tests that require it."""
    try:
        from pyspark.sql import SparkSession  # type: ignore
    except Exception:
        pytest.skip("pyspark not available in test environment")

    try:
        spark = SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()
    except Exception:
        pytest.skip("Could not initialize SparkSession in this test environment")

    yield spark
    spark.stop()


@pytest.fixture
def mock_spark_context() -> Dict[str, Any]:
    """Minimal context with mocked Spark and UC disabled by default."""
    mock_spark = MagicMock()
    # Ensure conf.get exists and returns 'false' by default (Unity Catalog disabled)
    mock_spark.conf.get.return_value = "false"
    return {"spark": mock_spark, "execution_mode": "local"}


@pytest.fixture
def mock_output_context() -> Dict[str, Any]:
    """Mocked output context with local path and minimal configuration."""
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = "false"
    return {
        "spark": mock_spark,
        "output_path": "/test/output",
        "output_config": {"test_output": {"format": "parquet", "filepath": "/test/path.parquet"}},
        "global_settings": {"fail_on_error": True},
    }


@pytest.fixture
def mock_unity_catalog_context() -> Dict[str, Any]:
    """Context with Unity Catalog enabled in Spark (mock)."""
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = "true"  # Unity Catalog enabled

    return {
        "spark": mock_spark,
        "output_path": "/test/output",
        "output_config": {
            "test_uc_output": {
                "format": "unity_catalog",
                "catalog_name": "test_catalog",
                "schema": "test_schema",
                "table_name": "test_table",
            }
        },
        "global_settings": {"fail_on_error": True},
    }
