import pytest
from unittest.mock import MagicMock
from typing import Dict, Generator, Any


@pytest.fixture(scope="session")
def spark_session() -> Generator[Any, None, None]:
    """Crea una SparkSession real para tests de integración que lo requieran."""
    try:
        from pyspark.sql import SparkSession
    except Exception:
        pytest.skip("pyspark no está disponible en el entorno de test")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("pytest-pyspark")
            .getOrCreate()
        )
    except Exception:
        pytest.skip("No se pudo inicializar SparkSession en este entorno de test")

    yield spark
    spark.stop()


@pytest.fixture
def mock_spark_context() -> Dict[str, Any]:
    """Contexto mínimo con Spark mockeado y UC deshabilitado por defecto."""
    mock_spark = MagicMock()
    # Asegurar que conf.get existe y devuelve 'false' por defecto (Unity Catalog deshabilitado)
    mock_spark.conf.get.return_value = "false"
    return {"spark": mock_spark, "execution_mode": "local"}


@pytest.fixture
def mock_output_context() -> Dict[str, Any]:
    """Contexto de output mockeado con ruta local y configuración mínima."""
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = "false"
    return {
        "spark": mock_spark,
        "output_path": "/test/output",
        "output_config": {
            "test_output": {"format": "parquet", "filepath": "/test/path.parquet"}
        },
        "global_settings": {"fail_on_error": True},
    }


@pytest.fixture
def mock_unity_catalog_context() -> Dict[str, Any]:
    """Contexto con Unity Catalog habilitado en Spark (mock)."""
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
