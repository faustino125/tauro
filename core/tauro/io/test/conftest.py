import pytest
from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def spark_session():
    """Fixture para crear una SparkSession de prueba"""
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
def mock_spark_context():
    """Fixture para mock de contexto Spark"""
    mock_spark = MagicMock()
    # asegurar que conf.get existe y devuelva 'false' por defecto (Unity Catalog deshabilitado)
    mock_spark.conf.get.return_value = "false"
    return {"spark": mock_spark, "execution_mode": "local"}


@pytest.fixture
def mock_output_context():
    """Fixture para mock de contexto de output"""
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
def mock_unity_catalog_context():
    """Fixture para mock de contexto con Unity Catalog habilitado"""
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
