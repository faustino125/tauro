import pytest
from unittest.mock import MagicMock, patch
from tauro.io.readers import CSVReader, ParquetReader, QueryReader
from tauro.io.exceptions import ReadOperationError, ConfigurationError
from tauro.io.constants import DEFAULT_CSV_OPTIONS


class TestCSVReader:
    @pytest.fixture
    def csv_reader(self):
        mock_spark = MagicMock()
        context = {"spark": mock_spark}
        return CSVReader(context)

    def test_read_success(self, csv_reader):
        mock_df = MagicMock()
        csv_reader._spark_read = MagicMock(return_value=mock_df)

        result = csv_reader.read("test.csv", {})
        assert result == mock_df
        # El CSVReader agrega opciones por defecto, debemos esperar esta configuración
        expected_config = {"options": DEFAULT_CSV_OPTIONS}
        csv_reader._spark_read.assert_called_once_with(
            "csv", "test.csv", expected_config
        )

    def test_read_failure(self, csv_reader):
        csv_reader._spark_read = MagicMock(side_effect=Exception("Read error"))

        with pytest.raises(ReadOperationError) as exc:
            csv_reader.read("test.csv", {})
        assert "Failed to read CSV" in str(exc.value)

    def test_read_with_custom_options(self, csv_reader):
        mock_df = MagicMock()
        csv_reader._spark_read = MagicMock(return_value=mock_df)

        config = {"options": {"delimiter": "|", "header": "false"}}
        result = csv_reader.read("test.csv", config)

        assert result == mock_df
        # Las opciones personalizadas deben fusionarse con las opciones por defecto
        expected_options = {**DEFAULT_CSV_OPTIONS, **config["options"]}
        expected_config = {"options": expected_options}
        csv_reader._spark_read.assert_called_once_with(
            "csv", "test.csv", expected_config
        )


class TestQueryReader:
    @pytest.fixture
    def query_reader(self):
        mock_spark = MagicMock()
        context = {"spark": mock_spark}
        return QueryReader(context)

    def test_read_valid_query(self, query_reader):
        mock_df = MagicMock()
        query_reader._get_spark = MagicMock(return_value=MagicMock())
        query_reader._get_spark().sql.return_value = mock_df

        config = {"query": "SELECT * FROM table"}
        result = query_reader.read("", config)

        assert result == mock_df
        query_reader._get_spark().sql.assert_called_once_with("SELECT * FROM table")

    def test_read_missing_query(self, query_reader):
        # QueryReader envuelve ConfigurationError en ReadOperationError
        with pytest.raises(ReadOperationError) as exc_info:
            query_reader.read("", {})
        # Verificamos que la causa sea ConfigurationError
        assert "Query format specified without SQL query" in str(exc_info.value)

        with pytest.raises(ReadOperationError) as exc_info:
            query_reader.read("", {"query": ""})
        assert "Query format specified without SQL query" in str(exc_info.value)

    def test_read_no_spark(self, query_reader):
        query_reader._get_spark = MagicMock(return_value=None)

        with pytest.raises(ReadOperationError):
            query_reader.read("", {"query": "SELECT * FROM table"})
