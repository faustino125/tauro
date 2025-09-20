import pytest
from unittest.mock import MagicMock, patch
from tauro.io.writers import (
    JSONWriter,
    DeltaWriter,
    CSVWriter,
    ParquetWriter,
    ORCWriter,
)
from tauro.io.exceptions import ConfigurationError, WriteOperationError
from tauro.io.constants import WriteMode, DEFAULT_CSV_OPTIONS


@pytest.fixture
def fake_df():
    """Fixture para crear un DataFrame mock."""
    df = MagicMock()
    df.isEmpty.return_value = False
    df.columns = ["col1", "col2"]
    return df


@pytest.fixture
def mock_spark_context():
    """Fixture para mock de contexto Spark."""
    mock_spark = MagicMock()
    return {"spark": mock_spark, "execution_mode": "local"}


class TestJSONWriter:
    @pytest.fixture
    def json_writer(self, mock_spark_context):
        return JSONWriter(mock_spark_context)

    def test_json_writer_ignores_replacewhere(self, json_writer, fake_df):
        """Test que verifica que JSONWriter ignora la estrategia replaceWhere y escribe correctamente."""
        config = {
            "overwrite_strategy": "replaceWhere",
            "partition_col": "date",
            "start_date": "2023-01-01",
            "end_date": "2023-01-02",
        }

        # Mock the _configure_spark_writer method
        with patch.object(json_writer, "_configure_spark_writer") as mock_configure:
            mock_writer = MagicMock()
            mock_configure.return_value = mock_writer

            json_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            mock_writer.save.assert_called_once_with("test_path")

    def test_json_writer_write_success(self, json_writer, fake_df):
        config = {"write_mode": WriteMode.OVERWRITE.value}

        # Mock the _configure_spark_writer method
        with patch.object(json_writer, "_configure_spark_writer") as mock_configure:
            mock_writer = MagicMock()
            mock_configure.return_value = mock_writer

            json_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            mock_writer.save.assert_called_once_with("test_path")


class TestDeltaWriter:
    @pytest.fixture
    def delta_writer(self, mock_spark_context):
        return DeltaWriter(mock_spark_context)

    def test_delta_writer_write_success(self, delta_writer, fake_df):
        config = {"write_mode": WriteMode.OVERWRITE.value}

        with patch.object(delta_writer, "_configure_spark_writer") as mock_configure:
            mock_writer = MagicMock()
            mock_configure.return_value = mock_writer

            delta_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            mock_writer.save.assert_called_once_with("test_path")

    def test_delta_writer_rejects_replacewhere_without_required_params(
        self, delta_writer, fake_df
    ):
        """Test que verifica que DeltaWriter lanza excepción si faltan parámetros para replaceWhere."""
        config = {
            "overwrite_strategy": "replaceWhere",
            "partition_col": "date"
            # Faltan start_date and end_date
        }

        # DeltaWriter envuelve ConfigurationError en WriteOperationError
        with pytest.raises(WriteOperationError) as exc_info:
            delta_writer.write(fake_df, "test_path", config)

        # Verificamos que el mensaje de error contenga la información esperada
        assert (
            "overwrite_strategy=replaceWhere requires partition_col, start_date and end_date"
            in str(exc_info.value)
        )


class TestCSVWriter:
    @pytest.fixture
    def csv_writer(self, mock_spark_context):
        return CSVWriter(mock_spark_context)

    def test_csv_writer_write_success(self, csv_writer, fake_df):
        config = {"write_mode": WriteMode.OVERWRITE.value}

        # Mock the _configure_spark_writer method
        with patch.object(csv_writer, "_configure_spark_writer") as mock_configure:
            # Create a mock writer that returns itself when option is called
            mock_writer = MagicMock()
            mock_writer.option.return_value = (
                mock_writer  # option() returns the same writer
            )
            mock_configure.return_value = mock_writer

            csv_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            # Verify that option was called for each CSV option
            expected_options = {**DEFAULT_CSV_OPTIONS, "quote": '"', "escape": '"'}
            for key, value in expected_options.items():
                mock_writer.option.assert_any_call(key, value)
            mock_writer.save.assert_called_once_with("test_path")

    def test_csv_writer_write_with_custom_options(self, csv_writer, fake_df):
        config = {
            "write_mode": WriteMode.OVERWRITE.value,
            "options": {"delimiter": "|", "header": "false"},
        }

        # Mock the _configure_spark_writer method
        with patch.object(csv_writer, "_configure_spark_writer") as mock_configure:
            # Create a mock writer that returns itself when option is called
            mock_writer = MagicMock()
            mock_writer.option.return_value = (
                mock_writer  # option() returns the same writer
            )
            mock_configure.return_value = mock_writer

            csv_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            # Verify that option was called for each CSV option (default + custom)
            expected_options = {
                **DEFAULT_CSV_OPTIONS,
                "quote": '"',
                "escape": '"',
                "delimiter": "|",
                "header": "false",
            }
            for key, value in expected_options.items():
                mock_writer.option.assert_any_call(key, value)
            mock_writer.save.assert_called_once_with("test_path")


class TestParquetWriter:
    @pytest.fixture
    def parquet_writer(self, mock_spark_context):
        return ParquetWriter(mock_spark_context)

    def test_parquet_writer_write_success(self, parquet_writer, fake_df):
        config = {"write_mode": WriteMode.OVERWRITE.value}

        with patch.object(parquet_writer, "_configure_spark_writer") as mock_configure:
            mock_writer = MagicMock()
            mock_configure.return_value = mock_writer

            parquet_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            mock_writer.save.assert_called_once_with("test_path")


class TestORCWriter:
    @pytest.fixture
    def orc_writer(self, mock_spark_context):
        return ORCWriter(mock_spark_context)

    def test_orc_writer_write_success(self, orc_writer, fake_df):
        config = {"write_mode": WriteMode.OVERWRITE.value}

        with patch.object(orc_writer, "_configure_spark_writer") as mock_configure:
            mock_writer = MagicMock()
            mock_configure.return_value = mock_writer

            orc_writer.write(fake_df, "test_path", config)

            mock_configure.assert_called_once_with(fake_df, config)
            mock_writer.save.assert_called_once_with("test_path")
