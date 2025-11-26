import pytest  # type: ignore
from unittest.mock import MagicMock

from engine.io.writers import (
    ParquetWriter,
    CSVWriter,
    JSONWriter,
    ORCWriter,
    DeltaWriter,
    SparkWriterMixin,
)
from engine.io.exceptions import ConfigurationError, WriteOperationError


class DummyDF:
    """Minimal dummy DataFrame-like object that exposes .write as an object with chained methods."""

    def __init__(self, writer_chain):
        self.write = writer_chain


@pytest.fixture(autouse=True)
def noop_data_validator(monkeypatch):
    """
    Prevent DataValidator from performing heavy checks (pandas/polars imports, etc.)
    by making validate_dataframe and validate_columns_exist no-ops in the writers module.
    """
    monkeypatch.setattr(
        "tauro.io.writers.DataValidator.validate_dataframe",
        lambda self, df, allow_empty=False: None,
    )
    monkeypatch.setattr(
        "tauro.io.writers.DataValidator.validate_columns_exist",
        lambda self, df, cols: None,
    )
    yield


def make_writer_chain():
    """Create a writer chain mock that mimics Spark DataFrameWriter fluent API."""
    w = MagicMock()
    # methods used in chain
    w.format.return_value = w
    w.mode.return_value = w
    w.partitionBy.return_value = w
    w.option.return_value = w
    w.save.return_value = None
    return w


class TestSparkWriterMixin_determine_mode_and_partition:
    def test_determine_write_mode_valid_and_invalid(self):
        pw = ParquetWriter({"spark": MagicMock()})
        # valid mode
        cfg = {"write_mode": "append"}
        assert pw._determine_write_mode(cfg) == "append"
        # invalid mode: fallback to overwrite
        cfg = {"write_mode": "invalid_mode"}
        assert pw._determine_write_mode(cfg) == "overwrite"

    def test_configure_writer_applies_partition_and_options_and_overwriteSchema(self):
        pw = ParquetWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        # create df with .write pointing to our chain
        df = DummyDF(writer_chain)
        config = {
            "write_mode": "overwrite",
            "partition": ["col_a", "col_b"],
            "overwrite_schema": True,
            "options": {"opt1": "v1", "opt2": "v2"},
        }

        # Call _configure_spark_writer (it validates and applies partitions/options)
        writer = pw._configure_spark_writer(df, config)

        # ensure mode was applied and partitionBy invoked with provided columns
        writer_chain.format.assert_called_once_with("parquet")
        writer_chain.mode.assert_called()  # mode is called, param validated above
        writer_chain.partitionBy.assert_called_once_with("col_a", "col_b")
        # overwriteSchema should have been applied because parquet supports it
        writer_chain.option.assert_any_call("overwriteSchema", "true")
        # options should be applied individually
        writer_chain.option.assert_any_call("opt1", "v1")
        writer_chain.option.assert_any_call("opt2", "v2")
        assert writer is writer_chain

    def test_configure_writer_handles_no_partition_and_defaults(self):
        pw = ParquetWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        config = {"options": {"a": "1"}}

        writer = pw._configure_spark_writer(df, config)
        writer_chain.partitionBy.assert_not_called()
        writer_chain.option.assert_any_call("a", "1")


class TestParquetCSVJSONORC_write_behaviour:
    def test_parquet_writer_save_called_and_success(self):
        pw = ParquetWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        # Call write: it will call _configure_spark_writer and then save
        pw.write(df, "/dest/path", {"format": "parquet", "options": {}})

        writer_chain.save.assert_called_once_with("/dest/path")

    def test_parquet_writer_save_raises_wrapped(self):
        pw = ParquetWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        # simulate save raising
        writer_chain.save.side_effect = Exception("disk error")
        df = DummyDF(writer_chain)

        with pytest.raises(WriteOperationError):
            pw.write(df, "/dest/path", {"format": "parquet"})

    def test_csv_writer_applies_csv_specific_options_and_saves(self):
        csvw = CSVWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        cfg = {"format": "csv", "options": {"sep": "|"}}
        csvw.write(df, "/csv/path", cfg)

        # default header and escape/quote options should be applied among others
        writer_chain.option.assert_any_call("header", "true")
        writer_chain.option.assert_any_call("quote", '"')
        writer_chain.option.assert_any_call("escape", '"')
        writer_chain.option.assert_any_call("sep", "|")
        writer_chain.save.assert_called_once_with("/csv/path")

    def test_json_writer_saves(self):
        jw = JSONWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        jw.write(df, "/json/path", {"format": "json"})
        writer_chain.save.assert_called_once_with("/json/path")

    def test_orc_writer_saves(self):
        ow = ORCWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        ow.write(df, "/orc/path", {"format": "orc"})
        writer_chain.save.assert_called_once_with("/orc/path")


class TestDeltaWriter:
    def test_delta_write_calls_save(self):
        dw = DeltaWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)

        # Simple write should call save without errors
        dw.write(df, "/delta/path", {"format": "delta"})
        writer_chain.save.assert_called_once_with("/delta/path")

    def test_delta_write_replacewhere_missing_params_raises(self):
        dw = DeltaWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)
        # set overwrite_strategy to replacewhere but missing params
        cfg = {"overwrite_strategy": "replacewhere", "write_mode": "overwrite"}
        # The DeltaWriter._apply_overwrite_and_replacewhere validates and will raise,
        # but DeltaWriter.write wraps ConfigurationError into WriteOperationError,
        # so the test should expect WriteOperationError.
        with pytest.raises(WriteOperationError):
            # call write -> inside it will call _apply_overwrite_and_replacewhere and raise
            dw.write(df, "/delta/path", cfg)

    def test_delta_write_replacewhere_applied_when_params_present(self):
        dw = DeltaWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        df = DummyDF(writer_chain)

        cfg = {
            "overwrite_strategy": "replacewhere",
            "write_mode": "overwrite",
            "partition_col": "date_col",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
        }

        # For valid dates the method should attach replaceWhere option and call save
        dw.write(df, "/delta/path", cfg)
        # ensure replaceWhere option was attempted to be applied
        writer_chain.option.assert_any_call(
            "replaceWhere",
            f"{cfg['partition_col']} BETWEEN '{cfg['start_date']}' AND '{cfg['end_date']}'",
        )
        writer_chain.save.assert_called_once_with("/delta/path")

    def test_delta_write_raises_writeoperationerror_on_save_failure(self):
        dw = DeltaWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        writer_chain.save.side_effect = Exception("io error")
        df = DummyDF(writer_chain)

        with pytest.raises(WriteOperationError):
            dw.write(df, "/delta/path", {"format": "delta"})


class Test_spark_writer_mixin_replaceWhere_and_overwrite_schema_edgecases:
    def test_apply_replace_where_strategy_only_for_delta(self):
        # Create a fake mixin instance by using ParquetWriter (whose _get_format returns "parquet"),
        # calling _apply_replace_where_strategy should raise because only delta is supported.
        pw = ParquetWriter({"spark": MagicMock()})
        writer_chain = make_writer_chain()
        cfg = {
            "partition_col": "c",
            "start_date": "2023-01-01",
            "end_date": "2023-01-02",
        }

        with pytest.raises(ConfigurationError):
            pw._apply_replace_where_strategy(writer_chain, cfg)

    def test_apply_replace_where_strategy_checks_date_format(self):
        # We create a tiny subclass whose _get_format returns "delta" so the mixin's replaceWhere can be exercised.
        class DeltaLike(SparkWriterMixin):
            def __init__(self):
                self.context = {}

            def write_dummy(self):
                pass

            def _get_format(self):
                return "delta"

        dl = DeltaLike()
        writer_chain = make_writer_chain()
        # bad date format
        cfg = {
            "partition_col": "c",
            "start_date": "20230101",  # wrong format
            "end_date": "2023-01-02",
        }
        with pytest.raises(ConfigurationError):
            dl._apply_replace_where_strategy(writer_chain, cfg)

        # good date formats should work and set options
        cfg2 = {
            "partition_col": "c",
            "start_date": "2023-01-01",
            "end_date": "2023-01-02",
        }
        # Should not raise
        res = dl._apply_replace_where_strategy(writer_chain, cfg2)
        assert writer_chain.option.called
        # ensure replaceWhere option contains the predicate
        writer_chain.option.assert_any_call(
            "replaceWhere", "c BETWEEN '2023-01-01' AND '2023-01-02'"
        )


class Test_error_conditions_and_input_validation:
    def test_writer_destination_empty_raises_configuration_error_parquet(self):
        pw = ParquetWriter({"spark": MagicMock()})
        with pytest.raises(ConfigurationError):
            pw.write(MagicMock(), "", {"format": "parquet"})

    def test_writer_destination_empty_raises_configuration_error_csv(self):
        cw = CSVWriter({"spark": MagicMock()})
        with pytest.raises(ConfigurationError):
            cw.write(MagicMock(), "", {"format": "csv"})
