import os
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from tauro.io.input import (
    InputLoader,
    SequentialLoadingStrategy,
)
from tauro.io.exceptions import ConfigurationError, ReadOperationError


@pytest.fixture
def basic_context(tmp_path) -> Dict[str, Any]:
    """Contexto base con execution_mode local y input_config vacío por defecto."""
    return {"execution_mode": "local", "input_config": {}}


def test_get_input_keys_string_and_list_and_invalid(basic_context):
    loader = InputLoader(basic_context)
    # string input
    keys = loader._get_input_keys({"input": "single_key"})
    assert keys == ["single_key"]

    # list of valid keys
    keys = loader._get_input_keys({"input": ["a", " b ", "c"]})
    assert keys == ["a", "b", "c"]

    # None returns empty list
    keys = loader._get_input_keys({"input": None})
    assert keys == []

    # invalid types raise
    with pytest.raises(ConfigurationError):
        loader._get_input_keys({"input": 123})


def test_get_dataset_config_missing(basic_context):
    strategy = SequentialLoadingStrategy(basic_context, MagicMock())
    # no input_config -> missing key
    with pytest.raises(ConfigurationError):
        strategy._get_dataset_config("nonexistent")


def test_get_filepath_cloud_uri(basic_context):
    strategy = SequentialLoadingStrategy(basic_context, MagicMock())
    config = {"filepath": "s3://bucket/path/file.parquet"}
    # Cloud URIs should be returned as-is
    fp = strategy._get_filepath(config, "k")
    assert fp == "s3://bucket/path/file.parquet"


def test_handle_local_filepath_existing_file(tmp_path, basic_context):
    file_path = tmp_path / "data.parquet"
    file_path.write_text("dummy")
    cfg = {"filepath": str(file_path)}
    strategy = SequentialLoadingStrategy(basic_context, MagicMock())
    # should return path when file exists
    res = strategy._handle_local_filepath(str(file_path))
    assert res == str(file_path)


def test_handle_local_filepath_nonexistent_no_glob(basic_context, tmp_path):
    nonexist = str(tmp_path / "nope.parquet")
    strategy = SequentialLoadingStrategy(basic_context, MagicMock())
    # nonexistent without glob should raise ConfigurationError
    with pytest.raises(ConfigurationError):
        strategy._handle_local_filepath(nonexist)


def test_handle_local_filepath_glob_matches(tmp_path, basic_context):
    # create parent and matching files
    parent = tmp_path / "parent"
    parent.mkdir()
    f1 = parent / "data-001.csv"
    f2 = parent / "data-002.csv"
    f1.write_text("a")
    f2.write_text("b")

    pattern = str(parent / "data-*.csv")
    cfg = {"filepath": pattern}
    strategy = SequentialLoadingStrategy(basic_context, MagicMock())

    # should recognize glob and return original pattern (Spark can handle it)
    res = strategy._handle_local_filepath(pattern)
    assert res == pattern


def test_contains_glob_pattern_true_and_false():
    strategy = SequentialLoadingStrategy(
        {"execution_mode": "local", "input_config": {}}, MagicMock()
    )
    assert strategy._contains_glob_pattern("data-*.csv") is True
    assert strategy._contains_glob_pattern("plain/path/file.parquet") is False


def test_sequential_load_inputs_success_local_file(tmp_path):
    # Create a local file and configure input_config
    file_path = tmp_path / "mydata.parq"
    file_path.write_text("x")
    ctx = {
        "execution_mode": "local",
        "input_config": {"ds1": {"format": "parquet", "filepath": str(file_path)}},
        "global_settings": {},
    }

    # Mock reader that returns a marker object
    reader = MagicMock()
    reader.read.return_value = "DATA_DS1"

    mock_factory = MagicMock()
    mock_factory.get_reader.return_value = reader

    strategy = SequentialLoadingStrategy(ctx, mock_factory)
    results = strategy.load_inputs(["ds1"])
    assert results == ["DATA_DS1"]
    # ensure reader.read was called with the resolved filepath
    mock_factory.get_reader.assert_called_once_with("parquet")
    reader.read.assert_called_once_with(
        str(file_path), {"format": "parquet", "filepath": str(file_path)}
    )


def test_sequential_load_inputs_query_format():
    ctx = {
        "execution_mode": "local",
        "input_config": {"q1": {"format": "query", "query": "SELECT 1"}},
        "global_settings": {},
    }
    reader = MagicMock()
    reader.read.return_value = "Q_RESULT"
    mock_factory = MagicMock()
    mock_factory.get_reader.return_value = reader

    strategy = SequentialLoadingStrategy(ctx, mock_factory)
    results = strategy.load_inputs(["q1"])
    assert results == ["Q_RESULT"]
    # For query format reader.read should be called with empty string
    reader.read.assert_called_once_with("", {"format": "query", "query": "SELECT 1"})


def test_sequential_load_inputs_fail_fast_false_fill_none(tmp_path):
    # First dataset is ok, second raises. We set fail_fast=False and fill_none_on_error=True
    good_file = tmp_path / "good.parq"
    good_file.write_text("ok")
    ctx = {
        "execution_mode": "local",
        "input_config": {
            "good": {"format": "parquet", "filepath": str(good_file)},
            "bad": {"format": "parquet", "filepath": "/does/not/exist"},
        },
        "global_settings": {"fill_none_on_error": True},
    }

    # good reader returns data, bad reader raises when read is called
    good_reader = MagicMock()
    good_reader.read.return_value = "GOOD_DATA"

    bad_reader = MagicMock()
    bad_reader.read.side_effect = Exception("read failed")

    def get_reader(fmt):
        if fmt == "parquet":
            # pop first call -> good then bad
            if not hasattr(get_reader, "called"):
                get_reader.called = 1
                return good_reader
            else:
                return bad_reader
        return MagicMock()

    mock_factory = MagicMock()
    mock_factory.get_reader.side_effect = get_reader

    strategy = SequentialLoadingStrategy(ctx, mock_factory)
    results = strategy.load_inputs(["good", "bad"], fail_fast=False)
    # good data and None for the failed one (because fill_none_on_error True)
    assert results == ["GOOD_DATA", None]


def test_load_single_dataset_missing_format_raises(tmp_path):
    # Missing format in dataset config
    ctx = {"execution_mode": "local", "input_config": {"x": {"filepath": "/tmp/a"}}}
    strategy = SequentialLoadingStrategy(ctx, MagicMock())
    # _load_single_dataset will re-raise as ReadOperationError
    with pytest.raises(ReadOperationError):
        strategy._load_single_dataset("x")


def test_handle_glob_pattern_parent_not_exists(tmp_path):
    # glob where parent does not exist should raise ConfigurationError inside _handle_glob_pattern
    p = tmp_path / "nonexistent_parent" / "pattern-*.csv"
    strategy = SequentialLoadingStrategy(
        {"execution_mode": "local", "input_config": {}}, MagicMock()
    )
    with pytest.raises(ConfigurationError):
        strategy._handle_glob_pattern(Path(str(p)), str(p))
