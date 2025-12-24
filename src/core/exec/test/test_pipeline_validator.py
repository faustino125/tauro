import pytest  # type: ignore

import core.exec.pipeline_validator as pv


class DummyPolicy:
    def __init__(self):
        self.checkpoint_required_inputs = {"kafka"}

    def is_supported_input(self, fmt):
        return str(fmt).lower() in {"kafka", "file_stream", "delta_stream"}

    def are_compatible(self, batch_fmt, stream_fmt):
        # only allow delta -> delta_stream or parquet -> file_stream
        batch_fmt = (batch_fmt or "").lower()
        stream_fmt = (stream_fmt or "").lower()
        if batch_fmt == "delta" and stream_fmt.startswith("delta"):
            return True
        if batch_fmt == "parquet" and stream_fmt == "file_stream":
            return True
        return False

    def get_supported_input_formats(self):
        return ["kafka", "file_stream", "delta_stream"]


@pytest.fixture(autouse=True)
def patch_format_policy(monkeypatch):
    monkeypatch.setattr(pv, "FormatPolicy", DummyPolicy)


def test_validate_dataframe_schema_spark_like_passes_and_warns(monkeypatch):
    class Schema:
        def __init__(self, fields):
            self.fields = fields

    class SparkLike:
        def __init__(self, fields, rows=1):
            self.schema = Schema(fields)
            self._rows = rows

        def limit(self, n):
            class L:
                def __init__(self, rows):
                    self.rows = rows

                def count(self_inner):
                    return self_inner.rows

            return L(self._rows)

    good = SparkLike(fields=[1], rows=0)
    # Should not raise; will issue a warning for 0 rows but test only ensures no exception
    pv.PipelineValidator.validate_dataframe_schema(good)

    bad = SparkLike(fields=[], rows=0)
    with pytest.raises(ValueError):
        pv.PipelineValidator.validate_dataframe_schema(bad)


def test_validate_dataframe_schema_pandas_like():
    class PandasLike:
        def __init__(self, columns, empty=False):
            self.columns = columns
            self.empty = empty

    ok = PandasLike(columns=["a", "b"], empty=False)
    pv.PipelineValidator.validate_dataframe_schema(ok)

    empty_df = PandasLike(columns=[], empty=True)
    with pytest.raises(ValueError):
        pv.PipelineValidator.validate_dataframe_schema(empty_df)


def test_check_batch_stream_compatibility_path_mismatch_and_incompatibility():
    # Build simple configs to test compatibility helper
    batch_config = {"output": {"format": "parquet", "path": "output/batch"}}
    streaming_config = {"input": {"format": "file_stream", "options": {"path": "output/other"}}}

    issues = []
    pv.PipelineValidator._check_batch_stream_compatibility(
        "batch1", batch_config, "stream1", streaming_config, pv.FormatPolicy(), issues
    )
    # Should produce a warning about path mismatch
    assert any("Path mismatch" in i.get("message", "") for i in issues) or any(
        i.get("severity") == "warning" for i in issues
    )

    # Now incompatible formats
    issues = []
    batch_config2 = {"output": {"format": "json", "path": "output/b"}}
    streaming_config2 = {"input": {"format": "kafka", "options": {"topic": "t"}}}
    pv.PipelineValidator._check_batch_stream_compatibility(
        "b2", batch_config2, "s2", streaming_config2, pv.FormatPolicy(), issues
    )
    assert any(i.get("severity") == "error" for i in issues)
