import pytest  # type: ignore

from core.config.loaders import (
    ConfigLoaderFactory,
    DSLConfigLoader,
    PythonConfigLoader,
)
from core.config.exceptions import ConfigLoadError


def test_load_json_string():
    factory = ConfigLoaderFactory()
    js = '{"a": 1, "b": "x"}'
    cfg = factory.load_config(js)
    assert cfg["a"] == 1 and cfg["b"] == "x"


def test_load_dsl_file(tmp_path):
    content = """
[global]
input_path = "/data/in"
output_path = "/data/out"
mode = "local"

[nodes.node1]
input = {"format": "json"}
output = {"format": "parquet"}

[pipelines.p1]
nodes = [node1]
type = "batch"
"""
    p = tmp_path / "test.tdsl"
    p.write_text(content, encoding="utf-8")
    loader = DSLConfigLoader()
    parsed = loader.load(p)
    assert "global" in parsed
    assert parsed["global"]["input_path"] == "/data/in"
    assert "nodes" in parsed and "node1" in parsed["nodes"]
    assert "pipelines" in parsed and "p1" in parsed["pipelines"]


def test_python_loader(tmp_path):
    py = tmp_path / "cfg.py"
    py.write_text(
        "config = {'global_settings': {'input_path': '/in', 'output_path': '/out', 'mode': 'local'}}"
    )
    loader = PythonConfigLoader()
    cfg = loader.load(py)
    assert isinstance(cfg, dict)
    assert "global_settings" in cfg


def test_factory_file_not_found(tmp_path):
    factory = ConfigLoaderFactory()
    missing = tmp_path / "nope.json"
    with pytest.raises(ConfigLoadError):
        factory.load_config(missing)
