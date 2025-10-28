import os
from tauro.config.interpolator import VariableInterpolator


def test_interpolate_env_precedence(monkeypatch):
    monkeypatch.setenv("FOO", "env_value")
    s = "path/${FOO}/and/${BAR}"
    result = VariableInterpolator.interpolate(s, {"BAR": "var_value", "FOO": "var_override"})
    # env has precedence for FOO, variables used for BAR
    assert result == "path/env_value/and/var_value"


def test_interpolate_config_paths_nested(tmp_path):
    cfg = {
        "section": {
            "filepath": "/data/${ENV}",
            "sub": [{"filepath": "/other/${ENV}"}],
        }
    }
    VariableInterpolator.interpolate_config_paths(cfg, {"ENV": "prod"})
    assert cfg["section"]["filepath"] == "/data/prod"
    assert cfg["section"]["sub"][0]["filepath"] == "/other/prod"


def test_interpolate_structure_recursive():
    data = {"a": "${X}", "b": ["one", "${Y}", {"c": "${Z}"}]}
    res = VariableInterpolator.interpolate_structure(data, {"X": 1, "Y": "two", "Z": "three"})
    assert res["a"] == "1"
    assert res["b"][1] == "two"
    assert res["b"][2]["c"] == "three"
