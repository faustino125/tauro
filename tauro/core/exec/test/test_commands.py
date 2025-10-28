import json
import time
from types import SimpleNamespace
from io import StringIO

import pytest
from loguru import logger

from tauro.exec.commands import NodeCommand, MLNodeCommand, ExperimentCommand


def test_node_command_executes_function_and_returns_value():
    def fn(a, b, start_date, end_date):
        assert start_date == "2020-01-01"
        assert end_date == "2020-01-02"
        return {"ok": True, "args": (a, b)}

    cmd = NodeCommand(
        function=fn,
        input_dfs=[1, 2],
        start_date="2020-01-01",
        end_date="2020-01-02",
        node_name="n1",
    )
    res = cmd.execute()
    assert res["ok"] is True
    assert res["args"] == (1, 2)


def test_node_command_raises_and_logs_on_error():
    def bad_fn(*args, start_date, end_date):
        raise RuntimeError("boom")

    cmd = NodeCommand(function=bad_fn, input_dfs=[], start_date="s", end_date="e", node_name="bad")

    # Attach a temporary Loguru sink that writes into an in-memory buffer so we can assert on the error message.
    buf = StringIO()
    sink_id = logger.add(buf, level="ERROR", format="{message}")

    try:
        with pytest.raises(RuntimeError):
            cmd.execute()

        logged = buf.getvalue()
        assert "Error executing node 'bad': boom" in logged
    finally:
        logger.remove(sink_id)


def test_ml_node_command_passes_ml_context_and_merges_hyperparams(monkeypatch):
    # function that accepts ml_context and returns some of its fields
    def ml_fn(*dfs, start_date, end_date, ml_context=None):
        return {
            "model_version": ml_context["model_version"],
            "hyperparams": ml_context["hyperparams"],
            "pipeline_config": ml_context["pipeline_config"],
            "start_date": start_date,
            "end_date": end_date,
        }

    # fake spark with conf.set recording
    class FakeConf:
        def __init__(self):
            self.set_calls = {}

        def set(self, key, value):
            self.set_calls[key] = value

    class FakeSpark:
        def __init__(self):
            self.conf = FakeConf()

    node_config = {"hyperparams": {"lr": 0.1}, "metrics": ["m1"], "description": "desc"}
    pipeline_config = {"hyperparams": {"batch": 16}, "model_name": "m"}
    ml_info = {
        "model_version": "mv1",
        "hyperparams": {"batch": 32, "global": True},
        "pipeline_config": pipeline_config,
        "project_name": "proj",
        "is_experiment": False,
    }

    spark = FakeSpark()

    cmd = MLNodeCommand(
        function=ml_fn,
        input_dfs=[],
        start_date="2021-01-01",
        end_date="2021-01-02",
        node_name="mlnode",
        model_version=ml_info["model_version"],
        hyperparams=ml_info["hyperparams"],
        node_config=node_config,
        pipeline_config=pipeline_config,
        spark=spark,
    )

    res = cmd.execute()
    # merged hyperparams should prefer node hyperparams over pipeline/passed ones
    assert res["model_version"] == "mv1"
    assert "lr" in res["hyperparams"]
    # spark conf should include tauro.ml.lr etc.
    assert spark.conf.set_calls.get("tauro.ml.lr") == str(node_config["hyperparams"]["lr"])


def test_ml_node_command_calls_function_without_ml_context_if_not_accepted(monkeypatch):
    # function that DOES NOT accept ml_context
    def simple_fn(*dfs, start_date, end_date):
        return {"s": start_date, "e": end_date}

    cmd = MLNodeCommand(
        function=simple_fn,
        input_dfs=[],
        start_date="s",
        end_date="e",
        node_name="node",
        model_version="v",
        hyperparams={},
        node_config={},
        pipeline_config={},
    )
    res = cmd.execute()
    assert res == {"s": "s", "e": "e"}


def test_experiment_command_requires_skopt(monkeypatch):
    # If skopt not installed, ExperimentCommand.execute should raise RuntimeError
    # Temporarily remove skopt if present
    import sys

    skopt_module = sys.modules.pop("skopt", None)
    try:
        ec = ExperimentCommand(lambda *a: 0, space=[(0, 1)], n_calls=1)
        with pytest.raises(RuntimeError):
            ec.execute()
    finally:
        if skopt_module is not None:
            sys.modules["skopt"] = skopt_module
