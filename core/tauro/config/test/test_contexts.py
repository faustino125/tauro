import pytest
from types import SimpleNamespace
from tauro.config.contexts import (
    Context,
    ContextFactory,
    MLContext,
    StreamingContext,
    HybridContext,
)
from tauro.config.exceptions import ConfigValidationError
from tauro.config.session import SparkSessionFactory


# Evitar crear sesiones reales de Spark durante los tests: devolver un objeto falso
@pytest.fixture(autouse=True)
def fake_spark_session(monkeypatch):
    """
    Reemplaza SparkSessionFactory.get_session por una versión que devuelve
    un objeto simple para que las pruebas no intenten lanzar la JVM de Spark.
    Aplica también reset_session para evitar efectos colaterales.
    """
    # Asegurarnos de que no haya sesión previa
    SparkSessionFactory.reset_session()

    monkeypatch.setattr(
        SparkSessionFactory,
        "get_session",
        lambda *args, **kwargs: SimpleNamespace(name="fake-spark-session"),
    )

    # También parcheamos reset_session para limpiar _session sin intentar parar una sesión real
    monkeypatch.setattr(
        SparkSessionFactory,
        "reset_session",
        lambda: setattr(SparkSessionFactory, "_session", None),
    )

    yield

    # limpieza final
    SparkSessionFactory.reset_session()


def make_minimal_configs():
    # Añadimos validators para desactivar strict en streaming y ml dentro de los tests
    global_settings = {
        "input_path": "/in",
        "output_path": "/out",
        "mode": "local",
        "validators": {
            "streaming": {"strict": False},
            "ml": {"strict": False},
        },
    }
    nodes = {
        "n1": {"input": {"format": "json"}, "output": {"format": "parquet"}},
        "n_ml": {
            "input": {},
            "output": {},
            "model": {"type": "spark_ml"},
            "hyperparams": {"lr": 0.1},
        },
    }
    pipelines = {
        "batch_p": {"nodes": ["n1"], "type": "batch"},
        "ml_p": {"nodes": ["n_ml"], "type": "ml"},
        "stream_p": {"nodes": ["n1"], "type": "streaming"},
        "hyb_p": {"nodes": ["n1", "n_ml"], "type": "hybrid"},
    }
    inp = {}
    out = {}
    return global_settings, pipelines, nodes, inp, out


def test_context_from_json_config_and_pipeline_manager():
    g, pipelines, nodes, inp, out = make_minimal_configs()
    ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
    names = ctx.list_pipeline_names()
    assert set(names) == set(pipelines.keys())
    p = ctx.get_pipeline("batch_p")
    assert isinstance(p, dict)
    assert p["nodes"][0]["name"] == "n1"


def test_context_factory_selects_hybrid():
    g, pipelines, nodes, inp, out = make_minimal_configs()
    base = Context.from_json_config(g, pipelines, nodes, inp, out)
    specialized = ContextFactory.create_context(base)
    assert isinstance(specialized, HybridContext)


def test_ml_and_streaming_context_creation():
    g, pipelines, nodes, inp, out = make_minimal_configs()
    base = Context.from_json_config(g, pipelines, nodes, inp, out)
    mlctx = MLContext.from_base_context(base)
    assert "n_ml" in mlctx.ml_nodes
    sctx = StreamingContext.from_base_context(base)
    assert "n1" in sctx.streaming_nodes
