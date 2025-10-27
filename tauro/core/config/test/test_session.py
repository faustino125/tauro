from types import SimpleNamespace
from tauro.config.session import SparkSessionFactory


class FakeBuilder:
    def __init__(self):
        self.configs = {}

    def config(self, k, v):
        self.configs[k] = v
        return self


def test_apply_ml_configs_respects_protected():
    builder = FakeBuilder()
    SparkSessionFactory.set_protected_configs(["forbid.me"])
    ml_cfg = {"forbid.me": "x", "ok.one": "y"}
    out = SparkSessionFactory._apply_ml_configs(builder, ml_cfg)
    assert out is builder
    assert "ok.one" in builder.configs and "forbid.me" not in builder.configs


def test_reset_session_no_error(monkeypatch):
    # reset when _session is None should be fine
    SparkSessionFactory.reset_session()

    # simulate a session object with stop that raises
    class BadSession:
        def stop(self):
            raise RuntimeError("stop fail")

    SparkSessionFactory._session = BadSession()
    SparkSessionFactory.reset_session()
    assert SparkSessionFactory._session is None
