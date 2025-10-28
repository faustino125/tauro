import sys
import types
import pytest


# Minimal dummy context used across tests
class DummyContext:
    def __init__(self):
        self.is_ml_layer = True
        self.global_settings = {}
        self.nodes_config = {}
        self.pipelines = {}
        self.default_model_version = "v1"
        self.default_hyperparams = {}
        self.project_name = "test_project"
        self.format_policy = None
        self.connection_pools = {}

    def get_node_ml_config(self, node_name):
        # Return empty config by default
        return {}

    def get_pipeline_ml_config(self, pipeline_name):
        return {}

    def get_model_registry(self):
        class MR:
            def get_model(self, name, version=None):
                return {"name": name, "version": version}

        return MR()


class DummyInputLoader:
    def __init__(self, ctx=None):
        self.ctx = ctx

    def load_inputs(self, node_config):
        # Return empty list by default; tests may override behavior by monkeypatching instance
        return []


class DummyOutputManager:
    def __init__(self, ctx=None):
        self.saved = []

    def save_output(self, env, **kwargs):
        self.saved.append((env, kwargs))


@pytest.fixture
def dummy_context():
    return DummyContext()


@pytest.fixture
def dummy_input_loader():
    return DummyInputLoader()


@pytest.fixture
def dummy_output_manager():
    return DummyOutputManager()


@pytest.fixture(autouse=True)
def clean_sys_modules():
    """
    Ensure that temporary test modules created and injected into sys.modules
    are removed after each test to avoid cross-test contamination.
    """
    before = set(sys.modules.keys())
    yield
    after = set(sys.modules.keys())
    new = after - before
    for name in list(new):
        if name.startswith("tests_dummy_module") or name.startswith("tmp_mod_"):
            sys.modules.pop(name, None)
