import sys
import types
import inspect
import pytest

from tauro.exec.node_executor import NodeExecutor, _import_module_cached


class DummyCtx:
    def __init__(self):
        self.is_ml_layer = False
        self.nodes_config = {}
        self.env = "test"
        self.global_settings = {}
        self.spark = None
        self.default_hyperparams = {}


def test_import_module_cached_and_load_function(monkeypatch):
    # create a temporary module
    mod_name = "tests_dummy_module_foo"
    mod = types.ModuleType(mod_name)

    def myfunc(a, b, start_date, end_date):
        return "ok"

    mod.myfunc = myfunc
    sys.modules[mod_name] = mod

    imported = _import_module_cached(mod_name)
    assert hasattr(imported, "myfunc")
    # create NodeExecutor and test _load_node_function via public API
    ctx = DummyCtx()
    ne = NodeExecutor(ctx, None, None, max_workers=1)
    func = ne._load_node_function({"module": mod_name, "function": "myfunc"})
    assert callable(func)

    # test missing module raises ImportError
    with pytest.raises(ImportError):
        ne._load_node_function({"module": "non_existent_mod_12345", "function": "f"})

    # test missing function raises AttributeError
    with pytest.raises(AttributeError):
        ne._load_node_function({"module": mod_name, "function": "nope_func"})

    # test when function doesn't declare required params - it should log a warning but return
    def bad_sig(x, y):
        return None

    mod.bad_sig = bad_sig
    func2 = ne._load_node_function({"module": mod_name, "function": "bad_sig"})
    assert callable(func2)
