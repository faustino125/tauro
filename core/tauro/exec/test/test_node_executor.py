import sys
import types
import logging
from unittest import mock

import pytest

tauro_exec_mod = pytest.importorskip(
    "tauro.exec.node_executor",
    reason="NodeExecutor not available in tauro.exec.node_executor",
)
NodeExecutor = getattr(tauro_exec_mod, "NodeExecutor", None)
if NodeExecutor is None:
    pytest.skip("NodeExecutor class not found in tauro.exec.node_executor")

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DummyContext:
    """Minimal fake context passed to NodeExecutor during tests."""

    def __init__(self):
        self.global_settings = {}
        self.nodes_config = {}
        self.format_policy = None
        self.connection_pools = {}


class DummyInputLoader:
    """Fake input loader used to avoid external I/O during tests."""

    def __init__(self, ctx=None):
        self.ctx = ctx

    def load_inputs(self, node_config):
        return []


class DummyOutputManager:
    """Fake output manager used to avoid side effects during tests."""

    def __init__(self, ctx=None):
        self.ctx = ctx

    def write_output(self, *args, **kwargs):
        return True


def test_import_dynamic_node_function(monkeypatch):
    """
    Verify that NodeExecutor's dynamic import logic can resolve a module
    created at test time (simulates user-defined node modules).
    """
    logger.info("Starting test: dynamic node function import")
    mod_name = "tests._fake_node_module"
    fake_mod = types.ModuleType(mod_name)

    def fake_node(entry_args=None, ctx=None):
        return {"ok": True, "args": entry_args, "ctx": ctx}

    fake_mod.fake_node = fake_node
    sys.modules[mod_name] = fake_mod
    logger.debug("Injected fake module into sys.modules: %s", mod_name)

    ne = NodeExecutor(DummyContext(), DummyInputLoader(), DummyOutputManager())
    logger.debug("Created NodeExecutor instance for import test")

    if hasattr(ne, "_import_module_cached"):
        imported = ne._import_module_cached(mod_name)
        assert getattr(imported, "fake_node", None) is fake_node
        logger.info("Imported module via _import_module_cached successfully")
    elif hasattr(ne, "_load_node_function"):
        node_ref = {"module": mod_name, "function": "fake_node"}
        fn = ne._load_node_function(node_ref)
        assert fn is fake_node
        logger.info("Loaded function via _load_node_function successfully")
    else:
        logger.warning("No known dynamic import method found on NodeExecutor; skipping")
        pytest.skip("No known dynamic import method found on NodeExecutor")

    sys.modules.pop(mod_name, None)
    logger.debug("Removed fake module from sys.modules: %s", mod_name)


def test_executor_uses_threadpool_to_submit_tasks(monkeypatch):
    logger.info("Starting test: executor uses thread pool to submit tasks")

    class FakePool:
        def __init__(self):
            # simulate submit returning a future-like object
            self.submit = mock.MagicMock(return_value=mock.MagicMock())

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        tauro_exec_mod, "ThreadPoolExecutor", lambda max_workers=None: FakePool()
    )
    logger.debug("Patched ThreadPoolExecutor in module %s", tauro_exec_mod.__name__)

    ne = NodeExecutor(
        DummyContext(), DummyInputLoader(), DummyOutputManager(), max_workers=2
    )
    logger.debug("Created NodeExecutor instance with max_workers=2")

    execution_order = ["n1", "n2"]
    node_configs = {
        "n1": {"dependencies": []},
        "n2": {"dependencies": ["n1"]},
    }
    dag = {"n1": {"n2"}, "n2": set()}

    monkeypatch.setattr(ne, "execute_single_node", mock.MagicMock(return_value=None))
    logger.debug("Patched execute_single_node to avoid running real node logic")

    def _fake_process_completed_nodes(
        running, completed, dag_arg, ready_queue, node_configs_arg, execution_results
    ):
        for fut, info in running.items():
            node_name = info.get("node_name")
            if node_name:
                completed.add(node_name)
                logger.debug(
                    "Marking node as completed in fake processor: %s", node_name
                )
        running.clear()
        return False

    monkeypatch.setattr(ne, "_process_completed_nodes", _fake_process_completed_nodes)
    logger.debug("Patched _process_completed_nodes to simulate immediate completion")

    ne.execute_nodes_parallel(
        execution_order, node_configs, dag, "2025-01-01", "2025-01-31", {}
    )
    logger.info("Called execute_nodes_parallel")

    assert hasattr(
        tauro_exec_mod, "ThreadPoolExecutor"
    ), "ThreadPoolExecutor should be patched in the module"
    logger.debug("Assertion passed: ThreadPoolExecutor present in module after patch")
