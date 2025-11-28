import pytest  # type: ignore

from core.exec.dependency_resolver import DependencyResolver


def test_build_dependency_graph_and_topological_sort_simple():
    nodes = ["a", "b", "c"]
    node_configs = {
        "a": {"dependencies": []},
        "b": {"dependencies": ["a"]},
        "c": {"dependencies": [{"b": {}}]},
    }

    dag = DependencyResolver.build_dependency_graph(nodes, node_configs)
    # dag maps a -> set(dependents)
    assert isinstance(dag, dict)
    # a should have b as dependent
    assert "b" in dag["a"]
    order = DependencyResolver.topological_sort(dag)
    # order should start with a
    assert order.index("a") < order.index("b") < order.index("c")


def test_build_dependency_graph_missing_node_raises():
    nodes = ["a"]
    node_configs = {"a": {"dependencies": ["b"]}}
    with pytest.raises(ValueError):
        DependencyResolver.build_dependency_graph(nodes, node_configs)


def test_topological_sort_detects_cycle():
    dag = {"a": {"b"}, "b": {"a"}}
    with pytest.raises(ValueError):
        DependencyResolver.topological_sort(dag)
