import pytest

from src.main.python.utils.modules import find_endpoints

# Simple tree with A being the start node and E the end node
dag1 = {
    "E": ["D", "C"],
    "D": ["C"],
    "C": ["B"],
    "B": ["A"],
    "A": ["ext_dep"]
}

# Tree with only one node in the DAG (needs to return identical
# start and end nodes as the endpoints)
dag2 = {
    "A": ["ext_dep"]
}

# Tree with outside dependencies. B has external dependency
# but should not be an endpoint (has dependency inside DAG).
dag3 = {
    "D": ["C"],
    "C": ["B", "A"],
    "B": ["A", "ext_dep"],
    "A": ["ext_dep"]
}

endpoints1 = {"start": {"A"}, "end": {"E"}}
endpoints2 = {"start": {"A"}, "end": {"A"}}
endpoints3 = {"start": {"A"}, "end": {"D"}}


@pytest.mark.parametrize("dags, endpoints", [
    [dag1, endpoints1], [dag2, endpoints2], [dag3, endpoints3]
])
def test_find_endpoints(dags, endpoints):
    assert find_endpoints(dags) == endpoints, "The endpoints were not correctly calculated"
