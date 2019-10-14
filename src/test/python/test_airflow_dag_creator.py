from src.main.python.airflow_dag_creator import calc_all_offsets, create_dag
from src.main.python.configuration import Configuration
from src.main.python.base_nodes import BaseTableNode, BaseDataSource
from src.main.python.spark_main import setup_sparkSession
import src.main.python.time_utils as tu


import pytest

setup_sparkSession(app_name="PyTest", master="local")

# @pytest.fixture(scope="module")
def load_modules():

    conf = Configuration(config_file="config/test_configuration.yml", node_name="PyTest")

    class DepA(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
            ]

    class NodeA(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
                DepA(configuration=conf, period_offset=tu.current())
            ]

    class NodeB(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
                NodeA(configuration=conf, period_offset=tu.previous(offset=2))
            ]

    class NodeC(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
                NodeB(configuration=conf, period_offset=tu.previous(offset=1)),
                NodeA(configuration=conf, period_offset=tu.previous(offset=2))
            ]

    class NodeD(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
                NodeC(configuration=conf, period_offset=tu.current())
            ]

    class NodeE(BaseDataSource):

        def set_dependencies(self):
            self.dependency_list = [
                NodeD(configuration=conf, period_offset=tu.previous(offset=1))
            ]

    node_dict = {}
    # create a random order of classes
    node_list = [NodeD, NodeA, NodeC, NodeB, NodeE]
    for node in node_list:
        node_dict[node.__name__] = node

    return node_dict


def test_calc_all_offsets():

    node_dict = load_modules()

    offsets = calc_all_offsets(node_dict)

    true_offsets = {
        'NodeE': {(0, 0)},
        'NodeD': {(1, 1)},
        'NodeC': {(1, 0)},
        'NodeB': {(2, 1)},
        'NodeA': {(4, 2), (3, 2)},
        'DepA': {(3, 0), (4, 0)}
    }

    assert offsets.keys() == true_offsets.keys(), "The returned node order was not correct"
    assert offsets == true_offsets, "The offsets differed from the expected values"
