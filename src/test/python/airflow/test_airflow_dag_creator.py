# Airflow
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

# Module
from src.main.python.airflow.airflow_dag_creator import calc_all_offsets, create_dag
from src.main.python.base_nodes import BaseDataSource
import src.main.python.utils.time as tu


def load_modules():

    class DepA(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = []

    class NodeA(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = [
                DepA(self.configuration, period_offset=tu.current())
            ]

    class NodeB(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = [
                NodeA(self.configuration, period_offset=tu.previous(offset=2))
            ]

    class NodeC(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = [
                NodeB(self.configuration, period_offset=tu.previous(offset=1)),
                NodeA(self.configuration, period_offset=tu.previous(offset=2))
            ]

    class NodeD(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = [
                NodeC(self.configuration, period_offset=tu.current())
            ]

    class NodeE(BaseDataSource):

        def __init__(self, configuration, period_offset=0):
            super().__init__(configuration, period_offset)

            self.dependency_list = [
                NodeD(self.configuration, period_offset=tu.previous(offset=1))
            ]

    node_dict = {}
    # create a random order of classes
    node_list = [NodeD, NodeA, NodeC, NodeB, NodeE]
    for node in node_list:
        node_dict[node.__name__] = node

    return node_dict


def test_calc_all_offsets():
    """
    Tests that a DAG of nodes, some of which have non-zero period offsets,
    will have the total offsets correctly summed through recursion.
    """

    node_dict = load_modules()

    offsets, endpoints = calc_all_offsets(node_dict)

    true_offsets = {
        "NodeE": {(0, 0)},
        "NodeD": {(1, 1)},
        "NodeC": {(1, 0)},
        "NodeB": {(2, 1)},
        "NodeA": {(4, 2), (3, 2)},
        "DepA": {(3, 0), (4, 0)}
    }

    assert offsets.keys() == true_offsets.keys(), "The returned node order was not correct"
    assert offsets == true_offsets, "The offsets differed from the expected values"


def test_create_dag():

    project_name = "TestProject"

    generated_dag = create_dag((project_name, load_modules()))

    assert generated_dag.dag_id == project_name, "The project name was different than what was expected"

    assert isinstance(generated_dag.roots[0], EmrCreateJobFlowOperator), \
        "The first node in the DAG should be the cluster creator (EmrCreateJobFlowOperator)."
    assert isinstance(generated_dag.leaves[0], EmrTerminateJobFlowOperator), \
        "The final node in the DAG should be the cluster terminator (EmrTerminateJobFlowOperator)."
