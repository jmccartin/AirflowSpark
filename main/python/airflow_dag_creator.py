# Airflow specific
import airflow.utils.helpers
from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.subdag_operator import SubDagOperator

# Python
from datetime import datetime
import logging
import os
from typing import List

# Project files
from src.main.python.configuration import Configuration
from src.main.python.module_utils import import_all_nodes, find_endpoints

configuration = Configuration(config_file="config/configuration.yml")

args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 9, 1)
}

dags = []

proj_dir = os.path.join("src", "main", "python", "projects")

spark_conf = {
    "master": configuration.settings.get("deploy_mode")
}

env_vars = {
    "PYSPARK_DRIVER_PYTHON": "python"
}

execution_date = "{{ ds }}"
application_main = "/Users/jmccartin/Outra/Github/outraflow/src/main/python/spark_main.py"
schedule_interval = "@monthly"


def calc_all_offsets(nodes_dict) -> dict:
    """
    Calculates the period offsets for each node in the project DAG.
    :param nodes_dict: Loaded node classes keyed by class names
    :return: A dictionary holding a pair of offsets (total and relative) as a tuple.
    Keyed by the node name.
    """

    node_dep_dict = {}

    # Create a dictionary of nodes and their dependencies, by name
    for node_name, cls in nodes_dict.items():
        node = cls(configuration, period_offset=0)
        node_dep_dict[node_name] = []
        for dep in node.dependency_list:
            node_dep_dict[node_name].append(dep.__class__.__name__)

    def get_all_paths(node, path=None):
        paths = []
        if path is None:
            path = []
        path.append(node)
        if node.dependency_list:
            for dep in node.dependency_list:
                dep.set_parent_offset(parent_offset=node.total_offset)
                paths.extend(get_all_paths(dep, path[:]))
        else:
            paths.append(path)
        return paths

    node_offsets = {}

    # Each endpoint in the flow needs to have its own DAG
    # Find all periods for the nodes to be generated
    for endpoint in find_endpoints(node_dep_dict):
        logging.debug(f"Finding paths for DAG endpoint: {endpoint}.")
        end_node = nodes_dict[endpoint](configuration, period_offset=0)
        paths = get_all_paths(node=end_node)
        for path in paths:
            for node in path:
                node_name = node.__class__.__name__
                if node_name not in node_offsets:
                    node_offsets[node_name] = set()
                # Add a tuple pair of the total and relative offsets
                node_offsets[node_name].add((node.total_offset, node.period_offset))

    return node_offsets


def create_dag(project_items):

    dag_name = project_items[0]
    loaded_nodes_dict = project_items[1]

    project_dag = DAG(
        dag_id=dag_name,  # Get the directory as the DAG name
        default_args=args,
        schedule_interval=schedule_interval,  # Use any period for the DAG
    )

    node_offsets = calc_all_offsets(loaded_nodes_dict)
    operators = {}

    # Loop through each period within each node
    for node_name in node_offsets.keys():

        # Dependency nodes out of the project will not be added to the project DAG
        if node_name not in loaded_nodes_dict:
            continue

        # For formatting of the task_id in the airflow DAG
        def offset2str(offset):
            if offset == 0:
                return "0"
            else:
                return f"-{offset}"

        # If there is more than one period, set up a sub-dag
        if len(node_offsets[node_name]) > 1:
            sub_dag_name = f"{dag_name}.{node_name}"
            logging.debug(f"Created sub-DAG with name: {sub_dag_name}")
            sub_dag = DAG(
                dag_id=sub_dag_name,
                default_args=args,
                schedule_interval=schedule_interval,
            )

            operators[node_name] = SubDagOperator(
                task_id=node_name,
                dag=project_dag,
                subdag=sub_dag
            )

            sub_operators = []

            for offsets_pair in node_offsets[node_name]:
                total_offset = str(offsets_pair[0])
                relative_offset = str(offsets_pair[1])
                sub_operators.append(
                    SparkSubmitOperator(
                        application=application_main,
                        name=f"{node_name}.Month.{offset2str(offsets_pair[0])}",
                        application_args=[
                            node_name,
                            execution_date,
                            total_offset,
                            relative_offset
                        ],
                        task_id=f"{node_name}.Month.{offset2str(offsets_pair[0])}",
                        dag=sub_dag,
                        env_vars=env_vars
                    )
                )

            airflow.utils.helpers.chain(*sub_operators)
        else:
            offsets_pair = node_offsets[node_name].pop()
            # Need to pass these separately to spark-submit (not as a tuple)
            total_offset = str(offsets_pair[0])
            relative_offset = str(offsets_pair[1])
            operators[node_name] = SparkSubmitOperator(
                    application=application_main,
                    name=f"{node_name}.Month.{offset2str(offsets_pair[0])}",
                    application_args=[
                        node_name,
                        execution_date,
                        total_offset,
                        relative_offset
                    ],
                    task_id=f"{node_name}.Month.{offset2str(offsets_pair[0])}",
                    dag=project_dag,
                    env_vars=env_vars
            )

    # Set the flow direction for each of the nodes in the project DAG
    for node_name in operators.keys():
        node = loaded_nodes_dict[node_name](configuration, period_offset=0)
        for dep in node.dependency_list:
            dep_name = dep.__class__.__name__
            if dep_name in loaded_nodes_dict:
                logging.debug(f"Setting {operators[node_name]} to be downstream of {operators[dep_name]}")
                operators[dep_name].set_downstream(operators[node_name])

    return project_dag

# Loop through each project and create the Airflow DAG
for project_items in import_all_nodes(proj_dir).items():
    dag_name = project_items[0]
    project_dag = create_dag(project_items)

    # Airflow only picks up a DAG object if it appears in globals
    globals()[dag_name] = project_dag

    logging.info(f"Added DAG with name: {dag_name}")
