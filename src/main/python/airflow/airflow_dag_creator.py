# Airflow specific
import airflow
import airflow.utils.helpers
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator

# Python
from datetime import datetime
import logging
import os
from typing import Dict, Tuple

# Project files
from src.main.python.configuration import Configuration
from src.main.python.airflow.airflow_emr import create_emr_cluster_operator, terminate_emr_cluster_operator, \
    emr_cluster_sensor, submit_spark_job
from src.main.python.utils.modules import import_all_nodes, find_endpoints

configuration = Configuration(config_file=os.path.join("config", "configuration.yml"))

args = {
    "owner": "Airflow",
    "start_date": datetime(2019, 9, 1)
}

dags = []

project_dir = os.path.join("src", "main", "python", "projects")


execution_date = "{{ execution_date.strftime('%Y-%m-%d') }}"
schedule_interval = "@monthly"


def calc_all_offsets(nodes_dict: Dict) -> Tuple[Dict[str, set], Dict[str, set]]:
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
    endpoints = find_endpoints(node_dep_dict)

    # Each endpoint in the flow needs to have its own DAG
    # Find all periods for the nodes to be generated
    for endpoint in endpoints["end"]:
        logging.info(f"Finding paths for DAG endpoint: {endpoint}.")
        end_node = nodes_dict[endpoint](configuration, period_offset=0)
        paths = get_all_paths(node=end_node)
        for path in paths:
            for node in path:
                node_name = node.__class__.__name__
                if node_name not in node_offsets:
                    node_offsets[node_name] = set()
                # Add a tuple pair of the total and relative offsets
                node_offsets[node_name].add((node.total_offset, node.period_offset))

    return (node_offsets, endpoints)


def create_dag(project_items: Tuple[str, dict]) -> airflow.models.DAG:
    """
    Creates an Airflow DAG for a given project.
    :param project_items: Tuple of the project name and the loaded nodes inside the project
    :return: Airflow DAG
    """

    dag_name = project_items[0]
    loaded_nodes_dict = project_items[1]

    project_dag = DAG(
        dag_id=dag_name,  # Get the directory as the DAG name
        default_args=args,
        schedule_interval=schedule_interval,  # Use any period for the DAG
        max_active_runs=1  # We only want one application on spark at a time
    )

    node_offsets, endpoints = calc_all_offsets(loaded_nodes_dict)
    operators = {}

    # Create the EMR cluster
    cluster_creator = create_emr_cluster_operator(dag=project_dag)
    cluster_sensor = emr_cluster_sensor(dag=project_dag, project_name=dag_name)
    cluster_creator.set_downstream(cluster_sensor)
    cluster_destroyer = terminate_emr_cluster_operator(dag=project_dag)

    # Loop through each period within each node
    for node_name in node_offsets.keys():

        # Dependency nodes out of the project will not be added to the project DAG
        if node_name not in loaded_nodes_dict:
            continue

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
                spark_operator = submit_spark_job(sub_dag, node_name, offsets_pair, execution_date)
                sub_operators.append(spark_operator)
            airflow.utils.helpers.chain(*sub_operators)

        else:
            for offsets_pair in node_offsets[node_name]:
                operators[node_name] = submit_spark_job(project_dag, node_name, offsets_pair, execution_date)

    # Set the flow direction for each of the nodes in the project DAG
    for node_name in operators.keys():
        node = loaded_nodes_dict[node_name](configuration, period_offset=0)
        for dep in node.dependency_list:
            dep_name = dep.__class__.__name__
            if dep_name in loaded_nodes_dict:
                logging.debug(f"Setting {operators[node_name]} to be downstream of {operators[dep_name]}")
                operators[dep_name].set_downstream(operators[node_name])
        if node_name in endpoints["start"]:
            logging.debug(f"Setting {operators[node_name]} to be downstream of the cluster sensor")
            # The start of the DAG needs to run after the EMR cluster is active
            cluster_sensor.set_downstream(operators[node_name])
        if node_name in endpoints["end"]:
            # Close the DAG by destroying the active cluster
            operators[node_name].set_downstream(cluster_destroyer)

    return project_dag


n_dags = 0

# Loop through each project and create the Airflow DAG
for project_items in import_all_nodes(project_dir).items():
    dag_name = project_items[0]
    project_dag = create_dag(project_items)

    # Airflow only picks up a DAG object if it appears in globals
    globals()[dag_name] = project_dag

    logging.info(f"Found project with name: {dag_name}")

    n_dags += 1

logging.info(f"Successfully added {n_dags} DAGs")
