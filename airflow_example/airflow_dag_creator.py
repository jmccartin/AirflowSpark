from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime
import logging
import os

from airflow_example.module_utils import import_all_nodes

args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 9, 1)
}

dags = []

proj_dir = os.path.join("airflow_example", "projects")

spark_conf = {
    "master": "local"
}

env_vars = {
    "PYSPARK_DRIVER_PYTHON": "python"
}

execution_date = "{{ ds }}"
application_main = "/Users/jmccartin/Code/AirflowExample/airflow_example/spark_main.py"


for project_items in import_all_nodes(proj_dir).items():

    dag_name = project_items[0]

    this_dag = DAG(
        dag_id=dag_name,  # Get the directory as the DAG name
        default_args=args,
        schedule_interval="@monthly",  # Use any period for the DAG
    )
    globals()[dag_name] = this_dag

    operators = {}
    node_names = [c.__name__ for c in project_items[1]]
    for node in node_names:
        operators[node] = SparkSubmitOperator(
            application=application_main,
            name=node,
            application_args=[
                node,
                execution_date
            ],
            task_id=node,
            dag=this_dag,
            env_vars=env_vars
        )

    # Create the dag based upon the class dependencies
    for node_name in project_items[1]:
        node = node_name()
        for dep in node.dependency_list:
            dep_name = dep.__class__.__name__
            if dep_name in node_names:
                logging.info(f"Setting {operators[node.__class__.__name__]} to be"
                             f" downstream of {operators[dep_name]}")
                operators[dep_name].set_downstream(operators[node.__class__.__name__])

    logging.info(f"Loaded project: {dag_name}")
