from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime
from importlib import import_module
import glob
import os
from pendulum import pendulum

from airflow_example.base_nodes import BaseTableNode
# from airflow_example.projects.example_dag.example_spark_nodes import Countries, People, LocaleHeights, CountryHeights

args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 9, 1)
}

dags = []

proj_dir = os.path.join("airflow_example", "projects")

import sys

for d in glob.glob(os.path.join(proj_dir, "*")):
    if os.path.isdir(d):
        paths = os.path.join(d, "*.py")
        project_files = glob.glob(paths)
        for py_file in project_files:
            if py_file.startswith("__"):  # skip the __init__.py files
                continue

            module = import_module(py_file.replace(os.path.sep, "."))
            to_import = [getattr(module, x) for x in dir(module)]
            # if isinstance(getattr(mod, x), type))  # if you need classes only

            for i in to_import:
                try:
                    setattr(sys.modules[__name__], i.__name__, i)
                except AttributeError:
                    pass
            # module = import_module(recipe)
            # print(module.__name__)
            # if issubclass(module, BaseTableNode):
            #     print(module.__name__, "is a subclass of BaseTableNode")
        # dags.append(
        #     DAG(
        #         dag_id=dir,
        #         default_args=args,
        #         schedule_interval=None,
        #     )
        # )

import sys; sys.exit()

dag = DAG(
    dag_id='example_dag',
    default_args=args,
    schedule_interval=None,
)

execution_date = "{{ ds }}"

node_0a_name = People.__name__
node_0b_name = Countries.__name__
node_1_name = LocaleHeights.__name__
node_2_name = CountryHeights.__name__

# spark_node_0a = SparkSubmitOperator(
#     application="/Users/jmccartin/Code/AirflowExample/airflow_example/spark_main.py",
#     application_args=[
#         People.__name__,
#         execution_date
#     ],
#     task_id="Run_" + node_0a_name,
#     dag = dag
# )
#
# spark_node_0b = SparkSubmitOperator(
#     application="/Users/jmccartin/Code/AirflowExample/airflow_example/spark_main.py",
#     application_args=[
#         Countries.__name__,
#         execution_date
#     ],
#     task_id="Run_" + node_0b_name,
#     dag = dag
# )

spark_conf = {
    "master": "local"
}

application_main = "../spark_main.py"

spark_node_1 = SparkSubmitOperator(
    application=application_main,
    name=LocaleHeights.__name__,
    application_args=[
        LocaleHeights.__name__,
        execution_date
    ],
    task_id="Run_" + node_1_name,
    dag=dag,
    env_vars={"PYSPARK_DRIVER_PYTHON": "python"}
)

spark_node_2 = SparkSubmitOperator(
    application=application_main,
    name=CountryHeights.__name__,
    application_args=[
        CountryHeights.__name__,
        execution_date
    ],
    task_id="Run_" + node_2_name,
    dag=dag,
    env_vars={"PYSPARK_DRIVER_PYTHON": "python"}
)


# def run_first_node(exec_date: pendulum.Pendulum):
#     """This is a function that will run within the DAG execution"""
#     sparkSession = setup_spark_context(app_name=LocaleHeights.__class__.__name__)
#     node = LocaleHeights(sparkSession)
#     node.write_table(exec_date.year, exec_date.month)
#     print("Finished writing table")
#
#
# first_node = PythonOperator(
#     task_id=LocaleHeights.__class__.__name__,
#     python_callable=run_first_node,
#     provide_context=True,
#     dag=dag,
# )
#
#
# def run_second_node(exec_date: pendulum.Pendulum):
#     """This is a function that will run within the DAG execution"""
#     sparkSession = setup_spark_context(app_name=CountryHeights.__class__.__name__)
#     node = CountryHeights(sparkSession)
#     node.write_table(exec_date.year, exec_date.month)
#     print("Finished writing table")
#
#
# second_node = PythonOperator(
#     task_id=CountryHeights.__class__.__name__,
#     python_callable=run_second_node,
#     provide_context=True,
#     dag=dag,
# )

# dependency_node_1.set_downstream(first_node)
# dependency_node_2.set_downstream(first_node)
# [spark_node_0a, spark_node_0b] >> \
spark_node_1 >> spark_node_2
