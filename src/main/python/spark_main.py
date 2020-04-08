# PySpark
from pyspark.sql import SparkSession

# Python
import argparse
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import os
import zipfile

# Module
from src.main.python.configuration import Configuration
from src.main.python.utils.modules import import_all_nodes

# When being run via spark-submit, extract the project files
# so that the relative paths remain consistent (for all
# configuration and other non-python files).
if os.path.exists("project_files.zip"):
    zipfile.ZipFile("project_files.zip").extractall()
else:
    raise FileNotFoundError("Could not find project_files.zip in the list of Spark files on the master node.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("execution_date")
    parser.add_argument("total_offset", type=int)
    parser.add_argument("relative_offset", type=int)
    parser.add_argument("--log", "-l", help="set the logging level", type=str, default="INFO")

    args = parser.parse_args()
    if args.node is None:
        print(parser.format_help())
        raise Exception("You have not specified a node to run")

    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {}".format(args.log))

    logging.basicConfig(level=numeric_level,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    configuration = Configuration(os.path.join("config", "configuration.yml"), node_name=args.node)

    # Initialise the Spark Context. Here, the master and configuration will be
    # set by the process submitting the job to the cluster. We just need to set
    # the app name so that the object will act as a singleton and be picked up
    # by the classes downstream.

    sparkSession = (
        SparkSession
        .builder
        .appName(args.node)
        .getOrCreate()
    )

    # Automatically load all project files. This needs to be done in order
    # to obtain class parameters of the node that is set to be run.
    project_path = os.path.join("src", "main", "python", "projects")
    if not os.path.exists(project_path):
        print(os.path.abspath(project_path))
        raise NotADirectoryError(f"Could not find the project path: {project_path}. Does it exist?")

    for project_items in import_all_nodes(path=project_path).items():
        for node_name, cls in project_items[1].items():
            globals()[node_name] = cls

    if args.node not in globals():
        raise ModuleNotFoundError(f"Could not find node: {args.node} in order for it to be run. "
                                  f"Has this been imported correctly?")

    # Initialise the node with the configuration and period offset
    node = globals()[args.node](configuration, args.relative_offset)
    node.init_sparkSession()

    # Sets the Spark checkpoint location for the entire project. Checkpointing is used
    # when needing to persist to disk in order to break lineage for complicated tasks.
    sc = sparkSession.sparkContext
    sc.setCheckpointDir(os.path.join(node.base_path, "tmp"))

    date_split = args.execution_date.split("-")
    if len(date_split) != 3:
        raise AttributeError("Please provide a date in the YYYY-MM-DD format.")

    # Calculate the actual date to be run on the data, relative to the
    # execution date of the endpoint node
    run_date = date(*[int(s) for s in date_split]) - relativedelta(months=args.total_offset)

    logging.info(f"Generating node: {args.node} for period: {run_date}")
    node.write_table(year=run_date.year, month=run_date.month)

    logging.info("Finished generating node.")
