import argparse
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import os
from typing import Tuple

from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.main.python.configuration import Configuration
from src.main.python.module_utils import import_all_nodes


def setup_sparkSession(app_name, master, sparkConf=None):

    if not sparkConf:
        sparkConf = SparkConf()

    # Initialise the Spark Context
    return (
        SparkSession
        .builder
        .config(conf=sparkConf)
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )


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

    # Initialise the Spark Context
    sparkSession = setup_sparkSession(args.node, master="local")
    configuration = Configuration("config/configuration.yml", node_name=args.node)

    # Automatically load all of the project files
    project_path = os.path.join("src", "main", "python", "projects")
    if not os.path.exists(project_path):
        raise NotADirectoryError(f"Could not find the project path: {project_path}. Does it exist?")

    for project_items in import_all_nodes(path=project_path).items():
        for node_name, cls in project_items[1].items():
            globals()[node_name] = cls

    if args.node not in globals():
        raise ModuleNotFoundError(f"Could not find node: {args.node} in order for it to be run. "
                                  f"Has this been imported correctly?")

    # Initialise the node with the sparkSession object
    node = globals()[args.node](configuration, args.relative_offset)

    date_split = args.execution_date.split("-")
    if len(date_split) != 3:
        raise AttributeError("Please provide a date in the YYYY-MM-DD format.")

    # Calculate the actual date to be run on the data, relative to the
    # execution date of the endpoint node
    run_date = date(*[int(s) for s in date_split]) - relativedelta(months=args.total_offset)

    logging.info(f"Generating node: {args.node} for period: {run_date}")
    node.write_table(year=run_date.year, month=run_date.month)

    logging.info("Finished generating node.")
