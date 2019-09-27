import argparse
import logging
import os

from pyspark.sql import SparkSession

from airflow_example.module_utils import import_all_nodes


def setup_spark_context(app_name):
    # Initialise the Spark Context
    return (
        SparkSession
        .builder
        .appName(app_name)
        .master("local")
        .getOrCreate()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("execution_date")
    parser.add_argument("--log", "-l", help="set the logging level", type=str, default="INFO")

    args = parser.parse_args()
    if args.node is None:
        print(parser.format_help())
        raise Exception("You have not specified a node to run")

    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {}".format(args.log))

    # logging.basicConfig(level=numeric_level,
    #                     format="%(asctime)s [%(levelname)s] %(message)s",
    #                     datefmt="%Y-%m-%d %H:%M:%S")

    # Initialise the Spark Context
    sparkSession = setup_spark_context(args.node)

    # Automatically load all of the project files
    project_path = os.path.join("airflow_example", "projects")
    for project_items in import_all_nodes(path=project_path).items():
        for node in project_items[1]:
            globals()[node.__name__] = node

    if args.node not in globals():
        raise ModuleNotFoundError(f"Could not find node: {args.node} in order for it to be run. "
                                  f"Has this been imported correctly?")

    # Initialise the node with the sparkSession object
    node = globals()[args.node](sparkSession)

    date_split = args.execution_date.split("-")
    if len(date_split) != 3:
        raise AttributeError("Please provide a date in the YYYY-MM-DD format.")

    logging.info(f"Generating node: {args.node}")
    node.write_table(year=date_split[0], month=date_split[1])

    logging.info("Finished generating node.")
