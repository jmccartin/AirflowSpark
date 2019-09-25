import argparse
import logging

from pyspark.sql import SparkSession

from airflow_example.projects.example_dag.example_spark_nodes import *

def setup_spark_context(app_name):

    # Initialise the Spark Context
    return (
        SparkSession
        .builder
        .appName(app_name)
        #.master("local")
        .getOrCreate()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node")
    parser.add_argument("execution_date")
    # parser.add_argument("year", type=int)
    # parser.add_argument("month", type=int)
    # parser.add_argument("--configuration", "-c", help="the path to the config folder", type=str)
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

    if args.node not in globals():
        raise ModuleNotFoundError(f"Could not find node: {args.node} in order for it to be run. "
                                  f"Has this been imported correctly?")

    node = globals()[args.node](sparkSession)

    date_split = args.execution_date.split("-")
    if len(date_split) != 3:
        raise AttributeError("Please provide a date in the YYYY-MM-DD format.")

    logging.info(f"Generating node: {args.node}")
    node.write_table(year=date_split[0], month=date_split[1])

    logging.info("Finished generating node.")



# import os
# import glob

# current_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
# print(current_dir)
# for file in glob.glob(os.path.join(current_dir, "projects", "*", "*.py")):
#     name = os.path.splitext(os.path.basename(file))[0]
#     print(name)
#     # add package prefix to name, if required
#     module = __import__(name)
#     for member in dir(module):
#         print(member)