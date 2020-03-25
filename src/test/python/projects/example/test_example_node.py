# PySpark
from pyspark.sql import functions as f
from pyspark.sql.types import *

# Python
import pytest

# Module
from src.main.python.projects.example.example_node import ExampleNode1
from src.main.python.utils.dataframe import assert_test_dfs_equal
from src.test.python.test_classes import create_test_data
from src.test.python.test_setup import configuration, spark

# Create test data
example_ds_data = [
    (1, "Joe", "2020-10-19"),
    (1, "Joe", "2020-10-20"),
    (2, "Jack", "2020-10-19"),
    (3, "Harry", "2020-10-19"),
    (3, "Harry", "2020-10-20"),
    (3, "Harry", "2020-10-21"),
    (5, "Sam", "2020-10-19"),
]

example_ds_schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("date", StringType())
])

expected_output_data = [
    (1, "Joe", 2),
    (2, "Jack", 1),
    (3, "Harry", 3)
]


def test_ExampleNode1():

    # Initialise the node
    node = ExampleNode1(configuration)
    node.init_sparkSession()

    # Mock the dependencies for the test
    node.dependency_list = [
        create_test_data(example_ds_data, schema=example_ds_schema)
    ]

    test_columns = ["id", "name", "count"]
    generated_df = node.generate_node(year=2019, month=10).orderBy("id").select(test_columns)
    expected_df = spark.createDataFrame(expected_output_data, schema=generated_df.schema)

    assert_test_dfs_equal(expected_df.select(test_columns), generated_df)
