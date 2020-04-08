# PySpark
from pyspark.sql import DataFrame
from pyspark.sql.types import *

# Python
from typing import List

# Module
from src.test.python.test_setup import spark


class MockedBaseTableNode:
    """
    Used to mock a BaseTableNode-type dependency
    for tests by simply returning the dataframe
    that was used at initialisation.
    """

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe

    def get_dataframe(self, year, month) -> DataFrame:
        return self.dataframe


def create_test_data(data: List[tuple],
                     schema: StructType() = None) -> MockedBaseTableNode:
    """
    Creates a Spark Dataframe from mocked data and returns a
    BaseTableNode-like type suitable for dependency injection.
    :param data: list of the data rows as a tuple
    :param schema: Spark StructType schema or list of column strings. Schema will be inferred if this is used.
    :return: MockedBaseTableNode
    """

    # Check the data length matches the schema length
    if len(data[0]) != len(schema):
        raise Exception("The first row of the data does not match the length of the schema or column list.")

    df = spark.createDataFrame(data, schema=schema)

    return MockedBaseTableNode(df)
