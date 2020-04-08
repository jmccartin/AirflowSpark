# PySpark
from pyspark.sql import functions as f, Window, DataFrame
from pyspark.sql.types import *

# Module
from src.main.python.base_nodes import BaseTableNode
from src.main.python.data_sources.example.example_ds import ExampleDataSource1
import src.main.python.utils.time as tu


class ExampleNode1(BaseTableNode):
    """
    Counts every occurrence of a person in a database, as an example project.
    The list of allowed names is a configurable parameter.
    """

    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

        self.schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("count", LongType(), True),
        ])

        self.dependency_list = [
            ExampleDataSource1(self.configuration, period_offset=tu.current())
        ]

    def generate_node(self, year, month) -> DataFrame:

        # Read dependencies
        names_df = self.dependency_list[0].get_dataframe(year, month)

        conf = self.get_project_configuration()

        allowed_names = conf.get("allowed_names")

        name_counts_df = (
            names_df
            .filter(f.col("name").isin(allowed_names))
            .groupBy("id", "name")
            .count()
        )

        return self.fit_to_schema(name_counts_df)
