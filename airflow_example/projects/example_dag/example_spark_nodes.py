from pyspark.sql import functions as f
from pyspark.sql.types import *

from airflow_example.base_nodes import BaseTableNode
from airflow_example.data_sources.countries import Countries
from airflow_example.data_sources.people import People
import airflow_example.time_utils as tu


class LocaleHeights(BaseTableNode):
    """
    Calculates the heights for each locale
    """
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

        self.dependency_list = [
            People(sparkSession).set_period(period_start=tu.last()),
            People(sparkSession).set_period(period_start=tu.last())
        ]

        self.period = tu.Monthly()

        self.schema = StructType([
            StructField("locale", StringType(), True),
            StructField("avg_height", DoubleType(), True),
            StructField("count", LongType(), True)
        ])

    def generate_node(self, year, month):
        input_df = self.dependency_list[0].get_dataframe()
        output_df = (
            input_df
            .withColumn("height", f.col("height").cast(DoubleType()))  # Mimesis generates strings for height
            .groupBy("locale")
            .agg(f.avg("height").alias("avg_height"),
                 f.count(f.lit(1)).alias("count"))
        )

        return self.fit_to_schema(output_df)


class CountryHeights(BaseTableNode):
    """
    Shows the top heights per country
    """
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

        self.dependency_list = [
            LocaleHeights(sparkSession).set_period(period_start=tu.last()),
            Countries(sparkSession).set_period(period_start=tu.last())
        ]

        self.period = tu.Monthly()

        self.schema = StructType([
            StructField("country", StringType(), True),
            StructField("avg_height", DoubleType(), True)
        ])

    def generate_node(self, year, month):

        local_heights_df = self.dependency_list[0].get_dataframe()
        countries_df = self.dependency_list[1].get_dataframe()

        output_df = local_heights_df.join(countries_df, on="locale")

        return self.fit_to_schema(output_df)
