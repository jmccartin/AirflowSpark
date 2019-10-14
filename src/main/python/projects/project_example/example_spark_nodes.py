from pyspark.sql import functions as f
from pyspark.sql.types import *

from src.main.python.base_nodes import BaseTableNode
from src.main.python.data_sources.countries import Countries
from src.main.python.data_sources.people import People
import src.main.python.time_utils as tu


class LocaleHeights(BaseTableNode):
    """
    Calculates the heights for each locale
    """
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

        self.schema = StructType([
            StructField("locale", StringType(), True),
            StructField("avg_height", DoubleType(), True),
            StructField("count", LongType(), True)
        ])

    def set_dependencies(self):
        self.dependency_list = [
            People(self.configuration, period_offset=tu.previous()),
        ]

    def generate_node(self, year, month):
        input_df = self.dependency_list[0].get_dataframe(year, month)
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
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

        self.schema = StructType([
            StructField("country", StringType(), True),
            StructField("avg_height", DoubleType(), True)
        ])

    def set_dependencies(self):
        self.dependency_list = [
            LocaleHeights(self.configuration, period_offset=tu.previous()),
            Countries(self.configuration, period_offset=tu.current()),
        ]

    def generate_node(self, year, month):

        local_heights_df = self.dependency_list[0].get_dataframe(year, month)
        countries_df = self.dependency_list[1].get_dataframe(year, month)

        output_df = local_heights_df.join(countries_df, on="locale")

        return self.fit_to_schema(output_df)


class CountryHeightsCount(BaseTableNode):
    """
    Simply counts the rows of the previous node
    """
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

        self.schema = StructType([
            StructField("count", LongType(), True),
        ])

    def set_dependencies(self):
        self.dependency_list = [
            CountryHeights(self.configuration, period_offset=tu.previous()),
            LocaleHeights(self.configuration, period_offset=tu.current())
        ]

    def generate_node(self, year, month):

        country_heights_df = self.dependency_list[0].get_dataframe(year, month)

        output_df = country_heights_df.groupBy().count()

        return self.fit_to_schema(output_df)
