from pyspark.sql import functions as f
from pyspark.sql.types import *

from src.main.python.base_nodes import BaseDataSource
import src.main.python.time_utils as tu


class People(BaseDataSource):
    def __init__(self, configuration, period_offset=0):
        super().__init__(configuration, period_offset)

        self.period = tu.Monthly()

        self.schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("gender", StringType(), True),
            StructField("height", StringType(), True),
            StructField("locale", StringType(), True),
        ])

    def set_dependencies(self):
        self.dependency_list = []
