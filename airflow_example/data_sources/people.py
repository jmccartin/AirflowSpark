from pyspark.sql import functions as f
from pyspark.sql.types import *

from airflow_example.base_nodes import BaseDataSource
import airflow_example.time_utils as tu


class People(BaseDataSource):
    def __init__(self, sparkSession):
        super(People, self).__init__(sparkSession)

        self.dependency_list = []
        self.period = tu.Monthly()

        self.schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("gender", StringType(), True),
            StructField("height", StringType(), True),
            StructField("locale", StringType(), True),
        ])
