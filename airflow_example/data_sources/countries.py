from pyspark.sql import functions as f
from pyspark.sql.types import *

from airflow_example.base_nodes import BaseDataSource
import airflow_example.time_utils as tu


class Countries(BaseDataSource):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

        self.dependency_list = []
        self.period = tu.Static()

        self.schema = StructType([
            StructField("locale", StringType(), True),
            StructField("country", StringType(), True),
        ])
